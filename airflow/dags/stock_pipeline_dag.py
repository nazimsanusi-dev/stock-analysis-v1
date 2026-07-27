"""
stock_pipeline_dag — daily batch pipeline: Extract → Parquet → dbt transform.

Pipeline overview
-----------------
                  ┌──────────────────┐
                  │  check_new_data  │  ← skip if today's partition exists
                  └────────┬─────────┘
            ┌──────────────┴──────────────┐
     ┌──────▼──────┐              ┌───────▼────────┐
     │extract_prices│             │extract_companies│
     └──────┬───────┘             └───────┬─────────┘
            └──────────────┬──────────────┘
                    ┌───────▼────────┐
                    │  dbt_staging   │  ← views over Parquet
                    └───────┬────────┘
                    ┌───────▼────────┐
                    │dbt_intermediate│  ← daily return calculations
                    └───────┬────────┘
                    ┌───────▼────────┐
                    │ dbt_snapshots  │  ← SCD Type 2 (company dim)
                    └───────┬────────┘
                    ┌───────▼────────┐
                    │   dbt_marts    │  ← star schema: fact + dims
                    └───────┬────────┘
                    ┌───────▼────────┐
                    │   dbt_test     │  ← not_null / unique / relationship checks
                    └───────┬────────┘
                    ┌───────▼────────┐
                    │  dbt_docs_gen  │  ← auto-generate lineage docs
                    └────────────────┘

Idempotency
-----------
``check_new_data`` pushes an XCom flag.  ``extract_prices`` and
``extract_companies`` honour the ``ParquetLake.price_partition_exists()``
check per-ticker, writing only partitions that are absent.

Schedule
--------
Runs at 01:00 UTC every weekday (Mon–Fri) so data is ready before market open.
``catchup=False`` prevents backfill on first deploy.
"""

import logging
import os
import sys
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow")

from etl.extract import StockAPIExtractor
from etl.load import ParquetLake
from etl.transform import StockDataTransformer
from airflow.dags.utils.callbacks import failure_alert, sla_miss_alert

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────
TICKERS = [
    t.strip().upper()
    for t in os.getenv(
        "SCREENER_TICKERS",
        "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META,BRK-B,JPM,JNJ,V,UNH,XOM,MA,HD",
    ).split(",")
    if t.strip()
]
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/opt/airflow/data/lake")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

# ── Default task arguments ─────────────────────────────────────────────────
default_args = {
    "owner":                    "data-team",
    "depends_on_past":          False,
    "start_date":               days_ago(1),
    "email_on_failure":         False,          # set True + email when SMTP configured
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      failure_alert,
}

# ── Callable task functions ────────────────────────────────────────────────

def _check_new_data(**context) -> bool:
    """
    ShortCircuit: returns False (skip downstream) if all today's price
    partitions already exist in the lake — enabling safe re-runs.
    """
    exec_date: date = context["logical_date"].date()
    lake = ParquetLake(DATA_LAKE_PATH)
    all_exist = all(
        lake.price_partition_exists(ticker, exec_date) for ticker in TICKERS
    )
    if all_exist:
        logger.info("All partitions already exist for %s — short-circuiting.", exec_date)
        return False
    missing = [t for t in TICKERS if not lake.price_partition_exists(t, exec_date)]
    logger.info("%d/%d tickers need extraction for %s", len(missing), len(TICKERS), exec_date)
    return True


def _extract_prices(**context) -> None:
    """
    Incrementally extract OHLCV prices for tickers missing today's partition.
    Writes Hive-partitioned Parquet files to the data lake.
    """
    exec_date: date = context["logical_date"].date()
    lake        = ParquetLake(DATA_LAKE_PATH)
    transformer = StockDataTransformer()
    extractor   = StockAPIExtractor(calls_per_minute=10)

    # Only fetch tickers whose partition is absent (incremental)
    pending = [t for t in TICKERS if not lake.price_partition_exists(t, exec_date)]
    logger.info("Extracting prices for %d tickers on %s", len(pending), exec_date)

    prices_df, _ = extractor.extract_batch(
        tickers=pending,
        start_date=exec_date,
        end_date=exec_date,
        include_company_info=False,
    )

    clean_df = transformer.clean_prices(prices_df)
    report   = transformer.validation_report(clean_df, "prices")
    logger.info("Validation report: %s", report)

    written = lake.write_prices(clean_df, exec_date)
    logger.info("Wrote %d price rows for %s", written, exec_date)


def _extract_companies(**context) -> None:
    """
    Extract company metadata for tickers whose partition is absent.
    Runs in parallel with ``_extract_prices`` (independent branch).
    """
    exec_date: date = context["logical_date"].date()
    lake        = ParquetLake(DATA_LAKE_PATH)
    transformer = StockDataTransformer()
    extractor   = StockAPIExtractor(calls_per_minute=10)

    pending = [t for t in TICKERS if not lake.company_partition_exists(t, exec_date)]
    logger.info("Extracting company info for %d tickers", len(pending))

    _, companies_df = extractor.extract_batch(
        tickers=pending,
        start_date=exec_date,
        end_date=exec_date,
        include_company_info=True,
    )

    clean_df = transformer.clean_company_info(companies_df)
    written  = lake.write_company_info(clean_df, exec_date)
    logger.info("Wrote %d company info rows", written)


# ── dbt command builder ────────────────────────────────────────────────────
def _dbt_cmd(subcommand: str, select: str | None = None) -> str:
    base = (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt {subcommand} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target {DBT_TARGET} "
        f"--no-use-colors"
    )
    if select:
        base += f" --select {select}"
    return base


# ── DAG definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="stock_pipeline",
    default_args=default_args,
    description="Daily stock ETL: extract → Parquet lake → dbt star schema",
    schedule_interval="0 1 * * 1-5",   # 01:00 UTC, Mon–Fri
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_alert,
    tags=["etl", "stocks", "dbt", "incremental"],
) as dag:

    # ── Task 0: Idempotency gate ───────────────────────────────────────
    check_new_data = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=_check_new_data,
    )

    # ── Task 1 & 2: Parallel extraction (independent branches) ────────
    extract_prices = PythonOperator(
        task_id="extract_prices",
        python_callable=_extract_prices,
    )

    extract_companies = PythonOperator(
        task_id="extract_companies",
        python_callable=_extract_companies,
    )

    # ── Task 3–7: dbt transformation chain ────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt_cmd("deps"),
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_dbt_cmd("run", "staging"),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=_dbt_cmd("run", "intermediate"),
    )

    dbt_snapshots = BashOperator(
        task_id="dbt_snapshots",
        bash_command=_dbt_cmd("snapshot"),
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_dbt_cmd("run", "marts"),
    )

    # ── Task 8: dbt tests ─────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt_cmd("test"),
    )

    # ── Task 9: generate docs + lineage ───────────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=_dbt_cmd("docs generate"),
        trigger_rule="all_success",
    )

    # ── Dependencies ──────────────────────────────────────────────────
    check_new_data >> [extract_prices, extract_companies]
    [extract_prices, extract_companies] >> dbt_deps
    dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_snapshots
    dbt_snapshots >> dbt_marts >> dbt_test >> dbt_docs
