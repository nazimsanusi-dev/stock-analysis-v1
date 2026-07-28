#!/usr/bin/env python3
"""
CLI entry-point for the stock ETL pipeline.

Runs Extract → Transform → Load for a single logical date,
defaulting to the most recent completed trading day (yesterday).

Usage
-----
    python -m etl.run_pipeline                  # yesterday
    python -m etl.run_pipeline 2024-06-14       # specific date
"""

import logging
import os
import sys
from datetime import date, timedelta

from etl.extract import StockAPIExtractor
from etl.load import ParquetLake
from etl.transform import StockDataTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

TICKERS = [
    t.strip().upper()
    for t in os.getenv(
        "SCREENER_TICKERS",
        "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META,BRK-B,JPM,JNJ,V,UNH,XOM,MA,HD",
    ).split(",")
    if t.strip()
]
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "data/lake")


def run(run_date: date) -> None:
    lake = ParquetLake(DATA_LAKE_PATH)
    transformer = StockDataTransformer()
    extractor = StockAPIExtractor(calls_per_minute=10)

    logger.info("ETL start  date=%s  tickers=%d  lake=%s", run_date, len(TICKERS), DATA_LAKE_PATH)

    prices_df, companies_df = extractor.extract_batch(
        tickers=TICKERS,
        start_date=run_date,
        end_date=run_date,
        include_company_info=True,
    )

    clean_prices = transformer.clean_prices(prices_df)
    clean_companies = transformer.clean_company_info(companies_df)

    logger.info("Prices validation: %s", transformer.validation_report(clean_prices, "prices"))
    logger.info("Companies validation: %s", transformer.validation_report(clean_companies, "company_info"))

    written_prices = lake.write_prices(clean_prices, run_date)
    written_companies = lake.write_company_info(clean_companies, run_date)

    logger.info("ETL done  prices_rows=%d  company_rows=%d", written_prices, written_companies)


def main() -> None:
    if len(sys.argv) > 1:
        try:
            run_date = date.fromisoformat(sys.argv[1])
        except ValueError:
            logger.error("Invalid date '%s' — expected YYYY-MM-DD", sys.argv[1])
            sys.exit(1)
    else:
        run_date = date.today() - timedelta(days=1)

    run(run_date)


if __name__ == "__main__":
    main()
