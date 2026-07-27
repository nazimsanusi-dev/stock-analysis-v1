"""
Parquet data-lake writer with Hive-style partitioning and idempotency checks.

Layout on disk
--------------
<lake_root>/raw/prices/year=YYYY/month=MM/day=DD/<ticker>.parquet
<lake_root>/raw/company_info/year=YYYY/month=MM/<ticker>.parquet

Idempotency
-----------
``partition_exists()`` checks whether a partition file is already present.
The Airflow DAG uses this to skip re-extraction of days already landed,
enabling safe re-runs and incremental loading with no data duplication.
"""

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetLake:
    """
    Writes Pandas DataFrames as partitioned Parquet files under *lake_root*.

    Parameters
    ----------
    lake_root : str | Path
        Root directory of the data lake (e.g. ``/opt/airflow/data/lake``).
    compression : str
        Parquet compression codec.  ``"snappy"`` gives a good speed/size trade-off.
    """

    def __init__(
        self,
        lake_root: str | Path,
        compression: str = "snappy",
    ) -> None:
        self.lake_root = Path(lake_root)
        self.compression = compression

    # ── Internal helpers ──────────────────────────────────────────────────

    def _prices_path(self, ticker: str, as_of: date) -> Path:
        return (
            self.lake_root
            / "raw"
            / "prices"
            / f"year={as_of.year}"
            / f"month={as_of.month:02d}"
            / f"day={as_of.day:02d}"
            / f"{ticker.upper()}.parquet"
        )

    def _company_path(self, ticker: str, as_of: date) -> Path:
        return (
            self.lake_root
            / "raw"
            / "company_info"
            / f"year={as_of.year}"
            / f"month={as_of.month:02d}"
            / f"{ticker.upper()}.parquet"
        )

    @staticmethod
    def _write(df: pd.DataFrame, path: Path, compression: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path, compression=compression)
        logger.debug("Wrote %d rows → %s", len(df), path)

    # ── Idempotency ───────────────────────────────────────────────────────

    def price_partition_exists(self, ticker: str, as_of: date) -> bool:
        """Return True if the price partition for *ticker* on *as_of* already exists."""
        return self._prices_path(ticker, as_of).exists()

    def company_partition_exists(self, ticker: str, as_of: date) -> bool:
        """Return True if the company-info partition for *ticker* on *as_of* already exists."""
        return self._company_path(ticker, as_of).exists()

    # ── Writers ───────────────────────────────────────────────────────────

    def write_prices(
        self,
        df: pd.DataFrame,
        execution_date: date,
        overwrite: bool = False,
    ) -> int:
        """
        Write price rows partitioned by ticker and execution_date.

        Parameters
        ----------
        df : pd.DataFrame
            Cleaned price data (output of ``StockDataTransformer.clean_prices``).
        execution_date : date
            The logical date of the pipeline run (used for the partition path).
        overwrite : bool
            If False (default) and the partition already exists, skip writing.

        Returns
        -------
        int
            Number of rows written (0 if skipped).
        """
        if df.empty:
            logger.warning("write_prices: empty DataFrame — nothing to write")
            return 0

        written = 0
        for ticker, group in df.groupby("ticker"):
            path = self._prices_path(str(ticker), execution_date)
            if not overwrite and path.exists():
                logger.info("Partition exists, skipping: %s", path)
                continue
            self._write(group.reset_index(drop=True), path, self.compression)
            written += len(group)

        logger.info("write_prices: %d rows written for %s", written, execution_date)
        return written

    def write_company_info(
        self,
        df: pd.DataFrame,
        execution_date: date,
        overwrite: bool = False,
    ) -> int:
        """
        Write company-info rows partitioned by ticker and execution_date.

        Parameters
        ----------
        df : pd.DataFrame
            Cleaned company data (output of ``StockDataTransformer.clean_company_info``).
        execution_date : date
            The logical date of the pipeline run.
        overwrite : bool
            If False (default) and the partition already exists, skip writing.

        Returns
        -------
        int
            Number of rows written (0 if skipped).
        """
        if df.empty:
            logger.warning("write_company_info: empty DataFrame — nothing to write")
            return 0

        written = 0
        for ticker, group in df.groupby("ticker"):
            path = self._company_path(str(ticker), execution_date)
            if not overwrite and path.exists():
                logger.info("Company partition exists, skipping: %s", path)
                continue
            self._write(group.reset_index(drop=True), path, self.compression)
            written += len(group)

        logger.info("write_company_info: %d rows written for %s", written, execution_date)
        return written

    # ── Inventory ─────────────────────────────────────────────────────────

    def list_price_dates(self) -> list[date]:
        """Return a sorted list of all dates for which price partitions exist."""
        root = self.lake_root / "raw" / "prices"
        dates: set[date] = set()
        if not root.exists():
            return []
        for day_dir in root.glob("year=*/month=*/day=*"):
            try:
                parts = {p.split("=")[0]: int(p.split("=")[1]) for p in day_dir.parts[-3:]}
                dates.add(date(parts["year"], parts["month"], parts["day"]))
            except (IndexError, ValueError):
                continue
        return sorted(dates)

    def latest_price_date(self) -> date | None:
        """Return the most recent date in the price lake, or None if empty."""
        dates = self.list_price_dates()
        return dates[-1] if dates else None
