"""
Data cleaning and validation for raw stock data before Parquet landing.

Responsibilities
----------------
- Enforce expected schema / column presence.
- Cast all columns to stable dtypes.
- Remove clearly invalid rows (null close price, zero volume for equities).
- Add partition columns (year, month, day) derived from trade_date.
- Surface row-level quality metrics via a validation report.
"""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

# Expected columns coming out of the extractor
PRICE_REQUIRED_COLS = {"ticker", "Date", "Open", "High", "Low", "Close", "Volume"}
COMPANY_REQUIRED_COLS = {"ticker", "company_name", "sector", "industry"}


class SchemaError(ValueError):
    """Raised when a required column is missing from the input DataFrame."""


class StockDataTransformer:
    """Cleans and enriches raw extraction DataFrames."""

    # ── Validation ────────────────────────────────────────────────────────

    @staticmethod
    def _assert_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
        missing = required - set(df.columns)
        if missing:
            raise SchemaError(f"{label}: missing columns {missing}")

    # ── Prices ────────────────────────────────────────────────────────────

    def clean_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and type-cast raw price rows.

        Steps
        -----
        1. Validate required columns exist.
        2. Cast types: Date→date, OHLC→float64, Volume→int64.
        3. Drop rows where Close is null or Volume ≤ 0.
        4. Remove exact duplicates (ticker + date).
        5. Add partition helper columns (year, month, day).
        """
        if df.empty:
            return df

        self._assert_columns(df, PRICE_REQUIRED_COLS, "prices")

        out = df.copy()

        # Normalise date column (yfinance may return tz-aware Timestamps)
        out["trade_date"] = pd.to_datetime(out["Date"]).dt.tz_localize(None).dt.date
        out.drop(columns=["Date"], inplace=True)

        # Cast OHLC to float64
        for col in ("Open", "High", "Low", "Close"):
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")

        out.rename(
            columns={
                "Open":   "open_price",
                "High":   "high_price",
                "Low":    "low_price",
                "Close":  "close_price",
            },
            inplace=True,
        )

        out["volume"] = pd.to_numeric(out["Volume"], errors="coerce").fillna(0).astype("int64")
        out.drop(columns=["Volume"], inplace=True, errors="ignore")

        # Drop invalid rows
        before = len(out)
        out = out.dropna(subset=["close_price"])
        out = out[out["volume"] > 0]
        dropped = before - len(out)
        if dropped:
            logger.warning("Dropped %d invalid price rows (null close or zero volume)", dropped)

        # Dedup: keep latest extraction per ticker+date
        out.sort_values("_extracted_at", ascending=False, inplace=True)
        out.drop_duplicates(subset=["ticker", "trade_date"], keep="first", inplace=True)

        # Partition columns
        out["year"]  = pd.to_datetime(out["trade_date"]).dt.year
        out["month"] = pd.to_datetime(out["trade_date"]).dt.month
        out["day"]   = pd.to_datetime(out["trade_date"]).dt.day

        logger.info("clean_prices: %d rows in → %d rows out", before, len(out))
        return out.reset_index(drop=True)

    # ── Company info ──────────────────────────────────────────────────────

    def clean_company_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and type-cast raw company metadata rows.
        """
        if df.empty:
            return df

        self._assert_columns(df, COMPANY_REQUIRED_COLS, "company_info")

        out = df.copy()
        out["market_cap"] = pd.to_numeric(out.get("market_cap"), errors="coerce")
        out["employees"]  = pd.to_numeric(out.get("employees"),  errors="coerce")

        # Normalise string columns: strip whitespace, replace empty with None
        str_cols = ["company_name", "sector", "industry", "country",
                    "exchange", "currency", "website"]
        for col in str_cols:
            if col in out.columns:
                out[col] = out[col].str.strip().replace("", None)

        out.drop_duplicates(subset=["ticker"], keep="last", inplace=True)
        logger.info("clean_company_info: %d company records", len(out))
        return out.reset_index(drop=True)

    # ── Validation report ─────────────────────────────────────────────────

    @staticmethod
    def validation_report(df: pd.DataFrame, label: str) -> dict:
        """Return a dict of basic quality metrics for logging / alerting."""
        if df.empty:
            return {"label": label, "rows": 0, "status": "empty"}
        report = {
            "label":       label,
            "rows":        len(df),
            "null_pct":    {c: round(df[c].isna().mean() * 100, 2) for c in df.columns},
            "date_range":  None,
        }
        if "trade_date" in df.columns:
            report["date_range"] = {
                "min": str(df["trade_date"].min()),
                "max": str(df["trade_date"].max()),
            }
        return report
