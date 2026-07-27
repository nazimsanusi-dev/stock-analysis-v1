"""
Stock data extraction with pagination, rate-limiting, and exponential-backoff retry.

Data source: Yahoo Finance via yfinance (public REST API wrapper).
Pagination pattern: splits any date range into fixed-size pages (default 90 days)
so the pipeline can resume mid-range after a transient failure.
Rate limiting: token-bucket enforcing a configurable calls-per-minute ceiling.
Retry logic: tenacity library — up to 5 attempts, wait doubles each time (4s → 64s).
"""

import logging
import time
from datetime import date, datetime, timedelta
from typing import Iterator

import pandas as pd
import yfinance as yf
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Simple token-bucket rate limiter.
    Sleeps the calling thread until the minimum inter-call interval has elapsed.
    """

    def __init__(self, calls_per_minute: int = 10) -> None:
        self._interval: float = 60.0 / max(calls_per_minute, 1)
        self._last_call: float = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        gap = self._interval - elapsed
        if gap > 0:
            logger.debug("Rate limiter sleeping %.2fs", gap)
            time.sleep(gap)
        self._last_call = time.monotonic()


class StockAPIExtractor:
    """
    Extracts OHLCV price history and company metadata from Yahoo Finance.

    Responsibilities
    ----------------
    - Paginate large date ranges into smaller chunks.
    - Respect the API rate limit with a configurable limiter.
    - Retry transient failures with exponential back-off.
    - Tag every row with an _extracted_at timestamp for lineage.
    """

    def __init__(self, calls_per_minute: int = 10, page_size_days: int = 90) -> None:
        self.rate_limiter = RateLimiter(calls_per_minute)
        self.page_size_days = page_size_days

    # ── Pagination ────────────────────────────────────────────────────────

    def _paginate_dates(
        self, start: date, end: date
    ) -> Iterator[tuple[date, date]]:
        """Yield (page_start, page_end) tuples covering [start, end]."""
        current = start
        while current <= end:
            page_end = min(current + timedelta(days=self.page_size_days - 1), end)
            yield current, page_end
            current = page_end + timedelta(days=1)

    # ── Single-page fetch (with retry) ───────────────────────────────────

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=64),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _fetch_price_page(
        self, ticker: str, start: date, end: date
    ) -> pd.DataFrame:
        """Fetch one date-range page; retried on any exception."""
        self.rate_limiter.wait()
        stock = yf.Ticker(ticker)
        hist = stock.history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            auto_adjust=True,
            actions=False,
        )
        if hist.empty:
            return pd.DataFrame()
        hist.reset_index(inplace=True)
        hist.rename(columns={"index": "Date"}, inplace=True)
        hist["ticker"] = ticker
        hist["_extracted_at"] = datetime.utcnow().isoformat()
        return hist

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=64),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _fetch_company_info(self, ticker: str) -> dict:
        """Fetch company metadata; retried on any exception."""
        self.rate_limiter.wait()
        info = yf.Ticker(ticker).info or {}
        return {
            "ticker":        ticker,
            "company_name":  info.get("longName", ""),
            "sector":        info.get("sector", ""),
            "industry":      info.get("industry", ""),
            "country":       info.get("country", ""),
            "exchange":      info.get("exchange", ""),
            "currency":      info.get("currency", "USD"),
            "market_cap":    info.get("marketCap"),
            "employees":     info.get("fullTimeEmployees"),
            "website":       info.get("website", ""),
            "description":   info.get("longBusinessSummary", "")[:500],
            "_extracted_at": datetime.utcnow().isoformat(),
        }

    # ── Public API ────────────────────────────────────────────────────────

    def fetch_ticker_history(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Fetch full OHLCV history for *ticker* across [start_date, end_date].
        Internally pages through the date range in ``page_size_days`` chunks.
        """
        pages: list[pd.DataFrame] = []
        for page_num, (ps, pe) in enumerate(
            self._paginate_dates(start_date, end_date), start=1
        ):
            logger.info("[%s] page %d: %s → %s", ticker, page_num, ps, pe)
            df = self._fetch_price_page(ticker, ps, pe)
            if not df.empty:
                pages.append(df)

        if not pages:
            logger.warning("[%s] no data returned for %s → %s", ticker, start_date, end_date)
            return pd.DataFrame()
        return pd.concat(pages, ignore_index=True)

    def fetch_company_info(self, ticker: str) -> dict:
        """Fetch company metadata for *ticker* (with retry)."""
        logger.info("[%s] fetching company info", ticker)
        return self._fetch_company_info(ticker)

    def extract_batch(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
        include_company_info: bool = True,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract prices (and optionally company info) for a list of tickers.

        Returns
        -------
        prices_df : pd.DataFrame
            All OHLCV rows for all tickers combined.
        companies_df : pd.DataFrame
            One metadata row per ticker.
        """
        price_frames: list[pd.DataFrame] = []
        company_records: list[dict] = []

        for ticker in tickers:
            try:
                prices = self.fetch_ticker_history(ticker, start_date, end_date)
                if not prices.empty:
                    price_frames.append(prices)
                if include_company_info:
                    company_records.append(self.fetch_company_info(ticker))
            except Exception:
                logger.exception("Failed to extract %s — skipping", ticker)

        prices_df = (
            pd.concat(price_frames, ignore_index=True) if price_frames else pd.DataFrame()
        )
        companies_df = pd.DataFrame(company_records) if company_records else pd.DataFrame()
        return prices_df, companies_df
