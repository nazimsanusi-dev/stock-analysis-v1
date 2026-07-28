#!/usr/bin/env python3
"""
Stock Analysis v1 — Fundamental & Technical Screener for U.S. Equities
Detects bullish trends, ATH conditions, and key fundamental metrics.
"""

import os
import sys
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from requests_cache import CacheMixin
from requests_ratelimiter import LimiterMixin
from requests import Session
import yfinance as yf
from dotenv import load_dotenv
from tabulate import tabulate
from colorama import Fore, Style, init as colorama_init

warnings.filterwarnings("ignore")
load_dotenv()
colorama_init(autoreset=True)

# ── Custom Session to bypass Yahoo Datacenter Blocks ────────────────────────
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    per_second=2,
    expire_after=3600,
)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# ── Configuration (env-driven) ─────────────────────────────────────────────
TICKERS_ENV = (os.getenv("SCREENER_TICKERS")
               or "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META,BRK-B,JPM,JNJ,V,UNH,XOM,MA,HD")
TICKERS = [t.strip().upper() for t in TICKERS_ENV.split(",") if t.strip()]
PERIOD = os.getenv("SCREENER_PERIOD") or "1y"
ATH_LOOKBACK = int(os.getenv("ATH_LOOKBACK_DAYS", "252"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MA_SHORT = int(os.getenv("MA_SHORT", "50"))
MA_LONG = int(os.getenv("MA_LONG", "200"))
RESULTS_DIR = "results"


# ── Technical Indicators ───────────────────────────────────────────────────

def compute_rsi(series: pd.Series, period: int = 14) -> float:
    """Relative Strength Index (Wilder smoothing)."""
    delta = series.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)


def compute_macd(series: pd.Series):
    """MACD line and signal (12/26/9 EMA)."""
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return round(float(macd.iloc[-1]), 4), round(float(signal.iloc[-1]), 4)


def is_golden_cross(close: pd.Series, short: int, long: int) -> bool:
    """True when short MA crosses above long MA (golden cross)."""
    ma_s = close.rolling(short).mean()
    ma_l = close.rolling(long).mean()
    return bool(ma_s.iloc[-1] > ma_l.iloc[-1] and ma_s.iloc[-2] <= ma_l.iloc[-2])


def is_above_both_mas(close: pd.Series, short: int, long: int) -> bool:
    ma_s = close.rolling(short).mean().iloc[-1]
    ma_l = close.rolling(long).mean().iloc[-1]
    price = close.iloc[-1]
    return bool(price > ma_s and price > ma_l)


def near_ath(close: pd.Series, lookback: int, threshold: float = 0.05) -> bool:
    """True when current price is within `threshold` of its ATH over `lookback` days."""
    ath = close.tail(lookback).max()
    current = close.iloc[-1]
    return bool((ath - current) / ath <= threshold)


def ath_pct_below(close: pd.Series, lookback: int) -> float:
    ath = close.tail(lookback).max()
    return round(float(((ath - close.iloc[-1]) / ath) * 100), 2)


def volume_surge(volume: pd.Series, window: int = 20) -> float:
    """Current volume vs. rolling average (ratio)."""
    avg = volume.tail(window).mean()
    return round(float(volume.iloc[-1] / avg), 2) if avg else 0.0


def price_change_pct(close: pd.Series, days: int) -> float:
    if len(close) < days + 1:
        return 0.0
    return round(float((close.iloc[-1] - close.iloc[-days - 1]) / close.iloc[-days - 1] * 100), 2)


# ── Bullish Signal Scoring ─────────────────────────────────────────────────

def bullish_score(row: dict) -> tuple[int, list[str]]:
    """Return (score, signals) where each signal adds 1 point."""
    signals = []
    if row.get("above_ma50_ma200"):
        signals.append("Price > MA50 & MA200")
    if row.get("golden_cross"):
        signals.append("Golden Cross")
    if row.get("near_ath"):
        signals.append("Near ATH (≤5%)")
    if row.get("rsi") and 50 < row["rsi"] < 70:
        signals.append(f"RSI bullish ({row['rsi']})")
    if row.get("macd_line") and row.get("macd_signal") and row["macd_line"] > row["macd_signal"]:
        signals.append("MACD > Signal")
    if row.get("volume_ratio") and row["volume_ratio"] > 1.5:
        signals.append(f"Vol surge x{row['volume_ratio']}")
    if row.get("pe_ratio") and 0 < row["pe_ratio"] < 30:
        signals.append(f"P/E attractive ({row['pe_ratio']})")
    if row.get("ret_1m") and row["ret_1m"] > 3:
        signals.append(f"1M +{row['ret_1m']}%")
    return len(signals), signals


# ── Single Ticker Analysis ─────────────────────────────────────────────────

def analyze_ticker(ticker: str) -> dict | None:
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=PERIOD)
        if hist.empty or len(hist) < MA_LONG + 5:
            return None

        info = stock.info or {}
        close = hist["Close"]
        volume = hist["Volume"]

        macd_line, macd_signal = compute_macd(close)

        row = {
            "ticker": ticker,
            "price": round(float(close.iloc[-1]), 2),
            "ret_1d": price_change_pct(close, 1),
            "ret_1m": price_change_pct(close, 21),
            "ret_3m": price_change_pct(close, 63),
            "ret_ytd": price_change_pct(close, min(len(close) - 1, 180)),
            "rsi": compute_rsi(close, RSI_PERIOD),
            "macd_line": macd_line,
            "macd_signal": macd_signal,
            "ma50": round(float(close.rolling(MA_SHORT).mean().iloc[-1]), 2),
            "ma200": round(float(close.rolling(MA_LONG).mean().iloc[-1]), 2),
            "above_ma50_ma200": is_above_both_mas(close, MA_SHORT, MA_LONG),
            "golden_cross": is_golden_cross(close, MA_SHORT, MA_LONG),
            "near_ath": near_ath(close, ATH_LOOKBACK),
            "pct_below_ath": ath_pct_below(close, ATH_LOOKBACK),
            "volume_ratio": volume_surge(volume),
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE"),
            "fwd_pe": info.get("forwardPE"),
            "eps_ttm": info.get("trailingEps"),
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "profit_margin": info.get("profitMargins"),
            "roe": info.get("returnOnEquity"),
            "debt_equity": info.get("debtToEquity"),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
        }

        score, signals = bullish_score(row)
        row["bull_score"] = score
        row["bull_signals"] = "; ".join(signals) if signals else "—"
        return row

    except Exception as exc:
        print(f"{Fore.RED}  ✗ {ticker}: {exc}{Style.RESET_ALL}")
        return None


# ── Formatting Helpers ─────────────────────────────────────────────────────

def fmt_pct(v) -> str:
    if v is None:
        return "N/A"
    color = Fore.GREEN if v >= 0 else Fore.RED
    return f"{color}{v:+.2f}%{Style.RESET_ALL}"


def fmt_float(v, decimals=2) -> str:
    return f"{v:.{decimals}f}" if v is not None else "N/A"


def fmt_market_cap(v) -> str:
    if v is None:
        return "N/A"
    if v >= 1e12:
        return f"${v/1e12:.2f}T"
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    return f"${v/1e6:.2f}M"


# ── Report ─────────────────────────────────────────────────────────────────

def print_banner():
    print(f"\n{Fore.CYAN}{'═'*70}")
    print(f"  📈  Stock Analysis v1  —  U.S. Equity Fundamental Screener")
    print(f"  🕐  {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'═'*70}{Style.RESET_ALL}\n")


def print_summary(results: list[dict]):
    bullish = [r for r in results if r["bull_score"] >= 4]
    near_ath_list = [r for r in results if r["near_ath"]]
    golden = [r for r in results if r["golden_cross"]]

    print(f"{Fore.YELLOW}── Summary ──────────────────────────────────────────────────{Style.RESET_ALL}")
    print(f"  Tickers scanned   : {len(results)}")
    print(f"  Strong bullish    : {len(bullish)}  (score ≥ 4)")
    print(f"  Near ATH          : {len(near_ath_list)}")
    print(f"  Golden Cross      : {len(golden)}")
    print()


def print_table(results: list[dict]):
    rows = []
    for r in sorted(results, key=lambda x: -x["bull_score"]):
        ath_flag = f"{Fore.GREEN}✔{Style.RESET_ALL}" if r["near_ath"] else f"{Fore.RED}✘{Style.RESET_ALL}"
        gc_flag = f"{Fore.GREEN}✔{Style.RESET_ALL}" if r["golden_cross"] else " "
        score_color = Fore.GREEN if r["bull_score"] >= 4 else (Fore.YELLOW if r["bull_score"] >= 2 else Fore.RED)
        rows.append([
            f"{Fore.CYAN}{r['ticker']:<6}{Style.RESET_ALL}",
            f"${r['price']:>9.2f}",
            fmt_pct(r["ret_1d"]),
            fmt_pct(r["ret_1m"]),
            fmt_pct(r["ret_3m"]),
            fmt_float(r["rsi"]),
            fmt_float(r["pe_ratio"]),
            fmt_market_cap(r["market_cap"]),
            ath_flag,
            gc_flag,
            f"{score_color}{r['bull_score']}/8{Style.RESET_ALL}",
        ])

    headers = ["Ticker", "Price", "1D", "1M", "3M", "RSI", "P/E", "Mkt Cap", "ATH", "GC", "Score"]
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))
    print()


def print_signals(results: list[dict]):
    print(f"{Fore.YELLOW}── Bullish Signals ──────────────────────────────────────────{Style.RESET_ALL}")
    for r in sorted(results, key=lambda x: -x["bull_score"]):
        if r["bull_score"] > 0:
            print(f"  {Fore.CYAN}{r['ticker']:<6}{Style.RESET_ALL}  {r['bull_signals']}")
    print()


def save_csv(results: list[dict]):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"analysis_{ts}.csv")
    df = pd.DataFrame(results)
    df.drop(columns=["bull_signals"], inplace=True, errors="ignore")
    df.to_csv(path, index=False)
    print(f"{Fore.GREEN}  ✔ Results saved → {path}{Style.RESET_ALL}\n")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print_banner()
    print(f"{Fore.WHITE}Scanning {len(TICKERS)} tickers: {', '.join(TICKERS)}{Style.RESET_ALL}\n")

    results = []
    for i, ticker in enumerate(TICKERS, 1):
        sys.stdout.write(f"\r  [{i:>2}/{len(TICKERS)}] Fetching {ticker:<8} ...")
        sys.stdout.flush()
        row = analyze_ticker(ticker)
        if row:
            results.append(row)

    print(f"\r{' ' * 50}\r", end="")  # clear progress line

    if not results:
        print(f"{Fore.RED}No data retrieved. Check your network or ticker symbols.{Style.RESET_ALL}")
        sys.exit(1)

    print_summary(results)
    print_table(results)
    print_signals(results)
    save_csv(results)


if __name__ == "__main__":
    main()
