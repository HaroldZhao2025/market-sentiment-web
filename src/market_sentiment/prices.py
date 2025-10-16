from __future__ import annotations

from typing import List
import pandas as pd
import yfinance as yf


def _normalize_single(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    yfinance.download for a single ticker returns an OHLCV frame with DatetimeIndex.
    Normalize to columns: date (tz-naive YYYY-MM-DD), ticker, open, close
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    df = raw.copy()
    # If MultiIndex (can happen in corner cases), flatten it
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["-".join([c for c in cols if c]) for cols in df.columns.to_list()]

    # Common column names are "Open" / "Close" (title case)
    # Some environments may lowercase; normalize defensively.
    cols = {c.lower(): c for c in df.columns}
    open_col = cols.get("open") or "Open"
    close_col = cols.get("close") or "Close"
    if open_col not in df.columns or close_col not in df.columns:
        # nothing usable
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    df = df.rename(columns={open_col: "open", close_col: "close"})
    df = df[["open", "close"]].copy()

    # Index -> date col (normalize to date, tz-naive)
    idx = pd.to_datetime(raw.index, utc=True, errors="coerce")
    # Convert to NY date and drop tz info
    idx = idx.tz_convert("America/New_York").tz_localize(None).normalize()
    df.insert(0, "date", idx)
    df["ticker"] = ticker
    return df[["date", "ticker", "open", "close"]].dropna().reset_index(drop=True)


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Single-ticker fetch with yfinance (free).
    """
    try:
        raw = yf.download(
            tickers=ticker,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=False,
            interval="1d",
        )
    except Exception:
        raw = None

    return _normalize_single(raw, ticker)
