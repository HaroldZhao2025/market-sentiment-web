# src/market_sentiment/prices.py
from __future__ import annotations

import pandas as pd
import numpy as np
import yfinance as yf


def _empty_prices(ticker: str) -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "ticker", "open", "close"])


def _normalize_price_frame(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize yfinance output into columns: date (naive, normalized), ticker, open, close.
    We DO NOT apply any timezone conversion here — we treat the daily index as a trading-day
    calendar date and just normalize to 00:00.
    """
    if raw is None or len(raw) == 0:
        return _empty_prices(ticker)

    df = raw.copy()

    # yfinance.download(...) returns Date in the index for daily bars
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    # Harmonize column names
    # Common yfinance columns: Date/Open/High/Low/Close/Adj Close/Volume
    rename = {}
    if "Date" in df.columns:
        rename["Date"] = "date"
    if "Datetime" in df.columns:
        rename["Datetime"] = "date"

    # Map any case variants to our canonical names
    for col in list(df.columns):
        low = col.lower()
        if low == "open":
            rename[col] = "open"
        elif low == "close":
            rename[col] = "close"

    df = df.rename(columns=rename)

    # If "close" missing, fall back to "Adj Close"
    if "close" not in df.columns and "Adj Close" in df.columns:
        df["close"] = df["Adj Close"]

    # Ensure numeric
    for c in ("open", "close"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Parse dates as naive, normalized calendar dates (no tz math!)
    if "date" not in df.columns:
        # As a last resort, try to find the first datetime-like column
        for c in df.columns:
            if "date" in c.lower():
                df = df.rename(columns={c: "date"})
                break
    if "date" not in df.columns:
        # If we still don't have a date, bail out
        return _empty_prices(ticker)

    d = pd.to_datetime(df["date"], errors="coerce")
    d = d.dt.normalize()  # keep as naive
    df["date"] = d

    # Attach ticker and keep required columns
    df["ticker"] = ticker
    out = df[["date", "ticker", "open", "close"]].dropna(subset=["date", "close"]).copy()
    return out


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch 1D OHLC for [start, end] inclusive using yfinance (free),
    normalize to (date, ticker, open, close).
    """
    # yfinance end is exclusive; add one day to include 'end'
    end_excl = (pd.to_datetime(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        raw = yf.download(
            tickers=ticker,
            start=start,
            end=end_excl,
            progress=False,
            auto_adjust=False,
            actions=False,
            interval="1d",
            group_by="ticker",
            threads=False,
        )
    except Exception:
        raw = pd.DataFrame()

    norm = _normalize_price_frame(raw, ticker)

    # Final calendar filter (inclusive)
    s = pd.to_datetime(start).normalize()
    e = pd.to_datetime(end).normalize()
    norm = norm[(norm["date"] >= s) & (norm["date"] <= e)].reset_index(drop=True)
    return norm


def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add forward returns:
      - ret_cc_1d: (next close / current close - 1)
      - ret_oc_1d: (current close / current open - 1), shifted forward to align with 'next day'
    """
    if df is None or df.empty:
        return df.copy()

    out = df.sort_values(["ticker", "date"]).copy()

    # Make sure required columns exist
    if "open" not in out.columns:
        out["open"] = np.nan
    if "close" not in out.columns:
        out["close"] = np.nan

    # Close-to-close next day
    # pct_change(fill_method=None) avoids the FutureWarning and doesn't forward-fill NAs
    ret_cc = out.groupby("ticker", group_keys=False)["close"].pct_change(fill_method=None).shift(-1)
    out["ret_cc_1d"] = ret_cc

    # Open-to-close on the same day, aligned to next day (so the signal at T maps to ret at T+1)
    oc = (out["close"] / out["open"]) - 1.0
    out["ret_oc_1d"] = oc.shift(-1)

    return out
