# src/market_sentiment/aggregate.py
from __future__ import annotations

import numpy as np
import pandas as pd


# ---------- time helpers ----------

def _to_utc_series(ts_like) -> pd.Series:
    """Coerce to tz-aware UTC pandas Series of datetimes."""
    return pd.to_datetime(ts_like, errors="coerce", utc=True)


def _to_ny_naive(ts_like) -> pd.Series:
    """
    Convert timestamps to America/New_York midnight (tz-naive date series).
    Works with Series/array-like of timestamps.
    """
    s = _to_utc_series(ts_like)
    # On Series, use the .dt accessor for tz conversion + normalize
    s = s.dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return s


# ---------- sentiment aggregation ----------

def _effective_date(ts: pd.Series | pd.Index, cutoff_minutes: int = 5) -> pd.Series:
    """
    Convert timestamps to effective trading date in America/New_York:
      1) ensure tz-aware UTC
      2) subtract cutoff minutes
      3) convert to NY midnight
      4) return tz-naive dates
    """
    s = _to_utc_series(ts)
    s = s - pd.to_timedelta(cutoff_minutes, unit="m")
    return _to_ny_naive(s)


def daily_sentiment_from_rows(
    rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5
) -> pd.DataFrame:
    """
    Aggregate per (ticker, date) daily sentiment.
    rows must have columns: ['ticker','ts','S'].
    kind: 'news' or 'earn'.
    Returns:
      news -> ['date','ticker','S_news','news_count']
      earn -> ['date','ticker','S_earn','earn_count']
    """
    required = {"ticker", "ts", "S"}
    if rows is None or len(rows) == 0:
        if kind == "news":
            return pd.DataFrame(columns=["date", "ticker", "S_news", "news_count"])
        elif kind == "earn":
            return pd.DataFrame(columns=["date", "ticker", "S_earn", "earn_count"])
        return pd.DataFrame(columns=["date", "ticker", "S", "count"])

    missing = required - set(rows.columns)
    if missing:
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows.copy()
    d["ts"] = _to_utc_series(d["ts"])
    d = d.dropna(subset=["ts"])
    d["ticker"] = d["ticker"].astype(str).str.upper()
    d["S"] = pd.to_numeric(d["S"], errors="coerce").fillna(0.0)
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    g = (
        d.groupby(["ticker", "date"], as_index=False)
        .agg(mean_S=("S", "mean"), count=("S", "size"))
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )

    if kind == "news":
        g = g.rename(columns={"mean_S": "S_news", "count": "news_count"})
        cols = ["date", "ticker", "S_news", "news_count"]
    elif kind == "earn":
        g = g.rename(columns={"mean_S": "S_earn", "count": "earn_count"})
        cols = ["date", "ticker", "S_earn", "earn_count"]
    else:
        g = g.rename(columns={"mean_S": "S", "count": "count"})
        cols = ["date", "ticker", "S", "count"]

    g["date"] = pd.to_datetime(g["date"]).dt.tz_localize(None)
    return g[cols]


def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Outer-join news & earnings daily aggregates and compute combined S.
    Returns: ['date','ticker','S','S_news','S_earn','news_count','earn_count']
    """
    base_cols = ["date", "ticker"]
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=base_cols + ["S_news", "news_count"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=base_cols + ["S_earn", "earn_count"])

    df = pd.merge(d_news, d_earn, on=base_cols, how="outer")

    for c, default in (
        ("S_news", 0.0),
        ("S_earn", 0.0),
        ("news_count", 0),
        ("earn_count", 0),
    ):
        df[c] = pd.to_numeric(df.get(c, default), errors="coerce").fillna(default)

    # Weighted combine (if both counts zero, S=0)
    denom = df["news_count"].astype(float) + df["earn_count"].astype(float)
    num = df["S_news"] * df["news_count"] + df["S_earn"] * df["earn_count"]
    df["S"] = np.where(denom > 0, num / np.where(denom == 0, 1, denom), 0.0)

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df[base_cols + ["S", "S_news", "S_earn", "news_count", "earn_count"]]


# ---------- prices -> forward returns ----------

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward 1D close-to-close return (ret_cc_1d) and same-day open-to-close (ret_oc_1d).
    Expects at least: ['date','ticker','close'] (and 'open' for ret_oc_1d).
    Ensures 'date' is tz-naive (NY trading date) to match daily sentiment.
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["date", "ticker", "close", "ret_cc_1d", "ret_oc_1d"])

    df = prices.copy()

    # Normalize date to NY trading day (naive)
    df["date"] = _to_ny_naive(df["date"])

    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Allow 'Open'/'Close' fallback spellings
    if "close" not in df.columns and "Close" in df.columns:
        df["close"] = df["Close"]
    if "open" not in df.columns and "Open" in df.columns:
        df["open"] = df["Open"]

    for c in ("close", "open"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Forward close->close return
    df["ret_cc_1d"] = (
        df.groupby("ticker", group_keys=False)["close"]
        .apply(lambda s: s.pct_change().shift(-1))
        .astype(float)
    )

    # Same-day open->close return (if open present)
    if "open" in df.columns:
        df["ret_oc_1d"] = (df["close"] - df["open"]) / df["open"]
    else:
        df["ret_oc_1d"] = np.nan

    return df
