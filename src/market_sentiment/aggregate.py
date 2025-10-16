# src/market_sentiment/aggregate.py
from __future__ import annotations

import pandas as pd
import numpy as np


# -----------------------------
# Price helpers
# -----------------------------

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure forward 1-day returns are added:
      - ret_cc_1d: close-to-close forward return
      - ret_oc_1d: open-to-close forward return (for the same day), then shifted forward by 1d
    Assumes df has columns: ticker, date, open, close
    Returns a copy with new columns.
    """
    if df.empty:
        return df.assign(ret_cc_1d=pd.Series(dtype=float),
                         ret_oc_1d=pd.Series(dtype=float))

    out = df.copy()

    # Ensure proper types/sort
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Close-to-close forward % change
    out["ret_cc_1d"] = (
        out.groupby("ticker", group_keys=False)["close"]
           .pct_change(fill_method=None)
           .shift(-1)
    )

    # Open-to-close same-day return, then use next day (forward)
    oc_same = out["close"] / out["open"] - 1.0
    out["ret_oc_1d"] = oc_same.groupby(out["ticker"]).shift(-1)

    return out


# -----------------------------
# Sentiment helpers
# -----------------------------

def _effective_date(ts_series: pd.Series, cutoff_minutes: int = 5) -> pd.Series:
    """
    Map UTC timestamps to effective 'trade date' in America/New_York with a cutoff.
    If a news/earnings item arrives within the last `cutoff_minutes` before 16:00 ET,
    treat it as next-day.
    Returns tz-naive date (midnight America/New_York).
    """
    ts = pd.to_datetime(ts_series, utc=True, errors="coerce")
    ny = ts.dt.tz_convert("America/New_York")

    close_dt = ny.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_dt - pd.Timedelta(minutes=cutoff_minutes)

    # If published after threshold, roll to next day
    eff = np.where(ny > threshold, ny + pd.Timedelta(days=1), ny)
    eff = pd.to_datetime(eff).tz_convert("America/New_York").normalize().tz_localize(None)
    return pd.Series(eff, index=ts_series.index)


def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    kind: str,
    cutoff_minutes: int = 5,
) -> pd.DataFrame:
    """
    Aggregate per (ticker, date) daily sentiment.
    Input rows must have: ['ticker','ts','S'] at minimum.
    Returns: ['date','ticker','S','count'] where S is the mean sentiment for the day.
    """
    required = {"ticker", "ts", "S"}
    if rows is None or rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S", "count"])
    if not required.issubset(rows.columns):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows[["ticker", "ts", "S"]].copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")
    d = d.dropna(subset=["ticker", "ts"])

    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    # Aggregate: mean S and item count per day
    g = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(S=("S", "mean"), count=("S", "size"))
    )

    # Sort + stable dtypes
    g = g.sort_values(["ticker", "date"]).reset_index(drop=True)
    g["S"] = g["S"].astype(float)
    g["count"] = g["count"].astype(int)
    return g[["date", "ticker", "S", "count"]]


def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Join daily news + earnings sentiment with clear column names and safe defaults.
    Returns columns: ['date','ticker','S','S_news','S_earn','news_count','earn_count']
    Where S is a weighted average of news/earnings by their counts (if available),
    otherwise falls back to whichever exists.
    """
    cols = ["date", "ticker", "S", "count"]

    # Empty guards – create empty frames with correct schema if needed
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=cols)
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=cols)

    # Ensure proper dtypes
    def _clean(x: pd.DataFrame) -> pd.DataFrame:
        y = x.copy()
        if "date" in y.columns:
            y["date"] = pd.to_datetime(y["date"], errors="coerce")
        if "ticker" in y.columns:
            y["ticker"] = y["ticker"].astype(str)
        if "S" in y.columns:
            y["S"] = pd.to_numeric(y["S"], errors="coerce")
        if "count" in y.columns:
            y["count"] = pd.to_numeric(y["count"], errors="coerce").fillna(0).astype(int)
        return y

    d_news = _clean(d_news)
    d_earn = _clean(d_earn)

    # Rename before merge to avoid the "columns overlap but no suffix specified" error
    d_news = d_news.rename(columns={"S": "S_news", "count": "news_count"})
    d_earn = d_earn.rename(columns={"S": "S_earn", "count": "earn_count"})

    # Outer merge on keys
    keys = ["date", "ticker"]
    df = pd.merge(d_news, d_earn, on=keys, how="outer")

    # Fill NaNs with safe defaults
    for c in ["S_news", "S_earn"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    for c in ["news_count", "earn_count"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # Weighted average S by counts; fallbacks to either if the other is zero
    tot = df["news_count"] + df["earn_count"]
    with np.errstate(divide="ignore", invalid="ignore"):
        weighted = (
            (df["S_news"] * df["news_count"] + df["S_earn"] * df["earn_count"])
            / tot.replace(0, np.nan)
        )
    df["S"] = weighted.fillna(0.0).astype(float)

    # Final ordering / cleanliness
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df[["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"]]
