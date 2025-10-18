# src/market_sentiment/aggregate.py
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def _ensure_date_dtype(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """
    Ensure df[col] is pandas datetime64[ns] **naive** (UTC calendar date).
    Accepts strings / tz-aware timestamps. Drops tz to naive in UTC.
    """
    if col not in df.columns:
        return df

    # Convert to tz-aware UTC timestamp, then drop tz to naive
    s = pd.to_datetime(df[col], errors="coerce", utc=True)
    # turn into naive UTC timestamps
    s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    df = df.copy()
    df[col] = s
    return df


def _effective_date(ts: pd.Series, cutoff_minutes: int) -> pd.Series:
    """
    Given a Series of timestamps (any parseable type), compute the 'effective'
    NY calendar date used for daily aggregation:
      - Convert to America/New_York
      - Shift backward by cutoff_minutes
      - Floor to day
      - Return **naive** UTC-like date (no tz)
    """
    # Always work on a Series (not DatetimeIndex)
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    # tz-aware -> America/New_York
    s_et = s.dt.tz_convert("America/New_York")
    # apply cutoff and floor to the day
    day_et = (s_et - pd.to_timedelta(cutoff_minutes, "m")).dt.floor("D")
    # return as naive (no tz)
    return day_et.dt.tz_localize(None)


def _coalesce_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    s = pd.to_numeric(df.get(col), errors="coerce")
    return s.fillna(default)


# ---------------------------------------------------------------------
# Sentiment aggregation
# ---------------------------------------------------------------------

def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    kind: str,
    cutoff_minutes: int = 5,
) -> pd.DataFrame:
    """
    Aggregate point-level rows -> daily sentiment.

    Required columns in `rows`:
      - 'ticker' (str)
      - 'ts'     (timestamp-like)
      - 'S'      (numeric sentiment)

    Returns columns: ['date','ticker','S'] where 'date' is naive datetime64[ns].
    """
    if rows is None or len(rows) == 0:
        return pd.DataFrame(columns=["date", "ticker", "S"])

    req = {"ticker", "ts", "S"}
    missing = req - set(rows.columns)
    if missing:
        raise KeyError(f"{kind} rows must have columns: {', '.join(sorted(req))}.")

    df = rows.copy()
    # effective NY date
    df["date"] = _effective_date(df["ts"], cutoff_minutes)
    # numeric S
    df["S"] = _coalesce_numeric(df, "S", default=0.0)

    # group within (date, ticker). Using mean by default.
    def _one_day(g: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "date": g["date"].iloc[0],
                "ticker": g["ticker"].iloc[0],
                "S": float(np.nanmean(g["S"].to_numpy(dtype=float))),
            }
        )

    daily = (
        df[["date", "ticker", "S"]]
        .groupby(["date", "ticker"], as_index=False)
        .apply(_one_day)
        .reset_index(drop=True)
    )

    # Ensure dtype for merge consistency
    daily = _ensure_date_dtype(daily, "date")
    return daily[["date", "ticker", "S"]]


def join_and_fill_daily(
    d_news: Optional[pd.DataFrame],
    d_earn: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Outer-join daily news and earnings sentiment.
    Returns columns: ['date','ticker','S_NEWS','S_EARN','S'] with S = S_NEWS + S_EARN.
    """
    # empty safe defaults
    cols = ["date", "ticker", "S"]
    dn = d_news if isinstance(d_news, pd.DataFrame) else pd.DataFrame(columns=cols)
    de = d_earn if isinstance(d_earn, pd.DataFrame) else pd.DataFrame(columns=cols)

    # normalize dtypes
    dn = _ensure_date_dtype(dn, "date")
    de = _ensure_date_dtype(de, "date")

    # rename before merge to avoid overlap issues
    if "S" in dn.columns:
        dn = dn.rename(columns={"S": "S_NEWS"})
    else:
        dn["S_NEWS"] = np.nan
    if "S" in de.columns:
        de = de.rename(columns={"S": "S_EARN"})
    else:
        de["S_EARN"] = np.nan

    base_cols = ["date", "ticker"]
    df = pd.merge(dn[base_cols + ["S_NEWS"]], de[base_cols + ["S_EARN"]], on=base_cols, how="outer")

    # Fill NaNs to 0 for combination; keep explicit per-source columns
    df["S_NEWS"] = pd.to_numeric(df["S_NEWS"], errors="coerce").fillna(0.0)
    df["S_EARN"] = pd.to_numeric(df["S_EARN"], errors="coerce").fillna(0.0)
    df["S"] = df["S_NEWS"] + df["S_EARN"]

    df = _ensure_date_dtype(df, "date")
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------
# Forward returns on prices
# ---------------------------------------------------------------------

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Given prices with columns ['date','ticker','open','close'],
    add:
      - ret_oc_1d : (close/open - 1)
      - ret_cc_1d : next_day_close / today_close - 1 (forward return)
    """
    need = {"date", "ticker", "open", "close"}
    missing = need - set(prices.columns)
    if missing:
        raise KeyError(f"prices must have columns: {', '.join(sorted(need))} (missing: {sorted(missing)})")

    df = prices.copy()
    df = _ensure_date_dtype(df, "date")

    def _by_ticker(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").reset_index(drop=True)
        # open->close same day
        g["ret_oc_1d"] = pd.to_numeric(g["close"], errors="coerce") / pd.to_numeric(g["open"], errors="coerce") - 1.0
        # close->close forward return aligned to today
        next_close = pd.to_numeric(g["close"].shift(-1), errors="coerce")
        today_close = pd.to_numeric(g["close"], errors="coerce")
        g["ret_cc_1d"] = (next_close / today_close) - 1.0
        return g

    out = df.groupby("ticker", as_index=False, group_keys=False).apply(_by_ticker)
    out = _ensure_date_dtype(out, "date")
    return out[["date", "ticker", "open", "close", "ret_oc_1d", "ret_cc_1d"]].reset_index(drop=True)


__all__ = [
    "_ensure_date_dtype",
    "daily_sentiment_from_rows",
    "join_and_fill_daily",
    "add_forward_returns",
]
