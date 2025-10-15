# src/market_sentiment/aggregate.py
from __future__ import annotations

import numpy as np
import pandas as pd


def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward one-day returns:
      ret_cc_1d: close-to-close forward return
      ret_oc_1d: open-to-close (same day) shifted to line up with next-day label
    """
    if prices.empty:
        return prices.assign(ret_cc_1d=pd.Series(dtype=float), ret_oc_1d=pd.Series(dtype=float))

    out = prices.sort_values(["ticker","date"]).reset_index(drop=True).copy()

    # use transform to get a Series aligned with original index
    out["ret_cc_1d"] = (
        out.groupby("ticker")["close"].transform(lambda s: s.pct_change(fill_method=None)).shift(-1)
    )

    out["ret_oc_1d"] = (
        out.groupby("ticker").apply(
            lambda g: (g["close"] / g["open"] - 1).shift(-1)
        ).reset_index(level=0, drop=True)
    )

    return out


def _effective_date(ts: pd.Series, cutoff_minutes: int = 5) -> pd.Series:
    """
    For each timestamp (UTC), assign an effective trading date in America/New_York:
      - News after (16:00 - cutoff_minutes) goes to T+1
    Returns tz-naive date (YYYY-MM-DD at midnight).
    """
    if ts.empty:
        return ts

    s = pd.to_datetime(ts, utc=True, errors="coerce")
    local = s.dt.tz_convert("America/New_York")
    close_time = local.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_time - pd.Timedelta(minutes=cutoff_minutes)
    eff = pd.Series(np.where(local > threshold, local + pd.Timedelta(days=1), local), index=local.index)
    eff = pd.to_datetime(eff).dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return eff


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    Aggregate scored rows (news or earnings) to daily level.
    Expects rows to already include columns: ticker, ts (UTC), S (float).
    Returns columns: date, ticker, S_<kind>, <kind>_count
    """
    need = {"ticker", "ts", "S"}
    if rows.empty:
        return pd.DataFrame(columns=["date","ticker", f"S_{kind}", f"{kind}_count"])
    if not need.issubset(set(rows.columns)):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")
    d = d.dropna(subset=["ts"])
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    g = (
        d.groupby(["ticker","date"], as_index=False)
         .agg(S=( "S", "mean"), count=("S","size"))
         .rename(columns={"S": f"S_{kind}", "count": f"{kind}_count"})
         .sort_values(["ticker","date"])
         .reset_index(drop=True)
    )
    return g


def combine_daily(news_daily: pd.DataFrame, earn_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Combine the two daily aggregates and compute S = S_news + S_earn (simple sum),
    filling any missing with zeros.
    """
    cols = ["date","ticker"]
    df = pd.merge(news_daily, earn_daily, on=cols, how="outer")

    for c, dflt in [
        ("S_news", 0.0),
        ("S_earn", 0.0),
        ("news_count", 0),
        ("earn_count", 0),
    ]:
        if c not in df.columns:
            df[c] = dflt
        df[c] = df[c].fillna(dflt)

    df["S"] = df["S_news"].astype(float) + df["S_earn"].astype(float)
    df = df.sort_values(["ticker","date"]).reset_index(drop=True)
    return df
