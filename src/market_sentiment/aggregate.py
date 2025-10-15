# src/market_sentiment/aggregate.py
from __future__ import annotations
import numpy as np
import pandas as pd

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns: ['date','ticker','open','high','low','close','volume']
    Returns the same + ['ret_cc_1d'] (close->close next day)
    """
    out = df.sort_values(["ticker","date"]).copy()
    # ensure single-level columns
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ['_'.join([c for c in col if c]) for col in out.columns.values]
    out["ret_cc_1d"] = (
        out.groupby("ticker")["close"]
           .pct_change()
           .shift(-1)
           .astype(float)
    )
    return out

def _effective_date(ts: pd.Series, cutoff_minutes: int) -> pd.Series:
    # localize/convert to NY time if tz-naive
    s = pd.to_datetime(ts, utc=True, errors="coerce")
    s = s.dt.tz_convert("America/New_York")
    close = s.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close - pd.Timedelta(minutes=cutoff_minutes)
    eff = np.where(s > threshold, (s + pd.Timedelta(days=1)), s)
    eff = pd.to_datetime(eff).tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return eff

def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    rows: ['ticker','ts','score'] (+ optional columns)
    Returns daily by ticker with columns:
      ['date','ticker','S','S_news','S_earn','news_count','earn_count']
    The 'S' is combined (sum of components present).
    """
    if rows is None or rows.empty:
        return pd.DataFrame(columns=["date","ticker","S","S_news","S_earn","news_count","earn_count"])

    d = rows.copy()
    if "ticker" not in d.columns: raise ValueError("rows must include 'ticker'")
    if "ts" not in d.columns: raise ValueError("rows must include 'ts'")
    if "score" not in d.columns: raise ValueError("rows must include 'score'")

    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)
    g = (
        d.groupby(["ticker","date"], as_index=False)
         .agg(S=( "score","mean"),
              count=("score","size"))
    )
    g["S_news"] = 0.0
    g["S_earn"] = 0.0
    g["news_count"] = 0
    g["earn_count"] = 0

    if kind == "news":
        g["S_news"] = g["S"]
        g["news_count"] = g["count"]
    elif kind == "earn":
        g["S_earn"] = g["S"]
        g["earn_count"] = g["count"]
    else:
        raise ValueError("kind must be 'news' or 'earn'")

    g["S"] = g["S_news"] + g["S_earn"]
    g = g.drop(columns=["count"])

    # final schema
    g = g[["date","ticker","S","S_news","S_earn","news_count","earn_count"]].copy()
    return g
