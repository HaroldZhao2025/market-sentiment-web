# src/market_sentiment/aggregate.py
from __future__ import annotations
import numpy as np
import pandas as pd

# --------- date helpers ---------

def _to_series_utc(ts_like) -> pd.Series:
    """
    Return a pandas Series[datetime64[ns, UTC]] no matter if input is Series, Index, list, or array.
    Keeps the original index when possible.
    """
    if isinstance(ts_like, pd.Series):
        s = ts_like.copy()
    elif isinstance(ts_like, pd.DatetimeIndex):
        s = pd.Series(ts_like, index=ts_like.index)
    else:
        s = pd.Series(ts_like)
    # If tz-naive, assume UTC; if tz-aware, convert to UTC
    s = pd.to_datetime(s, errors="coerce", utc=True)
    return s

def _effective_date(ts_like, cutoff_minutes: int = 5) -> pd.Series:
    """
    Convert timestamps to an *effective trading date*:
    - Convert to America/New_York.
    - If timestamp is after (16:00 - cutoff), roll it to next day.
    - Return timezone-naive dates (YYYY-MM-DD) as a Series aligned to input.
    """
    s_utc = _to_series_utc(ts_like)
    s_ny = s_utc.dt.tz_convert("America/New_York")

    close_time = s_ny.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_time - pd.Timedelta(minutes=int(cutoff_minutes))

    rolled = pd.Series(
        np.where(s_ny > threshold, s_ny + pd.Timedelta(days=1), s_ny),
        index=s_ny.index,
    )
    # normalize to date, drop tz (naive)
    date = pd.to_datetime(rolled).dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return date

# --------- returns & panels ---------

def _pick_close_column(df: pd.DataFrame) -> str:
    for c in ["close", "adj_close", "Adj Close", "Close"]:
        if c in df.columns:
            return c
    raise KeyError("No close/adj_close column found in prices DataFrame.")

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns: ['date','ticker', <close column>]
    Adds ret_cc_1d: forward close-to-close return (t -> t+1).
    Returns a *flat* DataFrame (no MultiIndex) suitable for merges.
    """
    if "date" not in prices.columns:
        raise KeyError("prices must have a 'date' column.")
    if "ticker" not in prices.columns:
        raise KeyError("prices must have a 'ticker' column.")

    out = prices.copy()
    # Normalize date to naive daily
    d = pd.to_datetime(out["date"], errors="coerce", utc=True)
    # if already naive, this converts treating as UTC; ok for daily use
    d = d.dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    out["date"] = d

    close_col = _pick_close_column(out)
    # Ensure it's numeric
    out[close_col] = pd.to_numeric(out[close_col], errors="coerce")

    # Compute forward return
    # Note: pct_change default fill_method='ffill' will change in future pandas;
    # we neutralize by filling missing values *before* pct_change, then shifting.
    def _group_forward(g: pd.DataFrame) -> pd.Series:
        cc = g[close_col].astype(float)
        # safe pct change
        r = cc.pct_change(fill_method=None).shift(-1)
        return r

    out["ret_cc_1d"] = (
        out.sort_values(["ticker", "date"])
           .groupby("ticker", group_keys=False)
           .apply(_group_forward)
           .reset_index(drop=True)
    )

    return out

# --------- daily sentiment aggregation ---------

def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    kind: str = "news",
    cutoff_minutes: int = 5
) -> pd.DataFrame:
    """
    Input rows must contain: ['ticker','ts','S'] where S in [-1,1].
    Returns daily aggregation:
      - for kind='news': columns ['date','ticker','S_news','news_count']
      - for kind='earn': columns ['date','ticker','S_earn','earn_count']
    """
    if rows is None or len(rows) == 0:
        if kind == "news":
            return pd.DataFrame(columns=["date","ticker","S_news","news_count"])
        return pd.DataFrame(columns=["date","ticker","S_earn","earn_count"])

    d = rows.copy()
    if "ticker" not in d.columns or "ts" not in d.columns or "S" not in d.columns:
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    agg = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(S_mean=("S", "mean"), count=("S", "size"))
         .sort_values(["ticker","date"])
    )

    if kind == "news":
        agg = agg.rename(columns={"S_mean":"S_news", "count":"news_count"})
    else:
        agg = agg.rename(columns={"S_mean":"S_earn", "count":"earn_count"})
    return agg

def combine_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Combine news & earnings daily aggregates into a single panel with:
      ['date','ticker','S','S_news','S_earn','news_count','earn_count']
    where S is a weighted average of available components (weights = counts).
    """
    if d_news is None or len(d_news) == 0:
        d_news = pd.DataFrame(columns=["date","ticker","S_news","news_count"])
    if d_earn is None or len(d_earn) == 0:
        d_earn = pd.DataFrame(columns=["date","ticker","S_earn","earn_count"])

    # Ensure proper types
    for col in ["date"]:
        if col in d_news.columns:
            d_news[col] = pd.to_datetime(d_news[col]).dt.normalize()
        if col in d_earn.columns:
            d_earn[col] = pd.to_datetime(d_earn[col]).dt.normalize()

    daily = (
        d_news.merge(d_earn, on=["date","ticker"], how="outer")
              .sort_values(["ticker","date"])
              .reset_index(drop=True)
    )

    daily["S_news"] = pd.to_numeric(daily.get("S_news", 0.0), errors="coerce").fillna(0.0)
    daily["S_earn"] = pd.to_numeric(daily.get("S_earn", 0.0), errors="coerce").fillna(0.0)
    daily["news_count"] = pd.to_numeric(daily.get("news_count", 0), errors="coerce").fillna(0).astype(int)
    daily["earn_count"] = pd.to_numeric(daily.get("earn_count", 0), errors="coerce").fillna(0).astype(int)

    w_news = daily["news_count"].clip(lower=0)
    w_earn = daily["earn_count"].clip(lower=0)
    denom = (w_news + w_earn).replace(0, np.nan)

    daily["S"] = (daily["S_news"] * w_news + daily["S_earn"] * w_earn) / denom
    daily["S"] = daily["S"].fillna(0.0)

    return daily[["date","ticker","S","S_news","S_earn","news_count","earn_count"]]

# --------- safe merges for writer/portfolio ---------

def safe_merge_prices_daily(prices: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """
    Left join prices with daily sentiment on ['date','ticker'].
    Ensures flat index and normalized dates; returns sorted by ['ticker','date'].
    """
    p = prices.copy()
    d = daily.copy()

    p["date"] = pd.to_datetime(p["date"]).dt.normalize()
    d["date"] = pd.to_datetime(d["date"]).dt.normalize()

    out = (
        p.merge(d, on=["date","ticker"], how="left")
         .sort_values(["ticker","date"])
         .reset_index(drop=True)
    )
    return out
