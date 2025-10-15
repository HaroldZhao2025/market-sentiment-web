from __future__ import annotations
import numpy as np
import pandas as pd

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add forward 1d close-to-close return per ticker as 'ret_cc_1d'.
    Requires columns: ['date','ticker','close']
    """
    if df.empty:
        return df.assign(ret_cc_1d=pd.Series(dtype=float))
    out = df.sort_values(["ticker","date"]).copy()
    # Make sure 'close' is a simple Series, not a DataFrame
    out["ret_cc_1d"] = (
        out.groupby("ticker", group_keys=False)["close"]
           .apply(lambda s: s.pct_change().shift(-1))
           .astype(float)
    )
    return out

def _effective_date(ts_series: pd.Series, cutoff_minutes: int = 5) -> pd.Series:
    ts = pd.to_datetime(ts_series, utc=True).copy()
    est = ts.dt.tz_convert("America/New_York")
    close = est.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close - pd.Timedelta(minutes=cutoff_minutes)
    eff = np.where(est > threshold, (est + pd.Timedelta(days=1)).dt.normalize(), est.dt.normalize())
    # return tz-naive normalized date
    return pd.to_datetime(eff).tz_localize(None)

def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    Aggregate per (ticker, date) with time-cutoff roll.
    Expects columns: ['ticker','ts','S'] (+ optional weights).
    Returns ['date','ticker','S','news_count'/'earn_count', 'S_news'/'S_earn']
    """
    need = {"ticker","ts","S"}
    if rows.empty:
        if kind == "news":
            return pd.DataFrame(columns=["date","ticker","S","S_news","news_count"])
        else:
            return pd.DataFrame(columns=["date","ticker","S","S_earn","earn_count"])
    if not need.issubset(rows.columns):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows.copy()
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    g = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(S=("S","mean"), count=("S","size"))
    )
    if kind == "news":
        g = g.rename(columns={"count":"news_count"}).assign(S_news=g["S"])
        return g[["date","ticker","S","S_news","news_count"]]
    else:
        g = g.rename(columns={"count":"earn_count"}).assign(S_earn=g["S"])
        return g[["date","ticker","S","S_earn","earn_count"]]

def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    cols = ["date","ticker"]
    df = pd.merge(d_news, d_earn, on=cols, how="outer", suffixes=("",""))
    for c in ["S_news","news_count","S_earn","earn_count","S"]:
        if c in df.columns:
            df[c] = df[c].fillna(0)
    # total S as sum of news + earnings components
    df["S"] = df.get("S_news", 0.0) + df.get("S_earn", 0.0)
    return df.sort_values(cols).reset_index(drop=True)
