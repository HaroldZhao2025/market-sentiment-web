# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd, numpy as np

NY = "America/New_York"

def effective_trading_date(ts: pd.Series, cutoff_minutes_before_close: int = 30) -> pd.Series:
    z = pd.to_datetime(ts, utc=True).dt.tz_convert(NY)
    close_time = z.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_time - pd.Timedelta(minutes=cutoff_minutes_before_close)
    eff = np.where(z > threshold, (z + pd.Timedelta(days=1)), z)
    return pd.to_datetime(eff).tz_localize(None).normalize()

def aggregate_daily(news_df: pd.DataFrame | None, earn_df: pd.DataFrame | None, cutoff_minutes_before_close: int = 30) -> pd.DataFrame:
    frames = []
    if news_df is not None and not news_df.empty:
        n = news_df.copy()
        n["date"] = effective_trading_date(n["ts"], cutoff_minutes_before_close)
        grp = n.groupby(["date","ticker"], as_index=False).agg(S_news=("S","sum"), news_count=("S","size"))
        frames.append(grp)
    if earn_df is not None and not earn_df.empty:
        e = earn_df.copy()
        e["date"] = effective_trading_date(e["ts"], cutoff_minutes_before_close=0)
        grp = e.groupby(["date","ticker"], as_index=False).agg(S_earn=("S","sum"), earn_count=("S","size"))
        frames.append(grp)
    if not frames:
        return pd.DataFrame(columns=["date","ticker","S_news","news_count","S_earn","earn_count","S_total","S_ew"])
    daily = frames[0]
    for f in frames[1:]:
        daily = pd.merge(daily, f, on=["date","ticker"], how="outer")
    for col, dflt, typ in [("S_news",0.0,float),("news_count",0,int),("S_earn",0.0,float),("earn_count",0,int)]:
        if col not in daily: daily[col] = dflt
        daily[col] = daily[col].fillna(dflt).astype(typ, copy=False)
    daily["S_total"] = daily["S_news"].astype(float) + daily["S_earn"].astype(float)
    daily["S_ew"] = np.where((daily["news_count"]>0)&(daily["earn_count"]>0),
                             0.5*daily["S_news"].astype(float) + 0.5*daily["S_earn"].astype(float),
                             daily["S_news"].astype(float) + daily["S_earn"].astype(float))
    return daily.sort_values(["ticker","date"]).reset_index(drop=True)
