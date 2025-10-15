# src/market_sentiment/portfolio.py
from __future__ import annotations
import pandas as pd
import numpy as np

def daily_long_short(panel: pd.DataFrame, long_q: float = 0.9, short_q: float = 0.1) -> pd.DataFrame:
    """
    panel columns: ['date','ticker','S','ret_cc_1d']
    """
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    # per-day quantiles on S
    q = df.groupby("date")["S"].quantile([short_q, long_q]).unstack()
    q = q.rename(columns={short_q:"q_short", long_q:"q_long"}).reset_index()
    df = df.merge(q, on="date", how="left")
    df["side"] = np.where(df["S"] >= df["q_long"], 1.0, np.where(df["S"] <= df["q_short"], -1.0, 0.0))
    # equal-weight within long and short each day
    w = df.groupby(["date","side"])["ticker"].transform(lambda s: 1.0 / max(len(s), 1.0))
    df["w"] = np.where(df["side"]==0.0, 0.0, w)
    df["y"] = df["ret_cc_1d"].fillna(0.0)
    pnl = df.groupby("date", as_index=False).apply(lambda g: float((g["w"]*g["side"]*g["y"]).sum()))
    pnl = pnl.rename(columns={None:"ret"}).reset_index(drop=True)
    pnl["cum"] = (1.0 + pnl["ret"]).cumprod()
    return pnl
