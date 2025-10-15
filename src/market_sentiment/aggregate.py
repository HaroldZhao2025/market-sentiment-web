# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd
import numpy as np

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["ticker","date"]).copy()
    # close-to-close forward return
    df["ret_cc_1d"] = df.groupby("ticker")["close"].pct_change().shift(-1)
    return df

def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    rows: columns ['ts','title','url','text','s','conf','ticker']
    kind: 'news' or 'earn'
    output: ['date','ticker', f'S_{kind}', f'{kind}_count']
    """
    if rows is None or rows.empty:
        return pd.DataFrame(columns=["date","ticker",f"S_{kind}",f"{kind}_count"])
    d = rows.copy()
    d["date"] = pd.to_datetime(d["ts"]).dt.tz_convert("America/New_York").dt.tz_localize(None)
    g = d.groupby(["ticker", d["date"].dt.normalize()], as_index=False).apply(
        lambda g: pd.Series({
            f"S_{kind}": float((g["s"] * g["conf"]).sum() / max(g["conf"].sum(), 1e-9)),
            f"{kind}_count": int(len(g))
        })
    ).reset_index().rename(columns={"level_1":"date"})
    g["date"] = pd.to_datetime(g["date"]).dt.normalize()
    return g[["date","ticker",f"S_{kind}",f"{kind}_count"]]

def combine_news_earn(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    # full outer; fill NaNs -> 0; create S = weighted combo
    df = pd.merge(d_news, d_earn, on=["date","ticker"], how="outer")
    for col, val in [("S_news",0.0),("S_earn",0.0),("news_count",0),("earn_count",0)]:
        if col in df: df[col] = df[col].fillna(val)
        else: df[col] = val
    # simple weight: earnings gets 2x weight on the day it occurs
    df["S"] = df["S_news"] + 2.0*df["S_earn"]
    df = df.sort_values(["ticker","date"]).reset_index(drop=True)
    return df
