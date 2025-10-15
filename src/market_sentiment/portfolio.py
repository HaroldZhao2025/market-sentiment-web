from __future__ import annotations
import numpy as np
import pandas as pd

def daily_long_short(panel: pd.DataFrame, q_hi: float = 0.9, q_lo: float = 0.1, fee_bps: float = 1.0) -> pd.DataFrame:
    """
    panel columns: ['date','ticker','score','y']  (score -> signal; y -> next-day return)
    Long top decile, short bottom decile. Equal weight within sides.
    fee_bps: per-trade bps transaction cost (round-turn approximated via weight turnover).
    Returns: ['date','ret','cost','ret_net','equity']
    """
    if panel.empty:
        return pd.DataFrame(columns=["date","ret","cost","ret_net","equity"])

    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.dropna(subset=["score","y"])

    # compute daily quantiles
    qhi = df.groupby("date")["score"].transform(lambda s: s.quantile(q_hi))
    qlo = df.groupby("date")["score"].transform(lambda s: s.quantile(q_lo))

    longs  = df["score"] >= qhi
    shorts = df["score"] <= qlo

    # counts per side
    n_long  = longs.groupby(df["date"]).transform("sum").replace(0, np.nan)
    n_short = shorts.groupby(df["date"]).transform("sum").replace(0, np.nan)

    # raw weights (+1 / n_long, -1 / n_short)
    w = np.where(longs,  1.0 / n_long, 0.0) + np.where(shorts, -1.0 / n_short, 0.0)
    w = np.nan_to_num(w, nan=0.0)
    df["w"] = w

    # daily gross return
    ret = (df["w"] * df["y"]).groupby(df["date"]).sum().rename("ret").reset_index()

    # turnover cost (sum abs(dw) across tickers)
    df = df.sort_values(["ticker","date"])
    df["w_prev"] = df.groupby("ticker")["w"].shift(1).fillna(0.0)
    df["turn"] = (df["w"] - df["w_prev"]).abs()
    # per-date turnover = sum abs(dw)
    turn = df.groupby("date")["turn"].sum().rename("turn").reset_index()
    fee = fee_bps / 10000.0
    cost = turn.assign(cost=lambda d: d["turn"] * fee)[["date","cost"]]

    pnl = ret.merge(cost, on="date", how="left").fillna({"cost": 0.0})
    pnl["ret_net"] = pnl["ret"] - pnl["cost"]
    pnl = pnl.sort_values("date").reset_index(drop=True)
    pnl["equity"] = (1.0 + pnl["ret_net"]).cumprod()
    return pnl
