# src/market_sentiment/portfolio.py
from __future__ import annotations
import pandas as pd, numpy as np

def daily_long_short(panel: pd.DataFrame, long_q: float = 0.9, short_q: float = 0.1,
                     score_col: str = "S_ew", ret_col: str = "ret_cc_1d") -> dict:
    """
    panel columns: ['date','ticker', score_col, ret_col]
    Build a simple daily long-short PnL: +1/N on top quantile, -1/N on bottom quantile.
    Returns { 'equity': [{'date':..., 'equity':...}, ...], 'meta': {...} }
    """
    df = panel.copy()
    df = df.dropna(subset=[score_col, ret_col])
    if df.empty:
        return {"equity": [], "meta": {"long_q": long_q, "short_q": short_q}}

    def _weights(g: pd.DataFrame) -> pd.DataFrame:
        q_hi = g[score_col].quantile(long_q)
        q_lo = g[score_col].quantile(short_q)
        long = g[g[score_col] >= q_hi].copy()
        short = g[g[score_col] <= q_lo].copy()
        if long.empty and short.empty:
            g["w"] = 0.0
            return g[["ticker","w", ret_col]]
        if not long.empty:
            long["w"] =  1.0 / max(len(long), 1)
        if not short.empty:
            short["w"] = -1.0 / max(len(short), 1)
        out = pd.concat([long, short], ignore_index=True)
        return out[["ticker","w", ret_col]]

    bt = df.groupby("date", group_keys=False).apply(_weights)
    bt = bt.reset_index().rename(columns={ret_col: "y"})
    # daily pnl = sum_i (w_i * y_i); make sure groups exclude 'date' col per pandas future change
    pnl = bt.groupby("date", as_index=False).apply(lambda x: float((x["w"] * x["y"]).sum()))
    pnl = pnl.rename(columns={0: "ret"})
    pnl = pnl.sort_values("date")
    pnl["equity"] = (1.0 + pnl["ret"].fillna(0.0)).cumprod()
    eq = [{"date": d.strftime("%Y-%m-%d"), "equity": float(e)} for d, e in zip(pnl["date"], pnl["equity"])]
    return {"equity": eq, "meta": {"long_q": long_q, "short_q": short_q, "score_col": score_col}}
