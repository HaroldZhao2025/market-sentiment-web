from __future__ import annotations
import pandas as pd

def daily_long_short(panel: pd.DataFrame, q_hi: float=0.9, q_lo: float=0.1) -> pd.DataFrame:
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    def _p(g: pd.DataFrame) -> float:
        g = g.dropna(subset=["x","y"])
        if g.empty: return 0.0
        hi, lo = g["x"].quantile(q_hi), g["x"].quantile(q_lo)
        long, short = g[g["x"]>=hi]["y"], g[g["x"]<=lo]["y"]
        if long.empty and short.empty: return 0.0
        wL = 0.5/max(len(long),1); wS = 0.5/max(len(short),1)
        return float((long*wL).sum() - (short*wS).sum())
    pnl = df.groupby("date", group_keys=False).apply(_p).reset_index()
    pnl.columns = ["date","ret"]
    pnl["cumret"] = (1.0 + pnl["ret"].fillna(0.0)).cumprod() - 1.0
    return pnl
