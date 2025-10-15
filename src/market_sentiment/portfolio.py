from __future__ import annotations
import pandas as pd
import numpy as np

def daily_long_short(panel: pd.DataFrame, q_hi: float = 0.9, q_lo: float = 0.1) -> pd.DataFrame:
    """
    panel: columns ['date','ticker','x','y'] where x = signal (S), y = next-day return.
    Returns a daily PnL with equal weight long top decile and short bottom decile.
    """
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    def _day_pnl(g: pd.DataFrame) -> float:
        g = g.dropna(subset=["x", "y"])
        if g.empty:
            return 0.0
        hi = g["x"].quantile(q_hi)
        lo = g["x"].quantile(q_lo)
        long = g[g["x"] >= hi]["y"]
        short = g[g["x"] <= lo]["y"]
        if long.empty and short.empty:
            return 0.0
        w_long = 0.5 / max(len(long), 1)
        w_short = 0.5 / max(len(short), 1)
        ret = (long * w_long).sum() - (short * w_short).sum()
        return float(ret)

    # Compute daily returns
    pnl_series = df.groupby("date", group_keys=False).apply(_day_pnl)
    pnl = pnl_series.reset_index()
    pnl.columns = ["date", "ret"]
    pnl["cumret"] = (1.0 + pnl["ret"].fillna(0.0)).cumprod() - 1.0
    return pnl
