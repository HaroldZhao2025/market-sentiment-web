from __future__ import annotations
import numpy as np
import pandas as pd


def _to_eastern_day(s: pd.Series) -> pd.Series:
    """Coerce to tz-aware America/New_York and normalize to day."""
    dt = pd.to_datetime(s, errors="coerce")
    try:
        has_tz = dt.dt.tz is not None
    except Exception:
        has_tz = False
    if not has_tz:
        dt = dt.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")
    else:
        dt = dt.dt.tz_convert("America/New_York")
    return dt.dt.normalize()


def daily_long_short(
    panel: pd.DataFrame,
    q_hi: float = 0.90,
    q_lo: float = 0.10,
    costs_bps: float = 0.0,
) -> pd.DataFrame:
    """
    Simple daily long/short backtest.

    Parameters
    ----------
    panel : DataFrame with columns ['date','ticker','score','y']
        date  : datetime-like; will be coerced to Eastern day
        score : signal used for ranking each day
        y     : next-day close-to-close return (already aligned to 'date')
    q_hi, q_lo : quantile thresholds for long / short
    costs_bps  : (optional) one-way trading cost in basis points, applied on turnover

    Returns
    -------
    DataFrame with columns ['date','ret','ret_net','equity'].
    """
    if panel.empty:
        return pd.DataFrame(columns=["date", "ret", "ret_net", "equity"])

    df = panel.copy()
    # Normalize dates to day-level
    df["date"] = _to_eastern_day(df["date"])

    # Keep only required columns and drop rows with missing essentials
    df = df[["date", "ticker", "score", "y"]].dropna(subset=["date", "ticker", "score", "y"])

    # Rank into long/short buckets per day
    def _weights_for_day(g: pd.DataFrame) -> pd.DataFrame:
        sc = g["score"].astype(float)
        hi = np.nanquantile(sc, q_hi)
        lo = np.nanquantile(sc, q_lo)
        long_mask = sc >= hi
        short_mask = sc <= lo

        w = np.zeros(len(g), dtype=float)
        n_long = int(long_mask.sum())
        n_short = int(short_mask.sum())

        # dollar-neutral: 50% long, 50% short
        if n_long > 0:
            w[long_mask.to_numpy()] = 0.5 / n_long
        if n_short > 0:
            w[short_mask.to_numpy()] = -0.5 / n_short

        out = g.copy()
        out["w"] = w
        return out[["date", "ticker", "w", "y"]]

    try:
        bt = df.groupby("date").apply(_weights_for_day, include_groups=False)
    except TypeError:  # pandas < 2.2 fallback
        bt = df.groupby("date").apply(_weights_for_day)
    bt = bt.reset_index(drop=True)

    # Daily gross return
    bt["gross"] = bt["w"] * bt["y"]
    pnl = (
        bt.groupby("date", as_index=False)["gross"]
        .sum()
        .rename(columns={"gross": "ret"})
        .sort_values("date")
        .reset_index(drop=True)
    )

    # (Optional) simple turnover-based costs
    if costs_bps and costs_bps > 0:
        # estimate turnover = sum |w_t - w_{t-1}| over all tickers (requires wide view)
        wid = bt.pivot_table(index="date", columns="ticker", values="w", fill_value=0.0).sort_index()
        dw = wid.diff().abs().sum(axis=1)  # total weight moved each day
        # cost per day (two-sided bps on moved notional)
        cost = (costs_bps / 10000.0) * dw
        cost = cost.reindex(pnl["date"]).fillna(0.0).to_numpy()
    else:
        cost = np.zeros(len(pnl), dtype=float)

    pnl["ret_net"] = pnl["ret"] - cost
    pnl["equity"] = (1.0 + pnl["ret_net"]).cumprod()

    return pnl[["date", "ret", "ret_net", "equity"]]
