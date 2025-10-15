# src/market_sentiment/aggregate.py
from __future__ import annotations

import numpy as np
import pandas as pd


def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure we have forward returns (close-to-close next day).
    Expected input columns: ticker, date, close (or adjclose),
    returns: adds 'ret_cc_1d'
    """
    out = df.copy()
    # Normalize column names for price
    if "close" not in out.columns:
        # try common alternatives
        for alt in ["Close", "adjclose", "Adj Close", "adj_close"]:
            if alt in out.columns:
                out = out.rename(columns={alt: "close"})
                break
    if "open" not in out.columns:
        for alt in ["Open", "open_price", "open_price_adj"]:
            if alt in out.columns:
                out = out.rename(columns={alt: "open"})
                break

    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    # forward close-to-close next day
    out["ret_cc_1d"] = (
        out.groupby("ticker", group_keys=False)["close"].apply(lambda s: s.pct_change().shift(-1))
    )
    return out


def _effective_date(ts: pd.Series, cutoff_minutes: int) -> pd.Series:
    """
    Map UTC timestamps to the 'effective' U.S. market date.
    News posted within cutoff minutes before 16:00 ET is attributed to T+1.
    """
    s = pd.to_datetime(ts, utc=True, errors="coerce")
    ny = s.dt.tz_convert("America/New_York")
    # 16:00 ET close
    close = ny.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close - pd.Timedelta(minutes=int(cutoff_minutes))
    eff = np.where(ny > threshold, ny + pd.Timedelta(days=1), ny)
    eff = pd.to_datetime(eff).tz_localize(None).normalize()  # naive dates
    return eff


def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    *,
    kind: str,           # "news" or "earn"
    cutoff_minutes: int
) -> pd.DataFrame:
    """
    rows must have: ['ticker','ts','S']   (we ignore title/url/text here)
    Returns: ['date','ticker','S', 'news_count' or 'earn_count']
    """
    required = {"ticker", "ts", "S"}
    missing = required - set(rows.columns)
    if missing:
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows[["ticker", "ts", "S"]].copy()
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    # Aggregate by (date, ticker)
    g = (
        d.groupby(["date", "ticker"], as_index=False)
         .agg(S=("S", "sum"), count=("S", "size"))
         .sort_values(["ticker", "date"])
    )

    # Rename count by kind
    g = g.rename(columns={"count": f"{kind}_count"})
    return g[["date", "ticker", "S", f"{kind}_count"]]
