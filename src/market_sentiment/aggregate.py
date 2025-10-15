# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd
import numpy as np

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward 1D close-to-close return per ticker.
    Output column: ret_cc_1d
    """
    if df is None or df.empty:
        return df
    out = df.sort_values(["ticker", "date"]).copy()
    out["ret_cc_1d"] = out.groupby("ticker")["close"].pct_change().shift(-1)
    return out


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    Aggregate row-level FinBERT scores to daily per-ticker signals.

    Input rows must contain:
      ['ts','ticker','s','conf']  (ts is tz-aware UTC)

    Output:
      ['date','ticker', f'S_{kind}', f'{kind}_count']

    Implementation notes:
    - Create a *real* 'date' column first (ET midnight, tz-naive) to avoid
      MultiIndex weirdness and future pandas behavior changes.
    - Use named aggregations (no .apply) for stability/performance.
    - Weighted score = sum(s*conf) / max(sum(conf), 1e-9).
    """
    cols_needed = {"ts", "ticker", "s", "conf"}
    if rows is None or rows.empty or not cols_needed.issubset(set(rows.columns)):
        return pd.DataFrame(columns=["date", "ticker", f"S_{kind}", f"{kind}_count"])

    d = rows.copy()
    # build a proper naive ET date column
    d["date"] = (
        pd.to_datetime(d["ts"])
        .dt.tz_convert("America/New_York")
        .dt.normalize()
        .dt.tz_localize(None)
    )

    # precompute weight
    d["w"] = d["s"].astype(float) * d["conf"].astype(float)

    agg = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(w_sum=("w", "sum"),
              conf_sum=("conf", "sum"),
              count=("s", "size"))
    )

    # stable weighted score
    eps = 1e-9
    agg[f"S_{kind}"] = (agg["w_sum"] / (agg["conf_sum"].abs() + eps)).astype(float)
    agg[f"{kind}_count"] = agg["count"].astype(int)

    out = agg[["date", "ticker", f"S_{kind}", f"{kind}_count"]].sort_values(["ticker", "date"])
    return out.reset_index(drop=True)


def combine_news_earn(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Outer-join daily news & earnings aggregates and produce a composite signal S.
    Missing values are filled with zeros.
    """
    # Ensure expected columns exist
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=["date", "ticker", "S_news", "news_count"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=["date", "ticker", "S_earn", "earn_count"])

    df = pd.merge(d_news, d_earn, on=["date", "ticker"], how="outer")

    for col, val in [("S_news", 0.0), ("S_earn", 0.0), ("news_count", 0), ("earn_count", 0)]:
        if col not in df.columns:
            df[col] = val
        else:
            # avoid future downcast warnings
            if isinstance(val, float):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(val).astype(float)
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(val).astype(int)

    # Simple composite: give earnings 2x weight on the day (tune later)
    df["S"] = df["S_news"].astype(float) + 2.0 * df["S_earn"].astype(float)

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df
