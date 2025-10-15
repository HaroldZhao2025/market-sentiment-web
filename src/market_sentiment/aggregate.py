# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd
import numpy as np


# ---------- helpers for price normalization ----------

def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate-named columns keeping the first occurrence."""
    if df is None or df.empty:
        return df
    return df.loc[:, ~df.columns.duplicated()].copy()


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a tz-naive 'date' column exists.
    - If index is DatetimeIndex, reset_index -> 'date'
    - Else rename common variants.
    """
    df = df.copy()
    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "date"})
        else:
            for cand in ("Date", "datetime", "DATETIME", "DATE"):
                if cand in df.columns:
                    df = df.rename(columns={cand: "date"})
                    break
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    else:
        df["date"] = pd.NaT
    return df


def _ensure_ticker_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a 'ticker' column exists (fallback to single placeholder if missing)."""
    df = df.copy()
    if "ticker" not in df.columns:
        # Try common alternatives
        for cand in ("symbol", "Symbol", "SYMBOL"):
            if cand in df.columns:
                df = df.rename(columns={cand: "ticker"})
                break
        if "ticker" not in df.columns:
            df["ticker"] = "TICKER"  # fallback; upstream should pass per-ticker frames anyway
    df["ticker"] = df["ticker"].astype(str)
    return df


def _ensure_close_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee a numeric 'close' column from common alternatives.
    Order of preference: 'close', 'Close', 'adj_close', 'Adj Close', 'price', 'Price', first numeric col.
    """
    df = df.copy()
    # normalize some names
    rename = {}
    if "Adj Close" in df.columns and "adj_close" not in df.columns:
        rename["Adj Close"] = "adj_close"
    if "Close" in df.columns and "close" not in df.columns:
        rename["Close"] = "close"
    if "Price" in df.columns and "price" not in df.columns:
        rename["Price"] = "price"
    if rename:
        df = df.rename(columns=rename)

    if "close" not in df.columns:
        for cand in ("close", "adj_close", "price", "Adj Close", "Close", "Price"):
            if cand in df.columns:
                df["close"] = df[cand]
                break

    if "close" not in df.columns:
        # last resort: first numeric column
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        df["close"] = df[num_cols[0]] if num_cols else np.nan

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


# ---------- public API ----------

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward 1D close-to-close return per ticker.
    Output column: 'ret_cc_1d'
    Robust to:
      - duplicate/ambiguous columns
      - date in index vs column
      - missing/alternate close column names
    """
    if df is None or df.empty:
        return df

    out = _dedupe_columns(df)
    out = _ensure_date_column(out)
    out = _ensure_ticker_column(out)
    out = _ensure_close_column(out)

    out = out.sort_values(["ticker", "date"]).copy()

    # Explicit behavior to avoid pandas FutureWarning on pct_change
    ret = (
        out.groupby("ticker", group_keys=False)["close"]
           .apply(lambda s: s.pct_change(fill_method=None).shift(-1))
    )
    # ret is a Series aligned with out's index; safe to assign
    out["ret_cc_1d"] = ret

    return out


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    Aggregate row-level FinBERT scores to daily per-ticker signals.

    Input rows must contain: ['ts','ticker','s','conf']  (ts is tz-aware UTC)
    Output: ['date','ticker', f'S_{kind}', f'{kind}_count']
    """
    cols_needed = {"ts", "ticker", "s", "conf"}
    if rows is None or rows.empty or not cols_needed.issubset(set(rows.columns)):
        return pd.DataFrame(columns=["date", "ticker", f"S_{kind}", f"{kind}_count"])

    d = rows.copy()
    d["date"] = (
        pd.to_datetime(d["ts"])
        .dt.tz_convert("America/New_York")
        .dt.normalize()
        .dt.tz_localize(None)
    )
    d["w"] = pd.to_numeric(d["s"], errors="coerce") * pd.to_numeric(d["conf"], errors="coerce")

    agg = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(w_sum=("w", "sum"),
              conf_sum=("conf", "sum"),
              count=("s", "size"))
    )

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
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=["date", "ticker", "S_news", "news_count"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=["date", "ticker", "S_earn", "earn_count"])

    df = pd.merge(d_news, d_earn, on=["date", "ticker"], how="outer")

    # fill and types
    for col, val in [("S_news", 0.0), ("S_earn", 0.0), ("news_count", 0), ("earn_count", 0)]:
        if col not in df.columns:
            df[col] = val
        else:
            if isinstance(val, float):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(val).astype(float)
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(val).astype(int)

    # composite signal: earnings has higher weight (tune later)
    df["S"] = df["S_news"].astype(float) + 2.0 * df["S_earn"].astype(float)

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df
