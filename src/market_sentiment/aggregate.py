# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd
import numpy as np


# ----------------- helpers -----------------

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns into single strings if needed."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            "_".join([str(x) for x in tup if x is not None and str(x) != ""]).strip("_")
            for tup in df.columns.to_list()
        ]
    return df


def _materialize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a tz-naive 'date' column exists:
      - If index is DatetimeIndex, move it to a column named 'date'
      - Else try common variants; coerce to datetime and drop tz
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


def _resolve_close_series(df: pd.DataFrame) -> pd.Series:
    """
    Return a SINGLE numeric Series for 'close', robust to:
      - duplicate 'close' columns (returns first)
      - alternative names: 'adj_close', 'Adj Close', 'price', 'Price', 'Close'
      - fallback to first numeric column if none found
    """
    # try direct 'close' (may be Series or DataFrame if duplicate names)
    if "close" in df.columns:
        obj = df["close"]
        if isinstance(obj, pd.DataFrame):  # duplicates; pick first
            s = pd.to_numeric(obj.iloc[:, 0], errors="coerce")
            return s
        return pd.to_numeric(obj, errors="coerce")

    # normalize common names
    candidates = []
    for name in ("adj_close", "Adj Close", "Close", "price", "Price"):
        if name in df.columns:
            candidates.append(name)
    if candidates:
        return pd.to_numeric(df[candidates[0]], errors="coerce")

    # fallback: first numeric column
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if num_cols:
        return pd.to_numeric(df[num_cols[0]], errors="coerce")

    # last resort: empty numeric series with correct index
    return pd.Series(np.nan, index=df.index, dtype="float64")


# ----------------- public API -----------------

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds forward 1D returns per ticker.

    Output columns:
      - ret_cc_1d : next-day close-to-close return
      - ret_oc_1d : next-day (close/open - 1) using same-day open & close
                    (computed if 'open' is present)

    Safe against duplicate/ambiguous 'close' columns and pandas 2.x changes.
    """
    if df is None or df.empty:
        return df

    out = _flatten_columns(df).copy()
    out = _materialize_date_column(out)

    if "ticker" not in out.columns:
        # We need ticker to compute per-ticker returns
        # If upstream forgot to include, bail gracefully
        out["ret_cc_1d"] = np.nan
        if "open" in out.columns:
            out["ret_oc_1d"] = np.nan
        return out

    # Resolve a single 'close' series and attach as a temporary column
    s_close = _resolve_close_series(out)
    out["_px_close_"] = s_close

    # Sort for proper pct_change/shift alignment
    out = out.sort_values(["ticker", "date"])

    # Forward close-to-close return; avoid implicit ffill to silence FutureWarning
    grp_close = out.groupby("ticker", sort=False)["_px_close_"]
    out["ret_cc_1d"] = grp_close.pct_change(fill_method=None).shift(-1)

    # Optional: next-day (close/open - 1)
    if "open" in out.columns:
        # Normalize 'open' to numeric safely
        out["_px_open_"] = pd.to_numeric(out["open"], errors="coerce")
        roc = (out["_px_close_"] / out["_px_open_"] - 1.0)
        out["ret_oc_1d"] = roc.groupby(out["ticker"], sort=False).shift(-1)

    # Clean up temp columns
    out = out.drop(columns=[c for c in ["_px_close_", "_px_open_"] if c in out.columns])

    return out


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    Aggregate row-level FinBERT scores to daily per-ticker signals.

    Input rows must contain: ['ts','ticker','s','conf']  (ts is tz-aware UTC)
    Output columns: ['date','ticker', f'S_{kind}', f'{kind}_count']
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

    d["w"] = d["s"].astype(float) * d["conf"].astype(float)
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
    Missing values are filled with zeros. Earnings gets 2x weight (tunable).
    """
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=["date", "ticker", "S_news", "news_count"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=["date", "ticker", "S_earn", "earn_count"])

    df = pd.merge(d_news, d_earn, on=["date", "ticker"], how="outer")

    # Fill with correct dtypes
    def _num(series, default):
        return pd.to_numeric(series, errors="coerce").fillna(default)

    for col, default in [("S_news", 0.0), ("S_earn", 0.0)]:
        df[col] = _num(df.get(col, default), default).astype(float)
    for col, default in [("news_count", 0), ("earn_count", 0)]:
        df[col] = _num(df.get(col, default), default).astype(int)

    df["S"] = df["S_news"] + 2.0 * df["S_earn"]
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)
