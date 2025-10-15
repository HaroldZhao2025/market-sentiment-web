# src/market_sentiment/writers.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import numpy as np


# ---------- helpers ----------

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
      - Otherwise, look for common variants ('Date', 'datetime') and coerce
    """
    df = df.copy()
    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "date"})
        else:
            # common variants
            for cand in ("Date", "datetime", "DATETIME", "DATE"):
                if cand in df.columns:
                    df = df.rename(columns={cand: "date"})
                    break

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    else:
        # create an empty date col to avoid KeyErrors later; it will be dropped
        df["date"] = pd.NaT
    return df


def _ensure_close_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee a 'close' column from common alternatives:
      - adj_close / Adj Close
      - Close
      - price / Price
      - else: first numeric column
    """
    df = df.copy()

    # first, normalize some usual names
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
        for cand in ("adj_close", "close", "price", "Adj Close", "Close"):
            if cand in df.columns:
                df["close"] = df[cand]
                break

    if "close" not in df.columns:
        # last resort: pick the first numeric column
        num_cols: List[str] = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            df["close"] = df[num_cols[0]]
        else:
            # if nothing numeric, create a NaN close to keep schema stable
            df["close"] = np.nan

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


def _normalize_price_df(p: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure price dataframe has:
      - flat columns
      - a tz-naive 'date' column
      - a numeric 'close' column
    """
    p = _flatten_columns(p)
    p = _materialize_date_column(p)
    p = _ensure_close_column(p)
    # keep only what we need for the merge/series JSON; preserve extra cols if you like
    return p


def _normalize_daily_df(d: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure daily signal dataframe has:
      - flat columns
      - tz-naive 'date' column
      - numeric 'S' column
    """
    if d is None:
        return pd.DataFrame(columns=["date", "S"])
    d = _flatten_columns(d).copy()

    # normalize date
    d = _materialize_date_column(d)

    # normalize S (allow variants)
    if "S" not in d.columns:
        # 's', 'S_news', 'S_earn' — build composite if needed
        lc = {c.lower(): c for c in d.columns}
        if "s" in lc:
            d = d.rename(columns={lc["s"]: "S"})
        elif "s_news" in lc or "s_earn" in lc:
            d["S"] = d.get("S_news", 0.0).fillna(0.0) + 2.0 * d.get("S_earn", 0.0).fillna(0.0)
        else:
            d["S"] = 0.0

    d["S"] = pd.to_numeric(d["S"], errors="coerce").fillna(0.0)
    return d[["date", "S"]]


# ---------- writer ----------

def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,
    daily: pd.DataFrame,
    top_news: pd.DataFrame
) -> Dict[str, Any]:
    """
    Returns an object for one ticker:
      {
        "ticker": <str>,
        "series": [{"date": "YYYY-MM-DD", "close": <float>, "S": <float>}, ...],
        "news":   [{"ts": <iso8601>, "title": <str>, "url": <str>, "s": <float>, "source": <str>}, ...]
      }
    """

    # --- prices ---
    if prices is None or prices.empty:
        return {"ticker": ticker, "series": [], "news": []}

    if "ticker" in prices.columns:
        p = prices.loc[prices["ticker"] == ticker].copy()
    else:
        p = prices.copy()

    p = _normalize_price_df(p)

    # --- daily sentiment ---
    if daily is not None and not daily.empty:
        if "ticker" in daily.columns:
            d = daily.loc[daily["ticker"] == ticker].copy()
        else:
            d = daily.copy()
    else:
        d = pd.DataFrame(columns=["date", "S"])

    d = _normalize_daily_df(d)

    # --- merge ---
    left = p[["date", "close"]].copy()
    right = d[["date", "S"]].copy()
    ser = left.merge(right, on="date", how="left").sort_values("date")
    ser["S"] = ser["S"].fillna(0.0)

    series = []
    for _, row in ser.iterrows():
        dt = row.get("date")
        px = row.get("close")
        if pd.isna(dt) or pd.isna(px):
            continue
        series.append({
            "date": pd.to_datetime(dt).strftime("%Y-%m-%d"),
            "close": float(px),
            "S": float(row.get("S", 0.0))
        })

    # --- news ---
    news_rows = []
    if top_news is not None and not top_news.empty:
        tn = top_news.copy()
        if "ticker" in tn.columns:
            tn = tn.loc[tn["ticker"] == ticker]
        if not tn.empty:
            tn = tn.sort_values("ts", ascending=False).head(20)
            for _, r in tn.iterrows():
                ts = pd.to_datetime(r.get("ts"), errors="coerce", utc=True)
                s_val = r.get("s", 0.0)
                try:
                    s_val = float(pd.to_numeric(s_val, errors="coerce"))
                except Exception:
                    s_val = 0.0
                news_rows.append({
                    "ts": ts.isoformat() if pd.notna(ts) else str(r.get("ts", "")),
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "s": s_val,
                    "source": r.get("source", "")
                })

    return {"ticker": ticker, "series": series, "news": news_rows}


def write_json(out_dir: Path, name: str, obj: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"{name}.json").open("w") as f:
        json.dump(obj, f, separators=(",", ":"))


def write_tickers(out_dir: Path, tickers: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "_tickers.json").open("w") as f:
        json.dump(sorted(tickers), f)
