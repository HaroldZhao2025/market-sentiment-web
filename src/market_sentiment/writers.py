# src/market_sentiment/writers.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    If df has a MultiIndex on columns, flatten it into single-level strings.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            "_".join([str(x) for x in tup if x is not None and str(x) != ""]).strip("_")
            for tup in df.columns.to_list()
        ]
    return df


def _normalize_price_df(p: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure price dataframe has flat columns and canonical names:
      - date (tz-naive), close (float), adj_close (optional)
    """
    p = _flatten_columns(p).copy()

    # common variants (e.g., yfinance may hand back "Adj Close")
    rename = {}
    if "Adj Close" in p.columns and "adj_close" not in p.columns:
        rename["Adj Close"] = "adj_close"
    if "Date" in p.columns and "date" not in p.columns:
        rename["Date"] = "date"
    if "Close" in p.columns and "close" not in p.columns:
        rename["Close"] = "close"
    if rename:
        p = p.rename(columns=rename)

    # last resort lowercase map
    lower_map = {c.lower(): c for c in p.columns}
    if "date" not in p.columns and "date" in lower_map:
        p = p.rename(columns={lower_map["date"]: "date"})
    if "close" not in p.columns and "close" in lower_map:
        p = p.rename(columns={lower_map["close"]: "close"})

    if "date" in p.columns:
        p["date"] = pd.to_datetime(p["date"], errors="coerce").dt.tz_localize(None)
    if "close" in p.columns:
        p["close"] = pd.to_numeric(p["close"], errors="coerce")

    return p


def _normalize_daily_df(d: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure daily signal dataframe has flat columns and canonical names:
      - date (tz-naive), S (float)
    """
    if d is None:
        return pd.DataFrame(columns=["date", "S"])
    d = _flatten_columns(d).copy()

    # allow variants (S_news/S_earn already combined upstream to S)
    if "S" not in d.columns:
        # prefer a direct "S" if present in different case
        cand = [c for c in d.columns if c.lower() == "s"]
        if cand:
            d = d.rename(columns={cand[0]: "S"})
        elif "S_news" in d.columns or "S_earn" in d.columns:
            d["S"] = d.get("S_news", 0.0).fillna(0.0) + 2.0 * d.get("S_earn", 0.0).fillna(0.0)
        else:
            d["S"] = 0.0

    # ensure 'date'
    if "date" not in d.columns:
        cand = [c for c in d.columns if c.lower() == "date"]
        if cand:
            d = d.rename(columns={cand[0]: "date"})
        else:
            d["date"] = pd.NaT

    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.tz_localize(None)
    d["S"] = pd.to_numeric(d["S"], errors="coerce").fillna(0.0)

    return d[["date", "S"]]


def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,
    daily: pd.DataFrame,
    top_news: pd.DataFrame
) -> Dict[str, Any]:
    """
    Returns:
      {
        "ticker": <str>,
        "series": [{"date": "YYYY-MM-DD", "close": <float>, "S": <float>}, ...],
        "news":   [{"ts": <iso8601>, "title": <str>, "url": <str>, "s": <float>, "source": <str>}, ...]
      }
    """

    # Filter to this ticker if a ticker column exists; otherwise assume 'prices'
    # is already per-ticker.
    if prices is None or prices.empty:
        return {"ticker": ticker, "series": [], "news": []}

    if "ticker" in prices.columns:
        p = prices.loc[prices["ticker"] == ticker].copy()
    else:
        p = prices.copy()

    p = _normalize_price_df(p)

    # If daily provided, filter by ticker too.
    if daily is not None and not daily.empty:
        if "ticker" in daily.columns:
            d = daily.loc[daily["ticker"] == ticker].copy()
        else:
            d = daily.copy()
    else:
        d = pd.DataFrame(columns=["date", "S"])

    d = _normalize_daily_df(d)

    # Merge on clean (flat) columns
    left = p[["date", "close"]].copy()
    right = d[["date", "S"]].copy()
    ser = left.merge(right, on="date", how="left").sort_values("date")
    ser["S"] = ser["S"].fillna(0.0)

    series = [
        {"date": row["date"].strftime("%Y-%m-%d"), "close": float(row["close"]), "S": float(row["S"])}
        for _, row in ser.iterrows()
        if pd.notna(row["date"]) and pd.notna(row["close"])
    ]

    # Prepare news list
    news_rows = []
    if top_news is not None and not top_news.empty:
        tn = top_news.copy()
        if "ticker" in tn.columns:
            tn = tn.loc[tn["ticker"] == ticker]
        if not tn.empty:
            tn = tn.sort_values("ts", ascending=False).head(20)
            for _, r in tn.iterrows():
                ts = pd.to_datetime(r.get("ts"), errors="coerce", utc=True)
                s_val = float(pd.to_numeric(r.get("s", 0.0), errors="coerce").fillna(0.0)) if hasattr(r.get("s", 0.0), "fillna") else float(pd.to_numeric(r.get("s", 0.0), errors="coerce"))
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
