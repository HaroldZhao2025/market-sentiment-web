# src/market_sentiment/writers.py
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd

from .aggregate import _pick_close_column

def _ensure_lists(x):
    return list(map(lambda v: None if pd.isna(v) else v, x))

def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,
    daily: pd.DataFrame,
    top_news: pd.DataFrame | None = None,
    ma_window: int = 7
) -> dict:
    """
    Compose a JSON-serializable payload for a ticker page.
    Expects:
      - prices: ['date','ticker',<close>,'ret_cc_1d'?]
      - daily : ['date','ticker','S', ...]
      - top_news: optional DataFrame ['ts','title','url','S'] already scored.
    """
    p = prices.copy()
    d = daily.copy()

    p["date"] = pd.to_datetime(p["date"]).dt.normalize()
    d["date"] = pd.to_datetime(d["date"]).dt.normalize()

    close_col = _pick_close_column(p)
    left = p[["date", close_col]].rename(columns={close_col: "close"})
    merged = left.merge(d[["date","S"]], on="date", how="left").sort_values("date")

    merged["S"] = merged["S"].fillna(0.0).astype(float)
    merged["close"] = merged["close"].astype(float)

    s_ma = merged["S"].rolling(ma_window, min_periods=1).mean()
    series = {
        "date": _ensure_lists(merged["date"].dt.strftime("%Y-%m-%d")),
        "price": _ensure_lists(merged["close"].round(4)),
        "sentiment": _ensure_lists(merged["S"].round(4)),
        "sentiment_ma7": _ensure_lists(s_ma.round(4)),
        "label": "FinBERT sentiment",
    }

    news_list = []
    if top_news is not None and len(top_news) > 0:
        tn = top_news.copy()
        # ensure columns exist
        for col in ("ts","title","url","S"):
            if col not in tn.columns:
                tn[col] = None
        tn = tn.sort_values("ts", ascending=False).head(25)
        for _, r in tn.iterrows():
            news_list.append({
                "ts": pd.to_datetime(r["ts"]).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(r["ts"]) else None,
                "title": r.get("title"),
                "url": r.get("url"),
                "S": None if pd.isna(r.get("S")) else float(r.get("S")),
            })

    return {
        "ticker": ticker,
        "series": series,
        "top_news": news_list,
    }

def write_json(obj: dict, outdir: Path, filename: str):
    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
