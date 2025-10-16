# src/market_sentiment/writers.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .aggregate import build_portfolio_timeseries

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _series_for_ticker(prices: pd.DataFrame, daily: pd.DataFrame) -> Dict[str, List]:
    """
    Build the time series for a ticker combining price + daily S.
    Returns a dict with arrays: date, price, S, ma7
    """
    p = prices.copy()
    p["date"] = pd.to_datetime(p["date"]).dt.tz_localize(None)
    p = p[["date", "close"]].rename(columns={"close": "price"})

    d = daily.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.tz_localize(None)
    d = d[["date", "S"]]

    ser = p.merge(d, on="date", how="left").sort_values("date")
    ser["S"] = ser["S"].fillna(0.0).astype(float)
    ser["ma7"] = ser["S"].rolling(7, min_periods=1).mean()

    return {
        "date": ser["date"].dt.strftime("%Y-%m-%d").tolist(),
        "price": [ _safe_float(v) for v in ser["price"].tolist() ],
        "S": [ _safe_float(v) for v in ser["S"].tolist() ],
        "ma7": [ _safe_float(v) for v in ser["ma7"].tolist() ],
    }

def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,
    daily: pd.DataFrame,
    top_news: pd.DataFrame | None = None
) -> Dict:
    """
    A compact JSON per-ticker for the UI.
    """
    ser = _series_for_ticker(prices, daily)

    news_items = []
    if top_news is not None and not top_news.empty:
        # keep a few most recent items
        n = top_news.sort_values("ts", ascending=False).head(10)
        for _, r in n.iterrows():
            news_items.append({
                "ts": pd.to_datetime(r["ts"], utc=True).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title": str(r.get("title", "")),
                "url": str(r.get("url", "")),
                "S": _safe_float(r.get("S", 0.0)),
            })

    return {
        "ticker": ticker,
        "series": ser,
        "news": news_items
    }

def write_outputs(
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    earn_rows: pd.DataFrame,
    out_dir: str
) -> None:
    """
    Writes:
      - _tickers.json                  (list of tickers)
      - ticker/<SYMBOL>.json           (per-ticker timeseries + top news)
      - earnings/<SYMBOL>.json         (raw scored earnings rows)
      - portfolio.json                 (long/short/ls daily series)
    """
    base = Path(out_dir)
    (base / "ticker").mkdir(parents=True, exist_ok=True)
    (base / "earnings").mkdir(parents=True, exist_ok=True)

    # Ticker list
    tickers = sorted(panel["ticker"].unique().tolist())
    (base / "_tickers.json").write_text(json.dumps(tickers), encoding="utf-8")

    # Portfolio series (daily)
    port = build_portfolio_timeseries(panel)
    port_out = {
        "dates": port["date"].dt.strftime("%Y-%m-%d").tolist() if not port.empty else [],
        "long": [ _safe_float(v) for v in (port["long"].tolist() if not port.empty else []) ],
        "short": [ _safe_float(v) for v in (port["short"].tolist() if not port.empty else []) ],
        "long_short": [ _safe_float(v) for v in (port["long_short"].tolist() if not port.empty else []) ],
    }
    (base / "portfolio.json").write_text(json.dumps(port_out), encoding="utf-8")

    # Per-ticker JSONs
    # Prepare helpers keyed by ticker to avoid repeated filtering
    prices_by = {t: g[["date","close","open"]].copy() for t, g in panel.groupby("ticker", sort=False)}
    daily_by  = {t: g[["date","ticker","S"]].copy() for t, g in panel.groupby("ticker", sort=False)}

    # Top news per ticker (optional)
    if news_rows is not None and not news_rows.empty:
        news_by = {t: g[["ts","title","url","S"]].copy() for t, g in news_rows.groupby("ticker", sort=False)}
    else:
        news_by = {}

    for t in tickers:
        p = prices_by.get(t, pd.DataFrame(columns=["date","close","open"]))
        d = daily_by.get(t, pd.DataFrame(columns=["date","ticker","S"]))
        top_news = news_by.get(t, pd.DataFrame(columns=["ts","title","url","S"]))
        obj = build_ticker_json(t, p, d, top_news)
        (base / "ticker" / f"{t}.json").write_text(json.dumps(obj), encoding="utf-8")

    # Raw earnings per ticker (optional file, useful for debugging)
    if earn_rows is not None and not earn_rows.empty:
        for t, g in earn_rows.groupby("ticker", sort=False):
            rows = g.sort_values("ts").to_dict(orient="records")
            (base / "earnings" / f"{t}.json").write_text(json.dumps(rows), encoding="utf-8")
