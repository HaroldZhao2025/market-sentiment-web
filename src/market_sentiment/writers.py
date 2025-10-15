# src/market_sentiment/writers.py
from __future__ import annotations
import json, pandas as pd
from pathlib import Path

def build_ticker_json(ticker: str,
                      prices: pd.DataFrame,
                      daily: pd.DataFrame,
                      news_rows: pd.DataFrame | None,
                      earn_rows: pd.DataFrame | None) -> dict:
    """
    Compose one ticker JSON document consumed by the web app.
    """
    t = ticker.upper()
    p = prices[prices["ticker"] == t].copy()
    d = daily[daily["ticker"] == t].copy()

    if not p.empty:
        p["date"] = pd.to_datetime(p["date"]).dt.tz_localize(None).dt.normalize()
    if not d.empty:
        d["date"] = pd.to_datetime(d["date"]).dt.tz_localize(None).dt.normalize()

    s = pd.merge(
        p[["date","close"]],
        d[["date","S_news","S_earn","S_total","S_ew","news_count","earn_count"]],
        on="date", how="left"
    ).sort_values("date")

    # fill missing with zeros for plotting
    for col in ["S_news","S_earn","S_total","S_ew","news_count","earn_count"]:
        if col not in s: s[col] = 0.0
        s[col] = s[col].fillna(0.0)

    series = [{
        "date": row["date"].strftime("%Y-%m-%d"),
        "close": float(row["close"]),
        "S": float(row["S_ew"]),
        "S_news": float(row["S_news"]),
        "S_earn": float(row["S_earn"]),
        "news_count": int(row["news_count"]) if pd.notna(row["news_count"]) else 0,
        "earn_count": int(row["earn_count"]) if pd.notna(row["earn_count"]) else 0,
    } for _, row in s.iterrows()]

    news_list = []
    if news_rows is not None and not news_rows.empty:
        nr = news_rows[news_rows["ticker"] == t].copy()
        nr = nr.sort_values("ts", ascending=False)
        for _, r in nr.head(50).iterrows():
            news_list.append({
                "ts": pd.to_datetime(r["ts"]).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(r["ts"]) else None,
                "title": r.get("title",""),
                "url": r.get("url",""),
                "S": float(r.get("S", 0.0))
            })

    earn_list = []
    if earn_rows is not None and not earn_rows.empty:
        er = earn_rows[earn_rows["ticker"] == t].copy()
        er = er.sort_values("ts", ascending=False)
        for _, r in er.head(20).iterrows():
            earn_list.append({
                "ts": pd.to_datetime(r["ts"]).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(r["ts"]) else None,
                "title": r.get("title",""),
                "url": r.get("url",""),
                "S": float(r.get("S", 0.0))
            })

    obj = {
        "symbol": t,
        "series": series,
        "news": news_list,
        "earnings": earn_list,
        "meta": {
            "date_start": series[0]["date"] if series else None,
            "date_end": series[-1]["date"] if series else None
        }
    }
    return obj

def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def write_portfolio(path: Path, portfolio_obj: dict) -> None:
    write_json(path, portfolio_obj)
