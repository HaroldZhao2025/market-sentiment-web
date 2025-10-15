# src/market_sentiment/writers.py
from __future__ import annotations
import json
import pandas as pd
from pathlib import Path

def build_ticker_json(ticker: str, prices: pd.DataFrame, daily: pd.DataFrame, top_news: pd.DataFrame) -> dict:
    p = prices[prices["ticker"] == ticker].copy()
    d = daily[daily["ticker"] == ticker].copy() if daily is not None else pd.DataFrame(columns=["date","ticker","S"])
    p = p.sort_values("date")
    d = d.sort_values("date")
    ser = p[["date","close"]].merge(d[["date","S"]], on="date", how="left").sort_values("date")
    ser["S"] = ser["S"].fillna(0.0)
    series = [{"date": str(x["date"]).split(" ")[0], "close": float(x["close"]), "S": float(x["S"])} for _, x in ser.iterrows()]
    news_rows=[]
    if top_news is not None and not top_news.empty:
        tn = top_news[top_news["ticker"] == ticker].copy()
        tn = tn.sort_values("ts", ascending=False).head(20)
        for _, r in tn.iterrows():
            news_rows.append({
                "ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else str(r["ts"]),
                "title": r["title"], "url": r["url"], "s": float(r["s"]), "source": r.get("source","")
            })
    return {"ticker": ticker, "series": series, "news": news_rows}

def write_json(out_dir: Path, name: str, obj: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"{name}.json").open("w") as f:
        json.dump(obj, f, separators=(",",":"))

def write_tickers(out_dir: Path, tickers: list[str]):
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "_tickers.json").open("w") as f:
        json.dump(sorted(tickers), f)
