# src/market_sentiment/writers.py
from __future__ import annotations
import math
import pandas as pd


def _series_for_chart(prices: pd.DataFrame, daily: pd.DataFrame) -> dict:
    """
    prices: date,ticker,close
    daily : date,ticker,S,(news_count,earn_count)
    """
    if prices.empty:
        return {"date": [], "price": [], "S": [], "ma7": []}

    p = prices.sort_values("date")[["date","close"]].copy()
    d = daily.sort_values("date")[["date","S"]].copy() if not daily.empty else pd.DataFrame(columns=["date","S"])

    ser = p.merge(d, on="date", how="left")
    ser["S"] = ser["S"].fillna(0.0).astype(float)
    ser["ma7"] = ser["S"].rolling(7, min_periods=1).mean()

    return {
        "date": ser["date"].astype(str).tolist(),
        "price": [float(x) if x is not None and not pd.isna(x) else None for x in ser["close"]],
        "S": ser["S"].astype(float).tolist(),
        "ma7": ser["ma7"].astype(float).tolist(),
    }


def _top_news(rows: pd.DataFrame, k: int = 10) -> list[dict]:
    if rows.empty:
        return []
    r = rows.sort_values("ts", ascending=False).head(k)
    out = []
    for _, row in r.iterrows():
        out.append({
            "ts": pd.to_datetime(row["ts"], utc=True).isoformat().replace("+00:00","Z"),
            "title": str(row.get("title") or ""),
            "url": str(row.get("url") or ""),
            "S": float(row.get("S") or 0.0),
        })
    return out


def build_ticker_json(ticker: str, prices: pd.DataFrame, daily: pd.DataFrame, top_rows: pd.DataFrame) -> dict:
    """
    Returns a JSON-able dict for a single ticker page.
    """
    p = prices[prices["ticker"] == ticker][["date","ticker","close"]].copy()
    d = daily[daily["ticker"] == ticker][["date","ticker","S","S_news","S_earn","news_count","earn_count"]].copy() if not daily.empty \
        else pd.DataFrame(columns=["date","ticker","S","S_news","S_earn","news_count","earn_count"])

    series = _series_for_chart(p, d)

    meta = {}
    if not d.empty:
        meta = {
            "mean_S": float(d["S"].mean()),
            "points": int((d["S"] != 0).sum()),
            "news_days": int((d["news_count"] > 0).sum()),
            "earn_days": int((d["earn_count"] > 0).sum()),
        }

    return {
        "symbol": ticker,
        "series": series,
        "meta": meta,
        "top_news": _top_news(top_rows[top_rows["ticker"] == ticker] if not top_rows.empty else pd.DataFrame()),
    }
