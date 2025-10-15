from __future__ import annotations
from typing import List
import os
import requests
import pandas as pd
import yfinance as yf

def _to_utc(s):
    return pd.to_datetime(s, errors="coerce", utc=True)

def _filter_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty: return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1)
    return df[(df["ts"] >= s) & (df["ts"] < e)].reset_index(drop=True)

def news_yfinance(ticker: str) -> pd.DataFrame:
    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        items = []
    rows = []
    for it in items:
        ts = it.get("providerPublishTime")
        title = it.get("title")
        url = it.get("link")
        src = (it.get("publisher") or it.get("provider") or "").strip()
        if ts is None or not title or not url:
            continue
        rows.append((ticker, pd.to_datetime(int(ts), unit="s", utc=True), src, title, url))
    return pd.DataFrame(rows, columns=["ticker","ts","source","title","url"])

def news_newsapi(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Optional: free dev key from https://newsapi.org/ (subject to their ToS).
    Set NEWS_API_KEY secret to enable. If not present, returns empty DataFrame.
    """
    api_key = os.getenv("NEWS_API_KEY", "")
    if not api_key:
        return pd.DataFrame(columns=["ticker","ts","source","title","url"])
    url = "https://newsapi.org/v2/everything"
    q = f'"{ticker}" OR {ticker}'
    params = {
        "q": q,
        "from": start,
        "to": end,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "apiKey": api_key,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json() if r.status_code == 200 else {}
        arts = data.get("articles", [])
    except Exception:
        arts = []
    rows = []
    for a in arts:
        title = a.get("title"); url = a.get("url"); dt = a.get("publishedAt")
        src = a.get("source", {}).get("name") or "NewsAPI"
        if not title or not url or not dt:
            continue
        rows.append((ticker, pd.to_datetime(dt, utc=True), src, title, url))
    return pd.DataFrame(rows, columns=["ticker","ts","source","title","url"])

def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    a = news_yfinance(ticker)
    b = news_newsapi(ticker, start, end)
    df = pd.concat([a, b], ignore_index=True) if not a.empty or not b.empty else pd.DataFrame(
        columns=["ticker","ts","source","title","url"]
    )
    if df.empty:
        return df
    df["ts"] = _to_utc(df["ts"])
    df = _filter_range(df, start, end)
    if df.empty:
        return df
    # dedupe by rounded minute + title
    x = df.copy()
    x["ts_min"] = x["ts"].dt.floor("min")
    x = x.sort_values("ts", ascending=False).drop_duplicates(["title","ts_min"]).drop(columns=["ts_min"])
    return x.reset_index(drop=True)
