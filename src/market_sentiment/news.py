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


def _get_json(url: str, params: dict) -> List[dict]:
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json() or []
    except Exception:
        pass
    return []


def news_fmp(ticker: str, start: str, end: str, api_key: str | None) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame(columns=["ticker","ts","source","title","url"])
    url_news = "https://financialmodelingprep.com/api/v3/stock_news"
    news = _get_json(url_news, {"tickers": ticker, "from": start, "to": end, "limit": 250, "apikey": api_key})
    rows = []
    for it in news:
        title = it.get("title"); url = it.get("url")
        src = (it.get("site") or it.get("source") or "FMP").strip()
        dt = it.get("publishedDate")
        if not title or not url or not dt:
            continue
        rows.append((ticker, pd.to_datetime(dt, utc=True), src, title, url))
    # press releases
    url_pr = f"https://financialmodelingprep.com/api/v3/press-releases/{ticker}"
    prs = _get_json(url_pr, {"from": start, "to": end, "page": 0, "apikey": api_key})
    for it in prs:
        title = it.get("title"); url = it.get("link") or it.get("url")
        dt = it.get("date")
        if not title or not url or not dt:
            continue
        rows.append((ticker, pd.to_datetime(dt, utc=True), "Press Release", title, url))
    return pd.DataFrame(rows, columns=["ticker","ts","source","title","url"])


def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    api_key = os.getenv("FMP_API_KEY", "")
    a = news_fmp(ticker, start, end, api_key)
    b = news_yfinance(ticker)

    # combine
    df = pd.concat([a, b], ignore_index=True) if not a.empty or not b.empty else pd.DataFrame(
        columns=["ticker","ts","source","title","url"]
    )
    if df.empty:
        return df

    df["ts"] = _to_utc(df["ts"])
    df = _filter_range(df, start, end)

    if df.empty:
        return df

    # dedupe by (rounded minute + title)
    x = df.copy()
    x["ts_min"] = x["ts"].dt.floor("min")
    x = x.sort_values("ts", ascending=False).drop_duplicates(["title","ts_min"]).drop(columns=["ts_min"])
    return x.reset_index(drop=True)
