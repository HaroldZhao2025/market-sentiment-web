from __future__ import annotations
import requests, time
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf

def _fetch_text(url: str, timeout: int = 20) -> str:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "lxml")
        for t in soup(["script","style","noscript"]): t.extract()
        text = " ".join(soup.get_text(" ").split())
        return text[:4000]
    except Exception:
        return ""

def news_yfinance(ticker: str, start: str, end: str) -> pd.DataFrame:
    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        items = []
    rows = []
    for it in items:
        ts = pd.to_datetime(it.get("providerPublishTime", None), unit="s", utc=True, errors="coerce")
        if pd.isna(ts): continue
        if not (pd.to_datetime(start) <= ts <= pd.to_datetime(end)): continue
        title = it.get("title") or ""
        url = it.get("link") or ""
        rows.append((ts, title, url))
    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text","ticker"])
    df = pd.DataFrame(rows, columns=["ts","title","url"])
    # try to fetch body text but fallback to title
    texts = []
    for u in df["url"].tolist():
        txt = _fetch_text(u)
        texts.append(txt if txt else "")
        time.sleep(0.1)
    df["text"] = texts
    return df

def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    df = news_yfinance(ticker, start, end)
    if df.empty:
        return pd.DataFrame(columns=["ts","title","url","text","ticker"])
    df["ticker"] = ticker
    return df[["ts","title","url","text","ticker"]]
