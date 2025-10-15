# src/market_sentiment/news.py
from __future__ import annotations
import pandas as pd, requests, yfinance as yf
from bs4 import BeautifulSoup

def _from_yf_news(ticker: str) -> pd.DataFrame:
    tk = yf.Ticker(ticker)
    items = getattr(tk, "news", []) or []
    rows = []
    for it in items:
        # providerPublishTime is unix seconds (sometimes missing)
        ts = pd.to_datetime(it.get("providerPublishTime"), unit="s", utc=True, errors="coerce")
        title = it.get("title") or ""
        url = it.get("link") or ""
        # summary is not always present; fall back to title (we'll still get a non-zero sentiment)
        text = (it.get("summary") or "").strip()
        if not text:
            text = title
        if pd.notna(ts) and title:
            rows.append((ts, title, url, text))
    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    return df

def _from_yahoo_rss(ticker: str) -> pd.DataFrame:
    # Free RSS fallback
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
    except Exception:
        return pd.DataFrame(columns=["ts","title","url","text"])
    soup = BeautifulSoup(r.content, "xml")  # RSS XML
    rows = []
    for item in soup.find_all("item"):
        title = (item.title.text if item.title else "").strip()
        link = (item.link.text if item.link else "").strip()
        pub  = (item.pubDate.text if item.pubDate else "").strip()
        ts = pd.to_datetime(pub, utc=True, errors="coerce")
        text = title
        if pd.notna(ts) and title:
            rows.append((ts, title, link, text))
    return pd.DataFrame(rows, columns=["ts","title","url","text"])

def news_yfinance(ticker: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """
    Primary: yfinance headlines; Fallback: Yahoo RSS.
    Output: ['ts'(UTC), 'title','url','text'].
    """
    t = ticker.upper()
    df1 = _from_yf_news(t)
    df2 = _from_yahoo_rss(t) if df1.empty else pd.DataFrame(columns=df1.columns)
    df = pd.concat([df1, df2], ignore_index=True) if not df1.empty else df2
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).drop_duplicates(subset=["title","url"]).sort_values("ts", ascending=False)
    if start:
        df = df[df["ts"] >= pd.to_datetime(start, utc=True)]
    if end:
        df = df[df["ts"] <= pd.to_datetime(end, utc=True)]
    # keep last ~200 to cap memory
    return df.head(200).reset_index(drop=True)
