 # src/market_sentiment/news.py
from __future__ import annotations
import html, time
from typing import List
import requests
import pandas as pd
from bs4 import BeautifulSoup

_UA = "Mozilla/5.0 (compatible; MarketSentimentBot/0.3; +https://github.com/)"
_SESS = requests.Session()
_SESS.headers.update({"User-Agent": _UA, "Accept": "*/*", "Connection": "keep-alive"})

def _get(url: str, sleep: float = 0.2):
    r = _SESS.get(url, timeout=30); time.sleep(sleep); r.raise_for_status(); return r

def _rss_items(url: str, source: str) -> List[dict]:
    try:
        r = _get(url)
        soup = BeautifulSoup(r.content, "xml")
        out=[]
        for it in soup.find_all(["item","entry"]):
            title = (it.title.text if it.title else "").strip()
            link = ""
            if it.link and it.link.has_attr("href"):
                link = it.link["href"]
            elif it.link and it.link.text:
                link = it.link.text.strip()
            elif it.find("guid"):
                link = it.find("guid").text.strip()
            pub = ""
            for tag in ("pubDate","published","updated"):
                if it.find(tag):
                    pub = it.find(tag).text.strip(); break
            out.append({"title": html.unescape(title), "url": link, "pub": pub, "source": source})
        return out
    except Exception:
        return []

def _read_article_text(url: str) -> str:
    """
    Best-effort article text: fetch and strip HTML. Keeps it dependency-light.
    """
    if not url: return ""
    try:
        r = _get(url, sleep=0.0)
    except Exception:
        return ""
    try:
        soup = BeautifulSoup(r.content, "lxml")
        for t in soup(["script","style","noscript"]): t.extract()
        # Heuristic: prefer <article> if present
        art = soup.find("article")
        text = " ".join((art.get_text(" ") if art else soup.get_text(" ")).split())
        return text[:20000]  # cap
    except Exception:
        return ""

def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Free multi-source news:
      1) Yahoo Finance RSS
      2) Google News RSS
    Returns df columns: ts,title,url,text,source
    """
    t = ticker.upper()
    feeds = [
        (f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US","yahoo"),
        (f"https://news.google.com/rss/search?q={t}+stock&hl=en-US&gl=US&ceid=US:en","google"),
    ]
    items=[]
    for url,src in feeds:
        items.extend(_rss_items(url, src))

    if not items:
        return pd.DataFrame(columns=["ts","title","url","text","source"])

    df = pd.DataFrame(items).drop_duplicates("url")
    df["ts"] = pd.to_datetime(df["pub"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts"])
    # NY time, day-level windowing
    s = pd.to_datetime(start); e = pd.to_datetime(end)
    df["date_et"] = df["ts"].dt.tz_convert("America/New_York").dt.tz_localize(None)
    df = df[(df["date_et"] >= s) & (df["date_et"] <= e)]
    if df.empty:
        return pd.DataFrame(columns=["ts","title","url","text","source"])
    # fetch bodies (best effort)
    texts = []
    for u in df["url"].tolist():
        texts.append(_read_article_text(u))
    df["text"] = texts
    # if body empty, backfill title
    df["text"] = df.apply(lambda r: r["text"] if r["text"] else r["title"], axis=1)
    df = df[["ts","title","url","text","source"]].reset_index(drop=True)
    return df
