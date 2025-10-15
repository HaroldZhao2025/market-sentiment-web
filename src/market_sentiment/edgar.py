# src/market_sentiment/edgar.py
from __future__ import annotations
import time, re
from typing import List, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup

_UA = "Mozilla/5.0 (compatible; MarketSentimentBot/0.3; +https://github.com/)"
_SESS = requests.Session()
_SESS.headers.update({
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.sec.gov/",
})

# earnings/transcript-like file name heuristics
_INDEX_RE = re.compile(r"(ex99|press|earnings|prepared|remarks|transcript)", re.I)

def _get(url: str, sleep: float = 0.25) -> requests.Response:
    for i in range(3):
        r = _SESS.get(url, timeout=30)
        if r.status_code in (429, 503):
            time.sleep(0.8 * (i + 1)); continue
        r.raise_for_status()
        if sleep: time.sleep(sleep)
        return r
    r.raise_for_status()  # last one

def _atom_entries_for_ticker(ticker: str) -> List[dict]:
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany&output=atom"
    r = _get(url)
    soup = BeautifulSoup(r.content, "xml")  # Atom XML
    out = []
    for e in soup.find_all("entry"):
        form = (e.find("category")["term"] if e.find("category") and e.find("category").has_attr("term") else "").strip()
        updated = (e.updated.text if e.updated else "").strip()
        filing_href = (e.find("link", {"rel": "alternate"})["href"] if e.find("link", {"rel": "alternate"}) else "").strip()
        title = (e.title.text if e.title else "").strip()
        out.append({"form": form, "updated": updated, "href": filing_href, "title": title})
    return out

def _doc_links_from_filing_index(index_url: str) -> List[Tuple[str, str]]:
    r = _get(index_url)
    soup = BeautifulSoup(r.content, "lxml")
    links = []
    for a in soup.select("a[href*='/Archives/edgar/data/']"):
        href = a.get("href", ""); name = (a.text or "").strip()
        if not href: continue
        if not href.startswith("http"): href = "https://www.sec.gov" + href
        links.append((name, href))
    seen=set(); uniq=[]
    for name, href in links:
        if href in seen: continue
        seen.add(href); uniq.append((name, href))
    return uniq

def _read_html_or_text(url: str) -> str:
    try:
        r = _get(url, sleep=0.0)
    except Exception:
        return ""
    content = r.content
    try:
        soup = BeautifulSoup(content, "lxml")
        for t in soup(["script","style","noscript"]): t.extract()
        text = " ".join(soup.get_text(separator=" ").split())
        if text and len(text) > 80: return text
    except Exception:
        pass
    try:
        return " ".join(content.decode(errors="ignore").split())
    except Exception:
        return ""

def fetch_earnings_docs(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Free-only, email-less EDGAR via Atom feed.
    Returns df(ts,title,url,text). On any 403/SEC block, returns empty df (do not raise).
    """
    try:
        entries = _atom_entries_for_ticker(ticker)
    except Exception:
        return pd.DataFrame(columns=["ts","title","url","text"])

    if not entries:
        return pd.DataFrame(columns=["ts","title","url","text"])

    s = pd.to_datetime(start); e = pd.to_datetime(end)
    rows = []
    for ent in entries:
        form = (ent.get("form") or "").strip().upper()
        if form not in {"8-K","10-Q","10-K"}: continue
        ts = pd.to_datetime(ent.get("updated") or "", utc=True, errors="coerce")
        if pd.isna(ts): continue
        d_naive = ts.tz_convert("America/New_York").tz_localize(None)
        if d_naive < s or d_naive > e: continue

        index_url = ent.get("href") or ""
        if not index_url: continue

        try:
            docs = _doc_links_from_filing_index(index_url)
        except Exception:
            continue

        picked = []
        for name, href in docs:
            nlow = name.lower()
            if not (nlow.endswith((".htm",".html",".txt"))): continue
            if _INDEX_RE.search(nlow): picked.append((name, href))
        if not picked:
            for name, href in docs:
                if name.lower().endswith((".htm",".html",".txt")):
                    picked.append((name, href)); break

        for name, href in picked[:4]:
            text = _read_html_or_text(href)
            if not text: continue
            rows.append((ts, f"{form} {name}", href, text))

    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df = df.sort_values("ts", ascending=False).drop_duplicates(["title"]).reset_index(drop=True)
    return df
