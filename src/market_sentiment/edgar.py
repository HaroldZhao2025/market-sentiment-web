from __future__ import annotations
import os, time
import requests
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

UA = os.getenv("SEC_UA", "market-sentiment-web/0.1 (+https://github.com/HaroldZhao2025/market-sentiment-web)")

def _get(url: str, params: dict | None = None, sleep: float = 0.2):
    headers = {"User-Agent": UA}
    r = requests.get(url, params=params or {}, headers=headers, timeout=30)
    time.sleep(sleep)
    r.raise_for_status()
    return r

def _atom_entries_for_ticker(ticker: str) -> list[dict]:
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany&output=atom"
    try:
        r = _get(url)
    except Exception:
        return []
    soup = BeautifulSoup(r.content, features="xml")
    return soup.find_all("entry") or []

def fetch_earnings_docs(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Pulls recent filings via Atom feed; attempts to fetch document HTML for text.
    Returns columns: ['ticker','ts','title','url','text'].
    """
    entries = _atom_entries_for_ticker(ticker)
    if not entries:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])

    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    rows = []
    for ent in entries:
        try:
            updated = ent.find("updated").text
            ts = pd.to_datetime(updated, utc=True)
            if not (s <= ts <= e):
                continue
            title = (ent.find("title").text or "").strip()
            link = ent.find("link")
            url = link.get("href") if link else ""
            # Best effort download human-readable text
            text = ""
            if url:
                try:
                    r = _get(url)
                    soup = BeautifulSoup(r.content, "lxml")
                    for t in soup(["script","style","noscript"]): t.extract()
                    text = " ".join(soup.get_text(separator=" ").split())
                except Exception:
                    text = ""
            rows.append((ticker, ts, title, url, text))
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ticker","ts","title","url","text"])
    # Keep probable earnings related
    mask = df["title"].str.contains(r"(earnings|8-K|10-Q|10-K|transcript|prepared|remarks|ex\.?99)", case=False, na=False)
    df = df[mask].drop_duplicates(["title","url"])
    return df.reset_index(drop=True)
