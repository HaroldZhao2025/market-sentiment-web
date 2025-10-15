# src/market_sentiment/edgar.py
from __future__ import annotations
import time, re, json
from typing import Dict, List
import requests
import pandas as pd
from bs4 import BeautifulSoup

# We DO NOT use any email in headers. Pure API access + generic UA.
_UA = "Mozilla/5.0 (compatible; MarketSentimentBot/0.2; +https://github.com/)"
_SESS = requests.Session()
_SESS.headers.update({
    "User-Agent": _UA,
    "Accept": "*/*",
    "Connection": "keep-alive",
})

_INDEX_RE = re.compile(r"(ex99|press|earnings|prepared|remarks|transcript)", re.I)

def _sec_get(url: str, params: dict | None = None, sleep: float = 0.25):
    """Minimal polite GET with short backoff on 429/503; no email."""
    tries = 3
    for i in range(tries):
        r = _SESS.get(url, params=params or {}, timeout=30)
        if r.status_code in (429, 503) and i < tries - 1:
            time.sleep(0.8 * (i + 1))
            continue
        r.raise_for_status()
        if sleep:
            time.sleep(sleep)
        return r
    # last attempt returned non-OK already raised_for_status
    return r

def _load_ticker_map() -> pd.DataFrame:
    """
    SEC's public mapping of tickers to CIKs.
    Returns DataFrame[ticker, cik].
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    r = _sec_get(url)
    j = r.json()
    # format: {"0": {"ticker":"A", "cik_str":320193, "title":"..."}, ...}
    rows = [(v["ticker"].upper(), int(v["cik_str"])) for v in j.values()]
    return pd.DataFrame(rows, columns=["ticker","cik"])

def _get_cik(ticker: str, cache: Dict[str, int]) -> int | None:
    t = ticker.upper()
    if t in cache:
        return cache[t]
    df = _load_ticker_map()
    m = df[df["ticker"] == t]
    if m.empty:
        return None
    cik = int(m["cik"].iloc[0])
    cache[t] = cik
    return cik

def _recent_filings(cik: int) -> pd.DataFrame:
    """
    Pull the submissions file (JSON) and flatten the 'recent' table.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    r = _sec_get(url)
    j = r.json()
    recent = j.get("filings", {}).get("recent", {})
    acc = recent.get("accessionNumber", [])
    prim = recent.get("primaryDocument", [])
    form = recent.get("form", [])
    filed = recent.get("filingDate", [])
    rows = [(a, p, f, d) for a, p, f, d in zip(acc, prim, form, filed)]
    return pd.DataFrame(rows, columns=["accession","primary","form","filed"])

def _file_list(cik: int, accession: str) -> List[dict]:
    """
    Directory listing JSON for a filing; if not available, return [].
    """
    acc_nodash = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/index.json"
    try:
        r = _sec_get(url, sleep=0.0)
        j = r.json()
        items = j.get("directory", {}).get("item", [])
        if isinstance(items, list):
            return items
    except Exception:
        pass
    return []

def _read_html_text(cik: int, accession: str, filename: str) -> str:
    """
    Retrieve the document and extract plain text. Handles .htm/.html/.txt.
    """
    acc_nodash = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{filename}"
    try:
        r = _sec_get(url, sleep=0.0)
    except Exception:
        return ""
    content = r.content
    # Try HTML first; fall back to decode text
    try:
        soup = BeautifulSoup(content, "lxml")
        for t in soup(["script", "style", "noscript"]):
            t.extract()
        text = " ".join(soup.get_text(separator=" ").split())
        if text and len(text) > 80:
            return text
    except Exception:
        pass
    try:
        text = content.decode(errors="ignore")
        return " ".join(text.split())
    except Exception:
        return ""

def fetch_earnings_docs(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Free-only EDGAR harvest for likely earnings docs:
      - Uses 'submissions' JSON
      - Filters date window
      - Keeps ['8-K','10-Q','10-K'] and scans exhibits / primary doc for earnings-like names
    Output columns: ['ts','title','url','text']
    """
    cache: Dict[str,int] = getattr(fetch_earnings_docs, "_cache", {})
    cik = _get_cik(ticker, cache)
    fetch_earnings_docs._cache = cache
    if not cik:
        return pd.DataFrame(columns=["ts","title","url","text"])

    rec = _recent_filings(cik)
    if rec.empty:
        return pd.DataFrame(columns=["ts","title","url","text"])

    # filter date window
    s = pd.to_datetime(start)
    e = pd.to_datetime(end)
    rec["filed"] = pd.to_datetime(rec["filed"], errors="coerce")
    rec = rec[(rec["filed"] >= s) & (rec["filed"] <= e)]
    # keep forms most likely to contain earnings PR/exhibits
    rec = rec[rec["form"].isin(["8-K","10-Q","10-K"])]
    if rec.empty:
        return pd.DataFrame(columns=["ts","title","url","text"])

    rows = []
    for _, r in rec.iterrows():
        files = _file_list(cik, r["accession"])
        # Always include the primary document as a candidate (especially for 8-K)
        if not files:
            files = [{"name": r["primary"]}]
        for f in files:
            name = f.get("name") or ""
            if not name.lower().endswith((".htm",".html",".txt")):
                continue
            # Heuristic: prefer earnings-like exhibits; still allow primary on 8-K
            if not _INDEX_RE.search(name):
                if r["form"] != "8-K" or name != r["primary"]:
                    continue
            text = _read_html_text(cik, r["accession"], name)
            if not text:
                continue
            ts = pd.to_datetime(r["filed"], utc=True, errors="coerce")
            if pd.isna(ts):
                continue
            acc_nodash = r["accession"].replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{name}"
            title = f"{r['form']} {name}"
            rows.append((ts, title, url, text))

    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    # Deduplicate on title; newest first
    df = df.sort_values("ts", ascending=False).drop_duplicates(["title"]).reset_index(drop=True)
    return df
