from __future__ import annotations
import os, time, re
import requests
import pandas as pd
from bs4 import BeautifulSoup

SEC_UA = os.getenv("SEC_EMAIL", "email@example.com")  # default UA

_INDEX_RE = re.compile(r'(ex99|press|earnings|prepared|remarks|transcript)', re.I)

def _sec_get(url: str, params: dict | None = None, sleep: float = 0.2):
    headers = {"User-Agent": f"MarketSentimentBot ({SEC_UA})"}
    r = requests.get(url, params=params or {}, headers=headers, timeout=30)
    time.sleep(sleep)
    r.raise_for_status()
    return r

def _load_ticker_map() -> pd.DataFrame:
    url = "https://www.sec.gov/files/company_tickers.json"
    r = _sec_get(url)
    j = r.json()
    rows = [(v["ticker"].upper(), int(v["cik_str"])) for v in j.values()]
    return pd.DataFrame(rows, columns=["ticker","cik"])

def _get_cik(ticker: str, cache: dict) -> int | None:
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
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    r = _sec_get(url)
    j = r.json()
    recent = j.get("filings", {}).get("recent", {})
    acc = recent.get("accessionNumber", [])
    prim = recent.get("primaryDocument", [])
    form = recent.get("form", [])
    filed = recent.get("filingDate", [])
    rows = []
    for a,p,f,d in zip(acc,prim,form,filed):
        rows.append((a, p, f, d))
    return pd.DataFrame(rows, columns=["accession","primary","form","filed"])

def _file_list(cik: int, accession: str) -> list[dict]:
    acc_nodash = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/index.json"
    try:
        r = _sec_get(url)
        j = r.json()
        return j.get("directory", {}).get("item", [])
    except Exception:
        return []

def _read_html_text(cik: int, accession: str, filename: str) -> str:
    acc_nodash = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{filename}"
    try:
        r = _sec_get(url)
    except Exception:
        return ""
    try:
        soup = BeautifulSoup(r.content, "lxml")
        for t in soup(["script","style","noscript"]): t.extract()
        text = " ".join(soup.get_text(separator=" ").split())
        return text
    except Exception:
        return ""

def fetch_earnings_docs(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Returns rows: ['ts','title','url','text'] for probable earnings PR/exhibits.
    """
    cik_cache: dict[str,int] = getattr(fetch_earnings_docs, "_cache", {})
    cik = _get_cik(ticker, cik_cache)
    fetch_earnings_docs._cache = cik_cache
    if not cik:
        return pd.DataFrame(columns=["ts","title","url","text"])

    rec = _recent_filings(cik)
    if rec.empty:
        return pd.DataFrame(columns=["ts","title","url","text"])

    s = pd.to_datetime(start)
    e = pd.to_datetime(end)
    rec["filed"] = pd.to_datetime(rec["filed"])
    rec = rec[(rec["filed"] >= s) & (rec["filed"] <= e)]
    rec = rec[rec["form"].isin(["8-K","10-Q","10-K"])]
    if rec.empty:
        return pd.DataFrame(columns=["ts","title","url","text"])

    rows = []
    for _, r in rec.iterrows():
        files = _file_list(cik, r["accession"])
        if not files:
            files = [{"name": r["primary"]}]
        for f in files:
            name = f.get("name") or ""
            if not name.lower().endswith((".htm",".html",".txt")):
                continue
            if not _INDEX_RE.search(name):
                if r["form"] != "8-K" or name != r["primary"]:
                    continue
            acc_nodash = r["accession"].replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{name}"
            text = _read_html_text(cik, r["accession"], name)
            if not text:
                continue
            ts = pd.to_datetime(r["filed"], utc=True)
            title = f"{r['form']} {name}"
            rows.append((ts, title, url, text))

    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df = df.sort_values("ts", ascending=False).drop_duplicates(["title"])
    return df.reset_index(drop=True)
