# src/market_sentiment/edgar.py
from __future__ import annotations
import time, re
from typing import List, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup

# No email anywhere; generic UA only.
_UA = "Mozilla/5.0 (compatible; MarketSentimentBot/0.3; +https://github.com/)"
_SESS = requests.Session()
_SESS.headers.update({
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.sec.gov/",
})

# Heuristics for earnings-like exhibits / transcripts
_INDEX_RE = re.compile(r"(ex99|press|earnings|prepared|remarks|transcript)", re.I)

def _get(url: str, sleep: float = 0.25) -> requests.Response:
    """Polite GET with tiny backoff on 429/503."""
    for i in range(3):
        r = _SESS.get(url, timeout=30)
        if r.status_code in (429, 503):
            time.sleep(0.8 * (i + 1))
            continue
        r.raise_for_status()
        if sleep:
            time.sleep(sleep)
        return r
    r.raise_for_status()  # last one

def _atom_entries_for_ticker(ticker: str) -> List[dict]:
    """
    Use EDGAR Atom feed directly with ticker (no CIK mapping).
    URL example:
      https://www.sec.gov/cgi-bin/browse-edgar?CIK=AAPL&owner=exclude&action=getcompany&output=atom
    """
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
    """
    From the filing index page (…-index.htm), scrape the Documents table.
    Returns list of (name, absolute_url).
    """
    r = _get(index_url)
    soup = BeautifulSoup(r.content, "lxml")
    links = []
    # The index page usually has a table with document links (a[href*='/Archives/edgar/data/'])
    for a in soup.select("a[href*='/Archives/edgar/data/']"):
        href = a.get("href", "")
        name = (a.text or "").strip()
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://www.sec.gov" + href
        links.append((name, href))
    # Deduplicate while preserving order
    seen = set(); uniq = []
    for name, href in links:
        if href in seen: 
            continue
        seen.add(href)
        uniq.append((name, href))
    return uniq

def _read_html_or_text(url: str) -> str:
    """
    Fetch and extract plain text (works for .htm/.html/.txt).
    """
    try:
        r = _get(url, sleep=0.0)
    except Exception:
        return ""
    content = r.content
    # Try HTML parser first; fallback to raw text
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
    Free-only, no-email EDGAR harvesting via Atom feed (ticker-based).
    Returns DataFrame columns: ['ts','title','url','text'].
    Filters forms ∈ {8-K, 10-Q, 10-K} and keeps likely earnings/transcript exhibits.
    """
    entries = _atom_entries_for_ticker(ticker)
    if not entries:
        return pd.DataFrame(columns=["ts","title","url","text"])

    s = pd.to_datetime(start)
    e = pd.to_datetime(end)

    rows = []
    for ent in entries:
        form = (ent.get("form") or "").strip().upper()
        if form not in {"8-K", "10-Q", "10-K"}:
            continue
        ts = pd.to_datetime(ent.get("updated") or "", utc=True, errors="coerce")
        if pd.isna(ts) or (ts.tz_localize(None) < s) or (ts.tz_localize(None) > e):
            continue

        index_url = ent.get("href") or ""
        if not index_url:
            continue

        # Scrape the filing index page to find documents
        try:
            docs = _doc_links_from_filing_index(index_url)
        except Exception:
            continue

        # Prefer earnings-like exhibits; if none match, allow primary HTML/TXT doc
        picked = []
        for name, href in docs:
            nlow = name.lower()
            if not (nlow.endswith(".htm") or nlow.endswith(".html") or nlow.endswith(".txt")):
                continue
            if _INDEX_RE.search(nlow):
                picked.append((name, href))

        # If nothing matched the heuristic, try the first HTML/TXT doc (often the 8-K body)
        if not picked:
            for name, href in docs:
                nlow = name.lower()
                if nlow.endswith(".htm") or nlow.endswith(".html") or nlow.endswith(".txt"):
                    picked.append((name, href))
                    break

        for name, href in picked[:4]:  # cap per filing to be polite
            text = _read_html_or_text(href)
            if not text:
                continue
            title = f"{form} {name}"
            rows.append((ts, title, href, text))

    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df = df.sort_values("ts", ascending=False).drop_duplicates(["title"]).reset_index(drop=True)
    return df
