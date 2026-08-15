from __future__ import annotations

import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

UA = os.getenv(
    "SEC_UA",
    "market-sentiment-web/0.1 (+https://github.com/HaroldZhao2025/market-sentiment-web)",
)

TRANSCRIPT_TERMS = (
    "transcript",
    "conference call transcript",
    "earnings call transcript",
    "prepared remarks",
    "question and answer",
    "questions and answers",
)
RELEASE_TERMS = (
    "earnings release",
    "press release",
    "results of operations",
    "financial results",
    "quarterly results",
)
PRESENTATION_TERMS = (
    "presentation",
    "investor presentation",
    "earnings presentation",
    "slides",
    "slide deck",
)
WEBCAST_TERMS = (
    "webcast",
    "replay",
    "conference call",
    "earnings call",
    "investor call",
)


def _get(url: str, params: dict | None = None, sleep: float = 0.2):
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    response = requests.get(url, params=params or {}, headers=headers, timeout=30)
    time.sleep(sleep)
    response.raise_for_status()
    return response


def _atom_entries_for_ticker(ticker: str) -> list:
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?CIK={ticker}&owner=exclude&action=getcompany&output=atom"
    )
    try:
        response = _get(url)
    except Exception:
        return []
    soup = BeautifulSoup(response.content, features="xml")
    return soup.find_all("entry") or []


def _clean_text(content: bytes) -> str:
    soup = BeautifulSoup(content, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    return " ".join(soup.get_text(separator=" ").split())


def _classify_document(description: str, doc_type: str, text: str = "") -> str:
    haystack = f" {description} {doc_type} {text[:5000]} ".lower()
    if any(term in haystack for term in TRANSCRIPT_TERMS):
        return "transcript"
    if any(term in haystack for term in RELEASE_TERMS):
        return "earnings_release"
    if any(term in haystack for term in PRESENTATION_TERMS):
        return "presentation"
    if any(term in haystack for term in WEBCAST_TERMS):
        return "webcast_or_replay"
    if str(doc_type).upper().startswith("EX-99"):
        return "exhibit_99"
    return "filing"


def _filing_documents(index_url: str) -> list[dict]:
    try:
        response = _get(index_url)
    except Exception:
        return []
    soup = BeautifulSoup(response.content, "lxml")
    rows: list[dict] = []
    for table in soup.find_all("table"):
        headers = [" ".join(th.get_text(" ", strip=True).split()).lower() for th in table.find_all("th")]
        if not headers or not any("document" in header for header in headers):
            continue
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue
            link = cells[2].find("a") if len(cells) > 2 else None
            if link is None:
                continue
            href = str(link.get("href") or "").strip()
            if not href:
                continue
            description = " ".join(cells[1].get_text(" ", strip=True).split()) if len(cells) > 1 else ""
            document = " ".join(cells[2].get_text(" ", strip=True).split())
            doc_type = " ".join(cells[3].get_text(" ", strip=True).split()) if len(cells) > 3 else ""
            rows.append(
                {
                    "description": description,
                    "document": document,
                    "doc_type": doc_type,
                    "url": urljoin(index_url, href),
                }
            )
        if rows:
            break
    return rows


def _public_links_from_text(text: str) -> list[str]:
    if not text:
        return []
    urls = re.findall(r"https?://[^\s<>'\"]+", text)
    out: list[str] = []
    for raw in urls:
        clean = raw.rstrip(".,);]")
        lower = clean.lower()
        if any(term in lower for term in ("webcast", "investor", "events", "earnings", "ir.")):
            if clean not in out:
                out.append(clean)
    return out[:10]


def fetch_earnings_evidence(ticker: str, start: str, end: str) -> list[dict]:
    """Return free SEC earnings evidence at filing-document/exhibit level."""
    entries = _atom_entries_for_ticker(ticker)
    if not entries:
        return []

    start_ts = pd.to_datetime(start, utc=True)
    end_ts = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    evidence: list[dict] = []

    for entry in entries:
        try:
            ts = pd.to_datetime(entry.find("updated").text, utc=True)
            if not (start_ts <= ts <= end_ts):
                continue
            title = (entry.find("title").text or "").strip()
            if not re.search(r"(8-K|10-Q|10-K|earnings|results)", title, flags=re.I):
                continue
            link = entry.find("link")
            index_url = str(link.get("href") if link else "").strip()
            if not index_url:
                continue

            docs = _filing_documents(index_url)
            if not docs:
                evidence.append(
                    {
                        "ts": ts.isoformat(),
                        "title": title,
                        "url": index_url,
                        "source": "SEC EDGAR",
                        "document_type": "filing",
                        "filing_title": title,
                        "filing_url": index_url,
                        "text": "",
                        "public_links": [],
                    }
                )
                continue

            for doc in docs:
                doc_type = str(doc.get("doc_type") or "")
                description = str(doc.get("description") or "")
                document_url = str(doc.get("url") or "")
                # Keep the filing itself and earnings-relevant exhibits; skip unrelated exhibits.
                likely_relevant = (
                    doc_type.upper() in {"8-K", "10-Q", "10-K"}
                    or doc_type.upper().startswith("EX-99")
                    or re.search(
                        r"(earnings|results|transcript|conference call|webcast|presentation|prepared remarks)",
                        description,
                        flags=re.I,
                    )
                )
                if not likely_relevant:
                    continue
                text = ""
                if document_url:
                    try:
                        text = _clean_text(_get(document_url).content)
                    except Exception:
                        text = ""
                category = _classify_document(description, doc_type, text)
                evidence.append(
                    {
                        "ts": ts.isoformat(),
                        "title": description or str(doc.get("document") or title),
                        "url": document_url or index_url,
                        "source": "SEC EDGAR",
                        "document_type": category,
                        "sec_form_type": doc_type,
                        "filing_title": title,
                        "filing_url": index_url,
                        "text": text,
                        "public_links": _public_links_from_text(text),
                    }
                )
        except Exception:
            continue

    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in evidence:
        key = (str(row.get("document_type") or ""), str(row.get("url") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def fetch_earnings_docs(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Compatibility dataframe backed by document-level SEC earnings evidence."""
    evidence = fetch_earnings_evidence(ticker, start, end)
    if not evidence:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "document_type"])
    rows = [
        (
            ticker,
            pd.to_datetime(row.get("ts"), utc=True, errors="coerce"),
            str(row.get("title") or ""),
            str(row.get("url") or ""),
            str(row.get("text") or ""),
            str(row.get("document_type") or "filing"),
        )
        for row in evidence
    ]
    return pd.DataFrame(rows, columns=["ticker", "ts", "title", "url", "text", "document_type"])
