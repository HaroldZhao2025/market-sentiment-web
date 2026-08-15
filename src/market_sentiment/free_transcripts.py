from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

from .finbert import FinBERT
from .v5_earnings import FORWARD_TERMS, TOPICS, UNCERTAINTY_TERMS, summarize_transcript

UA = "market-sentiment-web/1.0 (+https://github.com/HaroldZhao2025/market-sentiment-web)"
TRANSCRIPT_MARKERS = ("earnings call transcript", "conference call transcript", "financial call transcript")
ALLOWED_PUBLIC_HOSTS = ("fool.com", "finance.yahoo.com", "sixcolors.com")


@dataclass(frozen=True)
class TranscriptCandidate:
    url: str
    title: str
    source: str
    published: str = ""


def _get(url: str, timeout: int = 30, params: dict[str, Any] | None = None) -> requests.Response | None:
    try:
        response = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        return response
    except Exception:
        return None


def _allowed(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    return any(host == allowed or host.endswith(f".{allowed}") for allowed in ALLOWED_PUBLIC_HOSTS)


def _looks_like_transcript(title: str) -> bool:
    lower = f" {title.lower()} "
    return any(marker in lower for marker in TRANSCRIPT_MARKERS) or (" transcript " in lower and (" earnings " in lower or " financial call " in lower))


def _resolve_google_news_url(url: str) -> str:
    if "news.google." not in url:
        return url
    response = _get(url, timeout=20)
    return response.url if response is not None else url


def yahoo_search_transcript_candidates(symbol: str, company_name: str = "", limit: int = 12) -> list[TranscriptCandidate]:
    query = f"{symbol} earnings call transcript"
    if company_name:
        query = f"{company_name} {symbol} earnings call transcript"
    response = _get(
        "https://query1.finance.yahoo.com/v1/finance/search",
        params={"q": query, "quotesCount": 0, "newsCount": max(20, limit * 2), "enableFuzzyQuery": "false"},
    )
    if response is None:
        return []
    try:
        payload = response.json()
    except Exception:
        return []
    out: list[TranscriptCandidate] = []
    seen: set[str] = set()
    for row in payload.get("news") or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        url = str(row.get("link") or row.get("url") or "").strip()
        if not _looks_like_transcript(title) or not url or not _allowed(url) or url in seen:
            continue
        seen.add(url)
        out.append(TranscriptCandidate(url=url, title=title, source=str(row.get("publisher") or urlparse(url).hostname or "Yahoo public search"), published=str(row.get("providerPublishTime") or "")))
        if len(out) >= limit:
            break
    return out


def google_transcript_candidates(symbol: str, company_name: str = "", limit: int = 12) -> list[TranscriptCandidate]:
    terms = f'"{symbol}" earnings call transcript'
    if company_name:
        terms = f'"{company_name}" "{symbol}" earnings call transcript'
    url = f"https://news.google.com/rss/search?q={quote_plus(terms)}&hl=en-US&gl=US&ceid=US:en"
    response = _get(url)
    if response is None:
        return []
    soup = BeautifulSoup(response.content, "xml")
    out: list[TranscriptCandidate] = []
    seen: set[str] = set()
    for item in soup.find_all("item"):
        title = html.unescape(item.title.get_text(" ", strip=True) if item.title else "")
        if not _looks_like_transcript(title):
            continue
        link = item.link.get_text(strip=True) if item.link else ""
        resolved = _resolve_google_news_url(link)
        if not resolved or not _allowed(resolved) or resolved in seen:
            continue
        seen.add(resolved)
        source_tag = item.find("source")
        source = source_tag.get_text(" ", strip=True) if source_tag else (urlparse(resolved).hostname or "Public transcript")
        published = item.pubDate.get_text(" ", strip=True) if item.pubDate else ""
        out.append(TranscriptCandidate(resolved, title, source, published))
        if len(out) >= limit:
            break
    return out


def news_transcript_candidates(news_rows: list[dict[str, Any]], limit: int = 12) -> list[TranscriptCandidate]:
    out: list[TranscriptCandidate] = []
    seen: set[str] = set()
    for row in news_rows:
        title = str(row.get("title") or row.get("headline") or "").strip()
        url = str(row.get("url") or "").strip()
        if not title or not url or not _looks_like_transcript(title):
            continue
        resolved = _resolve_google_news_url(url)
        if not _allowed(resolved) or resolved in seen:
            continue
        seen.add(resolved)
        out.append(TranscriptCandidate(resolved, title, str(row.get("source") or row.get("provider") or urlparse(resolved).hostname or "Public transcript"), str(row.get("ts") or row.get("date") or "")))
        if len(out) >= limit:
            break
    return out


def discover_transcripts(symbol: str, company_name: str = "", news_rows: list[dict[str, Any]] | None = None, limit: int = 12) -> list[TranscriptCandidate]:
    candidates = news_transcript_candidates(news_rows or [], limit=limit)
    candidates.extend(yahoo_search_transcript_candidates(symbol, company_name, limit=limit))
    candidates.extend(google_transcript_candidates(symbol, company_name, limit=limit))
    out: list[TranscriptCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        out.append(candidate)
        if len(out) >= limit:
            break
    return out


def _article_blocks(soup: BeautifulSoup) -> list[str]:
    root = soup.find("article") or soup.find("main") or soup.body or soup
    blocks: list[str] = []
    for node in root.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        text = " ".join(node.get_text(" ", strip=True).split())
        if not text or len(text) < 3:
            continue
        if blocks and text == blocks[-1]:
            continue
        blocks.append(text)
    return blocks


def _trim_to_transcript(blocks: list[str]) -> list[str]:
    start = 0
    for i, block in enumerate(blocks):
        lower = block.lower()
        if "full conference call transcript" in lower or "full earnings call transcript" in lower:
            start = i + 1
            break
        if "transcript of the call" in lower and i > 2:
            start = i + 1
            break
    selected = blocks[start:]
    stop_markers = ("this article is a transcript", "stocks mentioned", "the motley fool has positions", "disclosure policy", "related articles", "subscribe to")
    trimmed: list[str] = []
    for block in selected:
        lower = block.lower()
        if trimmed and any(marker in lower for marker in stop_markers):
            break
        trimmed.append(block)
    return trimmed


def _speaker_from_block(block: str) -> tuple[str, str] | None:
    match = re.match(r"^([A-Z][A-Za-zÀ-ÖØ-öø-ÿ .,'’&()\-]{1,90}):\s*(.+)$", block)
    if not match:
        return None
    speaker = " ".join(match.group(1).split()).strip()
    text = " ".join(match.group(2).split()).strip()
    if len(text.split()) < 3:
        return None
    return speaker, text


def extract_public_transcript(candidate: TranscriptCandidate) -> dict[str, Any] | None:
    if not _allowed(candidate.url):
        return None
    response = _get(candidate.url)
    if response is None:
        return None
    soup = BeautifulSoup(response.content, "lxml")
    blocks = _trim_to_transcript(_article_blocks(soup))
    if not blocks:
        return None
    qa_started = False
    turns: list[dict[str, Any]] = []
    pending_speaker: str | None = None
    pending_text: list[str] = []

    def flush() -> None:
        nonlocal pending_speaker, pending_text
        if pending_speaker and pending_text:
            text = " ".join(pending_text).strip()
            if len(text.split()) >= 6:
                turns.append({"speaker": pending_speaker, "section": "qa" if qa_started else "prepared", "text": text})
        pending_speaker = None
        pending_text = []

    for block in blocks:
        lower = f" {block.lower()} "
        if " questions & answers " in lower or " question-and-answer " in lower or lower.strip() in {"q&a", "questions and answers"}:
            flush()
            qa_started = True
            continue
        parsed = _speaker_from_block(block)
        if parsed:
            speaker, text = parsed
            flush()
            if speaker.lower() == "operator" and "question" in text.lower():
                qa_started = True
            pending_speaker = speaker
            pending_text = [text]
        elif pending_speaker and len(block.split()) >= 4:
            pending_text.append(block)
    flush()
    if len(turns) < 4:
        return None
    word_count = sum(len(str(turn.get("text") or "").split()) for turn in turns)
    if word_count < 500:
        return None
    return {"candidate": candidate, "turns": turns, "word_count": word_count}


def _term_hits(text: str, terms: tuple[str, ...]) -> int:
    lower = f" {text.lower()} "
    return sum(1 for term in terms if term in lower)


def score_public_transcript(parsed: dict[str, Any], model: FinBERT | None = None) -> dict[str, Any]:
    raw_turns = [row for row in parsed.get("turns") or [] if isinstance(row, dict) and str(row.get("text") or "").strip()]
    if not raw_turns:
        return {}
    fb = model or FinBERT()
    texts = [str(row["text"]) for row in raw_turns]
    scores = fb.score(texts, batch_size=12)
    scored_internal: list[dict[str, Any]] = []
    safe_turns: list[dict[str, Any]] = []
    for index, (row, score) in enumerate(zip(raw_turns, scores)):
        text = str(row["text"])
        lower = f" {text.lower()} "
        topic_hits = [topic for topic, terms in TOPICS.items() if any(term in lower for term in terms)]
        internal = {"turn": index, "speaker": str(row.get("speaker") or "Unknown speaker"), "role": "", "section": str(row.get("section") or "prepared"), "text": text, "sentiment": round(float(score), 6)}
        scored_internal.append(internal)
        safe_turns.append({"turn": index, "speaker": internal["speaker"], "section": internal["section"], "sentiment": internal["sentiment"], "word_count": len(text.split()), "topic_hits": topic_hits, "uncertainty_hits": _term_hits(text, UNCERTAINTY_TERMS), "forward_looking_hits": _term_hits(text, FORWARD_TERMS)})
    summary = summarize_transcript(scored_internal)
    candidate: TranscriptCandidate = parsed["candidate"]
    return {"quarter": None, "date": candidate.published or None, "source": candidate.source, "source_url": candidate.url, "source_type": "free_public_transcript", "summary": summary, "turns": safe_turns, "transcript_word_count": int(parsed.get("word_count") or 0), "transcript_text_redistributed": False}


def fulfill_public_transcript(symbol: str, company_name: str = "", news_rows: list[dict[str, Any]] | None = None, model: FinBERT | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = discover_transcripts(symbol, company_name, news_rows or [])
    calls: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    seen_sources: set[str] = set()
    for candidate in candidates:
        evidence.append({"title": candidate.title, "url": candidate.url, "source": candidate.source, "ts": candidate.published})
        if candidate.url in seen_sources:
            continue
        parsed = extract_public_transcript(candidate)
        if parsed is None:
            continue
        call = score_public_transcript(parsed, model=model)
        if not call:
            continue
        seen_sources.add(candidate.url)
        calls.append(call)
        break
    return calls, evidence[:12]
