from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

UA = "market-sentiment-web/1.0 (+https://github.com/HaroldZhao2025/market-sentiment-web)"
ROLE_TERMS = (
    "chief executive", "chief financial", "ceo", "cfo", "president", "vice president", "vp ", "vp of",
    "director", "investor relations", "analyst", "chairman", "founder", "officer", "partner", "managing",
    "research", "evp", "svp", "treasurer", "controller", "secretary",
)
QA_START_TERMS = (
    "we will now begin the question-and-answer", "we'll now begin the question-and-answer",
    "we will now open the call for questions", "we'll now open the call for questions",
    "we will now take questions", "we'll now take questions", "our first question comes",
    "the first question comes", "our first question is", "first question is from",
    "begin the q&a", "begin the question-and-answer", "open the call to questions",
)
TOPICS: dict[str, tuple[str, ...]] = {
    "Guidance": ("guidance", "outlook", "forecast", "expect", "next quarter", "full year"),
    "Demand": ("demand", "orders", "bookings", "pipeline", "customer", "volume"),
    "Margins & costs": ("margin", "cost", "expense", "pricing", "productivity", "inflation"),
    "AI & technology": ("artificial intelligence", " ai ", "model", "cloud", "compute", "software", "chip"),
    "Capital allocation": ("buyback", "repurchase", "dividend", "capex", "debt", "cash flow"),
    "Macro & FX": ("macro", "economy", "consumer", "foreign exchange", "fx", "interest rate"),
    "Regulation & legal": ("regulation", "regulatory", "antitrust", "legal", "litigation", "tariff"),
}
UNCERTAINTY_TERMS = ("uncertain", "uncertainty", "volatile", "challenging", "risk", "headwind", "visibility", "cautious", "pressure")
FORWARD_TERMS = ("expect", "forecast", "outlook", "guidance", "anticipate", "next quarter", "full year", "going forward", "we believe")


class SentimentScorer(Protocol):
    def score(self, texts: list[str], batch_size: int = 16) -> list[float]: ...


@dataclass(frozen=True)
class BroadCandidate:
    url: str
    title: str
    source: str
    published: str = ""


def _get(url: str, timeout: int = 25) -> requests.Response | None:
    try:
        response = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        return response
    except Exception:
        return None


def _clean_symbol(symbol: str) -> str:
    return symbol.lower().replace(".", "-").replace("/", "-")


def _date_from_text(text: str) -> str:
    match = re.search(r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2}),\s+(20\d{2})\b", text, flags=re.I)
    if not match:
        return ""
    month_map = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
    month = month_map[match.group(1)[:3].lower()]
    return f"{int(match.group(3)):04d}-{month:02d}-{int(match.group(2)):02d}T00:00:00+00:00"


def stockanalysis_candidates(symbol: str, limit: int = 3) -> list[BroadCandidate]:
    slug = _clean_symbol(symbol)
    index_url = f"https://stockanalysis.com/stocks/{slug}/transcripts/"
    response = _get(index_url)
    if response is None:
        return []
    soup = BeautifulSoup(response.content, "lxml")
    heading = soup.find("h1")
    if heading is None or "earnings call transcripts" not in heading.get_text(" ", strip=True).lower():
        return []
    out: list[BroadCandidate] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        title = " ".join(anchor.get_text(" ", strip=True).split())
        href = str(anchor.get("href") or "")
        if not title.lower().startswith("earnings call:") or "/transcripts/" not in href:
            continue
        url = urljoin(index_url, href)
        if url in seen:
            continue
        seen.add(url)
        parent_text = " ".join((anchor.parent or anchor).get_text(" ", strip=True).split())
        out.append(BroadCandidate(url=url, title=f"{symbol.upper()} {title}", source="Stock Analysis / Quartr", published=_date_from_text(parent_text)))
        if len(out) >= limit:
            break
    return out


def marketbeat_candidates(symbol: str, limit: int = 2) -> list[BroadCandidate]:
    ticker = symbol.upper()
    variants = [ticker, ticker.replace("-", ".")]
    exchanges = ("NASDAQ", "NYSE", "AMEX")
    for exchange in exchanges:
        for variant in variants:
            index_url = f"https://www.marketbeat.com/stocks/{exchange}/{variant}/earnings/"
            response = _get(index_url)
            if response is None:
                continue
            soup = BeautifulSoup(response.content, "lxml")
            h1 = soup.find("h1")
            heading = h1.get_text(" ", strip=True) if h1 else ""
            if ticker.replace("-", ".") not in heading.upper() and f"({ticker})" not in heading.upper():
                continue
            out: list[BroadCandidate] = []
            seen: set[str] = set()
            for anchor in soup.find_all("a", href=True):
                text = " ".join(anchor.get_text(" ", strip=True).split())
                href = str(anchor.get("href") or "")
                lower = text.lower()
                if not ("conference call transcript" in lower or "read transcript" in lower):
                    continue
                if "/earnings/reports/" not in href:
                    continue
                url = urljoin(index_url, href)
                if url in seen:
                    continue
                seen.add(url)
                date_match = re.search(r"/earnings/reports/(20\d{2})-(\d{1,2})-(\d{1,2})-", url)
                published = f"{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}T00:00:00+00:00" if date_match else ""
                out.append(BroadCandidate(url=url, title=f"{ticker} Earnings Call Transcript", source="MarketBeat", published=published))
                if len(out) >= limit:
                    return out
            if out:
                return out
    return []


def discover_broad_candidates(symbol: str, limit: int = 5) -> list[BroadCandidate]:
    candidates = stockanalysis_candidates(symbol, limit=3)
    candidates.extend(marketbeat_candidates(symbol, limit=2))
    out: list[BroadCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        out.append(candidate)
        if len(out) >= limit:
            break
    return out


def _looks_like_role(text: str) -> bool:
    lower = f" {text.lower()} "
    return len(text) <= 140 and any(term in lower for term in ROLE_TERMS)


def _looks_like_name(text: str) -> bool:
    value = " ".join(text.split()).strip()
    if value == "Operator":
        return True
    if not value or len(value) > 90 or any(ch in value for ch in "?!:"):
        return False
    words = value.split()
    if not 1 <= len(words) <= 7:
        return False
    alpha = sum(ch.isalpha() for ch in value)
    return alpha >= max(2, int(len(value) * 0.55)) and not value.lower().startswith(("earnings call", "full transcript", "summary", "presentation", "participants"))


def _starts_qa(text: str) -> bool:
    lower = " ".join(text.lower().split())
    if "will follow" in lower or "after the prepared remarks" in lower:
        return False
    return any(term in lower for term in QA_START_TERMS)


def _stockanalysis_turns(soup: BeautifulSoup) -> list[dict[str, Any]]:
    root = soup.find("main") or soup.body or soup
    lines = [" ".join(text.split()) for text in root.stripped_strings if " ".join(text.split())]
    start = next((i + 1 for i, text in enumerate(lines) if text == "Full Transcript"), 0)
    lines = lines[start:]
    if "Summary" in lines[:8]:
        pos = lines.index("Summary")
        lines = lines[pos + 1 :]
    first = 0
    for i, text in enumerate(lines):
        if text == "Operator" or (_looks_like_name(text) and i + 1 < len(lines) and _looks_like_role(lines[i + 1])):
            first = i
            break
    lines = lines[first:]
    turns: list[dict[str, Any]] = []
    current_speaker: str | None = None
    current_role = ""
    current_text: list[str] = []
    qa = False

    def flush() -> None:
        nonlocal current_speaker, current_role, current_text
        if current_speaker and current_text:
            text = " ".join(current_text).strip()
            if len(text.split()) >= 5:
                turns.append({"speaker": current_speaker, "role": current_role, "section": "qa" if qa else "prepared", "text": text})
        current_speaker = None
        current_role = ""
        current_text = []

    i = 0
    while i < len(lines):
        text = lines[i]
        next_text = lines[i + 1] if i + 1 < len(lines) else ""
        is_speaker = text == "Operator" or (_looks_like_name(text) and _looks_like_role(next_text))
        if is_speaker:
            flush()
            current_speaker = text
            if text != "Operator" and _looks_like_role(next_text):
                current_role = next_text
                i += 1
            i += 1
            continue
        if current_speaker:
            if _starts_qa(text):
                qa = True
            current_text.append(text)
        i += 1
    flush()
    return turns


def _marketbeat_turns(soup: BeautifulSoup) -> list[dict[str, Any]]:
    root = soup.find("main") or soup.body or soup
    lines = [" ".join(text.split()) for text in root.stripped_strings if " ".join(text.split())]
    start = next((i + 1 for i, text in enumerate(lines) if re.search(r"earnings call transcript$", text, flags=re.I)), 0)
    lines = lines[start:]
    timestamp_re = re.compile(r"^\d{2}:\d{2}:\d{2}$")
    timestamp_idx = [i for i, text in enumerate(lines) if timestamp_re.match(text)]
    records: list[tuple[int, int, str, str]] = []
    for idx in timestamp_idx:
        speaker_idx = -1
        speaker = ""
        role = ""
        for back in range(1, 5):
            j = idx - back
            if j < 0:
                break
            candidate = lines[j]
            if _looks_like_name(candidate):
                speaker_idx = j
                speaker = candidate
                if j + 1 < idx and _looks_like_role(lines[j + 1]):
                    role = lines[j + 1]
                break
        if speaker_idx >= 0:
            records.append((speaker_idx, idx, speaker, role))
    turns: list[dict[str, Any]] = []
    qa = False
    for pos, (speaker_idx, time_idx, speaker, role) in enumerate(records):
        next_speaker_idx = records[pos + 1][0] if pos + 1 < len(records) else len(lines)
        text_parts = [part for part in lines[time_idx + 1 : next_speaker_idx] if not timestamp_re.match(part)]
        text = " ".join(text_parts).strip()
        if len(text.split()) < 5:
            continue
        if _starts_qa(text):
            qa = True
        turns.append({"speaker": speaker, "role": role, "section": "qa" if qa else "prepared", "text": text})
    return turns


def extract_broad_transcript(candidate: BroadCandidate) -> dict[str, Any] | None:
    response = _get(candidate.url)
    if response is None:
        return None
    soup = BeautifulSoup(response.content, "lxml")
    host = (urlparse(response.url).hostname or urlparse(candidate.url).hostname or "").lower()
    if "stockanalysis.com" in host:
        turns = _stockanalysis_turns(soup)
    elif "marketbeat.com" in host:
        turns = _marketbeat_turns(soup)
    else:
        return None
    if len(turns) < 6:
        return None
    word_count = sum(len(str(turn.get("text") or "").split()) for turn in turns)
    prepared = sum(1 for turn in turns if turn.get("section") == "prepared")
    qa = sum(1 for turn in turns if turn.get("section") == "qa")
    if word_count < 700 or prepared < 2 or qa < 2:
        return None
    return {"candidate": candidate, "turns": turns, "word_count": word_count}


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _term_rate(texts: list[str], terms: tuple[str, ...]) -> float | None:
    if not texts:
        return None
    hits = sum(any(term in f" {text.lower()} " for term in terms) for text in texts)
    return hits / len(texts)


def _summarize_transcript(turns: list[dict[str, Any]]) -> dict[str, Any]:
    scored = [turn for turn in turns if isinstance(turn.get("sentiment"), (int, float))]
    all_scores = [float(turn["sentiment"]) for turn in scored]
    prepared_scores = [float(turn["sentiment"]) for turn in scored if turn.get("section") == "prepared"]
    qa_scores = [float(turn["sentiment"]) for turn in scored if turn.get("section") == "qa"]
    prepared = _mean(prepared_scores)
    qa = _mean(qa_scores)
    topic_rows: list[dict[str, Any]] = []
    for topic, terms in TOPICS.items():
        matching = [turn for turn in scored if any(term in f" {str(turn.get('text') or '').lower()} " for term in terms)]
        if matching:
            values = [float(turn["sentiment"]) for turn in matching]
            topic_rows.append({"topic": topic, "mentions": len(matching), "sentiment": round(float(_mean(values) or 0.0), 6)})
    topic_rows.sort(key=lambda row: (-int(row["mentions"]), -abs(float(row["sentiment"]))))
    texts = [str(turn.get("text") or "") for turn in scored]
    return {
        "turn_count": len(turns),
        "scored_turn_count": len(scored),
        "overall_sentiment": round(float(_mean(all_scores)), 6) if all_scores else None,
        "prepared_sentiment": round(float(prepared), 6) if prepared is not None else None,
        "qa_sentiment": round(float(qa), 6) if qa is not None else None,
        "qa_tone_shift": round(float(qa - prepared), 6) if qa is not None and prepared is not None else None,
        "uncertainty_turn_rate": round(float(_term_rate(texts, UNCERTAINTY_TERMS)), 6) if texts else None,
        "forward_looking_turn_rate": round(float(_term_rate(texts, FORWARD_TERMS)), 6) if texts else None,
        "topics": topic_rows[:8],
    }


def _term_hits(text: str, terms: tuple[str, ...]) -> int:
    lower = f" {text.lower()} "
    return sum(1 for term in terms if term in lower)


def score_broad_transcript(parsed: dict[str, Any], model: SentimentScorer) -> dict[str, Any]:
    raw_turns = [row for row in parsed.get("turns") or [] if isinstance(row, dict) and str(row.get("text") or "").strip()]
    if not raw_turns:
        return {}
    texts = [str(row.get("text") or "") for row in raw_turns]
    scores = model.score(texts, batch_size=12)
    internal: list[dict[str, Any]] = []
    safe: list[dict[str, Any]] = []
    for index, (row, score) in enumerate(zip(raw_turns, scores)):
        text = str(row.get("text") or "")
        lower = f" {text.lower()} "
        topic_hits = [topic for topic, terms in TOPICS.items() if any(term in lower for term in terms)]
        internal_row = {
            "turn": index,
            "speaker": str(row.get("speaker") or "Unknown speaker"),
            "role": str(row.get("role") or ""),
            "section": str(row.get("section") or "prepared"),
            "text": text,
            "sentiment": round(float(score), 6),
        }
        internal.append(internal_row)
        safe.append({
            "turn": index,
            "speaker": internal_row["speaker"],
            "role": internal_row["role"],
            "section": internal_row["section"],
            "sentiment": internal_row["sentiment"],
            "word_count": len(text.split()),
            "topic_hits": topic_hits,
            "uncertainty_hits": _term_hits(text, UNCERTAINTY_TERMS),
            "forward_looking_hits": _term_hits(text, FORWARD_TERMS),
        })
    summary = _summarize_transcript(internal)
    candidate: BroadCandidate = parsed["candidate"]
    return {
        "quarter": None,
        "date": candidate.published or None,
        "source": candidate.source,
        "source_url": candidate.url,
        "source_type": "free_public_transcript",
        "summary": summary,
        "turns": safe,
        "transcript_word_count": int(parsed.get("word_count") or 0),
        "transcript_text_redistributed": False,
    }


def complete_call(call: dict[str, Any]) -> bool:
    summary = call.get("summary") if isinstance(call.get("summary"), dict) else {}
    return all(summary.get(key) is not None for key in ("overall_sentiment", "prepared_sentiment", "qa_sentiment", "qa_tone_shift"))


def fulfill_broad_transcript(symbol: str, model: SentimentScorer) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = discover_broad_candidates(symbol)
    evidence = [{"title": c.title, "url": c.url, "source": c.source, "ts": c.published} for c in candidates]
    fallback: list[dict[str, Any]] = []
    for candidate in candidates:
        parsed = extract_broad_transcript(candidate)
        if parsed is None:
            continue
        call = score_broad_transcript(parsed, model)
        if not call:
            continue
        if complete_call(call):
            return [call], evidence
        fallback.append(call)
    return fallback[:1], evidence
