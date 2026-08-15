from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd
import yfinance as yf

from .finbert import FinBERT

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
QA_MARKERS = ("question-and-answer", "question and answer", "questions and answers", "q&a", "operator instructions")


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _chunks(text: str, words: int = 160) -> list[str]:
    tokens = text.split()
    return [" ".join(tokens[i : i + words]) for i in range(0, len(tokens), words)] or []


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _term_rate(texts: list[str], terms: tuple[str, ...]) -> float | None:
    if not texts:
        return None
    hits = sum(any(term in f" {text.lower()} " for term in terms) for text in texts)
    return hits / len(texts)


def summarize_transcript(turns: list[dict[str, Any]]) -> dict[str, Any]:
    scored = [turn for turn in turns if finite(turn.get("sentiment")) is not None]
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


def earnings_history(symbol: str, limit: int = 8) -> list[dict[str, Any]]:
    try:
        frame = yf.Ticker(symbol).get_earnings_dates(limit=limit)
    except Exception:
        return []
    if frame is None or frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for idx, row in frame.sort_index(ascending=False).iterrows():
        rows.append(
            {
                "date": pd.Timestamp(idx).isoformat(),
                "eps_estimate": finite(row.get("EPS Estimate")),
                "reported_eps": finite(row.get("Reported EPS")),
                "surprise_pct": finite(row.get("Surprise(%)")),
            }
        )
    return rows


def _sec_transcript_turns(text: str, model: FinBERT) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    parts = [" ".join(part.split()) for part in re.split(r"\n{1,}|(?<=\.)\s{2,}", text) if len(part.split()) >= 12]
    if len(parts) < 3:
        parts = _chunks(text, words=140)
    qa_started = False
    texts: list[str] = []
    sections: list[str] = []
    for part in parts[:200]:
        lower = part.lower()
        if any(marker in lower for marker in QA_MARKERS):
            qa_started = True
        texts.append(part)
        sections.append("qa" if qa_started else "prepared")
    if not texts:
        return []
    scores = model.score(texts, batch_size=12)
    return [
        {
            "turn": index,
            "speaker": "SEC transcript segment",
            "role": "unparsed",
            "section": section,
            "text": part,
            "sentiment": round(float(score), 6),
        }
        for index, (part, section, score) in enumerate(zip(texts, sections, scores))
    ]


def _safe_turns(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe: list[dict[str, Any]] = []
    for turn in turns:
        text = str(turn.get("text") or "")
        lower = f" {text.lower()} "
        safe.append(
            {
                "turn": turn.get("turn"),
                "speaker": str(turn.get("speaker") or "Unknown speaker"),
                "role": str(turn.get("role") or ""),
                "section": str(turn.get("section") or "prepared"),
                "sentiment": finite(turn.get("sentiment")),
                "word_count": len(text.split()),
                "topic_hits": [topic for topic, terms in TOPICS.items() if any(term in lower for term in terms)],
                "uncertainty_hits": sum(1 for term in UNCERTAINTY_TERMS if term in lower),
                "forward_looking_hits": sum(1 for term in FORWARD_TERMS if term in lower),
            }
        )
    return safe


def build_free_earnings_intelligence(symbol: str, sec_evidence: list[dict[str, Any]]) -> dict[str, Any]:
    transcripts = [row for row in sec_evidence if row.get("document_type") == "transcript" and str(row.get("text") or "").strip()]
    calls: list[dict[str, Any]] = []
    model: FinBERT | None = None
    for row in transcripts[:4]:
        model = model or FinBERT()
        turns = _sec_transcript_turns(str(row.get("text") or ""), model)
        if not turns:
            continue
        calls.append(
            {
                "quarter": None,
                "date": str(row.get("ts") or "") or None,
                "source": "SEC EDGAR transcript exhibit",
                "source_url": str(row.get("url") or ""),
                "source_type": "sec_transcript_exhibit",
                "summary": summarize_transcript(turns),
                "turns": _safe_turns(turns),
                "transcript_word_count": sum(len(str(turn.get("text") or "").split()) for turn in turns),
                "transcript_text_redistributed": False,
            }
        )

    filing_fallback = []
    public_links: list[dict[str, Any]] = []
    seen_links: set[str] = set()
    for row in sec_evidence:
        item = {
            "ts": str(row.get("ts") or ""),
            "title": str(row.get("title") or ""),
            "url": str(row.get("url") or ""),
            "source": "SEC EDGAR",
            "document_type": str(row.get("document_type") or "filing"),
            "sec_form_type": str(row.get("sec_form_type") or ""),
        }
        if item["document_type"] != "transcript":
            filing_fallback.append(item)
        for url in row.get("public_links") or []:
            value = str(url or "").strip()
            if value and value not in seen_links:
                seen_links.add(value)
                public_links.append({"title": "Public webcast / investor-relations link", "url": value, "source": "SEC filing link", "ts": item["ts"]})

    return {
        "schema_version": 6,
        "symbol": symbol,
        "earnings_history": earnings_history(symbol),
        "calls": calls,
        "call_links": public_links[:10],
        "filing_fallback": filing_fallback[:30],
        "methodology": {
            "source_policy": "Free public sources only",
            "structured_transcript_source": "SEC transcript exhibits and free public transcript pages",
            "transcript_missing": "Absence of a free public transcript is preserved as missing; filings are never relabeled as calls.",
            "turn_sentiment": "ProsusAI/FinBERT P(positive) - P(negative) on transcript turns; third-party transcript text is not redistributed.",
        },
    }


def build_earnings_intelligence(symbol: str, api_key: str = "", quarters: int = 4) -> dict[str, Any]:
    del api_key, quarters
    return {
        "schema_version": 6,
        "symbol": symbol,
        "earnings_history": earnings_history(symbol),
        "calls": [],
        "call_links": [],
        "filing_fallback": [],
        "methodology": {"source_policy": "Free public sources only"},
    }
