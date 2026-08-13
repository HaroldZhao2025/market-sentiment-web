from __future__ import annotations

import math
from datetime import date
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from .finbert import FinBERT

ALPHA_VANTAGE_ENDPOINT = "https://www.alphavantage.co/query"

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


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def fiscal_quarters(count: int = 8, today: date | None = None) -> list[str]:
    current = today or date.today()
    quarter = (current.month - 1) // 3 + 1
    year = current.year
    out: list[str] = []
    for _ in range(max(1, count)):
        out.append(f"{year}Q{quarter}")
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return out


def fetch_transcript(symbol: str, quarter: str, api_key: str) -> dict[str, Any] | None:
    if not api_key:
        return None
    try:
        response = requests.get(
            ALPHA_VANTAGE_ENDPOINT,
            params={"function": "EARNINGS_CALL_TRANSCRIPT", "symbol": symbol, "quarter": quarter, "apikey": api_key},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("Information") or payload.get("Note") or payload.get("Error Message"):
        return None
    return payload if isinstance(payload.get("transcript"), list) and payload.get("transcript") else None


def normalize_turns(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("transcript")
    if not isinstance(raw, list):
        return []
    turns: list[dict[str, Any]] = []
    qa_started = False
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        speaker = str(item.get("speaker") or item.get("name") or "").strip()
        role = str(item.get("title") or item.get("role") or "").strip()
        text = str(item.get("content") or item.get("text") or "").strip()
        if not text:
            continue
        marker = f" {speaker} {role} {text[:140]} ".lower()
        if "analyst" in marker or "question-and-answer" in marker or "question and answer" in marker or " q&a " in marker:
            qa_started = True
        turns.append({"turn": index, "speaker": speaker, "role": role, "section": "qa" if qa_started else "prepared", "text": text, "provider_sentiment": finite(item.get("sentiment"))})
    return turns


def _chunks(text: str, words: int = 160) -> list[str]:
    tokens = text.split()
    return [" ".join(tokens[i : i + words]) for i in range(0, len(tokens), words)] or [""]


def score_turns(turns: list[dict[str, Any]], model: FinBERT | None = None) -> list[dict[str, Any]]:
    if not turns:
        return []
    fb = model or FinBERT()
    out: list[dict[str, Any]] = []
    for turn in turns:
        chunks = _chunks(str(turn.get("text") or ""))
        scores = fb.score(chunks, batch_size=12)
        row = dict(turn)
        row["sentiment"] = round(sum(scores) / len(scores), 6) if scores else None
        out.append(row)
    return out


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
        rows.append({"date": pd.Timestamp(idx).isoformat(), "eps_estimate": finite(row.get("EPS Estimate")), "reported_eps": finite(row.get("Reported EPS")), "surprise_pct": finite(row.get("Surprise(%)"))})
    return rows


def build_earnings_intelligence(symbol: str, api_key: str, quarters: int = 4) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    model: FinBERT | None = None
    for quarter in fiscal_quarters(max(quarters + 2, 6)):
        if len(calls) >= quarters:
            break
        payload = fetch_transcript(symbol, quarter, api_key)
        if not payload:
            continue
        turns = normalize_turns(payload)
        if not turns:
            continue
        model = model or FinBERT()
        scored = score_turns(turns, model=model)
        calls.append({"quarter": str(payload.get("quarter") or quarter), "date": str(payload.get("date") or payload.get("fiscalDateEnding") or "") or None, "source": "earnings call transcript", "summary": summarize_transcript(scored), "turns": scored})
    return {"schema_version": 2, "symbol": symbol, "earnings_history": earnings_history(symbol), "calls": calls, "methodology": {"turn_sentiment": "ProsusAI/FinBERT P(positive) - P(negative), chunk-averaged for long turns", "tone_shift": "mean Q&A sentiment minus mean prepared-remarks sentiment", "topics": "deterministic keyword families"}}
