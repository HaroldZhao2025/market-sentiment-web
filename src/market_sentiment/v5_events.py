from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

EVENT_RULES = (
    ("Earnings beat / miss", ("earnings", "eps", "revenue", "profit", "quarter", "beat", "miss")),
    ("Guidance & outlook", ("guidance", "outlook", "forecast", "expects")),
    ("Product & AI", ("artificial intelligence", " ai ", "product", "launch", "chip", "model", "cloud")),
    ("M&A & strategic deals", ("acquisition", "acquire", "merger", "deal", "stake", "joint venture")),
    ("Capital return & financing", ("buyback", "dividend", "debt", "offering", "financing")),
    ("Regulation & antitrust", ("regulator", "regulation", "antitrust", "ftc", "doj", "tariff", "ban", "probe")),
    ("Legal & litigation", ("lawsuit", "court", "legal", "settlement", "patent", "litigation")),
    ("Management change", ("ceo", "cfo", "executive", "appoints", "resigns", "steps down")),
    ("Operations & demand", ("demand", "supply", "shipment", "production", "orders", "factory", "inventory", "sales")),
    ("Analyst action", ("analyst", "rating", "price target", "upgrade", "downgrade")),
)


def classify_event(title: str, summary: str = "") -> str:
    text = f" {title} {summary} ".lower()
    for theme, terms in EVENT_RULES:
        if any(term in text for term in terms):
            return theme
    return "Other company news"


def stable_event_id(symbol: str, theme: str, day: str, title_key: str) -> str:
    raw = f"{symbol.upper()}|{theme}|{day[:10]}|{title_key}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20]


def article_events(company: dict[str, Any], news: list[dict[str, Any]]) -> list[dict[str, Any]]:
    symbol = str(company.get("ticker") or company.get("symbol") or "").upper()
    out: list[dict[str, Any]] = []
    for article in news:
        title = str(article.get("title") or article.get("headline") or "").strip()
        if not title:
            continue
        day = str(article.get("ts") or article.get("date") or "")[:10]
        theme = classify_event(title, str(article.get("summary") or ""))
        title_key = str(article.get("title_key") or title.lower()).strip()
        out.append({
            "event_id": stable_event_id(symbol, theme, day, title_key),
            "symbol": symbol,
            "name": company.get("name"),
            "sector": company.get("sector"),
            "industry": company.get("industry"),
            "universe": company.get("universe"),
            "date": day,
            "theme": theme,
            "sentiment": article.get("s"),
            "source": str(article.get("source") or article.get("provider") or ""),
            "title": title,
            "url": str(article.get("url") or ""),
        })
    return out


def merge_event_store(existing: dict[str, Any] | None, additions: list[dict[str, Any]]) -> dict[str, Any]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in (existing or {}).get("events", []):
        if isinstance(row, dict) and row.get("event_id"):
            indexed[str(row["event_id"])] = row
    for row in additions:
        indexed[str(row["event_id"])] = row
    events = sorted(indexed.values(), key=lambda row: (str(row.get("date") or ""), str(row.get("symbol") or "")), reverse=True)
    return {
        "schema_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "event_id_semantics": "stable hash of symbol, deterministic theme, date and normalized headline",
        "events": events,
    }
