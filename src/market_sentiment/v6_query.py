from __future__ import annotations

import hashlib
import json
from typing import Any

ALLOWED_SORTS = {"sentiment", "sentiment_change", "return_1d", "divergence", "contribution", "news", "novelty", "disagreement"}


def normalize_query(query: dict[str, Any]) -> dict[str, Any]:
    universe = str(query.get("universe") or "sp500").lower()
    if universe not in {"sp500", "extended"}:
        universe = "sp500"
    sort = str(query.get("sort") or "sentiment").lower()
    if sort not in ALLOWED_SORTS:
        sort = "sentiment"
    symbols = sorted({str(x).strip().upper().replace(".", "-") for x in query.get("symbols", []) if str(x).strip()})
    try:
        limit = max(1, min(100, int(query.get("limit", 25) or 25)))
    except (TypeError, ValueError):
        limit = 25
    return {
        "universe": universe,
        "symbols": symbols,
        "sector": str(query.get("sector") or "").strip(),
        "event_theme": str(query.get("event_theme") or "").strip(),
        "sort": sort,
        "limit": limit,
    }


def stable_query_id(query: dict[str, Any]) -> str:
    normalized = normalize_query(query)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:20]
