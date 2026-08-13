from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _day(value: object) -> datetime | None:
    raw = str(value or "")[:10]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def stable_instance_id(symbol: str, theme: str, anchor_day: str) -> str:
    raw = f"{symbol.upper()}|{theme}|{anchor_day[:10]}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20]


def cluster_event_instances(events: list[dict[str, Any]], window_days: int = 2) -> list[dict[str, Any]]:
    """Cluster article-level events into auditable symbol/theme event instances.

    Events are joined only when they share symbol and deterministic theme and occur
    within `window_days` of the current cluster anchor. This deliberately avoids
    semantic embedding or opaque LLM clustering.
    """
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        theme = str(row.get("theme") or "").strip()
        day = _day(row.get("date"))
        if not symbol or not theme or day is None:
            continue
        clean = dict(row)
        clean["_parsed_day"] = day
        grouped[(symbol, theme)].append(clean)

    instances: list[dict[str, Any]] = []
    for (symbol, theme), rows in grouped.items():
        rows.sort(key=lambda row: row["_parsed_day"])
        clusters: list[list[dict[str, Any]]] = []
        for row in rows:
            if not clusters:
                clusters.append([row])
                continue
            anchor = clusters[-1][0]["_parsed_day"]
            if row["_parsed_day"] - anchor <= timedelta(days=max(0, window_days)):
                clusters[-1].append(row)
            else:
                clusters.append([row])

        for cluster in clusters:
            anchor_day = cluster[0]["_parsed_day"].date().isoformat()
            sources = sorted({str(row.get("source") or "").strip() for row in cluster if str(row.get("source") or "").strip()})
            scores = [score for score in (finite(row.get("sentiment")) for row in cluster) if score is not None]
            mean = sum(scores) / len(scores) if scores else None
            disagreement = None
            if len(scores) >= 2 and mean is not None:
                disagreement = math.sqrt(sum((score - mean) ** 2 for score in scores) / len(scores))
            ordered = sorted(cluster, key=lambda row: (row["_parsed_day"], str(row.get("title") or "")), reverse=True)
            instances.append(
                {
                    "event_instance_id": stable_instance_id(symbol, theme, anchor_day),
                    "symbol": symbol,
                    "name": cluster[0].get("name"),
                    "sector": cluster[0].get("sector"),
                    "industry": cluster[0].get("industry"),
                    "universe": cluster[0].get("universe"),
                    "theme": theme,
                    "start_date": min(row["_parsed_day"] for row in cluster).date().isoformat(),
                    "end_date": max(row["_parsed_day"] for row in cluster).date().isoformat(),
                    "article_count": len(cluster),
                    "source_count": len(sources),
                    "sources": sources,
                    "sentiment_mean": round(mean, 6) if mean is not None else None,
                    "sentiment_disagreement": round(disagreement, 6) if disagreement is not None else None,
                    "articles": [
                        {
                            "event_id": row.get("event_id"),
                            "date": row.get("date"),
                            "title": row.get("title"),
                            "url": row.get("url"),
                            "source": row.get("source"),
                            "sentiment": row.get("sentiment"),
                        }
                        for row in ordered
                    ],
                }
            )

    instances.sort(key=lambda row: (str(row.get("end_date") or ""), str(row.get("symbol") or "")), reverse=True)
    return instances


def build_event_store_v3(article_store: dict[str, Any] | None, window_days: int = 2) -> dict[str, Any]:
    article_events = (article_store or {}).get("events", [])
    if not isinstance(article_events, list):
        article_events = []
    return {
        "schema_version": 3,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "clustering": {
            "method": "deterministic symbol + event theme + rolling calendar window",
            "window_days": int(window_days),
            "opaque_semantic_clustering": False,
        },
        "event_instances": cluster_event_instances(article_events, window_days=window_days),
        "article_events": article_events,
    }
