"""Fair, bounded scheduling for company news refreshes (no network dependencies)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _timestamp(value: Any, now: datetime) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        # Older artifacts may have omitted the UTC offset.
        result = result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)
        # A corrupt future timestamp must not suppress refresh indefinitely.
        return None if result > now + timedelta(minutes=5) else result
    except (ValueError, OverflowError):
        return None


def select_company_targets(
    companies: list[dict[str, Any]],
    news_dir: Path,
    history_dir: Path,
    attempts: dict[str, Any],
    batch_size: int,
    *,
    now: datetime | None = None,
    refresh_interval: timedelta = timedelta(hours=6),
    news_depth_target: int = 360,
    news_history_days_target: int = 1095,
) -> list[dict[str, Any]]:
    """Select least-recently-attempted eligible companies before coverage priority.

    Missing/thin archives are often permanently below the depth target. Giving them
    unconditional priority starves healthy but stale companies. All companies share
    a retry cooldown; coverage priority only breaks equal-age ties. Selection is
    deterministic and does not mutate articles, attempt timestamps, or caller data.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if refresh_interval <= timedelta(0):
        raise ValueError("refresh_interval must be positive")
    now = now or datetime.now(timezone.utc)
    now = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    oldest = datetime.min.replace(tzinfo=timezone.utc)
    candidates: list[tuple[datetime, int, str, dict[str, Any]]] = []
    seen: set[str] = set()

    for company in companies:
        symbol = str(company.get("ticker") or "").strip().upper()
        if not symbol or symbol in seen or any(c in symbol for c in ("/", "\\")) or symbol in {".", ".."}:
            continue
        seen.add(symbol)
        news = _load_object(news_dir / f"{symbol}.json")
        meta = attempts.get(symbol)
        meta = meta if isinstance(meta, dict) else {}
        # Prefer actual attempts, including failed/empty fetches, for retry pacing.
        last = _timestamp(meta.get("last_attempt_utc"), now)
        if "last_attempt_utc" not in meta:
            last = _timestamp(news.get("updated_at_utc"), now)
        if last is not None and now - last < refresh_interval:
            continue

        articles = news.get("articles")
        count = len(articles) if isinstance(articles, list) else 0
        history = _load_object(history_dir / f"{symbol}.json")
        dates, prices = history.get("date"), history.get("price")
        history_ok = isinstance(dates, list) and isinstance(prices, list) and min(len(dates), len(prices)) >= 30
        try:
            requested_days = int(news.get("history_days_requested") or 0)
        except (TypeError, ValueError, OverflowError):
            requested_days = 0
        priority = (0 if count == 0 else 1 if not history_ok else
                    2 if requested_days < news_history_days_target else
                    3 if count < news_depth_target else 4)
        candidates.append((last or oldest, priority, symbol, company))

    candidates.sort(key=lambda row: row[:3])
    return [row[3] for row in candidates[:batch_size]]
