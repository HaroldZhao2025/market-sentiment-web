"""Distinguish fetch attempts and archive changes from actual article recency."""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit


def article_timestamp(item: dict[str, Any]) -> datetime | None:
    value = item.get("ts") or item.get("date")
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        stamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _keys(item: dict[str, Any]) -> set[str]:
    title = unicodedata.normalize("NFKC", str(item.get("title") or item.get("headline") or "")).lower()
    title = " ".join(re.sub(r"[^\w]+", " ", title).split())
    stamp = article_timestamp(item)
    if not title or stamp is None:
        return set()
    keys = {f"title:{stamp.date().isoformat()}:{title}"}
    try:
        url = urlsplit(str(item.get("url") or item.get("link") or ""))
        if url.scheme in {"http", "https"} and url.hostname:
            query = urlencode(sorted((k, v) for k, v in parse_qsl(url.query, keep_blank_values=True)
                                     if not re.match(r"^(utm_.+|fbclid|gclid|mc_cid|mc_eid|guccounter|guce_referrer|guce_referrer_sig)$", k, re.I)))
            path = re.sub(r"/+", "/", url.path).rstrip("/")
            if path or query:
                keys.add(f"url:{url.netloc.lower().removeprefix('www.')}{path}?{query}")
    except ValueError:
        pass
    return keys


def latest_article_timestamp(articles: list[dict[str, Any]], now: datetime) -> str | None:
    stamps = [stamp for item in articles if isinstance(item, dict)
              if (stamp := article_timestamp(item)) is not None and stamp <= now + timedelta(hours=2)]
    return max(stamps).isoformat() if stamps else None


def news_refresh_metadata(
    previous: dict[str, Any], fetched: list[dict[str, Any]], retained: list[dict[str, Any]],
    now: datetime, *, fetch_error_type: str | None = None,
) -> dict[str, Any]:
    """Report this attempt without calling an empty provider response a successful fetch.

    Some existing collectors swallow provider errors and return an empty list. Such
    responses are explicitly ambiguous (empty_or_failed), not a verified success.
    New counts exclude retained old articles, repeated fetches and scoring changes.
    """
    previous = previous if isinstance(previous, dict) else {}
    existing = previous.get("articles")
    existing = existing if isinstance(existing, list) else []
    known = set().union(*(_keys(row) for row in existing if isinstance(row, dict)))
    fetched_keys: set[str] = set()
    fetched_count = 0
    for row in fetched:
        keys = _keys(row)
        if keys and not keys & fetched_keys:
            fetched_count += 1
        fetched_keys.update(keys)
    new_count = 0
    for row in retained:
        keys = _keys(row)
        if keys and keys & fetched_keys and not keys & known:
            new_count += 1
        known.update(keys)
    status = ("partial_error" if fetched_count else "error") if fetch_error_type else (
        "empty_or_failed" if not fetched_count else "updated" if new_count else "no_new_articles"
    )
    return {
        "last_attempt_utc": now.isoformat(),
        # Legacy update timestamps are retained, not retroactively certified as fresh.
        "updated_at_utc": now.isoformat() if retained != existing else previous.get("updated_at_utc"),
        "latest_article_at_utc": latest_article_timestamp(retained, now),
        "last_nonempty_fetch_utc": now.isoformat() if fetched_count else previous.get("last_nonempty_fetch_utc"),
        "fetched_article_count": fetched_count,
        "new_article_count": new_count,
        "refresh_status": status,
        "fetch_error_type": fetch_error_type,
    }
