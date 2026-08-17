from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_sentiment.cli import fulfill_company_data as base

NEWS_DEPTH_TARGET = 360
NEWS_HISTORY_DAYS_TARGET = 1095


def news_metadata(path: Path) -> tuple[int, int]:
    payload = base.load_json(path, {})
    if not isinstance(payload, dict):
        return 0, 0
    articles = payload.get("articles")
    article_count = len(articles) if isinstance(articles, list) else 0
    try:
        history_days = int(payload.get("history_days_requested") or 0)
    except (TypeError, ValueError):
        history_days = 0
    return article_count, history_days


def target_rows(
    companies: list[dict[str, Any]],
    news_dir: Path,
    history_dir: Path,
    attempts: dict[str, Any],
    batch_size: int,
) -> list[dict[str, Any]]:
    """Prioritize missing coverage, archive migration, thin news archives, then stale refreshes."""
    missing_news: list[dict[str, Any]] = []
    missing_history: list[dict[str, Any]] = []
    archive_migration: list[dict[str, Any]] = []
    shallow_news: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        news_path = news_dir / f"{symbol}.json"
        history_path = history_dir / f"{symbol}.json"
        news_ok = base.news_ready(news_path)
        history_ok = base.history_ready(history_path)
        if not news_ok:
            missing_news.append(company)
            continue
        if not history_ok:
            missing_history.append(company)
            continue

        count, history_days_requested = news_metadata(news_path)
        if history_days_requested < NEWS_HISTORY_DAYS_TARGET:
            archive_migration.append(company)
            continue
        if count < NEWS_DEPTH_TARGET:
            shallow_news.append(company)
            continue

        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        last = str(meta.get("last_attempt_utc") or "")
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00")) if last else None
        except ValueError:
            last_dt = None
        if last_dt is None or now - last_dt >= timedelta(days=7):
            stale.append(company)

    ordered = missing_news + missing_history + archive_migration + shallow_news + stale
    return ordered[: max(1, batch_size)]


def main() -> None:
    base.target_rows = target_rows
    base.main()


if __name__ == "__main__":
    main()
