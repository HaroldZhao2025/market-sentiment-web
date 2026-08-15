from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_sentiment.cli import fulfill_company_data as base


def target_rows(
    companies: list[dict[str, Any]],
    news_dir: Path,
    history_dir: Path,
    attempts: dict[str, Any],
    batch_size: int,
) -> list[dict[str, Any]]:
    """Prioritize absent news first, then absent price history, then stale rows."""
    missing_news: list[dict[str, Any]] = []
    missing_history: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        news_ok = base.news_ready(news_dir / f"{symbol}.json")
        history_ok = base.history_ready(history_dir / f"{symbol}.json")
        if not news_ok:
            missing_news.append(company)
            continue
        if not history_ok:
            missing_history.append(company)
            continue

        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        last = str(meta.get("last_attempt_utc") or "")
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00")) if last else None
        except ValueError:
            last_dt = None
        if last_dt is None or now - last_dt >= timedelta(days=7):
            stale.append(company)

    ordered = missing_news + missing_history + stale
    return ordered[: max(1, batch_size)]


def main() -> None:
    base.target_rows = target_rows
    base.main()


if __name__ == "__main__":
    main()
