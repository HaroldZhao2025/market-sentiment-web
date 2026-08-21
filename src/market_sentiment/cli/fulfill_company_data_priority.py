from __future__ import annotations

from collections import defaultdict
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


def _article_key(item: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("title_key") or item.get("title") or ""),
        str(item.get("url") or ""),
        str(item.get("ts") or ""),
    )


def _free_public(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Exclude credentialed/provider-specific items from the free-public archive."""
    return [item for item in items if str(item.get("provider") or "").lower() != "finnhub"]


def retain_history(items: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    """Retain recent depth while reserving evidence across historical calendar months."""
    clean = base.deduplicate_news(_free_public(items))
    limit = max(1, max_items)
    if len(clean) <= limit:
        return clean

    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in clean:
        stamp = base._ts(item.get("ts"))
        if stamp is None:
            continue
        by_month[stamp.strftime("%Y-%m")].append(item)

    month_count = max(1, len(by_month))
    per_month = min(3, max(1, limit // max(1, month_count * 2)))
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()

    for month in sorted(by_month, reverse=True):
        for item in by_month[month][:per_month]:
            key = _article_key(item)
            if key in selected_keys:
                continue
            selected.append(item)
            selected_keys.add(key)
            if len(selected) >= limit:
                break
        if len(selected) >= limit:
            break

    for item in clean:
        if len(selected) >= limit:
            break
        key = _article_key(item)
        if key in selected_keys:
            continue
        selected.append(item)
        selected_keys.add(key)

    selected.sort(key=lambda item: str(item.get("ts") or ""), reverse=True)
    return selected[:limit]


def collect_historical_news(ticker: str, company_name: str, days: int, max_items: int) -> list[dict[str, Any]]:
    """Search dated Yahoo/Google public discovery windows for up to three years."""
    now = base.pd.Timestamp.now(tz="UTC")
    items = base.collect_company_news(ticker, days=min(days, 180), max_items=max_items, company_name=company_name)
    horizon = min(max(1, days), NEWS_HISTORY_DAYS_TARGET)
    if horizon > 120:
        edges = list(range(0, horizon + 1, 120))
        if not edges or edges[-1] != horizon:
            edges.append(horizon)
        for left, right in zip(edges[:-1], edges[1:]):
            if right <= left:
                continue
            end = now - base.pd.Timedelta(days=left)
            start = now - base.pd.Timedelta(days=right)
            items.extend(base.google_news_window(ticker, company_name, start, end, count=100))
    return retain_history(items, max_items)


def collect_finnhub_history(companies: list[dict[str, Any]], days: int) -> dict[str, list[dict[str, Any]]]:
    """Production policy deliberately disables credentialed Finnhub history."""
    return {}


def merge_news(existing: list[dict[str, Any]], fresh: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    return retain_history([*fresh, *existing], max_items)


def history_payload(symbol: str, dates: list[str], prices: list[float | None], news: list[dict[str, Any]]) -> dict[str, Any]:
    payload = base.history_payload(symbol, dates, prices, _free_public(news))
    payload["source_policy"] = "free_public_only"
    payload["sentiment_source"] = "Scored retained Yahoo public + Google News RSS evidence; missing days remain missing"
    return payload


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

    return (missing_news + missing_history + archive_migration + shallow_news + stale)[: max(1, batch_size)]


def main() -> None:
    base.target_rows = target_rows
    base.collect_historical_news = collect_historical_news
    base.collect_finnhub_history = collect_finnhub_history
    base.merge_news = merge_news
    base.history_payload = history_payload
    base.main()


if __name__ == "__main__":
    main()
