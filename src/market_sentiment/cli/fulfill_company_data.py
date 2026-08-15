from __future__ import annotations

import argparse
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from market_sentiment.cli.build_v5_market import atomic_json, close_series, finite, load_json
from market_sentiment.v5_news import PUBLIC_UA, canonical_url, collect_company_news, deduplicate_news, normalize_title
from market_sentiment.v6_news import ReusableNewsScorer

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"


def rows(value: object) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def load_companies(public_root: Path) -> list[dict[str, Any]]:
    payload = load_json(public_root / "data" / "v5" / "universe.json", {})
    companies = rows(payload.get("companies") if isinstance(payload, dict) else None)
    companies.sort(key=lambda row: str(row.get("ticker") or ""))
    return companies


def _ts(value: object) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(parsed) else parsed


def google_news_window(ticker: str, company_name: str, start: pd.Timestamp, end: pd.Timestamp, count: int = 100) -> list[dict[str, Any]]:
    query_name = f'"{company_name}"' if company_name else ticker
    query = f'{query_name} {ticker} stock after:{start.date().isoformat()} before:{end.date().isoformat()}'
    try:
        response = requests.get(
            GOOGLE_NEWS_RSS,
            params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
            headers={"User-Agent": PUBLIC_UA},
            timeout=10,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    for item in soup.find_all("item")[: max(1, count)]:
        title = " ".join((item.title.get_text(" ", strip=True) if item.title else "").split())
        link = str(item.link.get_text(strip=True) if item.link else "").strip()
        published = _ts(item.pubDate.get_text(strip=True) if item.pubDate else "")
        if not title or published is None or published < start or published >= end:
            continue
        source_tag = item.find("source")
        source = " ".join(source_tag.get_text(" ", strip=True).split()) if source_tag else "Google News"
        description = item.description.get_text(" ", strip=True) if item.description else ""
        summary = " ".join(BeautifulSoup(description, "lxml").get_text(" ", strip=True).split())
        out.append(
            {
                "ts": published.isoformat(),
                "title": title,
                "summary": summary,
                "url": link,
                "source": source or "Google News",
                "provider": "google_news_rss",
            }
        )
    return out


def collect_historical_news(ticker: str, company_name: str, days: int, max_items: int) -> list[dict[str, Any]]:
    now = pd.Timestamp.now(tz="UTC")
    items = collect_company_news(ticker, days=min(days, 180), max_items=max_items, company_name=company_name)
    # Search older windows explicitly so new companies receive history immediately instead of waiting for future refreshes.
    if days > 120:
        edges = [0, 120, 240, min(days, 365)]
        for left, right in zip(edges[:-1], edges[1:]):
            if right <= left:
                continue
            end = now - pd.Timedelta(days=left)
            start = now - pd.Timedelta(days=right)
            items.extend(google_news_window(ticker, company_name, start, end, count=100))
    return deduplicate_news(items)[: max(1, max_items)]


def history_ready(path: Path) -> bool:
    payload = load_json(path, {})
    if not isinstance(payload, dict):
        return False
    dates = payload.get("date")
    prices = payload.get("price")
    return isinstance(dates, list) and isinstance(prices, list) and min(len(dates), len(prices)) >= 30


def news_ready(path: Path) -> bool:
    payload = load_json(path, {})
    return isinstance(payload, dict) and isinstance(payload.get("articles"), list) and len(payload.get("articles") or []) > 0


def target_rows(companies: list[dict[str, Any]], news_dir: Path, history_dir: Path, attempts: dict[str, Any], batch_size: int) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        if not history_ready(history_dir / f"{symbol}.json") or not news_ready(news_dir / f"{symbol}.json"):
            missing.append(company)
            continue
        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        last = str(meta.get("last_attempt_utc") or "")
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00")) if last else None
        except ValueError:
            last_dt = None
        if last_dt is None or now - last_dt >= timedelta(days=7):
            stale.append(company)
    return (missing + stale)[: max(1, batch_size)]


def download_price_history(symbols: list[str], period: str = "2y") -> dict[str, tuple[list[str], list[float | None]]]:
    if not symbols:
        return {}
    try:
        frame = yf.download(
            symbols,
            period=period,
            interval="1d",
            auto_adjust=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception:
        frame = pd.DataFrame()
    out: dict[str, tuple[list[str], list[float | None]]] = {}
    for symbol in symbols:
        series = close_series(frame, symbol)
        if series is None or series.empty:
            continue
        dates: list[str] = []
        prices: list[float | None] = []
        for idx, value in series.items():
            number = finite(value)
            if number is None:
                continue
            stamp = pd.Timestamp(idx)
            dates.append(stamp.date().isoformat())
            prices.append(float(number))
        if dates:
            out[symbol] = (dates, prices)
    return out


def merge_news(existing: list[dict[str, Any]], fresh: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    return deduplicate_news([*fresh, *existing])[: max(1, max_items)]


def daily_sentiment(news: list[dict[str, Any]]) -> dict[str, float]:
    bucket: dict[str, list[float]] = defaultdict(list)
    for item in news:
        value = finite(item.get("s"))
        stamp = _ts(item.get("ts"))
        if value is None or stamp is None:
            continue
        bucket[stamp.date().isoformat()].append(float(value))
    return {day: sum(values) / len(values) for day, values in bucket.items() if values}


def history_payload(symbol: str, dates: list[str], prices: list[float | None], news: list[dict[str, Any]]) -> dict[str, Any]:
    by_day = daily_sentiment(news)
    sentiment: list[float | None] = []
    observed: list[bool] = []
    for day in dates:
        value = by_day.get(day)
        sentiment.append(round(value, 6) if value is not None else None)
        observed.append(value is not None)
    return {
        "schema_version": 2,
        "symbol": symbol,
        "source_policy": "free_public_only",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "price_source": "Yahoo Finance public market data via yfinance",
        "sentiment_source": "Scored retained free-public news; missing days remain missing",
        "date": dates,
        "price": prices,
        "sentiment": sentiment,
        "sentiment_observed": observed,
    }


def coverage(companies: list[dict[str, Any]], news_dir: Path, history_dir: Path, attempts: dict[str, Any]) -> dict[str, Any]:
    news_count = 0
    history_count = 0
    attempted = 0
    company_rows: list[dict[str, Any]] = []
    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        n_ready = news_ready(news_dir / f"{symbol}.json")
        h_ready = history_ready(history_dir / f"{symbol}.json")
        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        if n_ready:
            news_count += 1
        if h_ready:
            history_count += 1
        if meta:
            attempted += 1
        company_rows.append(
            {
                "ticker": symbol,
                "news_ready": n_ready,
                "history_ready": h_ready,
                "attempted": bool(meta),
                "article_count": int(meta.get("article_count") or 0),
                "history_days": int(meta.get("history_days") or 0),
            }
        )
    total = max(1, len(companies))
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "free_public_only",
        "company_count": len(companies),
        "news_ready_count": news_count,
        "history_ready_count": history_count,
        "attempted_count": attempted,
        "news_coverage_rate": round(news_count / total, 6),
        "history_coverage_rate": round(history_count / total, 6),
        "companies": company_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--batch-size", type=int, default=120)
    parser.add_argument("--news-days", type=int, default=365)
    parser.add_argument("--news-max-items", type=int, default=120)
    parser.add_argument("--history-period", default="2y")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--score-cache", default="data/v5/headline_scores.json.gz")
    args = parser.parse_args()

    public_root = Path(args.public_root)
    v5 = public_root / "data" / "v5"
    news_dir = v5 / "news"
    history_dir = v5 / "history"
    news_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = v5 / "company_data_fulfillment_attempts.json"
    attempts_payload = load_json(attempts_path, {})
    attempts = attempts_payload.get("symbols") if isinstance(attempts_payload, dict) and isinstance(attempts_payload.get("symbols"), dict) else {}

    companies = load_companies(public_root)
    if len(companies) < 1300:
        raise RuntimeError(f"Company universe unexpectedly small: {len(companies)}")
    targets = target_rows(companies, news_dir, history_dir, attempts, args.batch_size)
    if not targets:
        result = coverage(companies, news_dir, history_dir, attempts)
        atomic_json(v5 / "company_data_coverage.json", result)
        print(
            f"COMPANY DATA FULFILLMENT OK | no pending companies | news={result['news_ready_count']}/{result['company_count']} "
            f"history={result['history_ready_count']}/{result['company_count']}"
        )
        return

    symbols = [str(row.get("ticker") or "").upper() for row in targets]
    prices = download_price_history(symbols, period=args.history_period)

    fetched: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 24))) as pool:
        futures = {
            pool.submit(
                collect_historical_news,
                str(company.get("ticker") or "").upper(),
                str(company.get("name") or ""),
                args.news_days,
                args.news_max_items,
            ): company
            for company in targets
        }
        for future in as_completed(futures):
            company = futures[future]
            symbol = str(company.get("ticker") or "").upper()
            try:
                fetched[symbol] = future.result()
            except Exception:
                fetched[symbol] = []

    scorer = ReusableNewsScorer(Path(args.score_cache), batch_size=48)
    now = datetime.now(timezone.utc)
    universe_payload = load_json(v5 / "universe.json", {})
    universe_companies = rows(universe_payload.get("companies") if isinstance(universe_payload, dict) else None)
    universe_by_symbol = {str(row.get("ticker") or "").upper(): row for row in universe_companies}

    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper()
        existing_payload = load_json(news_dir / f"{symbol}.json", {})
        existing = rows(existing_payload.get("articles") if isinstance(existing_payload, dict) else None)
        combined = merge_news(existing, fetched.get(symbol, []), args.news_max_items)
        if combined:
            combined = scorer.score(combined)
        scores = [value for value in (finite(item.get("s")) for item in combined) if value is not None]
        atomic_json(
            news_dir / f"{symbol}.json",
            {
                "schema_version": 6,
                "symbol": symbol,
                "source_policy": "free_public_only",
                "updated_at_utc": now.isoformat(),
                "history_days_requested": args.news_days,
                "article_count": len(combined),
                "scored_article_count": len(scores),
                "sentiment_mean": sum(scores) / len(scores) if scores else None,
                "articles": combined,
            },
        )

        date_price = prices.get(symbol)
        if date_price:
            dates, price_values = date_price
            atomic_json(history_dir / f"{symbol}.json", history_payload(symbol, dates, price_values, combined))
        else:
            old_history = load_json(history_dir / f"{symbol}.json", {})
            dates = old_history.get("date") if isinstance(old_history, dict) else []

        attempts[symbol] = {
            "last_attempt_utc": now.isoformat(),
            "article_count": len(combined),
            "history_days": len(dates) if isinstance(dates, list) else 0,
        }
        row = universe_by_symbol.get(symbol)
        if row is not None:
            row["news_count"] = len(combined)
            row["history_available"] = history_ready(history_dir / f"{symbol}.json")
            if scores:
                row["sentiment"] = sum(scores) / len(scores)
        if index % 20 == 0:
            print(f"[COMPANY DATA] completed {index}/{len(targets)}", flush=True)

    atomic_json(
        attempts_path,
        {"schema_version": 1, "updated_at_utc": now.isoformat(), "symbols": attempts},
    )
    result = coverage(companies, news_dir, history_dir, attempts)
    atomic_json(v5 / "company_data_coverage.json", result)

    if isinstance(universe_payload, dict):
        universe_payload["companies"] = universe_companies
        universe_payload["generated_at_utc"] = now.isoformat()
        universe_payload["news_artifact_count"] = result["news_ready_count"]
        universe_payload["history_artifact_count"] = result["history_ready_count"]
        universe_payload["company_data_coverage_rate"] = min(result["news_coverage_rate"], result["history_coverage_rate"])
        atomic_json(v5 / "universe.json", universe_payload)

    print(
        f"COMPANY DATA FULFILLMENT OK | targets={len(targets)} news={result['news_ready_count']}/{result['company_count']} "
        f"history={result['history_ready_count']}/{result['company_count']} attempted={result['attempted_count']}/{result['company_count']}"
    )


if __name__ == "__main__":
    main()
