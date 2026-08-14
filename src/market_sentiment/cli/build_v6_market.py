from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from market_sentiment.cli.build_v5_market import atomic_json, build_sec_fallback, close_series, finite, load_json
from market_sentiment.v5_earnings import build_earnings_intelligence
from market_sentiment.v5_events import article_events, merge_event_store
from market_sentiment.v5_news import collect_company_news
from market_sentiment.v5_universe import build_extended_universe
from market_sentiment.v6_events import build_event_store_v3
from market_sentiment.v6_news import ReusableNewsScorer


def object_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def rotation_subset(companies: list[dict[str, Any]], limit: int, salt: int = 0) -> list[dict[str, Any]]:
    if limit <= 0 or limit >= len(companies):
        return companies
    offset = (date.today().toordinal() * 17 + salt) % len(companies)
    ordered = companies[offset:] + companies[:offset]
    return ordered[:limit]


def market_snapshots(tickers: list[str]) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for start in range(0, len(tickers), 100):
        chunk = tickers[start : start + 100]
        try:
            frame = yf.download(chunk, period="10d", interval="1d", auto_adjust=False, group_by="ticker", threads=True, progress=False)
        except Exception:
            frame = pd.DataFrame()
        for ticker in chunk:
            series = close_series(frame, ticker)
            if series is None:
                continue
            values = [float(x) for x in series.dropna().tolist() if finite(x) is not None]
            if not values:
                continue
            latest = values[-1]
            previous = values[-2] if len(values) >= 2 else None
            out[ticker] = {
                "latest_price": latest,
                "return_1d": latest / previous - 1 if previous else None,
            }
    return out


def core_sentiment(public_root: Path, ticker: str) -> float | None:
    obj = load_json(public_root / "data" / "ticker" / f"{ticker}.json", {})
    values = obj.get("S") if isinstance(obj, dict) else None
    if isinstance(values, list) and values:
        return finite(values[-1])
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh extended U.S. company intelligence artifacts.")
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--state-root", default="data/v5")
    parser.add_argument("--news-refresh-limit", type=int, default=80)
    parser.add_argument("--news-days", type=int, default=45)
    parser.add_argument("--news-max-items", type=int, default=60)
    parser.add_argument("--earnings-limit", type=int, default=5)
    parser.add_argument("--earnings-quarters", type=int, default=4)
    parser.add_argument("--score-news", action="store_true")
    parser.add_argument("--universe-only", action="store_true")
    args = parser.parse_args()

    public_root = Path(args.public_root)
    state_root = Path(args.state_root)
    v5_public = public_root / "data" / "v5"
    v5_public.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)

    companies = build_extended_universe()
    if len(companies) < 1300:
        raise RuntimeError(f"Composite universe unexpectedly small: {len(companies)}")

    previous_payload = load_json(v5_public / "universe.json", {})
    previous_rows = object_rows(previous_payload.get("companies") if isinstance(previous_payload, dict) else None)
    previous = {str(row.get("ticker")): row for row in previous_rows if row.get("ticker")}

    snapshots = market_snapshots([str(company["ticker"]) for company in companies])
    for company in companies:
        ticker = str(company["ticker"])
        prior = previous.get(ticker, {})
        snap = snapshots.get(ticker, {})
        company["latest_price"] = snap.get("latest_price", prior.get("latest_price"))
        company["return_1d"] = snap.get("return_1d", prior.get("return_1d"))
        company["sentiment"] = core_sentiment(public_root, ticker)
        if company["sentiment"] is None:
            company["sentiment"] = prior.get("sentiment")
        company["news_count"] = prior.get("news_count")
        company["earnings_available"] = (v5_public / "earnings" / f"{ticker}.json").exists()

    refresh_companies: list[dict[str, Any]] = [] if args.universe_only else rotation_subset(companies, args.news_refresh_limit)
    finnhub_token = os.environ.get("FINNHUB_TOKEN", "").strip()
    scorer = ReusableNewsScorer(state_root / "headline_scores.json.gz", batch_size=32) if args.score_news else None
    new_events: list[dict[str, Any]] = []

    for index, company in enumerate(refresh_companies, 1):
        ticker = str(company["ticker"])
        news = collect_company_news(ticker, finnhub_token=finnhub_token, days=args.news_days, max_items=args.news_max_items)
        if scorer is not None and news:
            news = scorer.score(news)
        elif news:
            old = load_json(v5_public / "news" / f"{ticker}.json", {})
            old_articles = object_rows(old.get("articles") if isinstance(old, dict) else None)
            old_scores = {str(row.get("title_key")): row.get("s") for row in old_articles if row.get("title_key")}
            for row in news:
                if row.get("title_key") in old_scores:
                    row["s"] = old_scores[row["title_key"]]

        scores = [value for value in (finite(row.get("s")) for row in news) if value is not None]
        atomic_json(v5_public / "news" / f"{ticker}.json", {
            "schema_version": 3,
            "symbol": ticker,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "article_count": len(news),
            "scored_article_count": len(scores),
            "sentiment_mean": sum(scores) / len(scores) if scores else None,
            "articles": news,
        })
        company["news_count"] = len(news)
        if scores:
            company["sentiment"] = sum(scores) / len(scores)
        new_events.extend(article_events(company, news))
        if index % 20 == 0:
            print(f"[NEWS] refreshed {index}/{len(refresh_companies)}")

    # Earnings rotates across the entire Composite 1500 coverage, not just large caps.
    earnings_targets = [] if args.universe_only else rotation_subset(companies, args.earnings_limit, salt=97)
    earnings_key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    company_by_ticker = {str(company["ticker"]): company for company in companies}

    for company in earnings_targets:
        ticker = str(company["ticker"])
        if earnings_key:
            artifact = build_earnings_intelligence(ticker, earnings_key, quarters=max(1, args.earnings_quarters))
        else:
            artifact = {
                "schema_version": 2,
                "symbol": ticker,
                "earnings_history": [],
                "calls": [],
                "methodology": {"transcript_status": "Transcript provider not configured; SEC filing fallback only"},
            }
        artifact["filing_fallback"] = build_sec_fallback(ticker)
        atomic_json(v5_public / "earnings" / f"{ticker}.json", artifact)
        company_by_ticker[ticker]["earnings_available"] = bool(artifact.get("calls") or artifact.get("filing_fallback") or artifact.get("earnings_history"))

    existing_events = load_json(state_root / "events.json", load_json(v5_public / "events.json", {}))
    article_store = merge_event_store(existing_events if isinstance(existing_events, dict) else {}, new_events)
    event_store_v3 = build_event_store_v3(article_store, window_days=2)
    atomic_json(state_root / "events.json", article_store)
    atomic_json(v5_public / "events.json", article_store)
    atomic_json(v5_public / "event_instances.json", event_store_v3)

    payload = {
        "schema_version": 3,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "core_universe": "S&P 500 only for SPX weighting and attribution",
        "extended_universe": "S&P Composite 1500 coverage: S&P 500 + MidCap 400 + SmallCap 600, deduplicated",
        "company_count": len(companies),
        "news_refreshed_this_run": len(refresh_companies),
        "earnings_refreshed_this_run": len(earnings_targets),
        "companies": companies,
    }
    atomic_json(v5_public / "universe.json", payload)
    atomic_json(state_root / "universe.json", payload)

    article_events = object_rows(article_store.get("events") if isinstance(article_store, dict) else None)
    event_instances = object_rows(event_store_v3.get("event_instances") if isinstance(event_store_v3, dict) else None)
    print(
        f"EXTENDED REFRESH OK | companies={len(companies)} news={len(refresh_companies)} "
        f"earnings={len(earnings_targets)} article_events={len(article_events)} "
        f"event_instances={len(event_instances)}"
    )


if __name__ == "__main__":
    main()
