from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from market_sentiment.cli.build_v5_market import atomic_json, close_series, finite, load_json
from market_sentiment.edgar import fetch_earnings_evidence
from market_sentiment.v5_earnings import build_free_earnings_intelligence
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


def priority_subset(
    companies: list[dict[str, Any]],
    limit: int,
    artifact_dir: Path,
    list_field: str,
    salt: int = 0,
) -> list[dict[str, Any]]:
    """Prioritize companies with no usable artifact before normal rotation."""
    if limit <= 0:
        return []
    missing: list[dict[str, Any]] = []
    covered: list[dict[str, Any]] = []
    for company in companies:
        ticker = str(company.get("ticker") or "")
        payload = load_json(artifact_dir / f"{ticker}.json", {})
        rows = object_rows(payload.get(list_field) if isinstance(payload, dict) else None)
        if rows:
            covered.append(company)
        else:
            missing.append(company)
    if limit >= len(companies):
        return missing + covered
    rotated = rotation_subset(covered, len(covered), salt=salt)
    return (missing + rotated)[:limit]


def earnings_call_links(news: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    """Keep public call/transcript discovery links found in free news sources."""
    links: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in news:
        title = str(row.get("title") or row.get("headline") or "").strip()
        url = str(row.get("url") or "").strip()
        lower = title.lower()
        is_call = (
            "earnings call" in lower
            or "conference call" in lower
            or ("transcript" in lower and any(token in f" {lower} " for token in (" q1 ", " q2 ", " q3 ", " q4 ", " earnings ")))
        )
        if not title or not url or not is_call or url in seen:
            continue
        seen.add(url)
        links.append(
            {
                "title": title,
                "url": url,
                "ts": str(row.get("ts") or row.get("date") or ""),
                "source": str(row.get("source") or row.get("provider") or "Public source"),
                "provider": str(row.get("provider") or ""),
            }
        )
        if len(links) >= limit:
            break
    return links


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


def _merge_call_links(primary: list[dict[str, Any]], secondary: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [*primary, *secondary]:
        url = str(row.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(row)
        if len(out) >= limit:
            break
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh extended U.S. company intelligence using free public sources only.")
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--state-root", default="data/v5")
    parser.add_argument("--news-refresh-limit", type=int, default=200)
    parser.add_argument("--news-days", type=int, default=45)
    parser.add_argument("--news-max-items", type=int, default=40)
    parser.add_argument("--earnings-limit", type=int, default=25)
    parser.add_argument("--earnings-days", type=int, default=180)
    parser.add_argument("--score-news", action="store_true")
    parser.add_argument("--universe-only", action="store_true")
    args = parser.parse_args()

    public_root = Path(args.public_root)
    state_root = Path(args.state_root)
    v5_public = public_root / "data" / "v5"
    news_dir = v5_public / "news"
    earnings_dir = v5_public / "earnings"
    news_dir.mkdir(parents=True, exist_ok=True)
    earnings_dir.mkdir(parents=True, exist_ok=True)
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
        company["earnings_available"] = (earnings_dir / f"{ticker}.json").exists()

    existing_news_files = len(list(news_dir.glob("*.json")))
    bootstrap_news = existing_news_files < int(len(companies) * 0.90)
    effective_news_limit = max(args.news_refresh_limit, 200) if bootstrap_news else args.news_refresh_limit
    refresh_companies: list[dict[str, Any]] = (
        [] if args.universe_only else priority_subset(companies, effective_news_limit, news_dir, "articles", salt=31)
    )

    scorer = ReusableNewsScorer(state_root / "headline_scores.json.gz", batch_size=32) if args.score_news else None
    new_events: list[dict[str, Any]] = []

    for index, company in enumerate(refresh_companies, 1):
        ticker = str(company["ticker"])
        company_name = str(company.get("name") or "")
        news = collect_company_news(
            ticker,
            days=args.news_days,
            max_items=args.news_max_items,
            company_name=company_name,
        )
        if scorer is not None and news:
            news = scorer.score(news)
        elif news:
            old = load_json(news_dir / f"{ticker}.json", {})
            old_articles = object_rows(old.get("articles") if isinstance(old, dict) else None)
            old_scores = {str(row.get("title_key")): row.get("s") for row in old_articles if row.get("title_key")}
            for row in news:
                if row.get("title_key") in old_scores:
                    row["s"] = old_scores[row["title_key"]]

        scores = [value for value in (finite(row.get("s")) for row in news) if value is not None]
        atomic_json(
            news_dir / f"{ticker}.json",
            {
                "schema_version": 5,
                "symbol": ticker,
                "source_policy": "free_public_only",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "article_count": len(news),
                "scored_article_count": len(scores),
                "sentiment_mean": sum(scores) / len(scores) if scores else None,
                "articles": news,
            },
        )
        company["news_count"] = len(news)
        if scores:
            company["sentiment"] = sum(scores) / len(scores)
        new_events.extend(article_events(company, news))
        if index % 20 == 0:
            print(f"[NEWS] refreshed {index}/{len(refresh_companies)}")

    effective_earnings_limit = max(args.earnings_limit, 25)
    earnings_targets = (
        [] if args.universe_only else priority_subset(companies, effective_earnings_limit, earnings_dir, "earnings_history", salt=97)
    )
    company_by_ticker = {str(company["ticker"]): company for company in companies}
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=max(30, args.earnings_days))

    for index, company in enumerate(earnings_targets, 1):
        ticker = str(company["ticker"])
        try:
            sec_evidence = fetch_earnings_evidence(ticker, start.date().isoformat(), end.date().isoformat())
        except Exception:
            sec_evidence = []
        artifact = build_free_earnings_intelligence(ticker, sec_evidence)

        news_payload = load_json(news_dir / f"{ticker}.json", {})
        news_rows = object_rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        artifact["call_links"] = _merge_call_links(
            object_rows(artifact.get("call_links")),
            earnings_call_links(news_rows),
        )
        atomic_json(earnings_dir / f"{ticker}.json", artifact)
        company_by_ticker[ticker]["earnings_available"] = bool(
            artifact.get("calls")
            or artifact.get("call_links")
            or artifact.get("filing_fallback")
            or artifact.get("earnings_history")
        )
        if index % 10 == 0:
            print(f"[EARNINGS] refreshed {index}/{len(earnings_targets)}")

    # Backfill free public call links from all retained news artifacts.
    call_link_companies = 0
    for company in companies:
        ticker = str(company["ticker"])
        news_payload = load_json(news_dir / f"{ticker}.json", {})
        news_rows = object_rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        links = earnings_call_links(news_rows)
        if not links:
            continue
        call_link_companies += 1
        earnings_path = earnings_dir / f"{ticker}.json"
        artifact = load_json(earnings_path, {})
        if not isinstance(artifact, dict):
            artifact = {}
        artifact.setdefault("schema_version", 5)
        artifact["symbol"] = ticker
        artifact.setdefault("earnings_history", [])
        artifact.setdefault("calls", [])
        artifact.setdefault("filing_fallback", [])
        artifact.setdefault("methodology", {"source_policy": "Free public sources only"})
        artifact["call_links"] = _merge_call_links(object_rows(artifact.get("call_links")), links)
        atomic_json(earnings_path, artifact)
        company_by_ticker[ticker]["earnings_available"] = True

    existing_events = load_json(state_root / "events.json", load_json(v5_public / "events.json", {}))
    article_store = merge_event_store(existing_events if isinstance(existing_events, dict) else {}, new_events)
    event_store_v3 = build_event_store_v3(article_store, window_days=2)
    atomic_json(state_root / "events.json", article_store)
    atomic_json(v5_public / "events.json", article_store)
    atomic_json(v5_public / "event_instances.json", event_store_v3)

    structured_call_companies = 0
    earnings_artifact_count = 0
    sec_transcript_companies = 0
    for path in earnings_dir.glob("*.json"):
        artifact = load_json(path, {})
        if not isinstance(artifact, dict):
            continue
        earnings_artifact_count += 1
        calls = object_rows(artifact.get("calls"))
        if calls:
            structured_call_companies += 1
            if any(str(call.get("source") or "").startswith("SEC EDGAR") for call in calls):
                sec_transcript_companies += 1

    news_artifact_count = len(list(news_dir.glob("*.json")))
    payload = {
        "schema_version": 5,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "free_public_only",
        "news_sources": ["Yahoo public news via yfinance", "Google News RSS"],
        "earnings_sources": ["SEC EDGAR filings/exhibits", "public links discovered in SEC filings and public news", "Yahoo public earnings calendar"],
        "core_universe": "S&P 500 only for SPX weighting and attribution",
        "extended_universe": "S&P Composite 1500 coverage: S&P 500 + MidCap 400 + SmallCap 600, deduplicated",
        "company_count": len(companies),
        "news_refreshed_this_run": len(refresh_companies),
        "news_artifact_count": news_artifact_count,
        "earnings_refreshed_this_run": len(earnings_targets),
        "earnings_artifact_count": earnings_artifact_count,
        "earnings_call_link_companies": call_link_companies,
        "structured_call_companies": structured_call_companies,
        "sec_transcript_companies": sec_transcript_companies,
        "companies": companies,
    }
    atomic_json(v5_public / "universe.json", payload)
    atomic_json(state_root / "universe.json", payload)

    article_event_rows = object_rows(article_store.get("events") if isinstance(article_store, dict) else None)
    event_instance_rows = object_rows(event_store_v3.get("event_instances") if isinstance(event_store_v3, dict) else None)
    print(
        f"EXTENDED REFRESH OK | companies={len(companies)} news_refreshed={len(refresh_companies)} "
        f"news_artifacts={news_artifact_count} earnings_refreshed={len(earnings_targets)} "
        f"earnings_artifacts={earnings_artifact_count} call_links={call_link_companies} "
        f"structured_calls={structured_call_companies} sec_transcripts={sec_transcript_companies} "
        f"article_events={len(article_event_rows)} event_instances={len(event_instance_rows)}"
    )


if __name__ == "__main__":
    main()
