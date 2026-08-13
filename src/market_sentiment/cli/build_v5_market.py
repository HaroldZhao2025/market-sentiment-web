from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.v5_earnings import build_earnings_intelligence
from market_sentiment.v5_events import article_events, merge_event_store
from market_sentiment.v5_news import collect_company_news, score_news
from market_sentiment.v5_universe import build_extended_universe


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(tmp, path)


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if pd.notna(number) else None


def close_series(frame: pd.DataFrame, ticker: str) -> pd.Series | None:
    if frame is None or frame.empty:
        return None
    columns = frame.columns
    if isinstance(columns, pd.MultiIndex):
        for key in ((ticker, "Close"), ("Close", ticker), (ticker.replace("-", "."), "Close"), ("Close", ticker.replace("-", "."))):
            if key in columns:
                return pd.to_numeric(frame[key], errors="coerce")
        matches = [column for column in columns if "Close" in tuple(str(x) for x in column)]
        if len(matches) == 1:
            return pd.to_numeric(frame[matches[0]], errors="coerce")
    elif "Close" in columns:
        return pd.to_numeric(frame["Close"], errors="coerce")
    return None


def market_snapshots(tickers: list[str]) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for start in range(0, len(tickers), 80):
        chunk = tickers[start : start + 80]
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


def rotation_subset(companies: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or limit >= len(companies):
        return companies
    offset = date.today().toordinal() % len(companies)
    ordered = companies[offset:] + companies[:offset]
    return ordered[:limit]


def core_public_snapshot(public_root: Path, ticker: str) -> dict[str, Any]:
    return load_json(public_root / "data" / "ticker" / f"{ticker}.json", {})


def build_sec_fallback(symbol: str, days: int = 120) -> list[dict[str, Any]]:
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(days=days)
    try:
        frame = fetch_earnings_docs(symbol, start.date().isoformat(), end.date().isoformat())
    except Exception:
        return []
    if frame.empty:
        return []
    docs: list[dict[str, Any]] = []
    for _, row in frame.tail(20).iterrows():
        docs.append(
            {
                "ts": pd.Timestamp(row.get("ts")).isoformat() if row.get("ts") is not None else "",
                "title": str(row.get("title") or ""),
                "url": str(row.get("url") or ""),
                "source": "SEC EDGAR",
                "document_type": "filing",
            }
        )
    return docs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Phase 5 extended-market, richer-news, earnings and event artifacts.")
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--state-root", default="data/v5")
    parser.add_argument("--news-refresh-limit", type=int, default=120, help="Companies refreshed for rich news on this run; <=0 means all.")
    parser.add_argument("--news-days", type=int, default=45)
    parser.add_argument("--news-max-items", type=int, default=60)
    parser.add_argument("--score-news", action="store_true")
    parser.add_argument("--earnings-limit", type=int, default=12)
    parser.add_argument("--earnings-quarters", type=int, default=4)
    parser.add_argument("--refresh-all-news", action="store_true")
    args = parser.parse_args()

    public_root = Path(args.public_root)
    public_data = public_root / "data"
    state_root = Path(args.state_root)
    v5_public = public_data / "v5"
    v5_public.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)

    companies = build_extended_universe()
    if len(companies) < 700:
        raise RuntimeError(f"Extended universe unexpectedly small: {len(companies)}")

    tickers = [str(company["ticker"]) for company in companies]
    snapshots = market_snapshots(tickers)
    existing_universe = load_json(v5_public / "universe.json", {})
    previous_rows = existing_universe.get("companies") if isinstance(existing_universe, dict) else []
    previous = {str(row.get("ticker")): row for row in previous_rows if isinstance(row, dict) and row.get("ticker")}

    for company in companies:
        ticker = str(company["ticker"])
        core = core_public_snapshot(public_root, ticker)
        snap = snapshots.get(ticker, {})
        prior = previous.get(ticker, {})
        company["latest_price"] = snap.get("latest_price", prior.get("latest_price"))
        company["return_1d"] = snap.get("return_1d", prior.get("return_1d"))
        company["sentiment"] = core.get("S", [None])[-1] if isinstance(core.get("S"), list) and core.get("S") else prior.get("sentiment")
        company["news_count"] = prior.get("news_count")
        company["earnings_available"] = (v5_public / "earnings" / f"{ticker}.json").exists()

    refresh_limit = 0 if args.refresh_all_news else args.news_refresh_limit
    refresh_companies = rotation_subset(companies, refresh_limit)
    finnhub_token = os.environ.get("FINNHUB_TOKEN", "").strip()
    score_cache = state_root / "headline_scores.json.gz"
    new_events: list[dict[str, Any]] = []

    company_by_ticker = {str(company["ticker"]): company for company in companies}
    for index, company in enumerate(refresh_companies, 1):
        ticker = str(company["ticker"])
        news = collect_company_news(ticker, finnhub_token=finnhub_token, days=args.news_days, max_items=args.news_max_items)
        if args.score_news and news:
            news = score_news(news, score_cache)
        elif not args.score_news:
            old = load_json(v5_public / "news" / f"{ticker}.json", {})
            old_scores = {str(row.get("title_key")): row.get("s") for row in old.get("articles", []) if isinstance(row, dict)} if isinstance(old, dict) else {}
            for row in news:
                if row.get("title_key") in old_scores:
                    row["s"] = old_scores[row["title_key"]]
        scored = [finite(row.get("s")) for row in news]
        valid_scores = [value for value in scored if value is not None]
        atomic_json(
            v5_public / "news" / f"{ticker}.json",
            {
                "schema_version": 1,
                "symbol": ticker,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "article_count": len(news),
                "scored_article_count": len(valid_scores),
                "sentiment_mean": sum(valid_scores) / len(valid_scores) if valid_scores else None,
                "articles": news,
            },
        )
        company["news_count"] = len(news)
        if valid_scores:
            company["sentiment"] = sum(valid_scores) / len(valid_scores)
        new_events.extend(article_events(company, news))
        if index % 25 == 0:
            print(f"[V5 NEWS] refreshed {index}/{len(refresh_companies)}")

    earnings_key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    earnings_targets = [company for company in refresh_companies if company.get("universe") == "S&P 500"][: max(0, args.earnings_limit)]
    for company in earnings_targets:
        ticker = str(company["ticker"])
        artifact = build_earnings_intelligence(ticker, earnings_key, quarters=max(1, args.earnings_quarters)) if earnings_key else {
            "schema_version": 2,
            "symbol": ticker,
            "earnings_history": [],
            "calls": [],
            "methodology": {"transcript_status": "No transcript API key configured; SEC filing fallback only"},
        }
        artifact["filing_fallback"] = build_sec_fallback(ticker)
        atomic_json(v5_public / "earnings" / f"{ticker}.json", artifact)
        company_by_ticker[ticker]["earnings_available"] = bool(artifact.get("calls") or artifact.get("filing_fallback") or artifact.get("earnings_history"))

    existing_events = load_json(state_root / "events.json", load_json(v5_public / "events.json", {}))
    event_store = merge_event_store(existing_events if isinstance(existing_events, dict) else {}, new_events)
    atomic_json(state_root / "events.json", event_store)
    atomic_json(v5_public / "events.json", event_store)

    universe_payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "core_universe": "S&P 500 remains the only universe used for SPX index weighting and attribution",
        "extended_universe": "S&P 500 plus S&P MidCap 400, deduplicated",
        "company_count": len(companies),
        "news_refreshed_this_run": len(refresh_companies),
        "companies": companies,
    }
    atomic_json(v5_public / "universe.json", universe_payload)
    atomic_json(state_root / "universe.json", universe_payload)

    print(
        f"V5 OK | companies={len(companies)} news_refreshed={len(refresh_companies)} "
        f"events={len(event_store.get('events', []))} earnings_targets={len(earnings_targets)}"
    )


if __name__ == "__main__":
    main()
