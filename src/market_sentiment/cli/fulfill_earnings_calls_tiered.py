from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_sentiment.broad_transcripts import fulfill_broad_transcript
from market_sentiment.cli.build_v5_market import atomic_json, load_json
from market_sentiment.cli.fulfill_earnings_calls import (
    batch_targets,
    complete_call,
    coverage_summary,
    merge_artifact,
    rows,
    symbol_rows,
)
from market_sentiment.cli.fulfill_earnings_calls_fast import base_artifact, merge_sec_fields
from market_sentiment.edgar import fetch_earnings_evidence
from market_sentiment.finbert import FinBERT
from market_sentiment.free_transcripts import fulfill_public_transcript
from market_sentiment.v5_earnings import build_free_earnings_intelligence


def attempt_meta(attempts: dict[str, Any], symbol: str) -> dict[str, Any]:
    value = attempts.get(symbol)
    return value if isinstance(value, dict) else {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-index", type=int, default=0)
    parser.add_argument("--priority-symbols", default="AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA")
    parser.add_argument("--sec-days", type=int, default=550)
    parser.add_argument("--retry-cooldown-hours", type=int, default=1)
    args = parser.parse_args()

    public_root = Path(args.public_root)
    v5 = public_root / "data" / "v5"
    news_dir = v5 / "news"
    earnings_dir = v5 / "earnings"
    earnings_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = v5 / "earnings_fulfillment_attempts.json"
    attempts_payload = load_json(attempts_path, {})
    attempts = attempts_payload.get("symbols") if isinstance(attempts_payload, dict) and isinstance(attempts_payload.get("symbols"), dict) else {}

    companies = symbol_rows(public_root)
    if not companies:
        raise RuntimeError("No extended-universe companies available for fulfillment")
    priority_symbols = [value.strip().upper() for value in args.priority_symbols.split(",") if value.strip()]
    targets = batch_targets(
        companies,
        earnings_dir,
        args.batch_size,
        priority_symbols,
        attempts,
        args.retry_cooldown_hours,
    )

    if not targets:
        coverage = coverage_summary(companies, earnings_dir)
        atomic_json(v5 / "earnings_coverage.json", coverage)
        print(f"TIERED SWEEP OK | no eligible incomplete calls | coverage={coverage['complete_coverage_rate']:.1%}")
        return

    model: FinBERT | None = None
    results: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper()
        name = str(company.get("name") or symbol)
        previous = attempt_meta(attempts, symbol)
        prior_attempts = int(previous.get("attempts") or 0)
        stage = 1 if prior_attempts <= 0 else 2 if prior_attempts == 1 else 3
        print(f"[TIER {stage} {index}/{len(targets)}] {symbol} {name}", flush=True)

        existing = load_json(earnings_dir / f"{symbol}.json", {})
        base = base_artifact(symbol, existing)
        news_payload = load_json(news_dir / f"{symbol}.json", {})
        news_rows = rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        model = model or FinBERT()

        discovered_calls: list[dict[str, Any]] = []
        discovered_links: list[dict[str, Any]] = []
        sec_evidence: list[dict[str, Any]] = []

        # Tier 1: direct transcript libraries. This is the fastest, broadest pass.
        broad_calls, broad_links = fulfill_broad_transcript(symbol, model=model)
        discovered_calls.extend(broad_calls)
        discovered_links.extend(broad_links)
        search_path = "stockanalysis+marketbeat"

        # Tier 2: public search / retained-news transcript discovery.
        if stage >= 2 and not any(complete_call(call) for call in discovered_calls):
            public_calls, public_links = fulfill_public_transcript(symbol, name, news_rows, model=model)
            discovered_calls.extend(public_calls)
            discovered_links.extend(public_links)
            search_path += "+yahoo_motley_google"

        # Tier 3: SEC exhibit-level deep fallback. It is intentionally deferred so
        # slow EDGAR crawling cannot block the first full-market coverage pass.
        if stage >= 3 and not any(complete_call(call) for call in discovered_calls):
            end = datetime.now(timezone.utc).date()
            start = end.fromordinal(max(1, end.toordinal() - max(90, args.sec_days)))
            try:
                sec_evidence = fetch_earnings_evidence(symbol, start.isoformat(), end.isoformat())
            except Exception:
                sec_evidence = []
            sec_base = build_free_earnings_intelligence(symbol, sec_evidence)
            base = merge_sec_fields(base, sec_base)
            discovered_calls.extend(rows(sec_base.get("calls")))
            discovered_links.extend(rows(sec_base.get("call_links")))
            search_path += "+sec_edgar"

        artifact = merge_artifact(base, discovered_calls, discovered_links)
        artifact.setdefault("methodology", {})
        artifact["methodology"]["fulfillment_stage"] = stage
        atomic_json(earnings_dir / f"{symbol}.json", artifact)

        calls = rows(artifact.get("calls"))
        complete = [call for call in calls if complete_call(call)]
        result = {
            "symbol": symbol,
            "stage": stage,
            "structured_calls": len(calls),
            "complete_calls": len(complete),
            "call_links": len(rows(artifact.get("call_links"))),
            "broad_candidates": len(broad_links),
            "sec_evidence": len(sec_evidence),
            "search_path": search_path,
            "latest_source": str(complete[0].get("source") or "") if complete else "",
        }
        results.append(result)
        attempts[symbol] = {
            "attempts": prior_attempts + 1,
            "last_attempt_utc": now.isoformat(),
            "complete": bool(complete),
            "latest_source": result["latest_source"],
            "broad_candidates": result["broad_candidates"],
            "last_stage": stage,
            "search_path": search_path,
        }
        print(
            f"[TIER RESULT] {symbol} stage={stage} complete={len(complete)} "
            f"broad={len(broad_links)} sec={len(sec_evidence)} source={result['latest_source'] or '-'}",
            flush=True,
        )

    atomic_json(attempts_path, {
        "schema_version": 3,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": attempts,
    })
    coverage = coverage_summary(companies, earnings_dir)
    attempted_once = sum(1 for company in companies if int(attempt_meta(attempts, str(company.get("ticker") or "").upper()).get("attempts") or 0) >= 1)
    attempted_twice = sum(1 for company in companies if int(attempt_meta(attempts, str(company.get("ticker") or "").upper()).get("attempts") or 0) >= 2)
    attempted_thrice = sum(1 for company in companies if int(attempt_meta(attempts, str(company.get("ticker") or "").upper()).get("attempts") or 0) >= 3)
    coverage["search_progress"] = {
        "tier1_attempted": attempted_once,
        "tier2_attempted": attempted_twice,
        "tier3_attempted": attempted_thrice,
    }
    atomic_json(v5 / "earnings_coverage.json", coverage)
    atomic_json(v5 / "earnings_fulfillment_status.json", {
        "schema_version": 5,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_index": args.batch_index,
        "batch_size": args.batch_size,
        "source_policy": "free_public_only",
        "tier_policy": {
            "tier1": ["Stock Analysis / Quartr", "MarketBeat"],
            "tier2": ["Yahoo/Motley Fool public", "Google public discovery"],
            "tier3": ["SEC EDGAR exhibits"],
        },
        "results": results,
        "coverage": {
            "company_count": coverage["company_count"],
            "complete_company_count": coverage["complete_company_count"],
            "complete_coverage_rate": coverage["complete_coverage_rate"],
            "search_progress": coverage["search_progress"],
        },
    })
    successful = sum(1 for row in results if int(row["complete_calls"]) > 0)
    print(
        f"TIERED SWEEP OK | targets={len(results)} complete_in_batch={successful} "
        f"market_complete={coverage['complete_company_count']}/{coverage['company_count']} "
        f"coverage={coverage['complete_coverage_rate']:.1%} "
        f"tier1={attempted_once} tier2={attempted_twice} tier3={attempted_thrice}"
    )


if __name__ == "__main__":
    main()
