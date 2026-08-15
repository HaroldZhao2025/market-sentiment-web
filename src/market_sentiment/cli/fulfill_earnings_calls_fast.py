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
from market_sentiment.edgar import fetch_earnings_evidence
from market_sentiment.finbert import FinBERT
from market_sentiment.free_transcripts import fulfill_public_transcript
from market_sentiment.v5_earnings import build_free_earnings_intelligence


def base_artifact(symbol: str, existing: object) -> dict[str, Any]:
    if isinstance(existing, dict):
        artifact = dict(existing)
    else:
        artifact = {}
    artifact.setdefault("schema_version", 8)
    artifact["symbol"] = symbol
    artifact.setdefault("earnings_history", [])
    artifact.setdefault("calls", [])
    artifact.setdefault("call_links", [])
    artifact.setdefault("filing_fallback", [])
    artifact.setdefault("methodology", {})
    return artifact


def merge_sec_fields(base: dict[str, Any], sec_base: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    if rows(sec_base.get("earnings_history")):
        merged["earnings_history"] = rows(sec_base.get("earnings_history"))
    if rows(sec_base.get("filing_fallback")):
        merged["filing_fallback"] = rows(sec_base.get("filing_fallback"))
    existing_links = rows(merged.get("call_links"))
    seen = {str(row.get("url") or "") for row in existing_links}
    for row in rows(sec_base.get("call_links")):
        url = str(row.get("url") or "")
        if url and url not in seen:
            existing_links.append(row)
            seen.add(url)
    merged["call_links"] = existing_links
    return merged


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--batch-index", type=int, default=0)
    parser.add_argument("--priority-symbols", default="AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA")
    parser.add_argument("--sec-days", type=int, default=550)
    parser.add_argument("--retry-cooldown-hours", type=int, default=24)
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
        print(f"EARNINGS FAST SWEEP OK | no eligible incomplete calls | coverage={coverage['complete_coverage_rate']:.1%}")
        return

    model: FinBERT | None = None
    results: list[dict[str, Any]] = []
    attempt_time = datetime.now(timezone.utc)

    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper()
        name = str(company.get("name") or symbol)
        print(f"[FAST CALL {index}/{len(targets)}] {symbol} {name}", flush=True)

        existing = load_json(earnings_dir / f"{symbol}.json", {})
        base = base_artifact(symbol, existing)
        news_payload = load_json(news_dir / f"{symbol}.json", {})
        news_rows = rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        model = model or FinBERT()

        discovered_calls: list[dict[str, Any]] = []
        discovered_links: list[dict[str, Any]] = []
        broad_calls, broad_links = fulfill_broad_transcript(symbol, model=model)
        discovered_calls.extend(broad_calls)
        discovered_links.extend(broad_links)
        source_path = "broad"

        if not any(complete_call(call) for call in broad_calls):
            fallback_calls, fallback_links = fulfill_public_transcript(symbol, name, news_rows, model=model)
            discovered_calls.extend(fallback_calls)
            discovered_links.extend(fallback_links)
            source_path = "broad+public-search"

        sec_evidence: list[dict[str, Any]] = []
        if not any(complete_call(call) for call in discovered_calls):
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
            source_path = "broad+public-search+sec"

        artifact = merge_artifact(base, discovered_calls, discovered_links)
        atomic_json(earnings_dir / f"{symbol}.json", artifact)
        calls = rows(artifact.get("calls"))
        complete = [call for call in calls if complete_call(call)]
        result = {
            "symbol": symbol,
            "structured_calls": len(calls),
            "complete_calls": len(complete),
            "call_links": len(rows(artifact.get("call_links"))),
            "broad_candidates": len(broad_links),
            "sec_evidence": len(sec_evidence),
            "search_path": source_path,
            "latest_source": str(complete[0].get("source") or "") if complete else "",
        }
        results.append(result)
        previous = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        attempts[symbol] = {
            "attempts": int(previous.get("attempts") or 0) + 1,
            "last_attempt_utc": attempt_time.isoformat(),
            "complete": bool(complete),
            "latest_source": result["latest_source"],
            "broad_candidates": result["broad_candidates"],
            "search_path": source_path,
        }
        print(
            f"[FAST RESULT] {symbol} complete={len(complete)} broad={len(broad_links)} "
            f"sec={len(sec_evidence)} source={result['latest_source'] or '-'}",
            flush=True,
        )

    atomic_json(attempts_path, {
        "schema_version": 2,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": attempts,
    })
    coverage = coverage_summary(companies, earnings_dir)
    atomic_json(v5 / "earnings_coverage.json", coverage)
    atomic_json(v5 / "earnings_fulfillment_status.json", {
        "schema_version": 4,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_index": args.batch_index,
        "batch_size": args.batch_size,
        "source_policy": "free_public_only",
        "search_order": ["Stock Analysis / Quartr", "MarketBeat", "Yahoo/Motley Fool public", "SEC EDGAR"],
        "results": results,
        "coverage": {
            "company_count": coverage["company_count"],
            "complete_company_count": coverage["complete_company_count"],
            "complete_coverage_rate": coverage["complete_coverage_rate"],
        },
    })
    successful = sum(1 for row in results if int(row["complete_calls"]) > 0)
    print(
        f"EARNINGS FAST SWEEP OK | targets={len(results)} complete_in_batch={successful} "
        f"market_complete={coverage['complete_company_count']}/{coverage['company_count']} "
        f"coverage={coverage['complete_coverage_rate']:.1%} source_policy=free_public_only"
    )


if __name__ == "__main__":
    main()
