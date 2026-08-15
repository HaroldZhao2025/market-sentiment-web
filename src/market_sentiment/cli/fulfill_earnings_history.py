from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_sentiment.broad_transcripts import (
    complete_call,
    discover_broad_candidates,
    extract_broad_transcript,
    score_broad_transcript,
)
from market_sentiment.cli.build_v5_market import atomic_json, load_json
from market_sentiment.cli.fulfill_earnings_calls import coverage_summary, merge_artifact, rows, symbol_rows
from market_sentiment.finbert import FinBERT


def complete_calls(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    return [call for call in rows(payload.get("calls")) if complete_call(call)]


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def choose_targets(
    companies: list[dict[str, Any]],
    earnings_dir: Path,
    attempts: dict[str, Any],
    batch_size: int,
    desired_calls: int,
    cooldown_hours: int,
    priority: list[str],
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    priority_rank = {symbol: index for index, symbol in enumerate(priority)}

    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        artifact = load_json(earnings_dir / f"{symbol}.json", {})
        count = len(complete_calls(artifact))
        if count >= desired_calls:
            continue
        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        last = _parse_time(meta.get("last_attempt_utc"))
        if last is not None and now - last < timedelta(hours=max(1, cooldown_hours)):
            continue
        rank = priority_rank.get(symbol, len(priority) + 1)
        attempts_n = int(meta.get("attempts") or 0)
        candidates.append((rank, count * 10 + attempts_n, symbol, company))

    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    return [item[3] for item in candidates[: max(1, batch_size)]]


def discover_history_calls(symbol: str, model: FinBERT, desired_calls: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = discover_broad_candidates(symbol, limit=max(5, desired_calls + 1))
    evidence = [{"title": c.title, "url": c.url, "source": c.source, "ts": c.published} for c in candidates]
    calls: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    seen_urls: set[str] = set()

    for candidate in candidates:
        if candidate.url in seen_urls:
            continue
        parsed = extract_broad_transcript(candidate)
        if parsed is None:
            continue
        call = score_broad_transcript(parsed, model)
        if not call or not complete_call(call):
            continue
        date_key = str(call.get("date") or "")[:10]
        # The same quarterly call may appear on more than one public transcript library.
        # Prefer the first valid source rather than storing duplicate analytics.
        if date_key and date_key in seen_dates:
            continue
        seen_urls.add(candidate.url)
        if date_key:
            seen_dates.add(date_key)
        calls.append(call)
        if len(calls) >= desired_calls:
            break

    calls.sort(key=lambda call: str(call.get("date") or ""), reverse=True)
    return calls, evidence


def history_metrics(companies: list[dict[str, Any]], earnings_dir: Path, desired_calls: int) -> dict[str, Any]:
    distribution = {str(i): 0 for i in range(desired_calls + 1)}
    total_calls = 0
    two_plus = 0
    desired_plus = 0
    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        count = len(complete_calls(load_json(earnings_dir / f"{symbol}.json", {})))
        total_calls += count
        if count >= 2:
            two_plus += 1
        if count >= desired_calls:
            desired_plus += 1
        distribution[str(min(count, desired_calls))] += 1
    return {
        "desired_calls_per_company": desired_calls,
        "total_complete_calls": total_calls,
        "companies_with_2_plus_calls": two_plus,
        "companies_with_desired_history": desired_plus,
        "complete_call_distribution_capped": distribution,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill multi-quarter free-public earnings-call analytics.")
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--desired-calls", type=int, default=4)
    parser.add_argument("--cooldown-hours", type=int, default=72)
    parser.add_argument("--priority-symbols", default="AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA")
    args = parser.parse_args()

    public_root = Path(args.public_root)
    v5 = public_root / "data" / "v5"
    earnings_dir = v5 / "earnings"
    earnings_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = v5 / "earnings_history_attempts.json"
    attempt_payload = load_json(attempts_path, {})
    attempts = attempt_payload.get("symbols") if isinstance(attempt_payload, dict) and isinstance(attempt_payload.get("symbols"), dict) else {}
    companies = symbol_rows(public_root)
    if len(companies) < 1300:
        raise RuntimeError(f"Company universe unexpectedly small: {len(companies)}")

    desired = max(2, min(8, args.desired_calls))
    priority = [item.strip().upper() for item in args.priority_symbols.split(",") if item.strip()]
    targets = choose_targets(companies, earnings_dir, attempts, args.batch_size, desired, args.cooldown_hours, priority)

    if not targets:
        coverage = coverage_summary(companies, earnings_dir)
        coverage["history"] = history_metrics(companies, earnings_dir, desired)
        atomic_json(v5 / "earnings_coverage.json", coverage)
        print("EARNINGS HISTORY OK | no eligible history gaps")
        return

    model = FinBERT()
    now = datetime.now(timezone.utc)
    results: list[dict[str, Any]] = []
    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper()
        existing = load_json(earnings_dir / f"{symbol}.json", {})
        before = len(complete_calls(existing))
        calls, links = discover_history_calls(symbol, model, desired)
        artifact = merge_artifact(existing if isinstance(existing, dict) else {}, calls, links)
        artifact.setdefault("methodology", {})
        artifact["methodology"]["history_fulfillment"] = (
            f"Up to {desired} recent free-public transcript-derived calls; transcript body text is not redistributed."
        )
        atomic_json(earnings_dir / f"{symbol}.json", artifact)
        after = len(complete_calls(artifact))
        prior = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        attempts[symbol] = {
            "attempts": int(prior.get("attempts") or 0) + 1,
            "last_attempt_utc": now.isoformat(),
            "complete_calls_before": before,
            "complete_calls_after": after,
        }
        results.append({"symbol": symbol, "before": before, "after": after, "discovered": len(calls)})
        print(f"[CALL HISTORY {index}/{len(targets)}] {symbol} {before} -> {after}", flush=True)

    atomic_json(attempts_path, {"schema_version": 1, "updated_at_utc": now.isoformat(), "symbols": attempts})
    coverage = coverage_summary(companies, earnings_dir)
    coverage["history"] = history_metrics(companies, earnings_dir, desired)
    atomic_json(v5 / "earnings_coverage.json", coverage)
    atomic_json(v5 / "earnings_history_fulfillment_status.json", {
        "schema_version": 1,
        "generated_at_utc": now.isoformat(),
        "source_policy": "free_public_only",
        "desired_calls_per_company": desired,
        "results": results,
        "history": coverage["history"],
    })
    improved = sum(1 for row in results if row["after"] > row["before"])
    print(
        f"EARNINGS HISTORY OK | targets={len(results)} improved={improved} "
        f"two_plus={coverage['history']['companies_with_2_plus_calls']} "
        f"four_plus={coverage['history']['companies_with_desired_history']}"
    )


if __name__ == "__main__":
    main()
