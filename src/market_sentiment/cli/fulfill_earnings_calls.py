from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_sentiment.broad_transcripts import fulfill_broad_transcript
from market_sentiment.cli.build_v5_market import atomic_json, load_json
from market_sentiment.edgar import fetch_earnings_evidence
from market_sentiment.finbert import FinBERT
from market_sentiment.free_transcripts import fulfill_public_transcript
from market_sentiment.v5_earnings import build_free_earnings_intelligence


def rows(value: object) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def complete_call(call: dict[str, Any]) -> bool:
    summary = call.get("summary") if isinstance(call.get("summary"), dict) else {}
    return all(summary.get(key) is not None for key in ("overall_sentiment", "prepared_sentiment", "qa_sentiment", "qa_tone_shift"))


def symbol_rows(public_root: Path) -> list[dict[str, Any]]:
    payload = load_json(public_root / "data" / "v5" / "universe.json", {})
    companies = rows(payload.get("companies") if isinstance(payload, dict) else None)
    companies.sort(key=lambda row: str(row.get("ticker") or ""))
    return companies


def has_structured_call(path: Path) -> bool:
    payload = load_json(path, {})
    return any(complete_call(call) for call in rows(payload.get("calls") if isinstance(payload, dict) else None))


def _parse_iso(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def retry_eligible(meta: dict[str, Any], cooldown_hours: int, now: datetime) -> bool:
    last = _parse_iso(meta.get("last_attempt_utc"))
    return last is None or now - last >= timedelta(hours=max(1, cooldown_hours))


def batch_targets(
    companies: list[dict[str, Any]],
    earnings_dir: Path,
    batch_size: int,
    priority_symbols: list[str],
    attempts: dict[str, Any],
    cooldown_hours: int,
) -> list[dict[str, Any]]:
    """Sweep every unfulfilled company once before retrying recent misses."""
    now = datetime.now(timezone.utc)
    missing = [row for row in companies if not has_structured_call(earnings_dir / f"{row.get('ticker')}.json")]
    by_symbol = {str(row.get("ticker") or "").upper(): row for row in missing if row.get("ticker")}

    never_attempted: list[dict[str, Any]] = []
    retryable: list[dict[str, Any]] = []
    for row in missing:
        symbol = str(row.get("ticker") or "").upper()
        meta = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        if not meta:
            never_attempted.append(row)
        elif retry_eligible(meta, cooldown_hours, now):
            retryable.append(row)

    retryable.sort(key=lambda row: (
        int((attempts.get(str(row.get("ticker") or "").upper()) or {}).get("attempts") or 0),
        str((attempts.get(str(row.get("ticker") or "").upper()) or {}).get("last_attempt_utc") or ""),
        str(row.get("ticker") or ""),
    ))

    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for symbol in priority_symbols:
        row = by_symbol.get(symbol.upper())
        if row is None:
            continue
        meta = attempts.get(symbol.upper()) if isinstance(attempts.get(symbol.upper()), dict) else {}
        if not meta or retry_eligible(meta, cooldown_hours, now):
            ordered.append(row)
            seen.add(symbol.upper())

    for row in never_attempted + retryable:
        symbol = str(row.get("ticker") or "").upper()
        if symbol and symbol not in seen:
            ordered.append(row)
            seen.add(symbol)

    return ordered[: max(1, batch_size)]


def merge_artifact(base: dict[str, Any], public_calls: list[dict[str, Any]], public_links: list[dict[str, Any]]) -> dict[str, Any]:
    artifact = dict(base)
    existing_calls = rows(artifact.get("calls"))
    by_url = {str(call.get("source_url") or ""): call for call in existing_calls if call.get("source_url")}
    for call in public_calls:
        url = str(call.get("source_url") or "")
        if url:
            by_url[url] = call
    combined = list(by_url.values())
    combined.sort(key=lambda call: (not complete_call(call), str(call.get("date") or "")), reverse=False)
    artifact["calls"] = combined

    existing_links = rows(artifact.get("call_links"))
    link_urls = {str(link.get("url") or "") for link in existing_links}
    for link in public_links:
        url = str(link.get("url") or "")
        if url and url not in link_urls:
            existing_links.append(link)
            link_urls.add(url)
    artifact["call_links"] = existing_links[:30]
    artifact["schema_version"] = max(8, int(artifact.get("schema_version") or 0))
    artifact["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    artifact.setdefault("methodology", {})
    artifact["methodology"]["source_policy"] = "free_public_only"
    artifact["methodology"]["fulfillment"] = (
        "Free public transcript sweep: Stock Analysis/Quartr public pages, MarketBeat public transcripts, "
        "Yahoo/Motley Fool public pages, and SEC exhibits. Transcript text is analyzed transiently and not redistributed."
    )
    return artifact


def coverage_summary(companies: list[dict[str, Any]], earnings_dir: Path) -> dict[str, Any]:
    totals = {"complete": 0, "partial": 0, "link_only": 0, "no_structured_call": 0}
    by_universe: dict[str, dict[str, int]] = {}
    company_rows: list[dict[str, Any]] = []

    for company in companies:
        symbol = str(company.get("ticker") or "").upper()
        universe = str(company.get("universe") or "Other")
        artifact = load_json(earnings_dir / f"{symbol}.json", {})
        calls = rows(artifact.get("calls") if isinstance(artifact, dict) else None)
        complete = [call for call in calls if complete_call(call)]
        links = rows(artifact.get("call_links") if isinstance(artifact, dict) else None)
        filings = rows(artifact.get("filing_fallback") if isinstance(artifact, dict) else None)
        if complete:
            status = "complete"
        elif calls:
            status = "partial"
        elif links:
            status = "link_only"
        else:
            status = "no_structured_call"
        totals[status] += 1
        bucket = by_universe.setdefault(universe, {"total": 0, "complete": 0, "partial": 0, "link_only": 0, "no_structured_call": 0})
        bucket["total"] += 1
        bucket[status] += 1
        company_rows.append({
            "ticker": symbol,
            "status": status,
            "complete_calls": len(complete),
            "structured_calls": len(calls),
            "call_links": len(links),
            "filings": len(filings),
            "source": str(complete[0].get("source") or "") if complete else "",
        })

    total = max(1, len(companies))
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "free_public_only",
        "company_count": len(companies),
        "complete_company_count": totals["complete"],
        "complete_coverage_rate": round(totals["complete"] / total, 6),
        "totals": totals,
        "by_universe": by_universe,
        "companies": company_rows,
    }


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
    priority_symbols = [x.strip().upper() for x in args.priority_symbols.split(",") if x.strip()]
    targets = batch_targets(companies, earnings_dir, args.batch_size, priority_symbols, attempts, args.retry_cooldown_hours)
    if not targets:
        coverage = coverage_summary(companies, earnings_dir)
        atomic_json(v5 / "earnings_coverage.json", coverage)
        print(f"EARNINGS FULFILLMENT OK | no eligible incomplete calls | coverage={coverage['complete_coverage_rate']:.1%}")
        return

    model: FinBERT | None = None
    results: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper()
        name = str(company.get("name") or symbol)
        print(f"[CALL {index}/{len(targets)}] {symbol} {name}", flush=True)
        news_payload = load_json(news_dir / f"{symbol}.json", {})
        news_rows = rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        end = datetime.now(timezone.utc).date()
        start = end.fromordinal(max(1, end.toordinal() - max(90, args.sec_days)))
        try:
            sec_evidence = fetch_earnings_evidence(symbol, start.isoformat(), end.isoformat())
        except Exception:
            sec_evidence = []

        base = build_free_earnings_intelligence(symbol, sec_evidence)
        base_complete = any(complete_call(call) for call in rows(base.get("calls")))
        public_calls: list[dict[str, Any]] = []
        public_links: list[dict[str, Any]] = []
        broad_links: list[dict[str, Any]] = []

        if not base_complete:
            model = model or FinBERT()
            broad_calls, broad_links = fulfill_broad_transcript(symbol, model=model)
            public_calls.extend(broad_calls)
            public_links.extend(broad_links)
            if not any(complete_call(call) for call in broad_calls):
                fallback_calls, fallback_links = fulfill_public_transcript(symbol, name, news_rows, model=model)
                public_calls.extend(fallback_calls)
                public_links.extend(fallback_links)

        artifact = merge_artifact(base, public_calls, public_links)
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
            "latest_source": str(complete[0].get("source") or "") if complete else "",
        }
        results.append(result)
        previous = attempts.get(symbol) if isinstance(attempts.get(symbol), dict) else {}
        attempts[symbol] = {
            "attempts": int(previous.get("attempts") or 0) + 1,
            "last_attempt_utc": now.isoformat(),
            "complete": bool(complete),
            "latest_source": result["latest_source"],
            "broad_candidates": result["broad_candidates"],
        }
        print(
            f"[CALL RESULT] {symbol} complete={len(complete)} total={len(calls)} "
            f"links={result['call_links']} broad={result['broad_candidates']} sec={len(sec_evidence)}",
            flush=True,
        )

    atomic_json(attempts_path, {
        "schema_version": 1,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": attempts,
    })
    coverage = coverage_summary(companies, earnings_dir)
    atomic_json(v5 / "earnings_coverage.json", coverage)
    atomic_json(v5 / "earnings_fulfillment_status.json", {
        "schema_version": 3,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_index": args.batch_index,
        "batch_size": args.batch_size,
        "source_policy": "free_public_only",
        "results": results,
        "coverage": {
            "company_count": coverage["company_count"],
            "complete_company_count": coverage["complete_company_count"],
            "complete_coverage_rate": coverage["complete_coverage_rate"],
        },
    })
    successful = sum(1 for row in results if int(row["complete_calls"]) > 0)
    print(
        f"EARNINGS FULFILLMENT OK | targets={len(results)} complete_in_batch={successful} "
        f"market_complete={coverage['complete_company_count']}/{coverage['company_count']} "
        f"coverage={coverage['complete_coverage_rate']:.1%} source_policy=free_public_only"
    )


if __name__ == "__main__":
    main()
