from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def batch_targets(companies: list[dict[str, Any]], earnings_dir: Path, batch_size: int, batch_index: int, priority_symbols: list[str]) -> list[dict[str, Any]]:
    by_symbol = {str(row.get("ticker") or "").upper(): row for row in companies if row.get("ticker")}
    missing = [row for row in companies if not has_structured_call(earnings_dir / f"{row.get('ticker')}.json")]
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for symbol in priority_symbols:
        row = by_symbol.get(symbol.upper())
        if row is not None and not has_structured_call(earnings_dir / f"{symbol.upper()}.json"):
            ordered.append(row); seen.add(symbol.upper())
    for row in missing:
        symbol = str(row.get("ticker") or "").upper()
        if symbol and symbol not in seen:
            ordered.append(row); seen.add(symbol)
    if not ordered:
        return []
    size = max(1, batch_size)
    start = max(0, batch_index) * size
    if start >= len(ordered): start = 0
    batch = ordered[start:start + size]
    priority_set = {s.upper() for s in priority_symbols}
    priority_rows = [row for row in ordered if str(row.get("ticker") or "").upper() in priority_set]
    merged, merged_seen = [], set()
    for row in priority_rows + batch:
        symbol = str(row.get("ticker") or "").upper()
        if symbol and symbol not in merged_seen:
            merged.append(row); merged_seen.add(symbol)
    return merged[:size + len(priority_rows)]


def merge_artifact(base: dict[str, Any], public_calls: list[dict[str, Any]], public_links: list[dict[str, Any]]) -> dict[str, Any]:
    artifact = dict(base)
    existing_calls = rows(artifact.get("calls"))
    by_url = {str(call.get("source_url") or ""): call for call in existing_calls if call.get("source_url")}
    for call in public_calls:
        url = str(call.get("source_url") or "")
        if url: by_url[url] = call
    combined = list(by_url.values())
    combined.sort(key=lambda call: (not complete_call(call), str(call.get("date") or "")), reverse=False)
    artifact["calls"] = combined
    existing_links = rows(artifact.get("call_links"))
    link_urls = {str(link.get("url") or "") for link in existing_links}
    for link in public_links:
        url = str(link.get("url") or "")
        if url and url not in link_urls:
            existing_links.append(link); link_urls.add(url)
    artifact["call_links"] = existing_links[:20]
    artifact["schema_version"] = max(7, int(artifact.get("schema_version") or 0))
    artifact["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    artifact.setdefault("methodology", {})
    artifact["methodology"]["source_policy"] = "free_public_only"
    artifact["methodology"]["fulfillment"] = "SEC exhibits plus free public transcript pages; transcript text is analyzed transiently and not redistributed."
    return artifact


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-root", default="apps/web/public"); parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-index", type=int, default=0); parser.add_argument("--priority-symbols", default="AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA")
    parser.add_argument("--sec-days", type=int, default=550); args = parser.parse_args()
    public_root = Path(args.public_root); v5 = public_root / "data" / "v5"; news_dir = v5 / "news"; earnings_dir = v5 / "earnings"; earnings_dir.mkdir(parents=True, exist_ok=True)
    companies = symbol_rows(public_root)
    if not companies: raise RuntimeError("No extended-universe companies available for fulfillment")
    priority_symbols = [x.strip().upper() for x in args.priority_symbols.split(",") if x.strip()]
    targets = batch_targets(companies, earnings_dir, args.batch_size, args.batch_index, priority_symbols)
    if not targets: print("EARNINGS FULFILLMENT OK | no incomplete calls"); return
    model: FinBERT | None = None; results: list[dict[str, Any]] = []
    for index, company in enumerate(targets, 1):
        symbol = str(company.get("ticker") or "").upper(); name = str(company.get("name") or symbol)
        print(f"[CALL {index}/{len(targets)}] {symbol} {name}", flush=True)
        news_payload = load_json(news_dir / f"{symbol}.json", {}); news_rows = rows(news_payload.get("articles") if isinstance(news_payload, dict) else None)
        end = datetime.now(timezone.utc).date(); start = end.fromordinal(max(1, end.toordinal() - max(90, args.sec_days)))
        try: sec_evidence = fetch_earnings_evidence(symbol, start.isoformat(), end.isoformat())
        except Exception: sec_evidence = []
        base = build_free_earnings_intelligence(symbol, sec_evidence)
        base_complete = any(complete_call(call) for call in rows(base.get("calls")))
        if not base_complete:
            model = model or FinBERT(); public_calls, public_links = fulfill_public_transcript(symbol, name, news_rows, model=model)
        else: public_calls, public_links = [], []
        artifact = merge_artifact(base, public_calls, public_links); atomic_json(earnings_dir / f"{symbol}.json", artifact)
        calls = rows(artifact.get("calls")); complete = [call for call in calls if complete_call(call)]
        result = {"symbol": symbol, "structured_calls": len(calls), "complete_calls": len(complete), "call_links": len(rows(artifact.get("call_links"))), "sec_evidence": len(sec_evidence), "latest_source": str(complete[0].get("source") or "") if complete else ""}
        results.append(result); print(f"[CALL RESULT] {symbol} complete={len(complete)} total={len(calls)} links={result['call_links']} sec={len(sec_evidence)}", flush=True)
        if symbol == "AAPL" and "AAPL" in priority_symbols and not complete:
            atomic_json(v5 / "earnings_fulfillment_status.json", {"schema_version": 2, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "batch_index": args.batch_index, "batch_size": args.batch_size, "source_policy": "free_public_only", "results": results})
            raise RuntimeError("AAPL fulfillment gate failed: Overall, Prepared, Q&A and Tone Shift must all be available")
    atomic_json(v5 / "earnings_fulfillment_status.json", {"schema_version": 2, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "batch_index": args.batch_index, "batch_size": args.batch_size, "source_policy": "free_public_only", "results": results})
    successful = sum(1 for row in results if int(row["complete_calls"]) > 0)
    print(f"EARNINGS FULFILLMENT OK | targets={len(results)} complete={successful} source_policy=free_public_only")


if __name__ == "__main__": main()
