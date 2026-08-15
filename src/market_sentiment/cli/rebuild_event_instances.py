from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_sentiment.v5_events import article_events
from market_sentiment.v6_events import cluster_event_instances


def load_json(path: Path, fallback: object) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def object_rows(value: object) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild compact persistent event instances from retained company news.")
    parser.add_argument("--public-root", default="apps/web/public")
    parser.add_argument("--window-days", type=int, default=2)
    args = parser.parse_args()

    public_root = Path(args.public_root)
    v5 = public_root / "data" / "v5"
    universe = load_json(v5 / "universe.json", {})
    companies = object_rows(universe.get("companies") if isinstance(universe, dict) else None)
    if len(companies) < 1300:
        raise RuntimeError(f"Company universe unexpectedly small: {len(companies)}")

    news_dir = v5 / "news"
    all_events: list[dict[str, Any]] = []
    companies_with_news = 0
    for index, company in enumerate(companies, 1):
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        payload = load_json(news_dir / f"{symbol}.json", {})
        news = object_rows(payload.get("articles") if isinstance(payload, dict) else None)
        if news:
            companies_with_news += 1
            all_events.extend(article_events(company, news))
        if index % 250 == 0:
            print(f"[EVENTS] loaded {index}/{len(companies)} companies", flush=True)

    instances = cluster_event_instances(all_events, window_days=max(0, args.window_days))
    payload = {
        "schema_version": 4,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "free_public_only",
        "input": {
            "company_count": len(companies),
            "companies_with_retained_news": companies_with_news,
            "article_event_count": len(all_events),
        },
        "clustering": {
            "method": "deterministic symbol + event theme + rolling calendar window",
            "window_days": max(0, args.window_days),
        },
        "event_instance_count": len(instances),
        "event_instances": instances,
    }
    write_json(v5 / "event_instances.json", payload)
    print(
        f"EVENT INSTANCES OK | companies={companies_with_news}/{len(companies)} "
        f"article_events={len(all_events)} instances={len(instances)}"
    )


if __name__ == "__main__":
    main()
