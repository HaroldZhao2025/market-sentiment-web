from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_sentiment.cli.build_v5_market import atomic_json, finite, load_json


INDEX_DEFINITIONS: tuple[tuple[str, str, tuple[str, ...] | None], ...] = (
    ("SP500", "S&P 500", ("S&P 500",)),
    ("SP400", "S&P MidCap 400", ("S&P MidCap 400",)),
    ("SP600", "S&P SmallCap 600", ("S&P SmallCap 600",)),
    ("SP1500", "S&P Composite 1500", ("S&P 500", "S&P MidCap 400", "S&P SmallCap 600")),
    ("BROAD_US", "Broad U.S. Equity", None),
)


def rows(value: object) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def observed_daily(history_path: Path) -> list[tuple[str, float]]:
    payload = load_json(history_path, {})
    if not isinstance(payload, dict):
        return []
    dates = payload.get("date")
    sentiment = payload.get("sentiment")
    observed = payload.get("sentiment_observed")
    if not isinstance(dates, list) or not isinstance(sentiment, list):
        return []
    out: list[tuple[str, float]] = []
    n = min(len(dates), len(sentiment))
    for index in range(n):
        value = finite(sentiment[index])
        is_observed = bool(observed[index]) if isinstance(observed, list) and index < len(observed) else value is not None
        if value is None or not is_observed:
            continue
        day = str(dates[index] or "")
        if day:
            out.append((day, float(value)))
    return out


def build_index(
    code: str,
    name: str,
    companies: list[dict[str, Any]],
    history_dir: Path,
    allowed_universes: tuple[str, ...] | None,
) -> dict[str, Any]:
    members = [
        row for row in companies
        if allowed_universes is None or str(row.get("universe") or "") in allowed_universes
    ]
    daily_values: dict[str, list[float]] = defaultdict(list)
    companies_with_history = 0
    companies_with_sentiment = 0
    for company in members:
        symbol = str(company.get("ticker") or "").upper()
        if not symbol:
            continue
        history_path = history_dir / f"{symbol}.json"
        if history_path.is_file():
            companies_with_history += 1
        observations = observed_daily(history_path)
        if observations:
            companies_with_sentiment += 1
        for day, value in observations:
            daily_values[day].append(value)

    daily = []
    denominator = max(1, len(members))
    for day in sorted(daily_values):
        values = daily_values[day]
        daily.append(
            {
                "date": day,
                "sentiment_equal_weighted": sum(values) / len(values),
                "observed_tickers": len(values),
                "constituent_coverage": len(values) / denominator,
            }
        )

    return {
        "code": code,
        "name": name,
        "weighting": "equal_weight_observed",
        "member_count": len(members),
        "companies_with_history": companies_with_history,
        "companies_with_observed_sentiment": companies_with_sentiment,
        "daily": daily,
    }


def build(public_root: Path) -> dict[str, Any]:
    v5 = public_root / "data" / "v5"
    universe_payload = load_json(v5 / "universe.json", {})
    companies = rows(universe_payload.get("companies") if isinstance(universe_payload, dict) else None)
    if len(companies) < 1300:
        raise RuntimeError(f"Company universe unexpectedly small: {len(companies)}")
    history_dir = v5 / "history"
    indexes = [
        build_index(code, name, companies, history_dir, allowed)
        for code, name, allowed in INDEX_DEFINITIONS
    ]
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "free_public_only",
        "methodology": {
            "sentiment": "Equal-weight mean across constituents with a fresh observed company sentiment on that date.",
            "missingness": "No-news company-days are excluded rather than treated as neutral zero.",
            "broad_us": "Broad U.S. Equity uses the currently retained extended universe; IWV additions are labeled as a holdings-based proxy rather than exact Russell index membership.",
        },
        "indexes": indexes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build observed-sentiment histories for multiple U.S. equity index universes.")
    parser.add_argument("--public-root", default="apps/web/public")
    args = parser.parse_args()
    public_root = Path(args.public_root)
    payload = build(public_root)
    target = public_root / "data" / "v5" / "index_sentiment.json"
    atomic_json(target, payload)
    print(
        "MULTI INDEX SENTIMENT OK | "
        + " ".join(
            f"{row['code']}={row['member_count']}members/{len(row['daily'])}days"
            for row in payload["indexes"]
        )
    )


if __name__ == "__main__":
    main()
