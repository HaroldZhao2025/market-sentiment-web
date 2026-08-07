#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


def fail(message: str) -> None:
    raise SystemExit(f"VALIDATION ERROR: {message}")


def load_json(path: Path) -> Any:
    if not path.is_file() or path.stat().st_size == 0:
        fail(f"missing or empty file: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"invalid JSON in {path}: {exc}")


def finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def assert_sorted_unique(dates: Iterable[str], label: str) -> list[str]:
    values = [str(x) for x in dates]
    if values != sorted(values):
        fail(f"{label} dates are not sorted")
    if len(values) != len(set(values)):
        fail(f"{label} contains duplicate dates")
    return values


def row_close(row: dict[str, Any]) -> float | None:
    if finite_number(row.get("close")):
        return float(row["close"])
    for key, value in row.items():
        if key.startswith("close_") and finite_number(value):
            return float(value)
    return None


def validate_sp500(path: Path, max_stale_days: int) -> None:
    obj = load_json(path)
    rows = obj.get("daily") if isinstance(obj, dict) else None
    if not isinstance(rows, list) or len(rows) < 20:
        fail(f"S&P artifact has too few daily rows: {path}")

    dates = assert_sorted_unique((r.get("date", "") for r in rows), "S&P")
    closes = [row_close(r) for r in rows]
    valid = [(d, c) for d, c in zip(dates, closes) if c is not None and c > 0]
    if len(valid) < 20:
        fail("S&P artifact has too few valid closes")

    values = [c for _, c in valid]
    median = sorted(values)[len(values) // 2]
    if median < 1000:
        fail(
            f"S&P close scale looks like an ETF rather than the index (median={median:.2f}); "
            "do not use raw SPY prices as SPX levels"
        )
    for prev, cur in zip(values, values[1:]):
        ratio = cur / prev
        if ratio < 0.70 or ratio > 1.30:
            fail(f"implausible S&P level jump detected: {prev:.2f} -> {cur:.2f}")

    try:
        latest = datetime.fromisoformat(valid[-1][0]).date()
        stale_days = (date.today() - latest).days
        if stale_days > max_stale_days:
            fail(f"S&P prices are stale by {stale_days} days (latest={latest})")
    except ValueError:
        fail(f"invalid latest S&P date: {valid[-1][0]}")

    print(f"OK S&P: rows={len(rows)}, valid_closes={len(valid)}, latest={valid[-1][0]}")


def validate_portfolio(path: Path) -> None:
    obj = load_json(path)
    if not isinstance(obj, dict):
        fail("portfolio artifact must be a JSON object")
    dates = obj.get("dates")
    equity = obj.get("equity")
    returns = obj.get("portfolio_return")
    if not all(isinstance(x, list) for x in (dates, equity, returns)):
        fail("portfolio dates/equity/portfolio_return must be arrays")
    if not (len(dates) == len(equity) == len(returns)) or len(dates) < 10:
        fail(
            f"portfolio series length mismatch or too short: dates={len(dates)}, "
            f"equity={len(equity)}, returns={len(returns)}"
        )
    assert_sorted_unique(dates, "portfolio")
    if not all(finite_number(x) and float(x) > 0 for x in equity):
        fail("portfolio equity contains non-finite or non-positive values")
    if not all(finite_number(x) for x in returns):
        fail("portfolio returns contain non-finite values")
    if min(float(x) for x in returns) <= -1:
        fail("portfolio contains a return <= -100%")

    holdings = obj.get("holdings")
    if not isinstance(holdings, list) or not holdings:
        fail("portfolio has no holdings history")
    meta = obj.get("meta") or {}
    if int(meta.get("universe_size_used") or 0) < 10:
        fail("portfolio universe is too small")

    gross = obj.get("gross_exposure")
    if isinstance(gross, list) and gross:
        valid_gross = [float(x) for x in gross if finite_number(x)]
        if valid_gross and max(valid_gross) > 1.05:
            fail(f"portfolio gross exposure exceeds configured limit: {max(valid_gross):.4f}")

    print(
        f"OK portfolio: days={len(dates)}, holdings={len(holdings)}, "
        f"universe={meta.get('universe_size_used')}"
    )


def validate_tickers(path: Path, minimum: int) -> None:
    if not path.is_dir():
        fail(f"ticker directory missing: {path}")
    files = [p for p in path.glob("*.json") if p.stat().st_size > 0]
    if len(files) < minimum:
        fail(f"too few non-empty ticker JSON files: {len(files)} < {minimum}")
    print(f"OK tickers: files={len(files)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sp500", required=True, type=Path)
    ap.add_argument("--portfolio", required=True, type=Path)
    ap.add_argument("--ticker-dir", required=True, type=Path)
    ap.add_argument("--minimum-tickers", type=int, default=25)
    ap.add_argument("--max-stale-days", type=int, default=10)
    args = ap.parse_args()

    validate_sp500(args.sp500, max_stale_days=max(1, args.max_stale_days))
    validate_portfolio(args.portfolio)
    validate_tickers(args.ticker_dir, minimum=max(1, args.minimum_tickers))
    print("All generated artifacts passed validation.")


if __name__ == "__main__":
    main()
