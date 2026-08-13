from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from market_sentiment.cli import build_v6_market


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def _normalize_list_field(path: Path, field: str) -> None:
    if not path.exists():
        return
    payload = _load(path)
    if not isinstance(payload.get(field), list):
        payload[field] = []
        _write(path, payload)


def sanitize_existing_artifacts(public_root: Path, state_root: Path) -> None:
    v5_public = public_root / "data" / "v5"
    _normalize_list_field(v5_public / "universe.json", "companies")
    _normalize_list_field(v5_public / "events.json", "events")
    _normalize_list_field(state_root / "events.json", "events")

    news_dir = v5_public / "news"
    if news_dir.is_dir():
        for path in news_dir.glob("*.json"):
            _normalize_list_field(path, "articles")


def _arg_value(name: str, default: str) -> str:
    args = sys.argv[1:]
    try:
        index = args.index(name)
    except ValueError:
        return default
    return args[index + 1] if index + 1 < len(args) else default


def main() -> None:
    public_root = Path(_arg_value("--public-root", "apps/web/public"))
    state_root = Path(_arg_value("--state-root", "data/v5"))
    sanitize_existing_artifacts(public_root, state_root)
    build_v6_market.main()


if __name__ == "__main__":
    main()
