# src/market_sentiment/news_enforcer.py
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import List, Dict, Any

def _norm_ts(item: Dict[str, Any]) -> int:
    if "providerPublishTime" in item and isinstance(item["providerPublishTime"], (int, float)):
        return int(item["providerPublishTime"])
    ts = item.get("publishedAt")
    if isinstance(ts, str):
        from datetime import datetime, timezone
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            return 0
    return int(item.get("time_published", 0)) if isinstance(item.get("time_published", 0), (int, float)) else 0

def _norm_id(item: Dict[str, Any]) -> str:
    for k in ("link", "url"):
        if isinstance(item.get(k), str) and item[k]:
            return item[k]
    for k in ("uuid", "id"):
        if isinstance(item.get(k), str) and item[k]:
            return item[k]
    return f"{item.get('title','')}\x1f{_norm_ts(item)}"

def _dedupe_sort(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        key = _norm_id(it)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    out.sort(key=_norm_ts, reverse=True)
    return out

def _load_prev_news(output_dir: Path, symbol: str) -> List[Dict[str, Any]]:
    p = output_dir / "ticker" / f"{symbol.upper()}.json"
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        for key in ("news", "headlines", "articles"):
            if isinstance(data.get(key), list):
                return list(data[key])
    except Exception:
        pass
    return []

def ensure_top_n_news(symbol: str,
                      current_items: List[Dict[str, Any]],
                      output_dir: Path,
                      n: int = 10) -> List[Dict[str, Any]]:
    cur = _dedupe_sort(current_items or [])
    need = max(0, n - len(cur))
    if need > 0:
        prev = _load_prev_news(output_dir, symbol)
        merged = _dedupe_sort(cur + (prev or []))
        return merged[:n]
    return cur[:n]
