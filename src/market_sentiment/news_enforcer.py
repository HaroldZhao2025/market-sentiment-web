# src/market_sentiment/news_enforcer.py
from __future__ import annotations
import json, re
from pathlib import Path
from typing import List, Dict, Any, Iterable
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

_DROP_QUERY_KEYS = {"siteid", "yptr", "guccounter", "guce_referrer", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "soc_src", "soc_trk"}

def _canonical_url(url: str | None, raw: Dict[str, Any] | None) -> str:
    if raw:
        # yfinance 样式：raw.content.canonicalUrl.url 或 raw.canonicalUrl.url
        for path in (
            ("content", "canonicalUrl", "url"),
            ("canonicalUrl", "url"),
            ("content", "previewUrl"),
        ):
            cur = raw
            for k in path:
                cur = cur.get(k) if isinstance(cur, dict) else None
            if isinstance(cur, str) and cur:
                url = cur
                break

    if not isinstance(url, str) or not url:
        return ""

    pr = urlparse(url)
    if pr.query:
        q = pr.query.split("&")
        q = [kv for kv in q if not (kv.split("=")[0] in _DROP_QUERY_KEYS)]
        pr = pr._replace(query="&".join(q))
    netloc = pr.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    pr = pr._replace(netloc=netloc)
    return urlunparse(pr)

def _to_epoch_seconds(item: Dict[str, Any]) -> int:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    if isinstance(raw.get("datetime"), (int, float)):
        return int(raw["datetime"])

    def _parse_iso(s: str) -> int:
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0

    ts = item.get("ts")
    if isinstance(ts, str):
        return _parse_iso(ts)

    content = raw.get("content") if isinstance(raw.get("content"), dict) else raw
    for k in ("displayTime", "pubDate"):
        v = content.get(k)
        if isinstance(v, str):
            sec = _parse_iso(v)
            if sec:
                return sec
    maybe = item.get("time_published") or item.get("providerPublishTime")
    if isinstance(maybe, (int, float)):
        return int(maybe)
    return 0

def _dedupe_sort(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        url = _canonical_url(it.get("url"), it.get("raw"))
        title = (it.get("headline") or "").strip()
        if not url or not title:
            continue
        key = (url, _to_epoch_seconds(it))
        if key in seen:
            continue
        seen.add(key)
        it["url"] = url
        out.append(it)
    out.sort(key=_to_epoch_seconds, reverse=True)
    return out

def _iter_provider_files(data_dir: Path, symbol: str, provider: str) -> List[Path]:
    base = data_dir / symbol.upper() / "news" / provider
    if not base.is_dir():
        return []
    files = sorted(base.glob("*.json"), key=lambda p: p.stem, reverse=True)
    return files

def _read_json_array(p: Path) -> List[Dict[str, Any]]:
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _load_history(data_dir: Path, symbol: str, providers: Iterable[str], budget: int = 120) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for prov in providers:
        for fp in _iter_provider_files(data_dir, symbol, prov):
            if len(rows) >= budget:
                break
            arr = _read_json_array(fp)
            rows.extend(arr)
            if len(rows) >= budget:
                break
    return _dedupe_sort(rows)  
    
def ensure_top_n_news_from_store(
    symbol: str,
    current_items: List[Dict[str, Any]],
    data_dir: Path,
    n: int = 10,
    providers: Iterable[str] = ("yfinance", "finnhub", "newsapi"),
    history_budget: int = 200
) -> List[Dict[str, Any]]:
    cur = _dedupe_sort(current_items or [])
    if len(cur) >= n:
        return cur[:n]

    hist = _load_history(data_dir, symbol, providers, budget=history_budget)
    merged = _dedupe_sort(list(cur) + hist)
    return merged[:n]
