# src/market_sentiment/news_enforcer.py
from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen
import socket

_DROP_QUERY_KEYS = {
    "siteid","yptr","guccounter","guce_referrer",
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "soc_src","soc_trk"
}

def _canonical_url(url: Optional[str], raw: Optional[Dict[str, Any]]) -> str:
    if raw and isinstance(raw, dict):
        for path in (("content", "canonicalUrl", "url"),
                     ("canonicalUrl", "url"),
                     ("content", "previewUrl")):
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
        q = [kv for kv in pr.query.split("&") if kv.split("=")[0] not in _DROP_QUERY_KEYS]
        pr = pr._replace(query="&".join(q))
    netloc = pr.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    pr = pr._replace(netloc=netloc)
    return urlunparse(pr)

def _to_epoch_seconds(item: Dict[str, Any]) -> int:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    # finnhub: raw.datetime (epoch s)
    dtm = raw.get("datetime")
    if isinstance(dtm, (int, float)): return int(dtm)

    def _parse_iso(s: str) -> int:
        try:
            return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
        except Exception:
            return 0

    ts = item.get("ts")
    if isinstance(ts, str):
        sec = _parse_iso(ts)
        if sec: return sec

    content = raw.get("content") if isinstance(raw.get("content"), dict) else raw
    for k in ("displayTime","pubDate"):
        v = content.get(k)
        if isinstance(v, str):
            sec = _parse_iso(v)
            if sec: return sec

    maybe = item.get("time_published") or item.get("providerPublishTime")
    if isinstance(maybe, (int, float)): return int(maybe)
    return 0

def _dedupe_sort(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict): continue
        url = _canonical_url(it.get("url"), it.get("raw"))
        title = (it.get("headline") or it.get("title") or "").strip()
        if not url or not title: continue
        key = (url, _to_epoch_seconds(it))
        if key in seen: continue
        seen.add(key)
        it["url"] = url
        out.append(it)
    out.sort(key=_to_epoch_seconds, reverse=True)
    return out

def _iter_provider_files(data_dir: Path, symbol: str, provider: str) -> List[Path]:
    base = data_dir / symbol.upper() / "news" / provider
    if not base.is_dir(): return []
    return sorted(base.glob("*.json"), key=lambda p: p.stem, reverse=True)

def _read_json_array(p: Path) -> List[Dict[str, Any]]:
    try:
        arr = json.loads(p.read_text(encoding="utf-8"))
        return arr if isinstance(arr, list) else []
    except Exception:
        return []

def _load_history_from_data(data_dir: Optional[Path], symbol: str,
                            providers: Iterable[str], budget: int) -> List[Dict[str, Any]]:
    if not data_dir: return []
    rows: List[Dict[str, Any]] = []
    for prov in providers:
        for fp in _iter_provider_files(data_dir, symbol, prov):
            if len(rows) >= budget: break
            rows.extend(_read_json_array(fp))
            if len(rows) >= budget: break
    return _dedupe_sort(rows)

def _load_history_from_outdir(out_dir: Optional[Path], symbol: str) -> List[Dict[str, Any]]:
    if not out_dir: return []
    p = out_dir / "ticker" / f"{symbol.upper()}.json"
    if not p.exists(): return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("news","headlines","articles"):
                if isinstance(data.get(k), list):
                    return _dedupe_sort(data[k])
    except Exception:
        pass
    return []

def _load_history_from_pages(pages_base_url: Optional[str], symbol: str, timeout_sec: int = 6) -> List[Dict[str, Any]]:
    if not pages_base_url: return []
    url = f"{pages_base_url.rstrip('/')}/ticker/{symbol.upper()}.json"
    try:
        socket.setdefaulttimeout(timeout_sec)
        with urlopen(url, timeout=timeout_sec) as resp:
            if resp.status != 200:
                return []
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if isinstance(data, dict):
                for k in ("news","headlines","articles"):
                    if isinstance(data.get(k), list):
                        return _dedupe_sort(data[k])
    except Exception:
        return []
    return []

def ensure_top_n_news_from_store(
    symbol: str,
    current_items: List[Dict[str, Any]],
    data_dir: Optional[Path],
    n: int = 10,
    providers: Iterable[str] = ("yfinance","finnhub","newsapi"),
    history_budget: int = 200,
    out_dir: Optional[Path] = None,
    pages_base_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cur = _dedupe_sort(current_items or [])
    if len(cur) >= n:
        return cur[:n]

    hist = _load_history_from_data(data_dir, symbol, providers, budget=history_budget)
    merged = _dedupe_sort(list(cur) + hist)
    if len(merged) >= n:
        return merged[:n]

    hist2 = _load_history_from_outdir(out_dir, symbol)
    merged2 = _dedupe_sort(merged + hist2)
    if len(merged2) >= n:
        return merged2[:n]

    hist3 = _load_history_from_pages(pages_base_url, symbol)
    merged3 = _dedupe_sort(merged2 + hist3)
    return merged3[:n]
