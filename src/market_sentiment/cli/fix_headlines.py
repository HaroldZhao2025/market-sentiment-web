# src/market_sentiment/cli/fix_headlines.py
from __future__ import annotations
import argparse, json, os, re, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import yfinance as yf

# ---------- helpers ----------
def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_json(p: Path, obj: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def _norm_url(u: Optional[str]) -> Optional[str]:
    if not u: return None
    u = re.sub(r"[?#].*$", "", u.strip())
    return u

def _epoch(v: Any) -> Optional[int]:
    # accept iso string, epoch seconds, or providerPublishTime
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    s = str(v)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            pass
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
    except Exception:
        return None

def _dedup_keep_latest(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = {}
    out  = []
    for it in items:
        url = _norm_url(it.get("url") or it.get("link"))
        title = (it.get("title") or "").strip()
        k = (title.lower(), url or "")
        ts = _epoch(it.get("time") or it.get("publishedAt") or it.get("providerPublishTime"))
        cur = seen.get(k)
        if cur is None:
            seen[k] = {"i": it, "ts": ts or 0}
        else:
            if (ts or 0) > cur["ts"]:
                seen[k] = {"i": it, "ts": ts or 0}
    for v in seen.values():
        out.append(v["i"])
    out.sort(key=lambda x: (_epoch(x.get("time") or x.get("publishedAt") or x.get("providerPublishTime")) or 0), reverse=True)
    return out

def _shape(row: Dict[str, Any]) -> Dict[str, Any]:
    # Normalize to a single schema consumed by frontend
    ts  = _epoch(row.get("time") or row.get("publishedAt") or row.get("providerPublishTime"))
    src = row.get("source") or row.get("publisher") or row.get("provider") or ""
    return {
        "date": datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else None,
        "title": row.get("title") or "",
        "url": _norm_url(row.get("url") or row.get("link")),
        "source": src,
        # keep any precomputed sentiment score/label if present
        "sentiment": row.get("sentiment") or row.get("sentiment_label") or row.get("senti"),
        "score": row.get("score"),  # optional numeric
        "raw": None
    }

def _collect_from_existing(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    pools: List[List[Dict[str, Any]]] = []
    news = j.get("news") or {}
    # Accept a variety of shapes: news = {source: [..]}, or news = [..]
    if isinstance(news, list):
        pools.append(news)
    elif isinstance(news, dict):
        for v in news.values():
            if isinstance(v, list):
                pools.append(v)
    items: List[Dict[str, Any]] = []
    for arr in pools:
        for r in arr:
            items.append(_shape(r))
    return items

def _fetch_yf(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    try:
        n = yf.Ticker(symbol).news or []
    except Exception:
        n = []
    items = []
    for r in n[:limit]:
        items.append(_shape({
            "title": r.get("title"),
            "link": r.get("link"),
            "publisher": r.get("publisher"),
            "providerPublishTime": r.get("providerPublishTime"),
        }))
    return items

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="apps/web/public/data")
    ap.add_argument("--min", type=int, default=10, help="ensure at least this many headlines")
    ap.add_argument("--max", type=int, default=10, help="cap displayed headlines")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    tdir = data_dir / "ticker"
    tdir.mkdir(parents=True, exist_ok=True)

    for f in sorted(tdir.glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f) or {}
        items = _collect_from_existing(j)

        if len(items) < args.min:
            # top up from yfinance (fallback)
            add = _fetch_yf(sym, limit=max(args.min*2, 20))
            items = _dedup_keep_latest(items + add)

        # final slice
        items = _dedup_keep_latest(items)[: args.max]

        # persist as `top_headlines` while preserving existing structure
        j.setdefault("news", {})
        j["news"]["top_headlines"] = items

        _write_json(f, j)
        print(f"[fix_headlines] {sym}: wrote {len(items)} headlines")

if __name__ == "__main__":
    main()
