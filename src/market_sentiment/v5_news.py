from __future__ import annotations

import gzip
import hashlib
import json
import math
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pandas as pd
import yfinance as yf


def normalize_title(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_url(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        host = (parts.hostname or "").lower().removeprefix("www.")
        path = re.sub(r"/+", "/", parts.path or "/").rstrip("/")
        return f"{host}{path}" if host else ""
    except Exception:
        return ""


def _timestamp(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ivalue = int(value)
        if ivalue > 10_000_000_000:
            ivalue //= 1000
        return pd.Timestamp.fromtimestamp(ivalue, tz="UTC")
    except Exception:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        return None if pd.isna(parsed) else parsed


def yahoo_news(ticker: str, days: int = 30, count: int = 250) -> list[dict[str, Any]]:
    now = pd.Timestamp.now(tz="UTC")
    cutoff = now - pd.Timedelta(days=max(1, days))
    try:
        items = yf.Ticker(ticker).get_news(count=count, tab="all") or []
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        ts = (
            _timestamp(item.get("providerPublishTime"))
            or _timestamp(content.get("pubDate"))
            or _timestamp(content.get("displayTime"))
            or _timestamp(content.get("published"))
        )
        if ts is None or ts < cutoff or ts > now + pd.Timedelta(hours=2):
            continue
        title = str(content.get("title") or item.get("title") or "").strip()
        if not title:
            continue
        canonical = content.get("canonicalUrl") if isinstance(content.get("canonicalUrl"), dict) else {}
        click = content.get("clickThroughUrl") if isinstance(content.get("clickThroughUrl"), dict) else {}
        provider = content.get("provider")
        source = str(
            provider.get("displayName") or provider.get("name") or "Yahoo"
            if isinstance(provider, dict)
            else provider or "Yahoo"
        )
        rows.append(
            {
                "ts": ts.isoformat(),
                "title": title,
                "summary": str(content.get("summary") or content.get("description") or item.get("summary") or "").strip(),
                "url": str(canonical.get("url") or click.get("url") or item.get("link") or item.get("url") or ""),
                "source": source,
                "provider": "yfinance",
            }
        )
    return rows


def finnhub_news(ticker: str, token: str, days: int = 30) -> list[dict[str, Any]]:
    if not token:
        return []
    try:
        import finnhub
    except Exception:
        return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(1, days))
    try:
        items = finnhub.Client(api_key=token).company_news(ticker, _from=start.isoformat(), to=end.isoformat()) or []
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("headline") or "").strip()
        if not title:
            continue
        try:
            ts = pd.Timestamp.fromtimestamp(int(item.get("datetime")), tz="UTC").isoformat()
        except Exception:
            ts = ""
        rows.append(
            {
                "ts": ts,
                "title": title,
                "summary": str(item.get("summary") or "").strip(),
                "url": str(item.get("url") or ""),
                "source": str(item.get("source") or "Finnhub"),
                "provider": "finnhub",
            }
        )
    return rows


def deduplicate_news(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(items, key=lambda row: str(row.get("ts") or ""), reverse=True)
    seen_titles: set[str] = set()
    seen_urls: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in ordered:
        title_key = normalize_title(row.get("title"))
        url_key = canonical_url(row.get("url"))
        if not title_key:
            continue
        if title_key in seen_titles or (url_key and url_key in seen_urls):
            continue
        seen_titles.add(title_key)
        if url_key:
            seen_urls.add(url_key)
        clean = dict(row)
        clean["title_key"] = title_key
        out.append(clean)
    return out


def _load_score_cache(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            raw = json.load(handle)
        return {str(k): float(v) for k, v in raw.items() if math.isfinite(float(v))}
    except Exception:
        return {}


def _save_score_cache(path: Path, cache: dict[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(cache, handle, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def score_news(items: list[dict[str, Any]], cache_path: Path, batch_size: int = 16) -> list[dict[str, Any]]:
    from .finbert import FinBERT

    cache = _load_score_cache(cache_path)
    missing: list[tuple[str, str]] = []
    for row in items:
        key = hashlib.sha256(normalize_title(row.get("title")).encode("utf-8")).hexdigest()
        row["score_key"] = key
        if key not in cache:
            missing.append((key, str(row.get("title") or "")))
    if missing:
        model = FinBERT()
        scores = model.score([title for _, title in missing], batch_size=batch_size)
        for (key, _), score in zip(missing, scores):
            cache[key] = float(score)
        _save_score_cache(cache_path, cache)
    out: list[dict[str, Any]] = []
    for row in items:
        clean = dict(row)
        key = str(clean.pop("score_key", ""))
        score = cache.get(key)
        clean["s"] = round(float(score), 6) if score is not None else None
        out.append(clean)
    return out


def collect_company_news(
    ticker: str,
    finnhub_token: str = "",
    days: int = 30,
    max_items: int = 60,
) -> list[dict[str, Any]]:
    items = yahoo_news(ticker, days=days, count=max(250, max_items * 3))
    if finnhub_token:
        items.extend(finnhub_news(ticker, finnhub_token, days=days))
    return deduplicate_news(items)[:max_items]
