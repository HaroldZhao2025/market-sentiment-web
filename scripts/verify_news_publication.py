"""Verify that the exported and live company pages contain the newest input news."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

SYMBOLS = ("AAPL", "MSFT", "NVDA", "AMZN")
MANIFEST = "news-publication.json"


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def normalized(text: str) -> str:
    return " ".join(text.split())


def visible_text(content: bytes) -> str:
    parser = VisibleText()
    parser.feed(content.decode("utf-8"))
    return normalized(" ".join(parser.parts))


def latest_article(payloads: list[dict], now: datetime) -> tuple[str, str]:
    candidates: list[tuple[datetime, str]] = []
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        rows = payload.get("news") if isinstance(payload.get("news"), list) else payload.get("articles", [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or row.get("headline") or "").strip()
            if not any(c.isalnum() for c in unicodedata.normalize("NFKC", title)):
                continue
            for field in ("ts", "date"):
                raw = row.get(field)
                if not isinstance(raw, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?", raw.strip()):
                    continue
                try:
                    stamp = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
                    stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
                except ValueError:
                    continue
                if stamp > now + timedelta(hours=2):
                    raise RuntimeError("Future article timestamp cannot be used as freshness evidence")
                candidates.append((stamp, title))
                break
    if not candidates:
        raise RuntimeError("No valid dated article in either news input")
    stamp, title = max(candidates, key=lambda row: (row[0], row[1]))
    return stamp.isoformat(), title


def prepare(out: Path, run_id: str, commit: str, *, symbols=SYMBOLS,
            now: datetime | None = None, max_age_hours: float = 168) -> dict:
    now = now or datetime.now(timezone.utc)
    if max_age_hours <= 0 or not run_id or not commit:
        raise ValueError("A run ID, source commit and positive maximum age are required")
    entries = []
    for symbol in symbols:
        payloads = []
        for relative in (f"data/ticker/{symbol}.json", f"data/v5/news/{symbol}.json"):
            path = out / relative
            if path.exists():
                payloads.append(json.loads(path.read_text(encoding="utf-8")))
        latest, title = latest_article(payloads, now)
        if now - datetime.fromisoformat(latest) > timedelta(hours=max_age_hours):
            raise RuntimeError(f"{symbol}: latest real article is stale ({latest})")
        page_path = f"ticker/{symbol}/index.html"
        content = (out / page_path).read_bytes()
        text = visible_text(content)
        label = f"Latest article (UTC): {latest[:10]}"
        if label not in text or normalized(title) not in text:
            raise RuntimeError(f"{symbol}: exported visible page hides its latest input article ({latest})")
        entries.append({"symbol": symbol, "latest_article_at_utc": latest,
                        "title": title, "page_path": page_path,
                        "page_sha256": hashlib.sha256(content).hexdigest()})
    if not entries:
        raise ValueError("At least one verification symbol is required")
    result = {"schema_version": 1, "run_id": run_id, "source_commit": commit,
              "checked_at_utc": now.isoformat(), "samples": entries}
    (out / MANIFEST).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("NEWS EXPORT VERIFIED | " + json.dumps(result, ensure_ascii=False), flush=True)
    return result


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"Cache-Control": "no-cache", "Accept-Encoding": "identity",
                                   "User-Agent": "market-sentiment-web-publication-check"})
    with urlopen(request, timeout=15) as response:
        return response.read()


def verify(base_url: str, run_id: str, *, attempts: int = 6, delay: float = 10,
           fetch=None) -> dict:
    fetch = fetch or fetch_bytes
    parsed = urlsplit(base_url)
    if parsed.scheme != "https" or parsed.netloc != "haroldzhao2025.github.io" or parsed.path.rstrip("/") != "/market-sentiment-web":
        raise ValueError("Only the configured GitHub Pages site may be verified")
    if attempts < 1 or delay < 0 or not run_id:
        raise ValueError("Invalid verification retry settings or run ID")
    base = base_url.rstrip("/") + "/"
    last_error = ""
    for attempt in range(attempts):
        try:
            query = "?publication_run=" + quote(run_id, safe="")
            manifest = json.loads(fetch(base + MANIFEST + query))
            if str(manifest.get("run_id")) != run_id:
                raise RuntimeError("Live site is still serving another build's manifest")
            entries = manifest.get("samples")
            if not isinstance(entries, list) or {e.get("symbol") for e in entries} != set(SYMBOLS):
                raise RuntimeError("Live publication manifest has incomplete verification samples")
            for entry in entries:
                expected = f"ticker/{entry['symbol']}/index.html"
                if entry.get("page_path") != expected:
                    raise RuntimeError("Invalid manifest page path")
                content = fetch(base + expected + query)
                if hashlib.sha256(content).hexdigest() != entry["page_sha256"]:
                    raise RuntimeError(f"{entry['symbol']}: live page does not match the validated export")
                text = visible_text(content)
                if normalized(entry["title"]) not in text or f"Latest article (UTC): {entry['latest_article_at_utc'][:10]}" not in text:
                    raise RuntimeError(f"{entry['symbol']}: latest article is not visible on the live page")
            print("NEWS LIVE VERIFIED | " + json.dumps(manifest, ensure_ascii=False), flush=True)
            return manifest
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
            last_error = str(error)
            print(f"NEWS VERIFY RETRY | {attempt + 1}/{attempts}: {last_error}", flush=True)
            if attempt + 1 < attempts:
                time.sleep(delay)
    raise RuntimeError("Live publication verification failed: " + last_error)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "verify"))
    parser.add_argument("--out", type=Path, default=Path("apps/web/out"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", default="")
    parser.add_argument("--base-url", default="https://haroldzhao2025.github.io/market-sentiment-web/")
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare(args.out, args.run_id, args.commit)
    else:
        verify(args.base_url, args.run_id)


if __name__ == "__main__":
    main()
