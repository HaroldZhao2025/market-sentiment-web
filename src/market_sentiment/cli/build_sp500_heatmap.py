from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

import yfinance as yf

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
CACHE_TTL_DAYS = 7


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def parse_iso_date(value: str) -> date:
    return datetime.fromisoformat(value).date()


def normalize_ticker(value: str) -> str:
    return re.sub(r"\s+", "", (value or "").strip().upper())


def yfinance_symbol(symbol: str) -> str:
    return normalize_ticker(symbol).replace(".", "-")


def ticker_variants(symbol: str) -> List[str]:
    symbol = normalize_ticker(symbol)
    return list({symbol, symbol.replace(".", "-"), symbol.replace("-", ".")}) if symbol else []


def read_tickers_csv(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    out: List[str] = []
    seen = set()
    for i, row in enumerate(rows):
        if not row:
            continue
        value = normalize_ticker(row[0])
        if i == 0 and value in {"TICKER", "SYMBOL"}:
            continue
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def latest_trading_day_from_spx_index(path: Path) -> str:
    obj = read_json(path)
    daily = obj.get("daily") or []
    if not isinstance(daily, list) or not daily:
        raise ValueError(f"{path} has no daily rows")
    row = max(daily, key=lambda r: str(r.get("date") or ""))
    if not row.get("date"):
        raise ValueError("Latest SPX row missing date")
    return str(row["date"])


def fetch_url(url: str, user_agent: str) -> str:
    req = Request(url, headers={"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def strip_tags(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.S | re.I)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", "", html))).strip()


def parse_constituent_rows(html: str) -> List[List[str]]:
    match = re.search(r'(<table[^>]*id="constituents"[^>]*>.*?</table>)', html, flags=re.S | re.I)
    tables = [match.group(1)] if match else re.findall(r"(<table[^>]*>.*?</table>)", html, flags=re.S | re.I)
    table = next((t for t in tables if "GICS Sector" in t and "Symbol" in t), None)
    if not table:
        raise RuntimeError("Could not locate S&P 500 constituents table")
    rows: List[List[str]] = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", table, flags=re.S | re.I):
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", raw_row, flags=re.S | re.I)
        if cells:
            rows.append([strip_tags(cell) for cell in cells])
    return rows


@dataclass
class Meta:
    name: str
    sector: str
    industry: str
    source: str


def rows_to_meta_map(rows: List[List[str]]) -> Dict[str, Meta]:
    if not rows:
        return {}
    header = [c.strip().lower() for c in rows[0]]

    def col(*names: str) -> int:
        for name in names:
            if name.lower() in header:
                return header.index(name.lower())
        return -1

    i_symbol = col("Symbol")
    i_name = col("Security", "Company", "Name")
    i_sector = col("GICS Sector", "Sector")
    i_industry = col("GICS Sub-Industry", "Sub-Industry", "Industry")
    if min(i_symbol, i_sector, i_industry) < 0:
        raise RuntimeError(f"Wikipedia constituent header changed: {rows[0]}")

    out: Dict[str, Meta] = {}
    for row in rows[1:]:
        if len(row) <= max(i_symbol, i_sector, i_industry):
            continue
        symbol = re.sub(r"[^A-Z0-9.\-]", "", normalize_ticker(row[i_symbol]))
        if not symbol:
            continue
        meta = Meta(
            name=(row[i_name].strip() if i_name >= 0 and i_name < len(row) else ""),
            sector=row[i_sector].strip() or "Unknown",
            industry=row[i_industry].strip() or "Unknown",
            source="wikipedia",
        )
        for variant in ticker_variants(symbol):
            out[variant] = meta
    return out


def cache_is_fresh(obj: Any, ttl_days: int) -> bool:
    if not isinstance(obj, dict):
        return False
    raw = obj.get("fetched_at_utc")
    if not raw:
        return False
    try:
        stamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - stamp <= timedelta(days=ttl_days)
    except Exception:
        return False


def load_metadata(cache_path: Path, user_agent: str, refresh: bool, ttl_days: int) -> Dict[str, Meta]:
    cached: Any = None
    if cache_path.exists():
        try:
            cached = read_json(cache_path)
        except Exception:
            cached = None

    if cached and not refresh and cache_is_fresh(cached, ttl_days):
        return rows_to_meta_map(cached.get("rows") or [])

    try:
        rows = parse_constituent_rows(fetch_url(WIKI_URL, user_agent))
        write_json(cache_path, {
            "source": WIKI_URL,
            "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "rows": rows,
        })
        return rows_to_meta_map(rows)
    except Exception as exc:
        if cached:
            print(f"[heatmap] WARNING: metadata refresh failed; using stale cache: {exc}", file=sys.stderr)
            return rows_to_meta_map(cached.get("rows") or [])
        raise


def read_price_daily(data_root: Path, ticker: str) -> List[Dict[str, Any]]:
    path = data_root / ticker / "price" / "daily.json"
    try:
        obj = read_json(path)
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def find_close_for_date(rows: List[Dict[str, Any]], target: str) -> Tuple[Optional[float], Optional[str]]:
    parsed: List[Tuple[date, float]] = []
    for row in rows:
        try:
            parsed.append((parse_iso_date(str(row.get("date"))), float(row.get("close"))))
        except Exception:
            continue
    if not parsed:
        return None, None
    parsed.sort(key=lambda x: x[0])
    t = parse_iso_date(target)
    eligible = [(d, c) for d, c in parsed if d <= t]
    d, c = eligible[-1] if eligible else parsed[0]
    return c, d.isoformat()


def prev_close(rows: List[Dict[str, Any]], used_date: str) -> Optional[float]:
    target = parse_iso_date(used_date)
    parsed: List[Tuple[date, float]] = []
    for row in rows:
        try:
            parsed.append((parse_iso_date(str(row.get("date"))), float(row.get("close"))))
        except Exception:
            continue
    before = sorted((d, c) for d, c in parsed if d < target)
    return before[-1][1] if before else None


def read_sentiment(data_root: Path, ticker: str, asof: str) -> Tuple[Optional[float], Optional[int]]:
    path = data_root / ticker / "sentiment" / f"{asof}.json"
    try:
        obj = read_json(path)
    except Exception:
        return None, None
    raw = obj.get("score_mean", obj.get("sentiment"))
    try:
        score = float(raw) if raw is not None and math.isfinite(float(raw)) else None
    except Exception:
        score = None
    try:
        count = int(obj.get("n_total")) if obj.get("n_total") is not None else None
    except Exception:
        count = None
    return score, count


def fetch_market_metadata(tickers: List[str], wiki_map: Dict[str, Meta]) -> Tuple[Dict[str, float], Dict[str, Meta]]:
    caps: Dict[str, float] = {}
    meta: Dict[str, Meta] = {}
    batch_size = 50
    for start in range(0, len(tickers), batch_size):
        chunk = tickers[start:start + batch_size]
        yf_symbols = [yfinance_symbol(t) for t in chunk]
        multi = yf.Tickers(" ".join(yf_symbols))
        for original, yf_symbol in zip(chunk, yf_symbols):
            wiki = next((wiki_map.get(v) for v in ticker_variants(original) if wiki_map.get(v)), None)
            info: Dict[str, Any] = {}
            market_cap = None
            try:
                ticker = multi.tickers.get(yf_symbol) or yf.Ticker(yf_symbol)
                fast = getattr(ticker, "fast_info", None)
                market_cap = fast.get("market_cap") if isinstance(fast, dict) else getattr(fast, "market_cap", None)
                needs_info = market_cap is None or wiki is None or not wiki.name
                if needs_info:
                    raw = getattr(ticker, "info", {}) or {}
                    info = raw if isinstance(raw, dict) else {}
                    market_cap = market_cap or info.get("marketCap")
            except Exception:
                pass

            try:
                value = float(market_cap)
                if value > 0 and math.isfinite(value):
                    caps[original] = value
            except Exception:
                pass

            name = (wiki.name if wiki else "") or str(info.get("longName") or info.get("shortName") or "").strip()
            sector = (wiki.sector if wiki else "") or str(info.get("sector") or "Unknown").strip() or "Unknown"
            industry = (wiki.industry if wiki else "") or str(info.get("industry") or "Unknown").strip() or "Unknown"
            meta[original] = Meta(name=name, sector=sector, industry=industry, source="wikipedia" if wiki and wiki.name else "yfinance")
    return caps, meta


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Build S&P 500 constituent heatmap data with refreshed company metadata.")
    parser.add_argument("--universe", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--spx-index", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--asof", default=None)
    parser.add_argument("--wiki-cache", default=None)
    parser.add_argument("--refresh-wiki", action="store_true")
    parser.add_argument("--metadata-ttl-days", type=int, default=CACHE_TTL_DAYS)
    parser.add_argument("--user-agent", default="market-sentiment-web/2.0 (contact: github-actions)")
    args = parser.parse_args(argv)

    universe = read_tickers_csv(Path(args.universe))
    if not universe:
        raise RuntimeError("S&P universe is empty")
    data_root = Path(args.data_root)
    out_dir = Path(args.out)
    asof = args.asof or latest_trading_day_from_spx_index(Path(args.spx_index))
    cache = Path(args.wiki_cache) if args.wiki_cache else out_dir / "sp500_wikipedia_cache.json"

    wiki_map = load_metadata(cache, args.user_agent, args.refresh_wiki, max(1, args.metadata_ttl_days))
    caps, metadata = fetch_market_metadata(universe, wiki_map)
    if not caps:
        raise RuntimeError("Could not fetch any market caps")
    total_cap = sum(caps.values())

    tiles: List[Dict[str, Any]] = []
    missing_name = missing_classification = missing_price = missing_sentiment = 0
    yfinance_name_fallbacks = 0
    for ticker in universe:
        cap = caps.get(ticker)
        if cap is None or cap <= 0:
            continue
        meta = metadata.get(ticker) or Meta("", "Unknown", "Unknown", "missing")
        if not meta.name:
            missing_name += 1
        if meta.source == "yfinance" and meta.name:
            yfinance_name_fallbacks += 1
        if meta.sector == "Unknown" or meta.industry == "Unknown":
            missing_classification += 1

        prices = read_price_daily(data_root, ticker)
        close, used_date = find_close_for_date(prices, asof)
        if close is None:
            missing_price += 1
        previous = prev_close(prices, used_date) if used_date else None
        return_1d = close / previous - 1 if close is not None and previous not in (None, 0) else None
        sentiment, n_total = read_sentiment(data_root, ticker, asof)
        if sentiment is None:
            missing_sentiment += 1

        tiles.append({
            "symbol": ticker,
            "name": meta.name or ticker,
            "sector": meta.sector,
            "industry": meta.industry,
            "metadata_source": meta.source,
            "market_cap": cap,
            "weight": cap / total_cap,
            "date": used_date or asof,
            "price": close,
            "return_1d": return_1d,
            "sentiment": sentiment,
            "n_total": n_total,
        })

    tiles.sort(key=lambda row: float(row.get("market_cap") or 0), reverse=True)
    payload = {
        "symbol": "SPX",
        "name": "S&P 500 Index",
        "asof": asof,
        "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "stats": {
            "n_universe": len(universe),
            "n_tiles": len(tiles),
            "missing_name": missing_name,
            "missing_classification": missing_classification,
            "missing_price": missing_price,
            "missing_sentiment": missing_sentiment,
            "yfinance_name_fallbacks": yfinance_name_fallbacks,
            "metadata_cache_ttl_days": max(1, args.metadata_ttl_days),
        },
        "tiles": tiles,
    }
    write_json(out_dir / "sp500_heatmap.json", payload)
    print(f"[heatmap] wrote {len(tiles)} tiles asof={asof} missing_name={missing_name} fallback_names={yfinance_name_fallbacks}")

    if missing_name / max(1, len(tiles)) > 0.02:
        raise RuntimeError(f"Company-name coverage too low: missing {missing_name}/{len(tiles)}")
    if missing_classification / max(1, len(tiles)) > 0.10:
        print(f"[heatmap] WARNING: missing classification {missing_classification}/{len(tiles)}", file=sys.stderr)


if __name__ == "__main__":
    main()
