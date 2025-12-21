# src/market_sentiment/cli/build_sp500_heatmap.py
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.request import Request, urlopen

import yfinance as yf


WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def read_json(p: Path) -> Any:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def parse_iso_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def yfinance_symbol(sym: str) -> str:
    # BRK.B -> BRK-B for yfinance
    return sym.replace(".", "-")


def normalize_ticker(s: str) -> str:
    s = (s or "").strip().upper()
    # remove weird whitespace
    s = re.sub(r"\s+", "", s)
    return s


def ticker_variants(sym: str) -> List[str]:
    sym = normalize_ticker(sym)
    if not sym:
        return []
    out = {sym}
    out.add(sym.replace(".", "-"))
    out.add(sym.replace("-", "."))
    return list(out)


def read_tickers_csv(universe_csv: Path) -> List[str]:
    """
    data/sp500.csv has only tickers (per your description).
    This reads first column, skips header if present.
    """
    tickers: List[str] = []
    with universe_csv.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            v = normalize_ticker(row[0])
            if not v:
                continue
            # skip header-like first row
            if i == 0 and v in {"TICKER", "SYMBOL"}:
                continue
            tickers.append(v)
    # unique, preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def latest_trading_day_from_spx_index(spx_index_path: Path) -> str:
    obj = read_json(spx_index_path)
    daily = obj.get("daily") or []
    if not isinstance(daily, list) or not daily:
        raise ValueError(f"{spx_index_path} has no daily rows")
    daily_sorted = sorted(daily, key=lambda r: str(r.get("date", "")))
    d = daily_sorted[-1].get("date")
    if not d:
        raise ValueError("Latest SPX row missing date")
    return str(d)


def fetch_url(url: str, user_agent: str) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def extract_constituents_table_html(page_html: str) -> str:
    """
    Try to extract the constituents table from Wikipedia page HTML.

    Wikipedia currently has a table with id="constituents". We target it first.
    Fallback: any table containing 'GICS Sector' and 'GICS Sub-Industry'.
    """
    # Prefer id="constituents"
    m = re.search(r'(<table[^>]*id="constituents"[^>]*>.*?</table>)', page_html, flags=re.S | re.I)
    if m:
        return m.group(1)

    # Fallback: find a table that contains both headers
    tables = re.findall(r"(<table[^>]*>.*?</table>)", page_html, flags=re.S | re.I)
    for t in tables:
        if "GICS Sector" in t and "GICS Sub-Industry" in t and "Symbol" in t:
            return t

    raise RuntimeError("Could not locate constituents table in Wikipedia HTML")


def strip_tags(html: str) -> str:
    # remove scripts/styles
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.S | re.I)
    # remove tags
    text = re.sub(r"<[^>]+>", "", html)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_html_table(table_html: str) -> List[List[str]]:
    """
    Minimal HTML table parser (no external deps).
    Returns rows of cell texts.
    """
    rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.S | re.I)
    rows: List[List[str]] = []
    for rh in rows_html:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rh, flags=re.S | re.I)
        if not cells:
            continue
        rows.append([strip_tags(c) for c in cells])
    return rows


@dataclass
class WikiMeta:
    name: str
    sector: str
    industry: str


def build_wikipedia_map(
    cache_path: Path,
    user_agent: str,
    refresh: bool,
) -> Dict[str, WikiMeta]:
    """
    Returns mapping for ticker variants -> WikiMeta.
    Cache is written to cache_path.
    """
    if cache_path.exists() and not refresh:
        try:
            cached = read_json(cache_path)
            if isinstance(cached, dict) and "rows" in cached:
                return rows_to_wiki_map(cached["rows"])
        except Exception:
            pass

    html = fetch_url(WIKI_URL, user_agent=user_agent)
    table_html = extract_constituents_table_html(html)
    rows = parse_html_table(table_html)

    # write raw cache (rows) so we can parse later even if Wikipedia changes slightly
    write_json(
        cache_path,
        {
            "source": WIKI_URL,
            "fetched_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "rows": rows,
        },
    )

    return rows_to_wiki_map(rows)


def rows_to_wiki_map(rows: List[List[str]]) -> Dict[str, WikiMeta]:
    """
    Convert table rows -> mapping.
    Expected headers include: Symbol, Security, GICS Sector, GICS Sub-Industry
    """
    if not rows:
        raise RuntimeError("Wikipedia table rows empty")

    header = [c.strip() for c in rows[0]]
    header_l = [h.lower() for h in header]

    def find_col(names: List[str]) -> int:
        for nm in names:
            if nm.lower() in header_l:
                return header_l.index(nm.lower())
        return -1

    i_symbol = find_col(["Symbol"])
    i_name = find_col(["Security", "Company", "Name"])
    i_sector = find_col(["GICS Sector", "Sector"])
    i_ind = find_col(["GICS Sub-Industry", "GICS Sub-Industry ", "Sub-Industry", "Industry"])

    if i_symbol < 0 or i_sector < 0 or i_ind < 0:
        raise RuntimeError(f"Could not find needed columns in Wikipedia header: {header}")

    out: Dict[str, WikiMeta] = {}

    for r in rows[1:]:
        if len(r) <= max(i_symbol, i_sector, i_ind):
            continue

        sym_raw = r[i_symbol]
        sym = normalize_ticker(sym_raw)
        if not sym:
            continue

        # Some Wikipedia symbols can have notes; keep only ticker-ish chars
        sym = re.sub(r"[^A-Z0-9.\-]", "", sym)
        if not sym:
            continue

        name = r[i_name].strip() if (i_name >= 0 and i_name < len(r)) else ""
        sector = r[i_sector].strip()
        industry = r[i_ind].strip()

        meta = WikiMeta(name=name, sector=sector or "Unknown", industry=industry or "Unknown")
        for v in ticker_variants(sym):
            out[v] = meta

    return out


def read_price_daily(data_root: Path, ticker: str) -> List[Dict[str, Any]]:
    p = data_root / ticker / "price" / "daily.json"
    if not p.exists():
        return []
    try:
        arr = read_json(p)
        if isinstance(arr, list):
            return arr
    except Exception:
        return []
    return []


def find_close_for_date(price_rows: List[Dict[str, Any]], target: str) -> Tuple[Optional[float], Optional[str]]:
    """
    If target date not found, use latest date < target.
    Returns (close, used_date)
    """
    if not price_rows:
        return None, None

    parsed: List[Tuple[date, float]] = []
    for r in price_rows:
        d = r.get("date")
        c = r.get("close")
        if not d:
            continue
        try:
            dd = parse_iso_date(str(d))
            cc = float(c)
        except Exception:
            continue
        parsed.append((dd, cc))

    if not parsed:
        return None, None

    parsed.sort(key=lambda x: x[0])
    t = parse_iso_date(target)

    # exact
    for dd, cc in parsed:
        if dd == t:
            return cc, dd.isoformat()

    before = [(dd, cc) for dd, cc in parsed if dd < t]
    if before:
        dd, cc = before[-1]
        return cc, dd.isoformat()

    # otherwise earliest after
    dd, cc = parsed[0]
    return cc, dd.isoformat()


def prev_close(price_rows: List[Dict[str, Any]], used_date: str) -> Optional[float]:
    if not price_rows or not used_date:
        return None

    parsed: List[Tuple[date, float]] = []
    for r in price_rows:
        d = r.get("date")
        c = r.get("close")
        if not d:
            continue
        try:
            dd = parse_iso_date(str(d))
            cc = float(c)
        except Exception:
            continue
        parsed.append((dd, cc))

    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])

    u = parse_iso_date(used_date)
    before = [(dd, cc) for dd, cc in parsed if dd < u]
    if not before:
        return None
    return before[-1][1]


def read_sentiment(data_root: Path, ticker: str, asof: str) -> Tuple[Optional[float], Optional[int]]:
    p = data_root / ticker / "sentiment" / f"{asof}.json"
    if not p.exists():
        return None, None
    try:
        obj = read_json(p)
    except Exception:
        return None, None

    s = obj.get("score_mean")
    if s is None:
        s = obj.get("sentiment")

    n_total = obj.get("n_total")
    try:
        sval = float(s) if s is not None else None
    except Exception:
        sval = None

    try:
        nval = int(n_total) if n_total is not None else None
    except Exception:
        nval = None

    return sval, nval


def fetch_market_caps(tickers: List[str]) -> Dict[str, float]:
    """
    Robust market cap fetch via yfinance in batches.
    Returns {original_ticker: market_cap}.
    """
    out: Dict[str, float] = {}
    if not tickers:
        return out

    # batch for fewer requests
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        chunk = tickers[i : i + batch_size]
        yf_syms = [yfinance_symbol(t) for t in chunk]
        multi = yf.Tickers(" ".join(yf_syms))

        for orig, ys in zip(chunk, yf_syms):
            mc = None
            try:
                t = multi.tickers.get(ys) or yf.Ticker(ys)
                fi = getattr(t, "fast_info", None)

                # fast_info sometimes dict-like
                if isinstance(fi, dict):
                    mc = fi.get("market_cap")
                else:
                    mc = getattr(fi, "market_cap", None)

                if mc is None:
                    info = getattr(t, "info", {}) or {}
                    if isinstance(info, dict):
                        mc = info.get("marketCap")
            except Exception:
                mc = None

            if mc is not None:
                try:
                    mc_f = float(mc)
                    if mc_f > 0:
                        out[orig] = mc_f
                except Exception:
                    pass

    return out


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(
        description="Build SP500 heatmap JSON using Wikipedia for sector/industry, yfinance for market cap."
    )
    ap.add_argument("--universe", required=True, help="Universe CSV with only tickers, e.g. data/sp500.csv")
    ap.add_argument("--data-root", required=True, help="Root dir of per-ticker folders, e.g. data")
    ap.add_argument("--spx-index", required=True, help="Path to data/SPX/sp500_index.json (latest trading day)")
    ap.add_argument("--out", required=True, help="Output dir, e.g. data/SPX")
    ap.add_argument("--asof", default=None, help="YYYY-MM-DD. If omitted uses latest day in spx-index.")
    ap.add_argument("--wiki-cache", default=None, help="Cache file path for wikipedia rows (default: <out>/sp500_wikipedia_cache.json)")
    ap.add_argument("--refresh-wiki", action="store_true", help="Force refresh wikipedia cache")
    ap.add_argument(
        "--user-agent",
        default="market-sentiment-web/1.0 (contact: github-actions)",
        help="User-Agent for Wikipedia request",
    )
    args = ap.parse_args(argv)

    universe_path = Path(args.universe)
    data_root = Path(args.data_root)
    spx_index_path = Path(args.spx_index)
    out_dir = Path(args.out)
    wiki_cache = Path(args.wiki_cache) if args.wiki_cache else (out_dir / "sp500_wikipedia_cache.json")

    tickers = read_tickers_csv(universe_path)
    if not tickers:
        raise RuntimeError(f"No tickers found in {universe_path}")

    asof = args.asof or latest_trading_day_from_spx_index(spx_index_path)
    print(f"[heatmap] asof={asof} (tickers={len(tickers)})")

    wiki_map = build_wikipedia_map(
        cache_path=wiki_cache,
        user_agent=args.user_agent,
        refresh=args.refresh_wiki,
    )
    print(f"[heatmap] wikipedia map size={len(wiki_map)}")

    mcap_map = fetch_market_caps(tickers)
    if not mcap_map:
        raise RuntimeError("Could not fetch any market caps from yfinance")
    print(f"[heatmap] market caps fetched={len(mcap_map)}")

    # compute weights on tickers that have market cap
    total_mcap = sum(mcap_map.values())
    if total_mcap <= 0:
        raise RuntimeError("Total market cap <= 0")

    tiles: List[Dict[str, Any]] = []
    missing_class = 0
    missing_price = 0
    missing_sent = 0

    for tkr in tickers:
        mc = mcap_map.get(tkr)
        if mc is None or mc <= 0:
            continue

        meta = None
        for v in ticker_variants(tkr):
            meta = wiki_map.get(v)
            if meta is not None:
                break

        if meta is None:
            missing_class += 1
            name = ""
            sector = "Unknown"
            industry = "Unknown"
        else:
            name = meta.name
            sector = meta.sector or "Unknown"
            industry = meta.industry or "Unknown"

        price_rows = read_price_daily(data_root, tkr)
        close_today, used_date = find_close_for_date(price_rows, asof)
        if close_today is None:
            missing_price += 1

        ret_1d = None
        if close_today is not None and used_date:
            pc = prev_close(price_rows, used_date)
            if pc is not None and pc != 0:
                ret_1d = (close_today / pc) - 1.0

        sent, n_total = read_sentiment(data_root, tkr, asof)
        if sent is None:
            missing_sent += 1

        tiles.append(
            {
                "symbol": tkr,
                "name": name,
                "sector": sector,
                "industry": industry,  # GICS Sub-Industry
                "market_cap": mc,
                "weight": mc / total_mcap,
                "date": used_date or asof,
                "price": close_today,
                "return_1d": ret_1d,
                "sentiment": sent,
                "n_total": n_total,
            }
        )

    tiles.sort(key=lambda x: float(x.get("market_cap") or 0.0), reverse=True)

    payload = {
        "symbol": "SPX",
        "name": "S&P 500 Index",
        "asof": asof,
        "updated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "stats": {
            "n_universe": len(tickers),
            "n_tiles": len(tiles),
            "missing_classification": missing_class,
            "missing_price": missing_price,
            "missing_sentiment": missing_sent,
        },
        "tiles": tiles,
    }

    out_path = out_dir / "sp500_heatmap.json"
    write_json(out_path, payload)
    print(f"[heatmap] wrote {out_path}")

    # if classification missing is large, fail loudly (this prevents “NVDA/MSFT in Unknown”)
    miss_rate = missing_class / max(1, len(tiles))
    if miss_rate > 0.10:
        print(
            f"[heatmap] WARNING: high missing classification rate: {missing_class}/{len(tiles)} (~{miss_rate:.1%}).",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
