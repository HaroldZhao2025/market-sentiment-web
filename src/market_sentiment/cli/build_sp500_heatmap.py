from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

# ✅ 复用你现成的 market-cap / weight 逻辑（来自 build_sp500_index.py）
from market_sentiment.cli.build_sp500_index import (
    _find_symbol_column,
    _find_market_cap_column,
    fetch_market_caps_from_yf,
)

INDEX_SYMBOL = "SPX"
INDEX_NAME = "S&P 500 Index"


# -------------------------
# small utils
# -------------------------

def _read_json(p: Path) -> Any:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def _is_date_stem(s: str) -> bool:
    if len(s) != 10:
        return False
    try:
        datetime.fromisoformat(s)
        return True
    except Exception:
        return False

def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if not (v == v):  # NaN
            return None
        return v
    except Exception:
        return None

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# -------------------------
# universe + weights + optional classification
# -------------------------

def load_universe_with_meta(universe_csv: Path) -> pd.DataFrame:
    """
    Ensure columns:
      symbol, name?, sector?, industry?, market_cap, weight
    market_cap absent => fetch from yfinance (same as build_sp500_index.py)
    """
    uni = pd.read_csv(universe_csv)

    sym_col = _find_symbol_column(uni)
    uni = uni.rename(columns={sym_col: "symbol"})
    uni["symbol"] = uni["symbol"].astype(str)

    # optional meta columns
    name_col = _pick_col(uni, ["name", "Name", "security", "Security", "company_name", "Company", "Company Name"])
    sector_col = _pick_col(uni, ["sector", "Sector", "gics_sector", "GICS Sector", "GICS_Sector"])
    industry_col = _pick_col(uni, ["industry", "Industry", "subIndustry", "Sub-Industry", "GICS Sub-Industry", "gics_sub_industry"])

    if name_col and name_col != "name":
        uni = uni.rename(columns={name_col: "name"})
    if sector_col and sector_col != "sector":
        uni = uni.rename(columns={sector_col: "sector"})
    if industry_col and industry_col != "industry":
        uni = uni.rename(columns={industry_col: "industry"})

    mktcap_col = _find_market_cap_column(uni)
    if mktcap_col is None:
        syms = sorted(uni["symbol"].astype(str).unique().tolist())
        caps = fetch_market_caps_from_yf(syms)
        if caps.empty:
            raise ValueError("[SPX-HEATMAP] Could not fetch any market caps from yfinance")
        uni = uni.merge(caps, on="symbol", how="inner")
    else:
        if mktcap_col != "market_cap":
            uni = uni.rename(columns={mktcap_col: "market_cap"})

    uni = uni.dropna(subset=["market_cap"])
    uni = uni[uni["market_cap"] > 0].copy()

    total_cap = float(uni["market_cap"].sum())
    if total_cap <= 0:
        raise ValueError("[SPX-HEATMAP] Total market cap non-positive")

    uni["weight"] = uni["market_cap"] / total_cap

    if "name" not in uni.columns:
        uni["name"] = ""
    if "sector" not in uni.columns:
        uni["sector"] = None
    if "industry" not in uni.columns:
        uni["industry"] = None

    return uni[["symbol", "name", "sector", "industry", "market_cap", "weight"]].reset_index(drop=True)


def _load_cache(cache_path: Path) -> Dict[str, Dict[str, Any]]:
    if cache_path.exists():
        try:
            obj = _read_json(cache_path)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    return {}

def _save_cache(cache_path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    _write_json(cache_path, cache)

def fill_missing_sector_industry(universe: pd.DataFrame, cache_path: Path) -> pd.DataFrame:
    """
    Fill missing name/sector/industry using cache or yfinance.
    Cache prevents re-query each run.
    """
    cache = _load_cache(cache_path)

    def ok_str(x: Any) -> bool:
        return isinstance(x, str) and x.strip() != ""

    need: List[str] = []
    for i, r in universe.iterrows():
        sym = str(r["symbol"])
        name = r.get("name", "")
        sector = r.get("sector", None)
        industry = r.get("industry", None)

        cached = cache.get(sym, {})
        if (not ok_str(name)) and ok_str(cached.get("name")):
            universe.at[i, "name"] = cached["name"]
            name = cached["name"]
        if (not ok_str(sector)) and ok_str(cached.get("sector")):
            universe.at[i, "sector"] = cached["sector"]
            sector = cached["sector"]
        if (not ok_str(industry)) and ok_str(cached.get("industry")):
            universe.at[i, "industry"] = cached["industry"]
            industry = cached["industry"]

        if not (ok_str(name) and ok_str(sector) and ok_str(industry)):
            need.append(sym)

    if need:
        print(f"[SPX-HEATMAP] Fetching classification for {len(need)} tickers from yfinance ...")
        multi = yf.Tickers(" ".join(need))
        for sym in need:
            try:
                t = multi.tickers.get(sym) or yf.Ticker(sym)
                info = getattr(t, "info", {}) or {}
                if not isinstance(info, dict):
                    info = {}

                name = info.get("shortName") or info.get("longName") or info.get("displayName") or ""
                sector = info.get("sector") or ""
                industry = info.get("industry") or ""

                cache.setdefault(sym, {})
                if isinstance(name, str) and name.strip():
                    cache[sym]["name"] = name
                if isinstance(sector, str) and sector.strip():
                    cache[sym]["sector"] = sector
                if isinstance(industry, str) and industry.strip():
                    cache[sym]["industry"] = industry

                time.sleep(0.02)
            except Exception as e:
                print(f"[SPX-HEATMAP] Warning: yfinance classification failed for {sym}: {e!r}")

        _save_cache(cache_path, cache)

        # apply once more
        for i, r in universe.iterrows():
            sym = str(r["symbol"])
            cached = cache.get(sym, {})
            if not ok_str(r.get("name", "")) and ok_str(cached.get("name")):
                universe.at[i, "name"] = cached["name"]
            if not ok_str(r.get("sector", "")) and ok_str(cached.get("sector")):
                universe.at[i, "sector"] = cached["sector"]
            if not ok_str(r.get("industry", "")) and ok_str(cached.get("industry")):
                universe.at[i, "industry"] = cached["industry"]

    universe["sector"] = universe["sector"].fillna("Unknown").astype(str)
    universe["industry"] = universe["industry"].fillna("Unknown").astype(str)
    universe["name"] = universe["name"].fillna("").astype(str)
    return universe


# -------------------------
# dates
# -------------------------

def latest_trading_day_from_spx_index(spx_index_json: Path) -> str:
    obj = _read_json(spx_index_json)
    daily = obj.get("daily", [])
    if not isinstance(daily, list) or not daily:
        raise ValueError("[SPX-HEATMAP] sp500_index.json missing daily[]")
    dates = []
    for r in daily:
        if isinstance(r, dict) and isinstance(r.get("date"), str) and _is_date_stem(r["date"]):
            dates.append(r["date"])
    if not dates:
        raise ValueError("[SPX-HEATMAP] sp500_index.json has no valid date")
    return sorted(dates)[-1]

def current_date_from_sentiment(data_root: Path, symbols: List[str]) -> Optional[str]:
    best: Optional[str] = None
    for sym in symbols:
        folder = data_root / sym / "sentiment"
        if not folder.exists():
            continue
        stems = [p.stem for p in folder.glob("*.json")]
        stems = [s for s in stems if _is_date_stem(s)]
        if not stems:
            continue
        d = max(stems)
        if best is None or d > best:
            best = d
    return best


# -------------------------
# per-ticker reading
# -------------------------

def sentiment_for_date(data_root: Path, symbol: str, day: str) -> Optional[float]:
    p = data_root / symbol / "sentiment" / f"{day}.json"
    if not p.exists():
        return None
    try:
        obj = _read_json(p)
        if not isinstance(obj, dict):
            return None
        v = obj.get("score_mean")
        if v is None:
            v = obj.get("sentiment")
        return _safe_float(v)
    except Exception:
        return None


@dataclass
class PriceSeries:
    dates: List[str]
    closes: List[float]
    idx: Dict[str, int]


def load_price_series(data_root: Path, symbol: str) -> Optional[PriceSeries]:
    p = data_root / symbol / "price" / "daily.json"
    if not p.exists():
        return None
    try:
        rows = _read_json(p)
        if not isinstance(rows, list):
            return None
        pairs: List[Tuple[str, float]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            d = r.get("date")
            c = r.get("close")
            if not (isinstance(d, str) and _is_date_stem(d)):
                continue
            cv = _safe_float(c)
            if cv is None:
                continue
            pairs.append((d, float(cv)))
        if not pairs:
            return None
        pairs.sort(key=lambda x: x[0])
        dates = [d for d, _ in pairs]
        closes = [c for _, c in pairs]
        idx = {d: i for i, d in enumerate(dates)}
        return PriceSeries(dates=dates, closes=closes, idx=idx)
    except Exception:
        return None


def close_prev_ret(ps: Optional[PriceSeries], day: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if ps is None:
        return None, None, None
    i = ps.idx.get(day)
    if i is None:
        return None, None, None
    close = ps.closes[i]
    prev = ps.closes[i - 1] if i - 1 >= 0 else None
    ret = None
    if prev is not None and prev != 0:
        ret = (close - prev) / prev
    return close, prev, ret


# -------------------------
# snapshot build
# -------------------------

def _wavg(rows: List[Dict[str, Any]], metric: str) -> Optional[float]:
    num = 0.0
    den = 0.0
    for r in rows:
        v = r.get(metric)
        w = r.get("weight")
        if not isinstance(v, (int, float)) or not isinstance(w, (int, float)):
            continue
        if not (v == v) or not (w == w) or w <= 0:
            continue
        num += float(w) * float(v)
        den += float(w)
    return (num / den) if den > 0 else None


def build_snapshot(universe: pd.DataFrame, data_root: Path, day: str) -> Dict[str, Any]:
    price_cache: Dict[str, Optional[PriceSeries]] = {}
    rows_out: List[Dict[str, Any]] = []

    for _, r in universe.iterrows():
        sym = str(r["symbol"])

        if sym not in price_cache:
            price_cache[sym] = load_price_series(data_root, sym)

        close, prev, ret = close_prev_ret(price_cache[sym], day)
        sent = sentiment_for_date(data_root, sym, day)

        rows_out.append(
            {
                "symbol": sym,
                "name": str(r.get("name") or ""),
                "sector": str(r.get("sector") or "Unknown"),
                "industry": str(r.get("industry") or "Unknown"),
                "market_cap": float(r["market_cap"]),
                "weight": float(r["weight"]),
                "sentiment": sent,
                "close": close,
                "prev_close": prev,
                "return_1d": ret,
            }
        )

    # sector stats (cap-weighted)
    df = pd.DataFrame(rows_out)

    # contribution = weight * sentiment (missing sentiment treated as 0 contrib)
    df["contrib"] = df.apply(
        lambda x: float(x["weight"]) * float(x["sentiment"]) if x["sentiment"] is not None else 0.0,
        axis=1,
    )

    sector_stats: List[Dict[str, Any]] = []
    for sector, g in df.groupby("sector", dropna=False):
        rs = g.to_dict("records")
        sector_stats.append(
            {
                "sector": str(sector),
                "weight_sum": float(g["weight"].sum()),
                "market_cap_sum": float(g["market_cap"].sum()),
                "sentiment_wavg": _wavg(rs, "sentiment"),
                "return_wavg": _wavg(rs, "return_1d"),
                "contribution_sum": float(g["contrib"].sum()),
                "n": int(len(g)),
            }
        )

    sector_stats.sort(key=lambda x: x["weight_sum"], reverse=True)

    return {"date": day, "rows": rows_out, "sector_stats": sector_stats}


# -------------------------
# main
# -------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Build SPX heatmap payload from data/[ticker] price+sentiment + market-cap sizing.")
    ap.add_argument("--universe", required=True, type=str, help="e.g. data/sp500.csv")
    ap.add_argument("--data-root", required=True, type=str, help="root where data/[ticker] lives, e.g. data")
    ap.add_argument("--spx-index", required=True, type=str, help="e.g. data/SPX/sp500_index.json (for latest trading day)")
    ap.add_argument("--out", required=True, type=str, help="e.g. data/SPX")
    ap.add_argument("--also-out", default=None, type=str, help="optional second output dir, e.g. apps/web/public/data/SPX")
    ap.add_argument("--class-cache", default="data/SPX/sp500_class_cache.json", type=str, help="classification cache JSON path")
    args = ap.parse_args()

    universe = load_universe_with_meta(Path(args.universe))
    universe = fill_missing_sector_industry(universe, Path(args.class_cache))

    data_root = Path(args.data_root)
    latest_trading_day = latest_trading_day_from_spx_index(Path(args.spx_index))
    current_day = current_date_from_sentiment(data_root, universe["symbol"].astype(str).tolist()) or latest_trading_day

    print(f"[SPX-HEATMAP] latest_trading_day={latest_trading_day}, current={current_day}")

    payload = {
        "symbol": INDEX_SYMBOL,
        "name": INDEX_NAME,
        "asof": {
            "latest_trading_day": latest_trading_day,
            "current": current_day,
        },
        "snapshots": {
            "latest_trading_day": build_snapshot(universe, data_root, latest_trading_day),
            "current": build_snapshot(universe, data_root, current_day),
        },
    }

    out_dirs = [Path(args.out)]
    if args.also_out:
        out_dirs.append(Path(args.also_out))

    # default add apps/web/public/data/SPX (safe even if already passed)
    default_public = Path("apps/web/public/data/SPX")
    if default_public not in out_dirs:
        out_dirs.append(default_public)

    for d in out_dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
            _write_json(d / "sp500_heatmap.json", payload)
            print(f"[SPX-HEATMAP] wrote {d / 'sp500_heatmap.json'}")
        except Exception as e:
            print(f"[SPX-HEATMAP] warning: failed writing to {d}: {e!r}")


if __name__ == "__main__":
    main()
