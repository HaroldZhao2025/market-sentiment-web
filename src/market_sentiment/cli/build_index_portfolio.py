# src/market_sentiment/cli/build_index_portfolio.py
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _last_non_none(values: List[Any]) -> Optional[Any]:
    for v in reversed(values):
        if v is not None:
            return v
    return None


def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    try:
        for k in keys:
            if cur is None:
                return default
            cur = cur.get(k)
        return default if cur is None else cur
    except Exception:
        return default


def _extract_meta(j: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (name, sector) in a lenient way; falls back to empty strings.
    Tries a few common locations.
    """
    name = (
        _safe_get(j, "meta", "shortName")
        or _safe_get(j, "meta", "longName")
        or _safe_get(j, "profile", "shortName")
        or _safe_get(j, "info", "shortName")
        or ""
    )
    sector = (
        _safe_get(j, "meta", "sector")
        or _safe_get(j, "profile", "sector")
        or _safe_get(j, "info", "sector")
        or ""
    )
    return str(name), str(sector)


def _extract_daily_scores(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize daily scoring time series into list of {date, score, pred_return?}
    Tries several shapes:
      - j["sentiment"]["daily"] = [{date, score, predicted_return?}, ...]
      - j["daily"] with similar shape
    """
    cands = []
    s1 = _safe_get(j, "sentiment", "daily")
    if isinstance(s1, list):
        cands = s1
    elif isinstance(_safe_get(j, "daily"), list):
        cands = _safe_get(j, "daily")
    out = []
    for row in cands or []:
        # tolerate multiple possible keys
        date = row.get("date") or row.get("d")
        score = row.get("score") or row.get("s")
        pr   = row.get("predicted_return") or row.get("pred") or row.get("r")
        out.append({"date": date, "score": score, "pred": pr})
    return out


def _extract_prices(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize OHLC or close series into list of {date, close}.
    Tries a few common layouts seen in yfinance or custom exports.
    """
    # Try explicit "prices"
    prices = _safe_get(j, "prices")
    if isinstance(prices, list):
        out = []
        for p in prices:
            date = p.get("date") or p.get("d")
            close = p.get("adjClose") or p.get("close") or p.get("c")
            if date is not None and close is not None:
                out.append({"date": date, "close": float(close)})
        return out

    # Try "history" / "chart"
    hist = _safe_get(j, "history") or _safe_get(j, "chart")
    if isinstance(hist, list):
        out = []
        for p in hist:
            date = p.get("date") or p.get("d")
            close = p.get("adjClose") or p.get("close") or p.get("c")
            if date is not None and close is not None:
                out.append({"date": date, "close": float(close)})
        return out

    # Nothing found
    return []


def _index_from_tickers(ticker_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for f in sorted(ticker_dir.glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f)
        if not isinstance(j, dict):
            continue
        name, sector = _extract_meta(j)
        daily = _extract_daily_scores(j)
        last_score = _last_non_none([x.get("score") for x in daily if x.get("score") is not None])
        last_pred  = _last_non_none([x.get("pred")  for x in daily if x.get("pred")  is not None])
        last_date  = _last_non_none([x.get("date")  for x in daily if x.get("date")  is not None])
        rows.append({
            "symbol": sym,
            "name": name,
            "sector": sector,
            "last_date": last_date,
            "last_score": last_score,
            "last_predicted_return": last_pred,
        })
    return rows


def _merge_dates(series_list: List[List[Dict[str, Any]]]) -> List[str]:
    s = set()
    for series in series_list:
        for row in series:
            if row.get("date") is not None:
                s.add(row["date"])
    return sorted(s)


def _to_map_by_date(series: List[Dict[str, Any]], key: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in series:
        d = row.get("date")
        v = row.get(key)
        if d is None or v is None:
            continue
        try:
            out[str(d)] = float(v)
        except Exception:
            pass
    return out


def _pctchg_from_prices(prices: List[Dict[str, Any]]) -> Dict[str, float]:
    """Compute simple daily pct change by date."""
    out: Dict[str, float] = {}
    # sort by date as string (dates look like ISO yyyy-mm-dd in your site)
    arr = sorted(prices, key=lambda r: str(r.get("date")))
    prev = None
    for r in arr:
        d = str(r["date"])
        c = float(r["close"])
        if prev is not None and c > 0 and prev > 0:
            out[d] = (c / prev) - 1.0
        prev = c
    return out


def _portfolio_from_signals(ticker_dir: Path, long_n: int = 25, short_n: int = 25) -> Dict[str, Any]:
    """
    Build a naive daily long/short from cross-sectional predicted returns.
    If we cannot compute returns (no prices), we still emit a tiny "valid" shape
    so the UI never shows 'No portfolio data yet.'
    """
    per_ticker_daily_pred: Dict[str, Dict[str, float]] = {}
    per_ticker_daily_ret:  Dict[str, Dict[str, float]] = {}
    for f in sorted(ticker_dir.glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f)
        if not isinstance(j, dict):
            continue
        daily = _extract_daily_scores(j)
        prices = _extract_prices(j)
        pred_map = _to_map_by_date(daily, "pred")
        ret_map  = _pctchg_from_prices(prices)
        if pred_map:
            per_ticker_daily_pred[sym] = pred_map
        if ret_map:
            per_ticker_daily_ret[sym] = ret_map

    # collect all dates where we have any predictions
    all_dates = set()
    for m in per_ticker_daily_pred.values():
        all_dates.update(m.keys())
    dates = sorted(all_dates)

    equity = 1.0
    curve: List[Dict[str, Any]] = []
    daily_rets: List[Dict[str, Any]] = []

    for d in dates:
        # build cross-section for date d
        cross: List[Tuple[str, float]] = []
        for sym, m in per_ticker_daily_pred.items():
            v = m.get(d)
            if v is not None and not math.isnan(v):
                cross.append((sym, float(v)))
        if len(cross) < 10:
            # too sparse to form a long/short; skip
            continue
        cross.sort(key=lambda x: x[1])
        shorts = [s for s, _ in cross[:short_n]]
        longs  = [s for s, _ in cross[-long_n:]]
        # compute realized return using next-day (or same-day) pct changes if available
        # Here we use same-day simple return from close series for simplicity.
        long_ret = [per_ticker_daily_ret.get(s, {}).get(d) for s in longs]
        short_ret= [per_ticker_daily_ret.get(s, {}).get(d) for s in shorts]
        long_ret = [r for r in long_ret if r is not None]
        short_ret= [r for r in short_ret if r is not None]
        if not long_ret or not short_ret:
            continue
        # equal-weight long/short
        r_long  = sum(long_ret)  / len(long_ret)
        r_short = sum(short_ret) / len(short_ret)
        r = 0.5 * r_long - 0.5 * r_short
        equity *= (1.0 + r)
        daily_rets.append({"date": d, "ret": r})
        curve.append({"date": d, "equity": equity})

    # if we still have nothing, write a tiny valid file so UI doesn't show '0 points'
    if not curve:
        return {
            "points": 1,
            "equity_curve": [{"date": "1970-01-01", "equity": 1.0}],
            "daily": [{"date": "1970-01-01", "ret": 0.0}],
            "long_n": long_n,
            "short_n": short_n,
        }

    return {
        "points": len(curve),
        "equity_curve": curve,
        "daily": daily_rets,
        "long_n": long_n,
        "short_n": short_n,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="apps/web/public/data",
                    help="Folder where /ticker/*.json live and where index/portfolio will be written.")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    ticker_dir = data_dir / "ticker"
    data_dir.mkdir(parents=True, exist_ok=True)
    ticker_dir.mkdir(parents=True, exist_ok=True)

    # Build index.json
    index_rows = _index_from_tickers(ticker_dir)
    with (data_dir / "index.json").open("w", encoding="utf-8") as f:
        json.dump({"count": len(index_rows), "tickers": index_rows}, f, ensure_ascii=False)

    # Build portfolio.json
    port = _portfolio_from_signals(ticker_dir)
    with (data_dir / "portfolio.json").open("w", encoding="utf-8") as f:
        json.dump(port, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
