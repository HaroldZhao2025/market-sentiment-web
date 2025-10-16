from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


def _to_est_datestr(s: pd.Series) -> pd.Series:
    """
    Normalize any datetime-like series to America/New_York calendar day (YYYY-MM-DD).
    Works with naive or tz-aware input, and with strings/ints convertible to datetime.
    """
    # Parse first; keep tz info if present
    dt = pd.to_datetime(s, errors="coerce", utc=False)

    # If dtype is tz-aware, convert; otherwise localize
    if hasattr(dt, "dt"):
        try:
            if dt.dt.tz is not None:
                dt = dt.dt.tz_convert("America/New_York")
            else:
                dt = dt.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")
        except Exception:
            # As a fallback, force UTC then convert to EST
            dt = pd.to_datetime(s, errors="coerce", utc=True)
            dt = dt.dt.tz_convert("America/New_York")

        dt = dt.dt.normalize().dt.strftime("%Y-%m-%d")
    else:
        # Non-datetime input (all NaT), just return empty strings
        dt = pd.Series([""] * len(s), index=s.index)
    return dt


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_symbol(sym: str) -> str:
    # keep uppercase, replace path-unfriendly chars
    return str(sym).upper().replace("/", "-").replace("\\", "-")


def _json_default(o: Any):
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, (pd.Timestamp,)):
        # Write UTC ISO if tz-aware; otherwise date iso
        try:
            if o.tzinfo is not None:
                return o.tz_convert("UTC").isoformat()
        except Exception:
            pass
        return o.isoformat()
    if isinstance(o, (pd.Series,)):
        return o.tolist()
    if isinstance(o, (pd.DataFrame,)):
        return o.to_dict(orient="records")
    return str(o)


def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,
    daily: pd.DataFrame,
    top_news: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compose a per-ticker JSON payload for the frontend.

    Expected inputs:
      prices: rows for multiple tickers; needs at least ['ticker','date','close'] (open optional)
      daily:  rows for multiple tickers; needs at least ['ticker','date','S'] and optional ['S_news','S_earn']
      top_news: optional rows with ['ts','title','url','S'] for this ticker

    Returns:
      {
        "ticker": "AAPL",
        "series": {
          "date": [... YYYY-MM-DD ...],
          "price": [...],
          "S": [...],
          "ma7": [...],             # 7-day rolling mean of S
          "S_news": [...],          # optional, present if available
          "S_earn": [...],          # optional, present if available
        },
        "topNews": [
           {"ts": "...", "title": "...", "url": "...", "S": 0.12}, ...
        ]
      }
    """
    t = _safe_symbol(ticker)

    # Filter per-ticker
    p = prices.loc[prices["ticker"].str.upper() == t].copy()
    d = daily.loc[daily["ticker"].str.upper() == t].copy()

    # Normalize date keys to EST day strings to avoid tz/level merge issues
    p["ds"] = _to_est_datestr(p["date"])
    left = p[["ds"]].copy()
    # pick close column (handle either 'close' or 'Close' just in case)
    close_col = "close" if "close" in p.columns else ("Close" if "Close" in p.columns else None)
    if close_col is None:
        # allow build to continue with empty series if price missing
        left["price"] = np.nan
    else:
        left["price"] = pd.to_numeric(p[close_col], errors="coerce")

    # Daily signal block
    d["ds"] = _to_est_datestr(d["date"])
    right = pd.DataFrame({"ds": d["ds"]})
    # The main unified sentiment S
    right["S"] = pd.to_numeric(d.get("S", np.nan), errors="coerce")

    # Optional columns
    if "S_news" in d.columns:
        right["S_news"] = pd.to_numeric(d["S_news"], errors="coerce")
    if "S_earn" in d.columns:
        right["S_earn"] = pd.to_numeric(d["S_earn"], errors="coerce")

    # Merge safely on 'ds'
    ser = left.merge(right, on="ds", how="left").sort_values("ds")

    # Compute 7-day rolling MA on S
    ser["ma7"] = pd.Series(ser["S"]).rolling(7, min_periods=1).mean()

    # Build arrays
    out_dates = ser["ds"].fillna("").tolist()
    out_price = ser["price"].astype(float).fillna(np.nan).tolist()
    out_S = ser["S"].astype(float).fillna(0.0).tolist()
    out_ma7 = ser["ma7"].astype(float).fillna(0.0).tolist()

    payload: Dict[str, Any] = {
        "ticker": t,
        "series": {
            "date": out_dates,
            "price": out_price,
            "S": out_S,
            "ma7": out_ma7,
        },
    }

    # Include split sentiments if present
    if "S_news" in ser.columns:
        payload["series"]["S_news"] = ser["S_news"].astype(float).fillna(0.0).tolist()
    if "S_earn" in ser.columns:
        payload["series"]["S_earn"] = ser["S_earn"].astype(float).fillna(0.0).tolist()

    # Top news formatting (optional)
    top_news_list: List[Dict[str, Any]] = []
    if isinstance(top_news, pd.DataFrame) and not top_news.empty:
        tn = top_news.copy()
        # sort by absolute sentiment if available, otherwise by recency
        if "S" in tn.columns:
            tn = tn.sort_values("S", key=lambda s: s.abs(), ascending=False)
        elif "ts" in tn.columns:
            tn = tn.sort_values("ts", ascending=False)

        for _, r in tn.head(10).iterrows():
            item = {
                "ts": _json_default(pd.to_datetime(r.get("ts"))),
                "title": str(r.get("title", "")),
                "url": str(r.get("url", "")),
            }
            if "S" in tn.columns:
                try:
                    item["S"] = float(r.get("S", 0.0))
                except Exception:
                    item["S"] = 0.0
            top_news_list.append(item)

    payload["topNews"] = top_news_list
    return payload


def write_outputs(
    out_dir: str | Path,
    tickers: List[str],
    objects: Dict[str, Dict[str, Any]],
    portfolio: Optional[pd.DataFrame] = None,
    earnings: Optional[Dict[str, pd.DataFrame]] = None,
) -> Dict[str, Any]:
    """
    Write all artifacts required by the Next.js site.

    Args:
      out_dir: base data dir (e.g. apps/web/public/data)
      tickers: list of tickers (strings)
      objects: dict[ticker] -> per-ticker JSON object (from build_ticker_json)
      portfolio: optional DataFrame with at least ['date','ret'] (daily return)
      earnings: optional dict[ticker] -> DataFrame of earnings items
                Expected columns: ['ts','title','url','text','S'] (S optional)

    Returns a small manifest (counts) for logging.
    """
    base = Path(out_dir)
    tick_dir = base / "ticker"
    earn_dir = base / "earnings"
    _ensure_dir(base)
    _ensure_dir(tick_dir)
    _ensure_dir(earn_dir)

    # _tickers.json
    tickers_sorted = sorted({_safe_symbol(t) for t in tickers})
    (base / "_tickers.json").write_text(json.dumps(tickers_sorted, default=_json_default))

    # ticker/<T>.json files
    n_ticker_written = 0
    for t in tickers_sorted:
        obj = objects.get(t)
        if obj is None:
            # write an empty stub to avoid 404 in site
            obj = {
                "ticker": t,
                "series": {"date": [], "price": [], "S": [], "ma7": []},
                "topNews": [],
            }
        fn = tick_dir / f"{t}.json"
        fn.write_text(json.dumps(obj, default=_json_default))
        n_ticker_written += 1

    # earnings/<T>.json files
    n_earn_written = 0
    if isinstance(earnings, dict):
        for t, df in earnings.items():
            t2 = _safe_symbol(t)
            items: List[Dict[str, Any]] = []
            if isinstance(df, pd.DataFrame) and not df.empty:
                dd = df.copy()
                # Keep only useful keys for web (truncate text to keep payload reasonable)
                for _, r in dd.iterrows():
                    item = {
                        "ts": _json_default(pd.to_datetime(r.get("ts"))),
                        "title": str(r.get("title", "")),
                        "url": str(r.get("url", "")),
                    }
                    txt = str(r.get("text", ""))
                    if len(txt) > 1200:
                        txt = txt[:1200] + " ..."
                    item["text"] = txt
                    if "S" in dd.columns:
                        try:
                            item["S"] = float(r.get("S", 0.0))
                        except Exception:
                            item["S"] = 0.0
                    items.append(item)
            (earn_dir / f"{t2}.json").write_text(json.dumps(items, default=_json_default))
            n_earn_written += 1

    # portfolio.json
    wrote_portfolio = False
    if isinstance(portfolio, pd.DataFrame) and not portfolio.empty:
        pf = portfolio.copy()
        if "date" not in pf.columns:
            # handle index-as-date case
            pf = pf.reset_index()
        pf["date"] = _to_est_datestr(pf["date"])
        if "ret" not in pf.columns:
            # allow alternative naming, otherwise zeros
            if "return" in pf.columns:
                pf["ret"] = pd.to_numeric(pf["return"], errors="coerce")
            else:
                pf["ret"] = 0.0
        pf["ret"] = pd.to_numeric(pf["ret"], errors="coerce").fillna(0.0)
        pf = pf.sort_values("date")
        cum = (1.0 + pf["ret"].astype(float)).cumprod().fillna(1.0)

        portfolio_obj = {
            "date": pf["date"].tolist(),
            "ret": pf["ret"].astype(float).tolist(),
            "cum": cum.astype(float).tolist(),
        }
        (base / "portfolio.json").write_text(json.dumps(portfolio_obj, default=_json_default))
        wrote_portfolio = True

    return {
        "tickers": len(tickers_sorted),
        "ticker_files": n_ticker_written,
        "earnings_files": n_earn_written,
        "portfolio_written": wrote_portfolio,
        "out_dir": str(base),
    }
