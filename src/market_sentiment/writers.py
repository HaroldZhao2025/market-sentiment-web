# src/market_sentiment/writers.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd


# --------------------------
# Helpers
# --------------------------

def _fmt_eastern(ts) -> Optional[str]:
    """Format UTC timestamp as America/New_York ISO string; fall back to UTC ISO."""
    if ts is None or pd.isna(ts):
        return None
    try:
        ts = pd.to_datetime(ts, utc=True)
        et = ts.tz_convert("America/New_York")
        return et.isoformat()
    except Exception:
        try:
            return pd.to_datetime(ts, utc=True).isoformat()
        except Exception:
            return None


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _parse_args_for_write_outputs(args: Tuple[Any, ...]) -> Tuple[pd.DataFrame, str]:
    """
    Accept either:
      write_outputs(panel, news_rows, out_dir)
    or:
      write_outputs(panel, news_rows, earn_rows, out_dir)

    Return (earn_rows, out_dir) where earn_rows is an empty DataFrame if not provided.
    """
    if len(args) == 1:
        # (out_dir,)
        return pd.DataFrame(columns=["ticker", "ts", "title", "url"]), str(args[0])
    if len(args) == 2:
        # (earn_rows, out_dir)
        earn_rows, out_dir = args
        if not isinstance(earn_rows, pd.DataFrame):
            # Defensive: if the older caller passed strings by mistake, swap
            earn_rows, out_dir = pd.DataFrame(columns=["ticker", "ts", "title", "url"]), str(earn_rows)
        return earn_rows, str(out_dir)
    raise TypeError(
        "write_outputs expected 3 or 4 positional args:\n"
        "  write_outputs(panel, news_rows, out_dir)\n"
        "  write_outputs(panel, news_rows, earn_rows, out_dir)"
    )


# --------------------------
# Builders
# --------------------------

def build_ticker_json(
    ticker: str,
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    earn_rows: Optional[pd.DataFrame] = None,
    news_limit: Optional[int] = 200,
) -> Dict[str, Any]:
    """
    Build the per-ticker JSON that the web app consumes.

    Expected columns in `panel` (by ticker):
      date (datetime-like), ticker, open, close, S (daily sentiment), S_MA7

    Expected columns in `news_rows` / `earn_rows`:
      ticker, ts (UTC), title, url, (optional) text
    """
    df = panel[panel["ticker"] == ticker].copy()
    df = df.sort_values("date")
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    out: Dict[str, Any] = {}
    out["ticker"] = ticker
    out["date"] = df["date"].dt.date.astype(str).tolist() if "date" in df else []

    for col in ("open", "close", "S", "S_MA7"):
        if col in df:
            out[col] = pd.to_numeric(df[col], errors="coerce").round(6).fillna(0).tolist()
        else:
            out[col] = []

    # If S_MA7 missing but S exists, compute a simple 7D mean
    if (not out["S_MA7"]) and out["S"]:
        s = pd.Series(out["S"], dtype=float)
        out["S_MA7"] = s.rolling(7, min_periods=1).mean().round(6).tolist()

    # ---------- News ----------
    n = news_rows[news_rows["ticker"] == ticker].copy() if isinstance(news_rows, pd.DataFrame) else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text"]
    )
    n_total = int(len(n))
    n_days = int(pd.to_datetime(n["ts"], utc=True, errors="coerce").dt.date.nunique()) if n_total else 0

    n = n.sort_values("ts")
    if news_limit and n_total > news_limit:
        n = n.tail(news_limit)

    out["news"] = [
        {"ts": _fmt_eastern(r.get("ts")), "title": r.get("title") or "", "url": r.get("url") or ""}
        for _, r in n.iterrows()
    ]
    out["news_total"] = n_total
    out["news_day_count"] = n_days

    # ---------- Earnings (optional) ----------
    earns_list = []
    if isinstance(earn_rows, pd.DataFrame) and not earn_rows.empty:
        e = earn_rows[earn_rows["ticker"] == ticker].copy()
        if not e.empty:
            e = e.sort_values("ts")
            earns_list = [
                {"ts": _fmt_eastern(r.get("ts")), "title": r.get("title") or "", "url": r.get("url") or ""}
                for _, r in e.iterrows()
            ]
    out["earnings"] = earns_list

    return out


def write_outputs(panel: pd.DataFrame, news_rows: pd.DataFrame, *args: Any) -> None:
    """
    Save:
      - data/_tickers.json
      - data/ticker/<TICKER>.json
      - data/earnings/<TICKER>.json   (if earn_rows provided)
      - data/portfolio.json           (if long/short columns found)

    Backward compatible signatures:
      write_outputs(panel, news_rows, out_dir)
      write_outputs(panel, news_rows, earn_rows, out_dir)
    """
    earn_rows, out_dir = _parse_args_for_write_outputs(args)

    base = Path(out_dir)
    _ensure_dir(base)
    _ensure_dir(base / "ticker")
    _ensure_dir(base / "earnings")  # used if earn_rows provided

    # tickers
    tickers = sorted(pd.Series(panel.get("ticker", [])).dropna().unique().tolist())
    (base / "_tickers.json").write_text(json.dumps(tickers))

    # per-ticker JSON
    for t in tickers:
        obj = build_ticker_json(t, panel, news_rows, earn_rows=earn_rows, news_limit=200)
        # main payload
        (base / "ticker" / f"{t}.json").write_text(json.dumps(obj, ensure_ascii=False))

        # write a small earnings file too (for /earnings/[symbol] page)
        if obj.get("earnings"):
            (base / "earnings" / f"{t}.json").write_text(json.dumps(obj["earnings"], ensure_ascii=False))

    # portfolio.json if present
    try:
        cols = ["date", "long", "short", "long_short"]
        if all(c in panel.columns for c in cols[1:]):
            dd = panel.drop_duplicates("date").sort_values("date")[["date"] + cols[1:]].copy()
            dd["date"] = pd.to_datetime(dd["date"], utc=True, errors="coerce").dt.date.astype(str)
            portfolio = {
                "dates": dd["date"].tolist(),
                "long": pd.to_numeric(dd["long"], errors="coerce").fillna(0).tolist(),
                "short": pd.to_numeric(dd["short"], errors="coerce").fillna(0).tolist(),
                "long_short": pd.to_numeric(dd["long_short"], errors="coerce").fillna(0).tolist(),
            }
            (base / "portfolio.json").write_text(json.dumps(portfolio))
    except Exception:
        # Never fail the whole build on portfolio serialization
        pass
