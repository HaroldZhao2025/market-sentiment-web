# src/market_sentiment/writers.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Keep more news items per ticker if desired (default 600)
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "600"))


# ------------------------
# Helpers
# ------------------------

def _ensure_datetime_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    s = pd.to_datetime(df[col], utc=True, errors="coerce")
    return s


def _fmt_eastern(ts) -> Optional[str]:
    """
    Format a timestamp to America/New_York ISO string.
    Accepts pd.Timestamp, epoch-like, or str. Returns None if invalid.
    """
    if ts is None:
        return None
    try:
        t = pd.to_datetime(ts, utc=True, errors="coerce")
        if t is None or pd.isna(t):
            return None
        # convert to Eastern
        t = t.tz_convert("America/New_York")
        return t.strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        try:
            # If tz-naive slipped through
            t = pd.to_datetime(ts, errors="coerce")
            if t is None or pd.isna(t):
                return None
            if t.tzinfo is None:
                t = t.tz_localize("UTC")
            t = t.tz_convert("America/New_York")
            return t.strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return None


def _as_float_list(s: pd.Series, default: float = 0.0) -> List[float]:
    if s is None or len(s) == 0:
        return []
    out = pd.to_numeric(s, errors="coerce").fillna(default).astype(float)
    return out.tolist()


def _roll_ma(values: List[float], window: int = 7) -> List[float]:
    if not values:
        return []
    s = pd.Series(values, dtype="float64")
    ma = s.rolling(window=window, min_periods=1).mean().fillna(0.0)
    return ma.tolist()


# ------------------------
# JSON builders
# ------------------------

def build_ticker_json(
    ticker: str,
    panel: pd.DataFrame,
    news_rows: Optional[pd.DataFrame] = None,
    earn_rows: Optional[pd.DataFrame] = None,
    news_limit: int = NEWS_LIMIT,
) -> Dict:
    """
    Assemble one ticker's JSON payload.

    Expected columns in panel:
      - 'ticker', 'date' (date/datetime), 'open', 'close', optionally 'S'
    news_rows (optional):
      - 'ticker','ts','title','url','text'
    earn_rows (optional):
      - any columns; we just pipe through as a list of records if given
    """
    # ---- series / prices / sentiment
    p = panel.loc[panel["ticker"] == ticker].copy()
    if not p.empty:
        p["date"] = pd.to_datetime(p["date"], errors="coerce")
        p = p.sort_values("date").reset_index(drop=True)
    else:
        p = pd.DataFrame(columns=["date", "open", "close", "S"])

    dates = []
    if not p.empty:
        dates = p["date"].dt.strftime("%Y-%m-%d").fillna("").tolist()

    close = _as_float_list(p["close"]) if "close" in p.columns else []
    open_ = _as_float_list(p["open"]) if "open" in p.columns else []

    if "S" in p.columns:
        S = _as_float_list(p["S"])
    else:
        S = [0.0] * len(dates)

    S_MA7 = _roll_ma(S, window=7)

    # ---- news
    news_total = 0
    news_day_count = 0
    news_list: List[Dict] = []

    if isinstance(news_rows, pd.DataFrame) and not news_rows.empty:
        n = news_rows.loc[news_rows["ticker"] == ticker].copy()
        if not n.empty:
            n["ts"] = _ensure_datetime_col(n, "ts")
            n = n.dropna(subset=["ts"]).sort_values("ts", ascending=False)
            news_total = len(n)
            # Trim for payload size
            n = n.head(news_limit)
            # unique day count over the full (untrimmed) set
            news_day_count = (
                pd.to_datetime(news_rows.loc[news_rows["ticker"] == ticker, "ts"], utc=True, errors="coerce")
                .dt.date.dropna()
                .nunique()
            )

            news_list = []
            for _, r in n.iterrows():
                news_list.append(
                    {
                        "ts": _fmt_eastern(r.get("ts")),
                        "title": (r.get("title") or "").strip(),
                        "url": (r.get("url") or "").strip(),
                        # include short text/summary when present
                        "text": (r.get("text") or "").strip(),
                    }
                )

    # ---- earnings (optional; write what we have)
    earnings_items: List[Dict] = []
    if isinstance(earn_rows, pd.DataFrame) and not earn_rows.empty:
        e = earn_rows.loc[earn_rows["ticker"] == ticker].copy()
        if not e.empty:
            # Try to standardize any timestamp-like columns to strings
            for c in e.columns:
                if "date" in c.lower() or "ts" in c.lower():
                    e[c] = pd.to_datetime(e[c], errors="coerce")
            earnings_items = json.loads(e.to_json(orient="records", date_format="iso"))

    # ---- object (include aliases for UI compatibility)
    obj = {
        "ticker": ticker,
        "date": dates,             # primary
        "dates": dates,            # alias
        "close": close,            # primary
        "price": close,            # alias used by some components
        "open": open_,
        "S": S,                    # primary daily sentiment
        "sentiment": S,            # alias
        "S_MA7": S_MA7,            # primary MA
        "sentiment_ma7": S_MA7,    # alias
        "news": news_list,         # trimmed list
        "news_total": int(news_total),
        "news_day_count": int(news_day_count),
        "earnings": earnings_items,
    }
    return obj


# ------------------------
# Writers
# ------------------------

def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


def _write_tickers_list(base: Path, tickers: List[str]) -> None:
    _write_json(base / "_tickers.json", sorted(tickers))


def _write_portfolio(base: Path, panel: pd.DataFrame) -> None:
    """
    Minimal portfolio: per-day average S across tickers.
    Provides arrays for /portfolio page even in sample mode.
    """
    if panel is None or panel.empty or "S" not in panel.columns:
        # keep route alive with empty arrays
        obj = {"dates": [], "long": [], "short": [], "long_short": []}
        _write_json(base / "portfolio.json", obj)
        return

    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    daily = (
        df.groupby(df["date"].dt.strftime("%Y-%m-%d"), as_index=False)["S"]
        .mean(numeric_only=True)
        .rename(columns={"date": "dates", "S": "long_short"})
    )
    dates = daily["date"].tolist() if "date" in daily.columns else daily.iloc[:, 0].tolist()
    ls = daily["long_short"].astype(float).fillna(0.0).tolist()
    # We keep long/short empty for now; can be extended with actual strategies
    obj = {"dates": dates, "long": [], "short": [], "long_short": ls}
    _write_json(base / "portfolio.json", obj)


def write_outputs(panel: pd.DataFrame, news_rows_or_earn, maybe_earn_or_outdir=None, maybe_outdir: Optional[str] = None) -> None:
    """
    Backward + forward compatible signature.

    Supported call styles:
      - write_outputs(panel, news_rows, out_dir)
      - write_outputs(panel, news_rows, earn_rows, out_dir)

    We detect the form by argument types.
    """
    # ---- normalize args
    news_rows: Optional[pd.DataFrame]
    earn_rows: Optional[pd.DataFrame]
    out_dir: str

    if maybe_outdir is None and isinstance(maybe_earn_or_outdir, (str, os.PathLike)):
        # 3-arg style: (panel, news_rows, out_dir)
        news_rows = news_rows_or_earn if isinstance(news_rows_or_earn, pd.DataFrame) else pd.DataFrame()
        earn_rows = pd.DataFrame()
        out_dir = str(maybe_earn_or_outdir)
    else:
        # 4-arg style: (panel, news_rows, earn_rows, out_dir)
        news_rows = news_rows_or_earn if isinstance(news_rows_or_earn, pd.DataFrame) else pd.DataFrame()
        earn_rows = maybe_earn_or_outdir if isinstance(maybe_earn_or_outdir, pd.DataFrame) else pd.DataFrame()
        out_dir = str(maybe_outdir or "apps/web/public/data")

    base = Path(out_dir)
    base.mkdir(parents=True, exist_ok=True)

    # ---- list of tickers
    tickers = []
    if isinstance(panel, pd.DataFrame) and "ticker" in panel.columns:
        tickers = sorted(pd.Series(panel["ticker"]).dropna().astype(str).unique().tolist())

    # ---- write per-ticker files
    for t in tickers:
        obj = build_ticker_json(t, panel, news_rows=news_rows, earn_rows=earn_rows, news_limit=NEWS_LIMIT)
        _write_json(base / "ticker" / f"{t}.json", obj)

        # also write earnings passthrough per ticker (for /earnings route)
        if isinstance(earn_rows, pd.DataFrame) and not earn_rows.empty:
            e = earn_rows.loc[earn_rows["ticker"] == t].copy()
            if not e.empty:
                # best-effort normalization of any timestamp-like fields
                for c in e.columns:
                    if "date" in c.lower() or "ts" in c.lower():
                        e[c] = pd.to_datetime(e[c], errors="coerce")
                _write_json(base / "earnings" / f"{t}.json", json.loads(e.to_json(orient="records", date_format="iso")))
            else:
                _write_json(base / "earnings" / f"{t}.json", [])
        else:
            _write_json(base / "earnings" / f"{t}.json", [])

    # ---- _tickers.json
    _write_tickers_list(base, tickers)

    # ---- portfolio.json
    _write_portfolio(base, panel)
