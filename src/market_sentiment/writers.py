# src/market_sentiment/writers.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd


# ---- helpers ----

def _fmt_eastern(ts) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    # ts is UTC; display as ISO date-time in New York time for readability
    try:
        ts = pd.to_datetime(ts, utc=True)
        et = ts.tz_convert("America/New_York")
        return et.isoformat()
    except Exception:
        try:
            # fallback: just to UTC ISO
            return pd.to_datetime(ts, utc=True).isoformat()
        except Exception:
            return None


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# ---- main builders ----

def build_ticker_json(ticker: str, panel: pd.DataFrame, news_rows: pd.DataFrame, news_limit: int = 200) -> Dict[str, Any]:
    """
    Build the per-ticker JSON payload consumed by the web app.

    panel columns expected:
      - date (datetime64[ns])
      - ticker (str)
      - open, close (float)
      - S (daily sentiment), SMA_7 (float)
    news_rows columns expected:
      - ts (UTC Timestamp), title, url, text (optional), ticker
    """
    df = panel[panel["ticker"] == ticker].copy()
    df = df.sort_values("date")
    df["date"] = pd.to_datetime(df["date"], utc=True)

    out: Dict[str, Any] = {}

    # Series
    out["date"] = df["date"].dt.date.astype(str).tolist()
    out["ticker"] = ticker
    out["open"] = df["open"].astype(float).round(6).tolist() if "open" in df else []
    out["close"] = df["close"].astype(float).round(6).tolist() if "close" in df else []
    out["S"] = df["S"].astype(float).round(6).tolist() if "S" in df else []
    out["S_MA7"] = df["S_MA7"].astype(float).round(6).tolist() if "S_MA7" in df else (df["S"].rolling(7).mean().round(6).tolist() if "S" in df else [])

    # News (summarized list for UI) + coverage metadata
    n = news_rows[news_rows["ticker"] == ticker].copy() if isinstance(news_rows, pd.DataFrame) else pd.DataFrame(columns=["ticker","ts","title","url","text"])

    news_total = int(len(n))
    news_day_count = int(pd.to_datetime(n["ts"], utc=True).dt.date.nunique()) if news_total else 0

    n = n.sort_values("ts")
    if news_limit and news_total > news_limit:
        n = n.tail(news_limit)

    out["news"] = [
        {
            "ts": _fmt_eastern(r.get("ts")),
            "title": r.get("title") or "",
            "url": r.get("url") or "",
        }
        for _, r in n.iterrows()
    ]
    out["news_total"] = news_total
    out["news_day_count"] = news_day_count

    return out


def write_outputs(panel: pd.DataFrame, news_rows: pd.DataFrame, out_dir: str) -> None:
    """
    Save:
      - data/_tickers.json  (list of tickers)
      - data/portfolio.json (if panel has the long/short columns precomputed)
      - data/ticker/<TICKER>.json (per-ticker payload)

    Signature matches the current CLI call: write_outputs(panel, news_rows, out_dir)
    """
    base = Path(out_dir)
    _ensure_dir(base / "ticker")
    _ensure_dir(base / "earnings")  # kept for compatibility even if empty

    # tickers list
    tickers = sorted(panel["ticker"].dropna().unique().tolist())
    (base / "_tickers.json").write_text(json.dumps(tickers))

    # per ticker JSON
    for t in tickers:
        obj = build_ticker_json(t, panel, news_rows, news_limit=200)
        (base / "ticker" / f"{t}.json").write_text(json.dumps(obj, ensure_ascii=False))

    # Optional: portfolio.json if present in panel
    # Expect columns long, short, long_short with aligned dates, or keep existing behavior if upstream prepares it.
    try:
        cols = ["date", "long", "short", "long_short"]
        if all(c in panel.columns for c in cols[1:]):
            dd = panel.drop_duplicates("date").sort_values("date")[["date"] + cols[1:]].copy()
            dd["date"] = pd.to_datetime(dd["date"], utc=True).dt.date.astype(str)
            portfolio = {
                "dates": dd["date"].tolist(),
                "long": dd["long"].astype(float).tolist(),
                "short": dd["short"].astype(float).tolist(),
                "long_short": dd["long_short"].astype(float).tolist(),
            }
            (base / "portfolio.json").write_text(json.dumps(portfolio))
        else:
            # do nothing if the upstream didn’t compute portfolio columns
            pass
    except Exception:
        # never fail the whole build on portfolio serialization
        pass
