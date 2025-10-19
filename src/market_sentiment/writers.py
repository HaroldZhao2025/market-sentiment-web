# src/market_sentiment/writers.py
from __future__ import annotations

import json
import os
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)


def _fmt_eastern(ts: pd.Timestamp) -> str:
    """
    Format a UTC timestamp to America/New_York ISO string.
    Accepts tz-naive (assumed UTC) as well.
    """
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    if t is pd.NaT:
        return ""
    try:
        et = t.tz_convert("America/New_York")
    except Exception:
        # if naive: localize UTC then convert
        et = t.tz_localize("UTC").tz_convert("America/New_York")
    return et.strftime("%Y-%m-%d %H:%M:%S")


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([], dtype=float)


def _build_one_ticker(
    t: str,
    panel: pd.DataFrame,
    news_rows: Optional[pd.DataFrame],
    max_news: int = 50,
) -> Dict:
    """
    Build the ticker payload used by the Next.js page.
    Expected panel columns (per ticker daily): date, ticker, close, S
    Optional: S_ma7, price_ma5, ret1d, etc. We compute S_ma7 here if missing.
    """
    df = panel[panel["ticker"] == t].sort_values("date").reset_index(drop=True).copy()
    if "date" not in df.columns:
        return {}

    # enforce date dtype -> strings (YYYY-MM-DD)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.date.astype(str)

    # price
    price = _safe_series(df, "close").astype(float).tolist()

    # sentiment
    if "S" not in df.columns and "sentiment" in df.columns:
        df["S"] = df["sentiment"]
    if "S" not in df.columns:
        df["S"] = 0.0
    df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0)

    if "S_ma7" not in df.columns:
        df["S_ma7"] = df["S"].rolling(7, min_periods=1).mean()

    dates = df["date"].tolist()
    S = df["S"].round(6).tolist()
    S_ma7 = df["S_ma7"].round(6).tolist()

    # news slice
    out_news: List[Dict] = []
    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows[news_rows["ticker"] == t].copy()
        if len(nr) > 0:
            nr = nr.sort_values("ts", ascending=False).head(int(max_news))
            for _, r in nr.iterrows():
                out_news.append(
                    {
                        "ts": _fmt_eastern(r.get("ts")),
                        "title": r.get("title", ""),
                        "url": r.get("url", ""),
                        "text": r.get("text", ""),
                    }
                )

    return {
        "symbol": t,
        "date": dates,              # x-axis labels
        "price": price,             # close
        "S": S,                     # daily sentiment
        "S_ma7": S_ma7,             # 7-day MA for chart smoothing
        "news": out_news,           # last N items
    }


def _write_json(path: str, obj: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def write_outputs(panel, news_rows, *rest):
    """
    Backwards-compatible wrapper.

    Accepts either:
      - write_outputs(panel, news_rows, out_dir)
      - write_outputs(panel, news_rows, earn_rows, out_dir)

    Only per-ticker JSON + _tickers.json + portfolio.json are emitted here,
    which is all the web app requires. If `earn_rows` is provided, it will
    also write simple earnings stubs under /earnings.
    """
    # detect signature
    if len(rest) == 1:
        earn_rows = None
        out_dir = rest[0]
    elif len(rest) >= 2:
        earn_rows, out_dir = rest[0], rest[1]
    else:
        raise TypeError("write_outputs(panel, news_rows, [earn_rows,] out_dir)")

    if panel is None or len(panel) == 0:
        raise ValueError("Empty panel passed to write_outputs")

    out_dir = str(out_dir)
    tick_dir = os.path.join(out_dir, "ticker")
    earn_dir = os.path.join(out_dir, "earnings")
    _ensure_dir(out_dir)
    _ensure_dir(tick_dir)
    _ensure_dir(earn_dir)

    # standardize panel schema
    panel = panel.copy()
    if "date" not in panel.columns:
        raise KeyError("panel must contain 'date' column")
    if "ticker" not in panel.columns:
        raise KeyError("panel must contain 'ticker' column")
    panel["date"] = pd.to_datetime(panel["date"], utc=True, errors="coerce")

    tickers = sorted(panel["ticker"].dropna().astype(str).unique().tolist())
    _write_json(os.path.join(out_dir, "_tickers.json"), tickers)

    # portfolio (simple average S across tickers per date)
    pf = (
        panel.groupby("date", as_index=False)["S"]
        .mean(numeric_only=True)
        .rename(columns={"date": "dates"})
        .sort_values("dates")
    )
    pf["dates"] = pf["dates"].dt.date.astype(str)
    _write_json(
        os.path.join(out_dir, "portfolio.json"),
        {
            "dates": pf["dates"].tolist(),
            "S": pf["S"].fillna(0.0).round(6).tolist(),
        },
    )

    # per-ticker payloads
    for t in tickers:
        obj = _build_one_ticker(t, panel, news_rows, max_news=50)
        _write_json(os.path.join(tick_dir, f"{t}.json"), obj)

    # optional: earnings (very light format)
    if earn_rows is not None and len(earn_rows) > 0:
        er = earn_rows.copy()
        if "ticker" in er.columns and "date" in er.columns:
            er["date"] = pd.to_datetime(er["date"], errors="coerce").dt.date.astype(str)
            for t in tickers:
                sub = er[er["ticker"] == t].copy()
                if len(sub) == 0:
                    continue
                items = sub.sort_values("date")[["date"]].to_dict(orient="records")
                _write_json(os.path.join(earn_dir, f"{t}.json"), {"earnings": items})
