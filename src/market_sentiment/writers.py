from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List
import json
import math
import pandas as pd


def _safe_float(x) -> float:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _to_dates(dts: pd.Series) -> list[str]:
    # Expect tz-naive normalized dates here; if tz-aware, strip tz then normalize.
    idx = pd.to_datetime(dts, errors="coerce")
    if getattr(idx.dt, "tz", None) is not None:
        idx = idx.dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
    return [t.strftime("%Y-%m-%d") for t in idx]


def _ma(series: pd.Series, n: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return s.rolling(n, min_periods=1).mean()


def build_ticker_json(
    ticker: str,
    prices_one: pd.DataFrame,
    daily_one: pd.DataFrame,
    top_news_one: pd.DataFrame | None = None,
) -> Dict[str, Any]:
    """
    Frontend expects:
      {
        "symbol": "AAPL",
        "dates": ["2024-01-02", ...],
        "price": [189.1, ...],
        "sentiment": [0.12, ...],         # daily combined S
        "sentiment_ma7": [0.10, ...],     # 7d MA of S
        "news_count": [2, ...],
        "earn_count": [0, ...],
        "top_news": [{ "ts":"2024-05-02T13:00:00Z","title":"...","url":"...","S":0.33 }, ...]
      }
    """
    p = prices_one.copy()
    d = daily_one.copy()

    # Ensure needed cols
    for col in ["date", "close"]:
        if col not in p.columns:
            raise KeyError(f"prices missing required column: {col}")

    # Normalize daily columns
    d = d.rename(
        columns={
            "S": "S",
            "S_news": "S_news",
            "S_earn": "S_earn",
            "news_count": "news_count",
            "earn_count": "earn_count",
        }
    )
    for col, default in [("S", 0.0), ("news_count", 0), ("earn_count", 0)]:
        if col not in d.columns:
            d[col] = default

    # Merge series on date
    left = p[["date", "close"]].copy()
    left["date"] = pd.to_datetime(left["date"], utc=False, errors="coerce").dt.normalize()
    right = d[["date", "S", "news_count", "earn_count"]].copy()
    right["date"] = pd.to_datetime(right["date"], utc=False, errors="coerce").dt.normalize()

    ser = (
        left.merge(right, on="date", how="left")
            .sort_values("date")
            .reset_index(drop=True)
    )

    ser["S"] = pd.to_numeric(ser["S"], errors="coerce").fillna(0.0)
    ser["news_count"] = pd.to_numeric(ser["news_count"], errors="coerce").fillna(0).astype(int)
    ser["earn_count"] = pd.to_numeric(ser["earn_count"], errors="coerce").fillna(0).astype(int)

    out: Dict[str, Any] = {
        "symbol": ticker,
        "dates": _to_dates(ser["date"]),
        "price": [ _safe_float(x) for x in ser["close"] ],
        "sentiment": [ _safe_float(x) for x in ser["S"] ],
        "sentiment_ma7": [ _safe_float(x) for x in _ma(ser["S"], 7) ],
        "news_count": ser["news_count"].tolist(),
        "earn_count": ser["earn_count"].tolist(),
        "top_news": [],
    }

    # Optional top news (already filtered upstream)
    if top_news_one is not None and not top_news_one.empty:
        tn = top_news_one.copy()
        tn["ts"] = pd.to_datetime(tn["ts"], utc=True, errors="coerce")
        if "S" not in tn.columns:
            tn["S"] = 0.0
        tn = tn.sort_values("ts", ascending=False).head(15)
        out["top_news"] = [
            {
                "ts": (ts.isoformat().replace("+00:00", "Z") if pd.notna(ts) else None),
                "title": str(row.get("title", ""))[:500],
                "url": str(row.get("url", "")),
                "S": _safe_float(row.get("S", 0.0)),
            }
            for ts, row in tn.set_index("ts").iterrows()
        ]

    return out


def write_outputs(
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    earn_rows: pd.DataFrame,
    out_dir: str | Path,
) -> None:
    """
    Writes:
      apps/web/public/data/_tickers.json          -> ["A","AAPL",...]
      apps/web/public/data/ticker/<SYM>.json      -> per-ticker series (shape above)
      apps/web/public/data/earnings/<SYM>.json    -> earnings docs meta (optional use)
      apps/web/public/data/portfolio.json         -> long/short backtest (if present in panel)
    """
    base = Path(out_dir)
    (base / "ticker").mkdir(parents=True, exist_ok=True)
    (base / "earnings").mkdir(parents=True, exist_ok=True)

    # Tickers list
    tickers = sorted(panel["ticker"].dropna().unique().tolist())
    with open(base / "_tickers.json", "w") as f:
        json.dump(tickers, f)

    # Earnings docs (for the /earnings page)
    if not earn_rows.empty:
        for sym, df_e in earn_rows.groupby("ticker"):
            df_e = df_e.copy()
            df_e["ts"] = pd.to_datetime(df_e["ts"], utc=True, errors="coerce")
            rows = [
                {
                    "ts": (ts.isoformat().replace("+00:00", "Z") if pd.notna(ts) else None),
                    "title": str(r.get("title", ""))[:500],
                    "url": str(r.get("url", "")),
                    "S": _safe_float(r.get("S", 0.0)),
                }
                for ts, r in df_e.set_index("ts").iterrows()
            ]
            with open(base / "earnings" / f"{sym}.json", "w") as f:
                json.dump(rows, f)

    # Per-ticker time series
    for sym, p1 in panel.groupby("ticker"):
        p1 = p1.sort_values("date")
        # Pull daily (already merged onto panel), but safer to reconstruct:
        d1 = (
            p1[["date", "ticker", "S", "news_count", "earn_count"]]
            .groupby(["date", "ticker"], as_index=False)
            .agg({"S":"mean", "news_count":"sum", "earn_count":"sum"})
        )
        # Top news subset from news_rows
        tn = None
        if not news_rows.empty:
            tn = news_rows[news_rows["ticker"] == sym].copy()
        obj = build_ticker_json(sym, p1, d1, tn)
        with open(base / "ticker" / f"{sym}.json", "w") as f:
            json.dump(obj, f)

    # Portfolio (optional)
    # If panel has backtest/ret columns aggregated elsewhere, keep file present to avoid 404
    port_path = base / "portfolio.json"
    if not port_path.exists():
        with open(port_path, "w") as f:
            json.dump({"dates": [], "long": [], "short": [], "long_short": []}, f)
