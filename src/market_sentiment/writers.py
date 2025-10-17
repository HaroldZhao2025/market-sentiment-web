# src/market_sentiment/writers.py
from __future__ import annotations
from pathlib import Path
import json
from typing import Dict, List
import pandas as pd


# -------- Helpers --------
def _ensure_dt_str(series: pd.Series) -> List[str]:
    d = pd.to_datetime(series, utc=False, errors="coerce")
    # normalize to date (naive), then ISO date string
    return d.dt.tz_localize(None).dt.strftime("%Y-%m-%d").tolist()


def _fmt_eastern(ts_utc) -> str:
    """
    Accepts scalar ts (epoch/int/str/Timestamp/NaT).
    Returns 'YYYY-MM-DD HH:MM' in America/New_York or '' if invalid.
    """
    # Parse; coerce invalid -> NaT. Force UTC so tz_convert works.
    ts = pd.to_datetime(ts_utc, utc=True, errors="coerce")
    if ts is pd.NaT or pd.isna(ts):
        return ""
    # If somehow naive slipped through, localize to UTC
    if getattr(ts, "tzinfo", None) is None:
        try:
            ts = ts.tz_localize("UTC")
        except Exception:
            return ""
    try:
        return ts.tz_convert("America/New_York").strftime("%Y-%m-%d %H:%M")
    except Exception:
        # As a last resort, just date
        try:
            return ts.tz_convert("America/New_York").strftime("%Y-%m-%d")
        except Exception:
            return ""


# -------- Ticker JSON builder (what the web UI consumes) --------
def build_ticker_json(symbol: str, panel: pd.DataFrame, news_rows: pd.DataFrame) -> Dict:
    df = panel[panel.get("ticker") == symbol].copy()
    if df.empty:
        return {"dates": [], "price": [], "sentiment": [], "sentiment_ma7": [], "news": []}

    df = df.sort_values("date")
    if "close" not in df.columns:
        df["close"] = 0.0
    if "S" not in df.columns:
        df["S"] = 0.0

    out = {
        "dates": _ensure_dt_str(df["date"]),
        "price": df["close"].astype(float).tolist(),
        "sentiment": df["S"].astype(float).fillna(0.0).tolist(),
        "sentiment_ma7": (
            df["S"].astype(float).rolling(7, min_periods=1).mean().fillna(0.0).tolist()
        ),
        "news": [],
    }

    if isinstance(news_rows, pd.DataFrame) and not news_rows.empty:
        n = news_rows[news_rows.get("ticker") == symbol].copy()
        if not n.empty:
            n = n.sort_values("ts").tail(50)  # limit size
            out["news"] = [
                {
                    "ts": _fmt_eastern(r.get("ts")),
                    "title": str(r.get("title") or ""),
                    "url": str(r.get("url") or ""),
                    "S": float(r.get("S") or 0.0),
                }
                for _, r in n.iterrows()
            ]

    return out


# -------- Portfolio builder (equal-weight long/short by daily S) --------
def build_portfolio(panel: pd.DataFrame) -> Dict:
    if panel.empty:
        return {"dates": [], "long": [], "short": [], "long_short": []}

    df = panel.copy()
    if "ret_oc_1d" not in df.columns:
        df["ret_oc_1d"] = 0.0
    if "S" not in df.columns:
        df["S"] = 0.0

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values(["date", "ticker"])

    def one_day(g: pd.DataFrame):
        g = g.dropna(subset=["S", "ret_oc_1d"])
        if g.empty:
            return pd.Series({"long": 0.0, "short": 0.0, "long_short": 0.0})
        q20 = g["S"].quantile(0.2)
        q80 = g["S"].quantile(0.8)
        long = g[g["S"] >= q80]["ret_oc_1d"].mean()
        short = -g[g["S"] <= q20]["ret_oc_1d"].mean()
        long = float(long if pd.notna(long) else 0.0)
        short = float(short if pd.notna(short) else 0.0)
        return pd.Series({"long": long, "short": short, "long_short": long + short})

    daily = df.groupby("date", as_index=False).apply(one_day).reset_index(drop=True)
    return {
        "dates": daily["date"].dt.strftime("%Y-%m-%d").tolist(),
        "long": daily["long"].tolist(),
        "short": daily["short"].tolist(),
        "long_short": daily["long_short"].tolist(),
    }


# -------- Main writer (matches CLI call signature) --------
def write_outputs(
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    earn_rows: pd.DataFrame,
    out_dir: str | Path,
) -> None:
    """
    panel:     daily panel with ['date','ticker','close','S','ret_oc_1d',...]
    news_rows: ['ticker','ts','title','url','S']   (can be empty)
    earn_rows: ['ticker','ts','title','url','S']   (can be empty)
    out_dir:   destination directory (apps/web/public/data)
    """
    base = Path(out_dir)
    (base / "ticker").mkdir(parents=True, exist_ok=True)
    (base / "earnings").mkdir(parents=True, exist_ok=True)

    # ---- tickers list ----
    tickers = sorted(pd.Series(panel.get("ticker")).dropna().unique().tolist())
    (base / "_tickers.json").write_text(json.dumps(tickers))

    # ---- per-ticker files ----
    for t in tickers:
        obj = build_ticker_json(t, panel, news_rows)
        (base / "ticker" / f"{t}.json").write_text(json.dumps(obj))

    # ---- earnings files (minimal) ----
    if isinstance(earn_rows, pd.DataFrame) and not earn_rows.empty:
        for t in tickers:
            e = earn_rows[earn_rows.get("ticker") == t].copy()
            items = []
            if not e.empty:
                e = e.sort_values("ts").tail(50)
                items = [
                    {
                        "ts": _fmt_eastern(r.get("ts")),
                        "title": str(r.get("title") or ""),
                        "url": str(r.get("url") or ""),
                        "S": float(r.get("S") or 0.0),
                    }
                    for _, r in e.iterrows()
                ]
            (base / "earnings" / f"{t}.json").write_text(json.dumps({"items": items}))
    else:
        for t in tickers:
            (base / "earnings" / f"{t}.json").write_text(json.dumps({"items": []}))

    # ---- portfolio ----
    port = build_portfolio(panel)
    (base / "portfolio.json").write_text(json.dumps(port))
