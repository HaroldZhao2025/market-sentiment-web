# src/market_sentiment/writers.py
from __future__ import annotations
from pathlib import Path
import json
import pandas as pd
from typing import Dict

# panel: columns ['date','ticker','open','close','ret_cc_1d','ret_oc_1d','S', ...]
# news_rows: ['ticker','ts','title','url','text','S']
# earn_rows: ['ticker','ts','title','url','text','S'] (optional)

def _ensure_dt_str(series: pd.Series) -> list[str]:
    d = pd.to_datetime(series, utc=False, errors="coerce")
    # normalize to date-only
    return d.dt.tz_localize(None).dt.strftime("%Y-%m-%d").tolist()

def build_ticker_json(symbol: str, panel: pd.DataFrame, news_rows: pd.DataFrame) -> Dict:
    df = panel[panel["ticker"] == symbol].copy()
    if df.empty:
        return {"dates": [], "price": [], "sentiment": [], "sentiment_ma7": [], "news": []}

    df = df.sort_values("date")
    out = {
        "dates": _ensure_dt_str(df["date"]),
        "price": df["close"].astype(float).tolist(),
        "sentiment": df["S"].astype(float).fillna(0.0).tolist(),
        "sentiment_ma7": (
            df["S"].astype(float).rolling(7, min_periods=1).mean().fillna(0.0).tolist()
        ),
        "news": [],
    }

    if not news_rows.empty:
        n = news_rows[news_rows["ticker"] == symbol].copy()
        if not n.empty:
            n = n.sort_values("ts").tail(50)  # keep light
            out["news"] = [
                {
                    "ts": pd.to_datetime(r["ts"], utc=True).tz_convert("US/Eastern").strftime("%Y-%m-%d %H:%M"),
                    "title": str(r.get("title") or ""),
                    "url": str(r.get("url") or ""),
                    "S": float(r.get("S") or 0.0),
                }
                for _, r in n.iterrows()
            ]

    return out

def build_portfolio(panel: pd.DataFrame) -> Dict:
    # Simple equal-weight long-short: long top 20% S, short bottom 20% S, next-day OC return
    if panel.empty:
        return {"dates": [], "long": [], "short": [], "long_short": []}
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"])
    def one_day(g: pd.DataFrame):
        if g.empty:
            return pd.Series({"long": 0.0, "short": 0.0, "long_short": 0.0})
        g = g.dropna(subset=["S","ret_oc_1d"]).copy()
        if g.empty:
            return pd.Series({"long": 0.0, "short": 0.0, "long_short": 0.0})
        q20 = g["S"].quantile(0.2)
        q80 = g["S"].quantile(0.8)
        long = g[g["S"] >= q80]["ret_oc_1d"].mean()
        short = -g[g["S"] <= q20]["ret_oc_1d"].mean()
        ls = (long if pd.notna(long) else 0.0) + (short if pd.notna(short) else 0.0)
        return pd.Series({
            "long": float(long if pd.notna(long) else 0.0),
            "short": float(short if pd.notna(short) else 0.0),
            "long_short": float(ls),
        })
    daily = df.groupby("date", as_index=False).apply(one_day).reset_index(drop=True)
    return {
        "dates": daily["date"].dt.strftime("%Y-%m-%d").tolist(),
        "long": daily["long"].tolist(),
        "short": daily["short"].tolist(),
        "long_short": daily["long_short"].tolist(),
    }

def write_outputs(panel: pd.DataFrame, news_rows: pd.DataFrame, out_dir: str | Path) -> None:
    base = Path(out_dir)
    (base / "ticker").mkdir(parents=True, exist_ok=True)
    (base / "earnings").mkdir(parents=True, exist_ok=True)

    # tickers list
    tickers = sorted(panel["ticker"].dropna().unique().tolist())
    (base / "_tickers.json").write_text(json.dumps(tickers))

    # per-ticker
    for t in tickers:
        obj = build_ticker_json(t, panel, news_rows)
        (base / "ticker" / f"{t}.json").write_text(json.dumps(obj))

    # earnings (optional – keep file present even if empty)
    if "earn_count" in panel.columns:
        # derive minimal items per symbol from news_rows tagged as earnings if you have them
        pass

    # portfolio
    port = build_portfolio(panel)
    (base / "portfolio.json").write_text(json.dumps(port))
