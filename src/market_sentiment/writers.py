# src/market_sentiment/writers.py
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from typing import Dict, Any

def build_ticker_json(ticker: str, prices: pd.DataFrame, daily: pd.DataFrame, top_news: pd.DataFrame) -> Dict[str, Any]:
    # normalize frames
    p = prices.copy()
    d = daily.copy()
    n = top_news.copy()

    # ensure schema
    if "date" in p.columns: p["date"] = pd.to_datetime(p["date"]).dt.strftime("%Y-%m-%d")
    if "date" in d.columns: d["date"] = pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d")

    left = p[["date","close"]].copy() if {"date","close"}.issubset(p.columns) else pd.DataFrame(columns=["date","close"])
    right = d[["date","S"]].copy() if {"date","S"}.issubset(d.columns) else pd.DataFrame(columns=["date","S"])

    ser = left.merge(right, on="date", how="left").sort_values("date")
    series = [{"date": r["date"], "close": float(r["close"]), "S": float(r["S"]) if pd.notna(r["S"]) else 0.0}
              for _, r in ser.iterrows()]

    # news top: keep last 20 by abs(score)
    if not n.empty:
        n = n.sort_values("ts", ascending=False)
        n["s"] = n["score"].astype(float)
        top = n.nlargest(20, "s").copy()
        top["ts"] = pd.to_datetime(top["ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        news = [{"ts": r["ts"], "title": r["title"], "url": r["url"], "s": float(r["s"])} for _, r in top.iterrows()]
    else:
        news = []

    return {"ticker": ticker, "series": series, "news": news}

def write_ticker_json(outdir: Path, ticker: str, obj: Dict[str, Any]) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / f"{ticker}.json", "w") as f:
        json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)

def write_tickers_index(outdir: Path, tickers: list[str]) -> None:
    with open(outdir / "_tickers.json", "w") as f:
        json.dump(sorted(list(set(tickers))), f, separators=(",", ":"), ensure_ascii=False)

def write_portfolio_json(outdir: Path, panel: pd.DataFrame) -> None:
    """
    panel: ['date','ticker','y','signal']
    Long top decile, short bottom decile equal-weight, hold 1d forward.
    """
    if panel is None or panel.empty:
        # minimal file so UI loads
        obj = {"dates": [], "equity": [], "ret": [], "stats": {}}
        with open(outdir / "portfolio.json", "w") as f:
            json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
        return

    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"])
    # daily ranks
    def _day_pnl(g: pd.DataFrame) -> float:
        if g.empty: return 0.0
        q_hi = g["signal"].quantile(0.9)
        q_lo = g["signal"].quantile(0.1)
        long = g[g["signal"] >= q_hi]
        short = g[g["signal"] <= q_lo]
        nL, nS = max(len(long), 1), max(len(short), 1)
        wL = (1.0 / nL) if nL else 0.0
        wS = (1.0 / nS) if nS else 0.0
        ret = (wL * long["y"].sum()) - (wS * short["y"].sum())
        return float(ret)

    daily = df.groupby("date", as_index=False).apply(lambda g: _day_pnl(g)).rename(columns={None:"ret"})
    daily["ret"] = daily.iloc[:, -1] if "ret" not in daily.columns else daily["ret"]
    daily = daily[["date","ret"]].copy()
    daily = daily.sort_values("date")
    rets = daily["ret"].tolist()
    dates = daily["date"].dt.strftime("%Y-%m-%d").tolist()
    equity = []
    eq = 1.0
    for r in rets:
        eq *= (1.0 + (r if pd.notna(r) else 0.0))
        equity.append(float(eq))
    obj = {"dates": dates, "equity": equity, "ret": [float(x) for x in rets], "stats": {}}
    with open(outdir / "portfolio.json", "w") as f:
        json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
