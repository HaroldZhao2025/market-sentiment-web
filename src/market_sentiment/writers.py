from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

def _price_sent_series(ticker: str, prices: pd.DataFrame, daily: pd.DataFrame) -> dict:
    p = prices[prices["ticker"] == ticker][["date","close"]].copy()
    d = daily[daily["ticker"] == ticker][["date","S"]].copy()

    left = p.rename(columns={"close":"price"})
    right = d.rename(columns={"S":"sentiment"})

    ser = pd.merge(left, right, on="date", how="left").sort_values("date").reset_index(drop=True)
    ser["sentiment"] = ser["sentiment"].fillna(0.0)
    ser["sentiment_ma7"] = ser["sentiment"].rolling(7, min_periods=1).mean()

    out = {
        "date": ser["date"].dt.strftime("%Y-%m-%d").tolist(),
        "price": ser["price"].astype(float).round(6).tolist(),
        "sentiment": ser["sentiment"].astype(float).round(6).tolist(),
        "sentiment_ma7": ser["sentiment_ma7"].astype(float).round(6).tolist(),
    }
    return out

def _top_news_payload(top_news: pd.DataFrame, ticker: str) -> list[dict]:
    tn = top_news[top_news["ticker"] == ticker].copy()
    if tn.empty:
        return []
    tn = tn.sort_values("absS", ascending=False).head(8)
    tn["date"] = pd.to_datetime(tn["ts"]).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    return [
        {
            "date": r["date"],
            "title": r["title"],
            "url": r["url"],
            "S": float(r["S"]),
        }
        for _, r in tn.iterrows()
    ]

def build_ticker_json(ticker: str, prices: pd.DataFrame, daily: pd.DataFrame, top_news: pd.DataFrame) -> dict:
    series = _price_sent_series(ticker, prices, daily)
    news_items = _top_news_payload(top_news, ticker)
    return {
        "symbol": ticker,
        "series": series,
        "topNews": news_items,
    }

def write_outputs(out_dir: Path, tickers: list[str], panel: pd.DataFrame, per_ticker_objs: dict[str, dict]):
    out_dir.mkdir(parents=True, exist_ok=True)
    # list of tickers
    (out_dir / "_tickers.json").write_text(json.dumps(tickers, indent=2))
    # portfolio (if present)
    if not panel.empty and "ret_cc_1d" in panel.columns and "S" in panel.columns:
        # trivial daily long-short portfolio on S (top/bottom decile)
        df = panel.copy()
        def _weights(group: pd.DataFrame):
            cut_hi = group["S"].quantile(0.9)
            cut_lo = group["S"].quantile(0.1)
            w = (group["S"] >= cut_hi).astype(float) - (group["S"] <= cut_lo).astype(float)
            # normalize long and short separately if both sides present
            pos = (w > 0).sum()
            neg = (w < 0).sum()
            w = w.where(w<=0, w/pos if pos else 0).where(w>=0, w/neg if neg else 0)
            return w
        wt = df.groupby("date", group_keys=False).apply(_weights).rename("w")
        df = df.join(wt)
        df["ret"] = df["w"] * df["ret_cc_1d"]
        pnl = df.groupby("date", as_index=False)["ret"].sum().rename(columns={"ret":"daily_return"})
        pnl["cum_return"] = (1.0 + pnl["daily_return"]).cumprod() - 1.0
        port = {
            "date": pnl["date"].dt.strftime("%Y-%m-%d").tolist(),
            "daily_return": pnl["daily_return"].round(6).tolist(),
            "cum_return": pnl["cum_return"].round(6).tolist(),
        }
        (out_dir / "portfolio.json").write_text(json.dumps(port, indent=2))

    # per-ticker files
    for t, obj in per_ticker_objs.items():
        (out_dir / f"{t}.json").write_text(json.dumps(obj))
