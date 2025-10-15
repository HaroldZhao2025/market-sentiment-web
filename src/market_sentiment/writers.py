from __future__ import annotations
import json
import pandas as pd
from typing import Dict, Any, List
from .aggregate import normalize_date_local, _ensure_plain_columns

def build_ticker_json(ticker: str, prices: pd.DataFrame, daily_sent: pd.DataFrame, recent_news: pd.DataFrame) -> Dict[str, Any]:
    p = _ensure_plain_columns(prices); s = _ensure_plain_columns(daily_sent)
    p["date"] = normalize_date_local(p["date"])
    s["date"] = pd.to_datetime(s["date"]).dt.tz_localize(None)
    base = (p[["date","close"]]
            .merge(s[["date","S","S_news","S_earn","news_count","earn_count"]], on="date", how="left")
            .sort_values("date"))
    for col, fill in [("S",0.0),("S_news",0.0),("S_earn",0.0),("news_count",0),("earn_count",0)]:
        if col not in base.columns: base[col]=fill
        base[col]=base[col].fillna(fill)
    headlines=[]
    if recent_news is not None and not recent_news.empty:
        rn=recent_news.copy()
        rn["ts"]=pd.to_datetime(rn["ts"], utc=True, errors="coerce")
        rn=rn.sort_values("ts", ascending=False).head(30)
        if "S_item" in rn.columns:
            for _,r in rn.iterrows():
                s_item=float(r.get("S_item",0.0))
                headlines.append({
                    "ts": r["ts"].isoformat(),
                    "title": str(r.get("title",""))[:300],
                    "url": str(r.get("url","")),
                    "score": {"pos": max(s_item,0.0), "neg": max(-s_item,0.0)}
                })
        else:
            for _,r in rn.iterrows():
                headlines.append({
                    "ts": r["ts"].isoformat(),
                    "title": str(r.get("title",""))[:300],
                    "url": str(r.get("url","")),
                })
    return {
        "symbol": ticker,
        "series": {
            "date": base["date"].astype(str).tolist(),
            "close": [float(x) if pd.notna(x) else None for x in base["close"]],
            "S": [float(x) if pd.notna(x) else 0.0 for x in base["S"]],
            "S_news": [float(x) if pd.notna(x) else 0.0 for x in base["S_news"]],
            "S_earn": [float(x) if pd.notna(x) else 0.0 for x in base["S_earn"]],
            "news_count": [int(x) if pd.notna(x) else 0 for x in base["news_count"]],
            "earn_count": [int(x) if pd.notna(x) else 0 for x in base["earn_count"]],
        },
        "recent_headlines": headlines,
    }

def write_index(out_dir, tickers: List[str]):
    path = out_dir / "_tickers.json"
    with open(path, "w") as f:
        json.dump(sorted(tickers), f)
    return path

def write_portfolio_json(out_dir, pnl_df: pd.DataFrame, top_syms: List[str], bot_syms: List[str]):
    obj = {
        "series": {
            "date": pnl_df["date"].astype(str).tolist(),
            "daily_ret": [float(x) for x in pnl_df["ret"].fillna(0.0)],
            "cumret": [float(x) for x in pnl_df["cumret"].fillna(0.0)],
        },
        "top": top_syms,
        "bottom": bot_syms,
    }
    with open(out_dir / "portfolio.json", "w") as f:
        json.dump(obj, f, separators=(",",":"))
