from __future__ import annotations
import pandas as pd
from typing import Dict, Any

from .aggregate import normalize_date_local, _ensure_plain_columns

NY_TZ = "America/New_York"

def build_ticker_json(ticker: str,
                      prices: pd.DataFrame,
                      daily_sent: pd.DataFrame,
                      recent_news: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a compact JSON blob used by the Next.js site.
    Structure:
      {
        "symbol": "AAPL",
        "series": {
          "date": [... ISO yyyy-mm-dd ...],
          "close": [...],
          "S": [...],             # combined daily signal
          "S_news": [...],
          "S_earn": [...],
          "news_count": [...],
          "earn_count": [...]
        },
        "recent_headlines": [
           {"ts": "...", "title":"...", "url":"...", "score":{"pos":..., "neg":..., "conf":...}},
           ...
        ]
      }
    """
    p = _ensure_plain_columns(prices)
    s = _ensure_plain_columns(daily_sent)

    # Normalize dates to naive yyyy-mm-dd (local NY date)
    p["date"] = normalize_date_local(p["date"])
    s["date"] = pd.to_datetime(s["date"]).dt.tz_localize(None)

    # Join price close with S (some dates may have S missing -> fill 0)
    base = (
        p[["date", "close"]]
        .merge(s[["date","S","S_news","S_earn","news_count","earn_count"]],
               on="date", how="left")
        .sort_values("date")
    )
    for col, fillv in [
        ("S", 0.0), ("S_news", 0.0), ("S_earn", 0.0),
        ("news_count", 0), ("earn_count", 0),
    ]:
        if col not in base.columns:
            base[col] = fillv
        base[col] = base[col].fillna(fillv)

    # Recent headlines (top N by recency)
    headlines = []
    if recent_news is not None and not recent_news.empty:
        rn = recent_news.copy()
        rn["ts"] = pd.to_datetime(rn["ts"], utc=True, errors="coerce")
        rn = rn.sort_values("ts", ascending=False).head(30)
        # accept optional per-row S_item if already computed upstream
        if "S_item" in rn.columns:
            def pack(row):
                pos = float(max(row.get("S_item", 0.0), 0.0))
                neg = float(max(-row.get("S_item", 0.0), 0.0))
                return {
                    "ts": row["ts"].isoformat(),
                    "title": str(row.get("title", ""))[:300],
                    "url": str(row.get("url", "")),
                    "score": {"pos": pos, "neg": neg}
                }
            headlines = [pack(r) for _, r in rn.iterrows()]
        else:
            headlines = [
                {
                    "ts": r["ts"].isoformat(),
                    "title": str(r.get("title", ""))[:300],
                    "url": str(r.get("url", "")),
                }
                for _, r in rn.iterrows()
            ]

    obj = {
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
    return obj
