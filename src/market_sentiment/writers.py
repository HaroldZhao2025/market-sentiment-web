from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
from .utils import dump_json

def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df

def _to_eastern_day(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    try:
        tzinfo = s.dt.tz
    except Exception:
        tzinfo = None
    if tzinfo is None:
        s = s.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")
    else:
        s = s.dt.tz_convert("America/New_York")
    return s.dt.normalize()

def _clip_to_price_dates(d: pd.DataFrame, p: pd.DataFrame) -> pd.DataFrame:
    if d.empty or p.empty:
        return d
    lo, hi = p["date"].min(), p["date"].max()
    return d[(d["date"] >= lo) & (d["date"] <= hi)].reset_index(drop=True)

def build_ticker_json(ticker: str, prices: pd.DataFrame, daily_combined: pd.DataFrame,
                      recent_news: pd.DataFrame, earnings_events: pd.DataFrame | None = None) -> dict:
    p = _flatten(prices).copy()
    d = _flatten(daily_combined).copy()
    p["date"] = _to_eastern_day(p["date"])
    if "date" in d.columns:
        d["date"] = _to_eastern_day(d["date"])
        d = _clip_to_price_dates(d, p)

    series = (
        p[["date","close"]]
        .merge(d, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    for col in ["S_news","S_earn","S_total","news_count"]:
        if col not in series.columns:
            series[col] = 0.0 if col != "news_count" else 0
    series["S_news"] = series["S_news"].fillna(0.0)
    series["S_earn"] = series["S_earn"].fillna(0.0)
    series["S_total"] = series["S_total"].fillna(0.0)
    series["news_count"] = series["news_count"].fillna(0).astype(int)

    # rolling means
    def _ma(x, w): return x.rolling(w, min_periods=1).mean()
    s_news7  = _ma(series["S_news"], 7)
    s_earn7  = _ma(series["S_earn"], 7)
    s_total7 = _ma(series["S_total"], 7)

    last_total7 = float(s_total7.iloc[-1]) if len(series) else 0.0
    predicted_return = float(np.tanh(last_total7 / 2.0) * 0.02)

    # headlines
    news = recent_news.copy()
    if not news.empty:
        ts = pd.to_datetime(news["ts"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
        news["ts"] = ts
        news = news.sort_values("ts", ascending=False)
    top_news = news.head(30)[["ts","title","source","url"]] if not news.empty else pd.DataFrame(
        columns=["ts","title","source","url"]
    )

    return {
        "ticker": ticker,
        "meta": {
            "last_updated": pd.Timestamp.now(tz="America/New_York").isoformat(),
            "S_total_7d": last_total7,
            "news_7d": int(series["news_count"].rolling(7, min_periods=1).sum().iloc[-1]) if len(series) else 0,
        },
        "insights": {
            "live_sentiment": "Positive" if last_total7 > 0.5 else ("Negative" if last_total7 < -0.5 else "Neutral"),
            "predicted_return": predicted_return,
            "advisory": ("Strong Buy" if last_total7 > 2 else ("Buy" if last_total7 > 0.5 else ("Hold" if last_total7 > -0.5 else "Sell"))),
        },
        "series": {
            "date": series["date"].astype(str).tolist(),
            "price": series["close"].astype(float).tolist(),
            "sentiment_total": series["S_total"].astype(float).tolist(),
            "sentiment_total_ma7": s_total7.astype(float).tolist(),
            "sentiment_news": series["S_news"].astype(float).tolist(),
            "sentiment_news_ma7": s_news7.astype(float).tolist(),
            "sentiment_earnings": series["S_earn"].astype(float).tolist(),
            "sentiment_earnings_ma7": s_earn7.astype(float).tolist(),
            "news_count": series["news_count"].astype(int).tolist(),
        },
        "recent_headlines": top_news.to_dict(orient="records"),
    }

def write_ticker_json(obj: dict, out_dir: Path) -> Path:
    out = Path(out_dir) / f"{obj['ticker'].upper()}.json"
    dump_json(obj, out)
    return out

def write_index_json(summary_df: pd.DataFrame, out_dir: Path) -> Path:
    out = Path(out_dir) / "index.json"
    dump_json(summary_df.to_dict(orient="records"), out)
    return out

def write_portfolio_json(pnl_df: pd.DataFrame, out_dir: Path) -> Path:
    out = Path(out_dir) / "portfolio.json"
    dump_json({
        "date": pnl_df["date"].astype(str).tolist(),
        "equity": pnl_df["equity"].astype(float).tolist(),
        "ret_net": pnl_df["ret_net"].astype(float).tolist(),
    }, out)
    return out
