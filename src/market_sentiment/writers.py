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

def build_ticker_json(ticker: str, price_df: pd.DataFrame, daily_sent: pd.DataFrame, recent_news: pd.DataFrame) -> dict:
    p = _flatten(price_df).copy()
    d = _flatten(daily_sent).copy()
    p["date"] = _to_eastern_day(p["date"])
    if "date" in d.columns:
        d["date"] = _to_eastern_day(d["date"])

    series = (
        p[["date", "close"]]
        .merge(
            (d[["date", "S", "news_count"]] if "news_count" in d.columns else d[["date", "S"]]),
            on="date",
            how="left",
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    series["S"] = series["S"].fillna(0.0)
    if "news_count" not in series.columns:
        series["news_count"] = 0
    series["news_count"] = series["news_count"].fillna(0).astype(int)

    # rolling features (stronger UI signal)
    s7 = series["S"].rolling(7, min_periods=1).mean()
    n7 = series["news_count"].rolling(7, min_periods=1).sum()
    last_S = float(series["S"].iloc[-1]) if len(series) else 0.0
    last_S7 = float(s7.iloc[-1]) if len(series) else 0.0
    # map to % range with a bit more amplitude
    predicted_return = float(np.tanh(last_S7 / 2.0) * 0.02)  # ~±2%

    # headlines
    news = recent_news.copy()
    if not news.empty:
        ts = pd.to_datetime(news["ts"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
        news["ts"] = ts
        news = news.sort_values("ts", ascending=False)
    top_news = news.head(30)[["ts", "title", "source", "url"]] if not news.empty else pd.DataFrame(
        columns=["ts", "title", "source", "url"]
    )

    return {
        "ticker": ticker,
        "meta": {
            "last_updated": pd.Timestamp.now(tz="America/New_York").isoformat(),
            "S_1d": last_S,
            "S_7d": last_S7,
            "news_7d": int(n7.iloc[-1]) if len(series) else 0,
        },
        "insights": {
            "live_sentiment": "Positive" if last_S7 > 0.5 else ("Negative" if last_S7 < -0.5 else "Neutral"),
            "predicted_return": predicted_return,
            "advisory": ("Strong Buy" if last_S7 > 2 else ("Buy" if last_S7 > 0.5 else ("Hold" if last_S7 > -0.5 else "Sell"))),
        },
        "series": {
            "date": series["date"].astype(str).tolist(),
            "price": series["close"].astype(float).tolist(),
            "sentiment": series["S"].astype(float).tolist(),
            "sentiment_ma7": s7.astype(float).tolist(),
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
    dump_json(
        {
            "date": pnl_df["date"].astype(str).tolist(),
            "equity": pnl_df["equity"].astype(float).tolist(),
            "ret_net": pnl_df["ret_net"].astype(float).tolist(),
        },
        out,
    )
    return out

def write_earnings_json(ticker: str, df: pd.DataFrame, out_dir: Path) -> Path:
    out = Path(out_dir) / "earnings" / f"{ticker.upper()}.json"
    events = df.sort_values("ts").assign(ts=lambda d: d["ts"].astype(str))[["ts", "quarter", "year", "text"]]
    dump_json({"ticker": ticker, "events": events.to_dict(orient="records")}, out)
    return out
