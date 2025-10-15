# src/market_sentiment/writers.py
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
    """
    Robustly convert a datetime Series to America/New_York and normalize to day-level.
    Handles both tz-naive and tz-aware inputs.
    """
    s = pd.to_datetime(series, errors="coerce")

    # If tz-naive → localize; if tz-aware → convert
    try:
        tzinfo = s.dt.tz  # None if tz-naive
    except Exception:
        tzinfo = None

    if tzinfo is None:
        s = s.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")
    else:
        s = s.dt.tz_convert("America/New_York")

    return s.dt.normalize()


def build_ticker_json(
    ticker: str,
    price_df: pd.DataFrame,
    daily_sent: pd.DataFrame,
    recent_news: pd.DataFrame,
) -> dict:
    # Defensive: flatten possible MultiIndex columns
    p = _flatten(price_df).copy()
    d = _flatten(daily_sent).copy()

    # Normalize dates to Eastern day-level
    p["date"] = _to_eastern_day(p["date"])
    if "date" in d.columns:
        d["date"] = _to_eastern_day(d["date"])
    else:
        d["date"] = pd.NaT

    # Merge price with sentiment score S
    left = p[["date", "close"]].copy()
    right = d[["date", "S"]].copy() if "S" in d.columns else pd.DataFrame({"date": [], "S": []})
    s = (
        left.merge(right, on="date", how="left")
            .sort_values("date")
            .reset_index(drop=True)
    )
    s["S"] = s["S"].fillna(0.0)

    # Last sentiment → simple predicted return
    last_S = float(s["S"].iloc[-1]) if len(s) else 0.0
    predicted_return = float(np.tanh(last_S / 5.0) * 0.01)

    # Recent headlines (ts to Eastern)
    news = recent_news.copy()
    if not news.empty:
        ts = pd.to_datetime(news["ts"], errors="coerce", utc=True)
        news["ts"] = ts.dt.tz_convert("America/New_York")
        news = news.sort_values("ts", ascending=False)
    top_news = (
        news.head(20)[["ts", "title", "source", "url"]] if not news.empty
        else pd.DataFrame(columns=["ts", "title", "source", "url"])
    )

    return {
        "ticker": ticker,
        "insights": {
            "live_sentiment": "Positive" if last_S > 0 else ("Negative" if last_S < 0 else "Neutral"),
            "predicted_return": predicted_return,
            "advisory": (
                "Strong Buy" if last_S > 1 else
                ("Buy" if last_S > 0.3 else ("Hold" if last_S > -0.3 else "Sell"))
            ),
        },
        "series": {
            "date": s["date"].astype(str).tolist(),
            "price": s["close"].astype(float).tolist(),
            "sentiment": s["S"].astype(float).tolist(),
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
    events = (
        df.sort_values("ts")
          .assign(ts=lambda d: d["ts"].astype(str))[["ts", "quarter", "year", "text"]]
    )
    dump_json({"ticker": ticker, "events": events.to_dict(orient="records")}, out)
    return out
