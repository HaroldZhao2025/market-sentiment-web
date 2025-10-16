# src/market_sentiment/writers.py
from __future__ import annotations

import pandas as pd


def _as_date_col(x: pd.Series) -> pd.Series:
    # Ensure naive normalized dates for joining/JSON
    d = pd.to_datetime(x, errors="coerce")
    return d.dt.normalize()


def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,          # columns: date, ticker, close (open optional)
    daily_sent: pd.DataFrame,      # columns: date, ticker, S (daily combined sentiment)
    top_news: pd.DataFrame | None  # columns: ts, title, url, S (optional), etc.
) -> dict:
    """
    Create the JSON object the web app expects:
    {
      symbol, series: { date[], price[], sentiment[], sentiment_ma7[] }, top_news: [...]
    }
    Merge is on naive normalized calendar date; we DO NOT use any tz conversion here.
    """

    # Filter relevant ticker and normalize dates
    p = prices[prices["ticker"] == ticker].copy()
    p["date"] = _as_date_col(p["date"])

    d = daily_sent[daily_sent["ticker"] == ticker].copy()
    d["date"] = _as_date_col(d["date"])

    # Merge price & daily S
    ser = (
        p[["date", "close"]]
        .merge(d[["date", "S"]], on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # If no sentiment for a date, set to 0
    ser["S"] = ser["S"].fillna(0.0).astype(float)
    ser["S_ma7"] = ser["S"].rolling(7, min_periods=1).mean()

    # Prepare news list (limit top N to keep files small)
    news_list = []
    if isinstance(top_news, pd.DataFrame) and not top_news.empty:
        cols = [c for c in ["ts", "title", "url", "S"] if c in top_news.columns]
        news_list = top_news[cols].copy()
        if "ts" in news_list.columns:
            news_list["ts"] = pd.to_datetime(news_list["ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        # keep only a few
        news_list = news_list.head(12).to_dict("records")

    obj = {
        "symbol": ticker,
        "series": {
            "date": ser["date"].dt.strftime("%Y-%m-%d").tolist(),
            "price": ser["close"].astype(float).round(6).tolist(),
            "sentiment": ser["S"].round(6).tolist(),
            "sentiment_ma7": ser["S_ma7"].round(6).tolist(),
        },
        "top_news": news_list,
    }
    return obj
