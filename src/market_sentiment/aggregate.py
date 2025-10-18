# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd

TZ_NY = "America/New_York"

def _to_utc(series) -> pd.DatetimeIndex:
    """Coerce to UTC-aware DateTimeIndex."""
    return pd.to_datetime(series, utc=True, errors="coerce")

def _effective_date(ts_like, cutoff_minutes: int = 5) -> pd.Series:
    """
    Convert timestamps to an 'effective' Eastern DATE (naive midnight).
    We first convert to America/New_York, subtract a small cutoff to push
    just-after-midnight items back to the previous calendar day, normalize(),
    then drop tz (naive date).
    """
    s = _to_utc(ts_like)
    et = s.tz_convert(TZ_NY) - pd.to_timedelta(cutoff_minutes, unit="m")
    day = et.normalize()
    return day.tz_localize(None)

def daily_sentiment_from_rows(rows: pd.DataFrame,
                              kind: str = "news",
                              cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    Input rows must have: ['ticker','ts','S'].
    Returns per-(date,ticker) averages as:
      - 'S_NEWS' when kind='news'
      - 'S_EARN' when kind='earn'
    """
    col = "S_NEWS" if kind == "news" else "S_EARN"
    if rows is None or len(rows) == 0:
        return pd.DataFrame(columns=["date", "ticker", col])

    required = {"ticker", "ts", "S"}
    missing = required - set(rows.columns)
    if missing:
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S. Missing: {sorted(missing)}")

    df = rows[["ticker", "ts", "S"]].copy()
    df["date"] = _effective_date(df["ts"], cutoff_minutes=cutoff_minutes)
    df = df.dropna(subset=["date"])

    # Stable, non-deprecated aggregation
    out = (
        df.groupby(["date", "ticker"], as_index=False)["S"]
          .mean()
          .rename(columns={"S": col})
    )

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["ticker"] = out["ticker"].astype(str).str.upper()
    return out.sort_values(["ticker", "date"]).reset_index(drop=True)

def join_and_fill_daily(d_news: pd.DataFrame | None,
                        d_earn: pd.DataFrame | None) -> pd.DataFrame:
    """
    Outer-join per-day sentiment from news/earnings and create a combined 'S'.
    Ensures both 'date' columns are datetime64[ns] NAIVE and fills missing with 0.0.
    """
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=["date", "ticker", "S_NEWS"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    for df in (d_news, d_earn):
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str).str.upper()

    cols = ["date", "ticker"]
    df = pd.merge(d_news, d_earn, on=cols, how="outer")

    # Ensure the columns exist, then fill NaN with 0.0
    for c in ("S_NEWS", "S_EARN"):
        if c not in df.columns:
            df[c] = 0.0
    df[["S_NEWS", "S_EARN"]] = df[["S_NEWS", "S_EARN"]].fillna(0.0)

    # Combined score (simple sum; change to weighted if desired)
    df["S"] = df["S_NEWS"] + df["S_EARN"]

    return df.sort_values(["ticker", "date"]).reset_index(drop=True)

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns: ['date','ticker','open','close'].
    Produces:
      - 'ret_oc_1d' = close/open - 1 (same day)
      - 'ret_cc_1d' = next_close/close - 1 (next trading day)
    """
    if prices is None or prices.empty:
        return prices

    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.dropna(subset=["date"]).sort_values(["ticker", "date"]).reset_index(drop=True)

    df["ret_oc_1d"] = (df["close"] / df["open"] - 1.0).astype(float)
    df["next_close"] = df.groupby("ticker")["close"].shift(-1)
    df["ret_cc_1d"] = (df["next_close"] / df["close"] - 1.0).astype(float)
    df = df.drop(columns=["next_close"])
    return df

def build_daily_sentiment(news_rows: pd.DataFrame | None,
                          earn_rows: pd.DataFrame | None,
                          cutoff_minutes: int = 5) -> pd.DataFrame:
    """Helper to generate the combined daily frame in one call."""
    dn = daily_sentiment_from_rows(news_rows if news_rows is not None else pd.DataFrame(),
                                   "news", cutoff_minutes)
    de = daily_sentiment_from_rows(earn_rows if earn_rows is not None else pd.DataFrame(),
                                   "earn", cutoff_minutes)
    return join_and_fill_daily(dn, de)
