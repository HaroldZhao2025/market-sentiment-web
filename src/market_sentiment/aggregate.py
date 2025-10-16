# src/market_sentiment/aggregate.py
from __future__ import annotations
import pandas as pd

# ---------- Prices ----------

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Expect columns: date (tz-naive or tz-aware), ticker, open, close
    Output adds:
      ret_oc_1d: close/open - 1 (same day)
      ret_cc_1d: close(t+1)/close(t) - 1 (next-day close-to-close, aligned to t)
    """
    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # same-day open→close
    df["ret_oc_1d"] = (df["close"] / df["open"]) - 1.0

    # next-day close/close aligned to day t
    df["ret_cc_1d"] = (
        df.groupby("ticker", group_keys=False)["close"].apply(lambda s: s.pct_change().shift(-1))
    )
    return df

# ---------- Sentiment aggregation ----------

def _effective_date(ts: pd.Series | pd.DatetimeIndex, cutoff_minutes: int = 5) -> pd.Series:
    """
    Map timestamps to effective trading dates (US/Eastern), applying a cutoff.

    - If news hits after 'cutoff_minutes' before midnight ET, push to next day.
      (e.g., treats very-late news as next-day signal.)
    """
    # normalize to tz-aware UTC series
    s = pd.to_datetime(ts, utc=True, errors="coerce")
    # convert to US/Eastern for cutoff logic
    et = s.tz_convert("America/New_York")
    # midnight ET of each day
    midnight = et.normalize()
    # if ts is within the last 'cutoff' minutes of the day -> move to next day
    too_late = (midnight + pd.Timedelta(days=1) - et) <= pd.Timedelta(minutes=cutoff_minutes)
    eff = et.where(~too_late, et + pd.Timedelta(days=1))
    # return tz-naive date (YYYY-MM-DD)
    return eff.normalize().tz_localize(None)

def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    kind: str,  # "news" or "earn"
    cutoff_minutes: int = 5,
) -> pd.DataFrame:
    """
    Input rows columns: ['ticker','ts','S'] (+ any)
    Output columns:
      date (tz-naive), ticker, S_news/S_earn, news_count/earn_count
    """
    required = {"ticker", "ts", "S"}
    have = set(rows.columns)
    if not required.issubset(have):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows[["ticker", "ts", "S"]].copy()
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)
    d = d.dropna(subset=["date"])

    # average S by (ticker, date); also count items per day
    g = (
        d.groupby(["ticker", "date"], as_index=False)
         .agg(S_mean=("S", "mean"), count=("S", "size"))
    )

    if kind == "news":
        g = g.rename(columns={"S_mean": "S_news", "count": "news_count"})
    else:
        g = g.rename(columns={"S_mean": "S_earn", "count": "earn_count"})
    return g[["date", "ticker"] + ([f"S_{kind}"] if kind in ("news", "earn") else []) +
             ([f"{kind}_count"] if kind in ("news", "earn") else [])]

def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Merge daily news/earn sentiment; fill missing zeros and compute combined S.
    Output columns:
      date, ticker, S_news, S_earn, news_count, earn_count, S
    """
    cols = ["date", "ticker"]
    df = pd.merge(d_news, d_earn, on=cols, how="outer")

    for c, default in [
        ("S_news", 0.0), ("S_earn", 0.0),
        ("news_count", 0), ("earn_count", 0),
    ]:
        if c not in df.columns:
            df[c] = default
        df[c] = df[c].fillna(default)

    # Weighted by counts (if both zero, S=0)
    total = df["news_count"].astype(float) + df["earn_count"].astype(float)
    n = df["S_news"].astype(float) * df["news_count"].astype(float)
    e = df["S_earn"].astype(float) * df["earn_count"].astype(float)
    df["S"] = (n + e) / total.replace(0.0, pd.NA)
    df["S"] = df["S"].fillna(0.0)

    # tidy
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df[["date","ticker","S","S_news","S_earn","news_count","earn_count"]]

# ---------- Portfolio ----------

def build_portfolio_timeseries(panel: pd.DataFrame, top_q: float = 0.9, bot_q: float = 0.1) -> pd.DataFrame:
    """
    panel columns expected: date, ticker, S, ret_cc_1d
    Returns DataFrame: date, long, short, long_short  (daily returns)
    """
    df = panel[["date", "ticker", "S", "ret_cc_1d"]].copy()
    if df.empty:
        return pd.DataFrame(columns=["date","long","short","long_short"])

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.dropna(subset=["S", "ret_cc_1d"])
    if df.empty:
        return pd.DataFrame(columns=["date","long","short","long_short"])

    def one_day(g: pd.DataFrame) -> pd.Series:
        if len(g) < 20:  # avoid tiny cross-sections
            return pd.Series({"long": 0.0, "short": 0.0})
        q_hi = g["S"].quantile(top_q)
        q_lo = g["S"].quantile(bot_q)
        long = g.loc[g["S"] >= q_hi, "ret_cc_1d"].mean()
        short = -g.loc[g["S"] <= q_lo, "ret_cc_1d"].mean()
        long = float(0.0 if pd.isna(long) else long)
        short = float(0.0 if pd.isna(short) else short)
        return pd.Series({"long": long, "short": short})

    daily = df.groupby("date", as_index=False).apply(one_day).reset_index(drop=True)
    daily["long_short"] = daily["long"] + daily["short"]
    return daily[["date", "long", "short", "long_short"]].sort_values("date").reset_index(drop=True)
