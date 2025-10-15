from __future__ import annotations
import numpy as np
import pandas as pd

_EASTERN = "America/New_York"

_SRC_WEIGHT = {
    # adjustable source reliability weights
    "Reuters": 1.2,
    "Bloomberg": 1.2,
    "CNBC": 1.1,
    "Wall Street Journal": 1.2,
    "Press Release": 0.8,
}

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["ticker","date"]).copy()
    df["ret_cc_1d"] = df.groupby("ticker")["close"].pct_change().shift(-1)
    return df

def apply_cutoff_and_roll(news_df: pd.DataFrame, cutoff_min: int) -> pd.DataFrame:
    """
    Items published within [close - cutoff_min, close] roll into T+1.
    Market close: 16:00 America/New_York.
    """
    if news_df.empty:
        return news_df

    news = news_df.copy()
    ts = pd.to_datetime(news["ts"], errors="coerce", utc=True).dt.tz_convert(_EASTERN)
    news["ts_local"] = ts
    close_time = ts.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_time - pd.Timedelta(minutes=cutoff_min)

    eff = np.where(ts > threshold, (ts + pd.Timedelta(days=1)), ts)
    eff = pd.to_datetime(eff).tz_convert(_EASTERN).normalize()
    news["effective_date"] = eff
    return news

def _src_w(s: str | float | None) -> float:
    if not s or not isinstance(s, str):
        return 1.0
    return float(_SRC_WEIGHT.get(s, 1.0))

def aggregate_daily(scored_news: pd.DataFrame) -> pd.DataFrame:
    """
    Input scored_news columns:
      ['ticker','ts','source','title','url','pos','neg','neu','conf', 'effective_date'(from cutoff)]
    Returns daily per-ticker DataFrame: ['date','ticker','S','news_count']
    """
    if scored_news.empty:
        return pd.DataFrame({"date": [], "ticker": [], "S": [], "news_count": []})

    x = scored_news.copy()
    if "effective_date" not in x.columns:
        x = apply_cutoff_and_roll(x, cutoff_min=30)

    for col in ["pos","neg","neu","conf"]:
        if col not in x.columns:
            x[col] = 0.0

    # weight and signed score
    x["w_src"] = x["source"].map(_src_w).fillna(1.0)
    x["w"] = x["conf"].fillna(1.0) * x["w_src"]
    x["signed"] = x["w"] * (x["pos"].astype(float) - x["neg"].astype(float))

    g = x.groupby(["effective_date","ticker"], as_index=False).agg(
        S=("signed","sum"),
        news_count=("title","count"),
    )
    g = g.rename(columns={"effective_date":"date"})
    return g.sort_values(["ticker","date"]).reset_index(drop=True)
