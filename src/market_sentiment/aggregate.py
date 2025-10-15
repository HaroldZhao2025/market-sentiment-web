from __future__ import annotations
import pandas as pd, numpy as np
NY_TZ = "America/New_York"

def _ensure_plain_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.index, pd.MultiIndex) or out.index.name in ("date","ticker"):
        out = out.reset_index()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ["_".join([str(c) for c in col if c not in (None,"")]).strip() for col in out.columns.to_list()]
    if "symbol" in out.columns and "ticker" not in out.columns:
        out = out.rename(columns={"symbol":"ticker"})
    if "Date" in out.columns and "date" not in out.columns:
        out = out.rename(columns={"Date":"date"})
    return out

def normalize_date_local(dts) -> pd.Series:
    s = pd.to_datetime(dts, utc=True, errors="coerce")
    return s.dt.tz_convert(NY_TZ).dt.normalize().dt.tz_localize(None)

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    p = _ensure_plain_columns(prices)
    for c in ["date","ticker","open","close"]:
        if c not in p.columns: raise ValueError(f"prices missing {c}")
    p["date"] = normalize_date_local(p["date"])
    p = p.sort_values(["ticker","date"]).reset_index(drop=True)
    def _f(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        g["ret_cc_1d"] = g["close"].shift(-1)/g["close"] - 1.0
        g["ret_oc_1d"] = g["open"].shift(-1)/g["close"] - 1.0
        return g
    p = p.groupby("ticker", group_keys=False).apply(_f).reset_index(drop=True)
    return p[["date","ticker","ret_cc_1d","ret_oc_1d"]]

def apply_cutoff_and_roll(news_df: pd.DataFrame, cutoff_min: int) -> pd.DataFrame:
    if news_df.empty: return news_df.assign(effective_date=pd.NaT)
    news = news_df.copy()
    news["ts"] = pd.to_datetime(news["ts"], utc=True, errors="coerce")
    ts_local = news["ts"].dt.tz_convert(NY_TZ)
    close_time = ts_local.dt.normalize() + pd.Timedelta(hours=16)
    threshold = close_time - pd.Timedelta(minutes=cutoff_min)
    eff = np.where(ts_local > threshold, ts_local + pd.Timedelta(days=1), ts_local)
    eff = pd.to_datetime(eff).tz_convert(NY_TZ).normalize().tz_localize(None)
    news["effective_date"] = eff
    return news

def daily_news_signal(news_df: pd.DataFrame) -> pd.DataFrame:
    if news_df.empty:
        return pd.DataFrame(columns=["date","ticker","S_news","news_count"])
    g = (news_df.groupby(["ticker","effective_date"])
         .agg(S_news=("S_item","mean"), news_count=("S_item","size"))
         .reset_index().rename(columns={"effective_date":"date"}))
    g["date"] = pd.to_datetime(g["date"]).dt.tz_localize(None)
    g["S_news"] = g["S_news"].astype(float); g["news_count"]=g["news_count"].astype(int)
    return g[["date","ticker","S_news","news_count"]]

def daily_earnings_signal(earn_df: pd.DataFrame) -> pd.DataFrame:
    if earn_df.empty:
        return pd.DataFrame(columns=["date","ticker","S_earn","earn_count"])
    earn = earn_df.copy()
    earn["date"] = normalize_date_local(earn["ts"])
    g = (earn.groupby(["ticker","date"])
         .agg(S_earn=("S_item","mean"), earn_count=("S_item","size"))
         .reset_index())
    g["S_earn"]=g["S_earn"].astype(float); g["earn_count"]=g["earn_count"].astype(int)
    return g[["date","ticker","S_earn","earn_count"]]

def combine_daily_signals(news_daily: pd.DataFrame, earn_daily: pd.DataFrame) -> pd.DataFrame:
    n = _ensure_plain_columns(news_daily); e = _ensure_plain_columns(earn_daily)
    out = pd.merge(n, e, on=["date","ticker"], how="outer")
    for col, fillv, typ in [("S_news",0.0,float),("news_count",0,int),("S_earn",0.0,float),("earn_count",0,int)]:
        if col not in out.columns: out[col]=fillv
        out[col] = out[col].fillna(fillv)
        if out[col].dtype=="O": out[col]=out[col].infer_objects(copy=False)
        out[col]=out[col].astype(typ)
    out["S"] = 0.5*out["S_news"].astype(float) + 0.5*out["S_earn"].astype(float)
    out["date"]=pd.to_datetime(out["date"]).dt.tz_localize(None)
    return out.sort_values(["ticker","date"]).reset_index(drop=True)[["date","ticker","S_news","news_count","S_earn","earn_count","S"]]

def safe_merge_on_date_ticker(left: pd.DataFrame, right: pd.DataFrame, how: str="left") -> pd.DataFrame:
    l = _ensure_plain_columns(left); r = _ensure_plain_columns(right)
    if "date" not in l.columns or "ticker" not in l.columns: raise KeyError("left needs date,ticker")
    if "date" not in r.columns or "ticker" not in r.columns: raise KeyError("right needs date,ticker")
    l["date"]=pd.to_datetime(l["date"]).dt.tz_localize(None)
    r["date"]=pd.to_datetime(r["date"]).dt.tz_localize(None)
    return _ensure_plain_columns(l.merge(r, on=["date","ticker"], how=how))
