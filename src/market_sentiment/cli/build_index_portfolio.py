# src/market_sentiment/cli/build_index_portfolio.py
from __future__ import annotations
import argparse, json, math, statistics, datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

# ------------------------- JSON helpers -------------------------

def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    try:
        for k in keys:
            if cur is None: return default
            cur = cur.get(k)
        return default if cur is None else cur
    except Exception:
        return default

def _date_str(x) -> str:
    """
    Normalize any date-like (pd.Timestamp / datetime / date / str) to 'YYYY-MM-DD'.
    Never raises; best-effort.
    """
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if isinstance(ts, pd.Timestamp):
            # strip tz if present
            return ts.tz_localize(None) if ts.tzinfo else ts
        return ts
    except Exception:
        ts = None
    if ts is None:
        try:
            return str(x)[:10]
        except Exception:
            return "1970-01-01"
    return str(getattr(ts, "date", lambda: ts)())

# ------------------------- Per-ticker extractors -------------------------

def _extract_meta(j: Dict[str, Any]) -> Tuple[str, str, Optional[float]]:
    name = (
        _safe_get(j, "meta", "longName")
        or _safe_get(j, "meta", "shortName")
        or _safe_get(j, "info", "longName")
        or _safe_get(j, "profile", "shortName")
        or ""
    )
    sector = (
        _safe_get(j, "meta", "sector")
        or _safe_get(j, "info", "sector")
        or _safe_get(j, "profile", "sector")
        or ""
    )
    mcap = _safe_get(j, "meta", "marketCap") or _safe_get(j, "info", "marketCap")
    try:
        mcap = float(mcap) if mcap is not None else None
    except Exception:
        mcap = None
    return str(name), str(sector), mcap

def _extract_daily_scores(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    cand = _safe_get(j, "sentiment", "daily")
    if not isinstance(cand, list):
        cand = _safe_get(j, "daily")
    out = []
    if isinstance(cand, list):
        for row in cand:
            if not isinstance(row, dict): continue
            d = row.get("date") or row.get("d")
            sc= row.get("score") or row.get("s")
            pr= row.get("predicted_return") or row.get("pred") or row.get("r")
            out.append({"date": d, "score": sc, "pred": pr})
    return out

def _extract_prices(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("prices", "history", "chart"):
        v = j.get(key)
        if isinstance(v, list):
            out=[]
            for r in v:
                if not isinstance(r, dict): continue
                d = r.get("date") or r.get("d")
                c = r.get("adjClose") or r.get("close") or r.get("c")
                if d is None or c is None: continue
                try:
                    out.append({"date": str(d), "close": float(c)})
                except Exception:
                    pass
            if out: return out
    return []

def _pctchg(prices: List[Dict[str, Any]]) -> Dict[str, float]:
    arr = sorted(prices, key=lambda r: r["date"])
    out = {}
    prev=None
    for r in arr:
        c = float(r["close"])
        if prev is not None and c>0 and prev>0:
            out[r["date"]] = (c/prev)-1.0
        prev=c
    return out

# ------------------------- Index assembly -------------------------

def _index_from_tickers(ticker_dir: Path, sp500_csv: Optional[Path]) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    name_map: Dict[str, str]   = {}
    sect_map: Dict[str, str]   = {}
    mcap_map: Dict[str, float] = {}

    if sp500_csv and sp500_csv.exists():
        try:
            df = pd.read_csv(sp500_csv)
            symcol = next(c for c in df.columns if c.lower() in ("symbol","ticker"))
            namecol= next((c for c in df.columns if c.lower() in ("security","name","company")), None)
            sectcol= next((c for c in df.columns if "sector" in c.lower()), None)
            mcapcol= next((c for c in df.columns if "mcap" in c.lower() or "marketcap" in c.lower()), None)
            for _,r in df.iterrows():
                s = str(r[symcol]).upper().replace(".", "-")
                if namecol: name_map[s] = str(r[namecol])
                if sectcol: sect_map[s] = str(r[sectcol])
                if mcapcol:
                    try: mcap_map[s] = float(r[mcapcol])
                    except: pass
        except Exception:
            pass

    rows: List[Dict[str, Any]] = []
    local_mcaps: Dict[str, float] = {}

    for f in sorted((ticker_dir).glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f)
        if not isinstance(j, dict): continue
        name, sector, mcap = _extract_meta(j)
        if not name:   name   = name_map.get(sym, "")
        if not sector: sector = sect_map.get(sym, "")
        if not mcap:   mcap   = mcap_map.get(sym)

        daily = _extract_daily_scores(j)
        last_date = None
        last_score= None
        last_pred = None
        for row in reversed([x for x in daily if isinstance(x, dict)]):
            if row.get("score") is not None or row.get("pred") is not None:
                last_date  = row.get("date")
                last_score = row.get("score")
                last_pred  = row.get("pred")
                break

        rows.append({
            "symbol": sym, "name": name, "sector": sector,
            "last_date": last_date, "last_score": last_score, "last_predicted_return": last_pred
        })
        if mcap: local_mcaps[sym] = float(mcap)

    return rows, local_mcaps

def _fetch_missing_mcaps(symbols: List[str], have: Dict[str, float]) -> Dict[str, float]:
    missing = [s for s in symbols if s not in have]
    if not missing: return have
    for s in missing:
        try:
            tkr = yf.Ticker(s)
            mcap = None
            try:
                fi = getattr(tkr, "fast_info", None)
                mcap = getattr(fi, "market_cap", None) if fi is not None else None
            except Exception:
                mcap = None
            if mcap is None:
                info = tkr.info or {}
                mcap = info.get("marketCap")
            if mcap: have[s] = float(mcap)
        except Exception:
            # tolerate 404s / delistings / bad symbols quietly
            pass
    return have

def _sp500_sentiment(rows: List[Dict[str, Any]], market_caps: Dict[str, float]) -> Tuple[str, Optional[float], Optional[float]]:
    dates = [r["last_date"] for r in rows if r.get("last_date")]
    date = max(dates) if dates else None

    vals: Dict[str, float] = {}
    for r in rows:
        v = r.get("last_predicted_return")
        if v is None:
            v = r.get("last_score")
        if v is not None:
            vals[r["symbol"]] = float(v)

    if not vals:
        return date or "", None, None

    ew = sum(vals.values()) / len(vals)

    weights = []
    vlist   = []
    total_mcap = sum(market_caps.get(s, 0.0) for s in vals.keys())
    if total_mcap > 0:
        for s,v in vals.items():
            w = market_caps.get(s, 0.0) / total_mcap
            weights.append(w); vlist.append(v)
        cw = sum(w*v for w,v in zip(weights, vlist))
    else:
        cw = None

    return date or "", ew, cw

# ------------------------- Portfolio (long-only) -------------------------

def _daily_signal_maps(ticker_dir: Path) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    preds: Dict[str, Dict[str, float]] = {}
    scores: Dict[str, Dict[str, float]] = {}
    for f in sorted(ticker_dir.glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f)
        if not isinstance(j, dict): continue
        daily = _extract_daily_scores(j)
        pm: Dict[str,float] = {}
        sm: Dict[str,float] = {}
        for row in daily:
            d=row.get("date")
            if d is None: continue
            if row.get("pred") is not None:
                try: pm[str(d)] = float(row["pred"])
                except: pass
            if row.get("score") is not None:
                try: sm[str(d)] = float(row["score"])
                except: pass
        if pm: preds[sym]=pm
        if sm: scores[sym]=sm
    return preds, scores

def _daily_returns_map(ticker_dir: Path) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for f in sorted(ticker_dir.glob("*.json")):
        sym = f.stem.upper()
        j = _read_json(f)
        if not isinstance(j, dict): continue
        ret = _pctchg(_extract_prices(j))
        if ret: out[sym]=ret
    return out

def _build_long_only(preds: Dict[str, Dict[str, float]], scores: Dict[str, Dict[str, float]], rets: Dict[str, Dict[str, float]], top_n: int=50) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    all_dates = set()
    for m in list(preds.values()) + list(scores.values()):
        all_dates.update(m.keys())
    dates = sorted(all_dates)
    equity = 1.0
    curve=[]; daily=[]
    for d in dates:
        cross=[]
        for sym in rets.keys():
            v = preds.get(sym, {}).get(d)
            if v is None: v = scores.get(sym, {}).get(d)
            if v is not None and (d in rets.get(sym, {})):
                try: cross.append((sym, float(v)))
                except: pass
        if len(cross) < max(10, top_n//2):
            continue
        cross.sort(key=lambda x: x[1], reverse=True)
        longs=[s for s,_ in cross[:top_n]]
        rlist=[rets[s][d] for s in longs if d in rets[s]]
        if not rlist: 
            continue
        r = sum(rlist)/len(rlist)
        equity *= (1.0 + r)
        daily.append({"date": d, "ret": r})
        curve.append({"date": d, "equity": equity})
    return curve, daily

def _metrics(daily_rets: List[Dict[str,Any]]) -> Dict[str, float]:
    if not daily_rets: 
        return {"days":0,"cagr":0,"vol":0,"sharpe":0,"max_drawdown":0,"hit_ratio":0}
    ser = [float(x["ret"]) for x in daily_rets]
    n = len(ser)
    mean = statistics.fmean(ser)
    vol  = statistics.pstdev(ser) if n>1 else 0.0
    ann_mean = mean * 252
    ann_vol  = vol  * (252**0.5)
    sharpe = (ann_mean/ann_vol) if ann_vol>0 else 0.0
    eq=1.0; peak=1.0; maxdd=0.0
    for r in ser:
        eq*=(1+r)
        peak=max(peak,eq)
        dd = (eq/peak)-1.0
        maxdd=min(maxdd, dd)
    years = n/252
    cagr = (eq**(1/years)-1.0) if years>0 else 0.0
    hits = sum(1 for r in ser if r>0)/n
    return {"days":n, "cagr":cagr, "vol":ann_vol, "sharpe":sharpe, "max_drawdown":maxdd, "hit_ratio":hits}

# ------------------------- Benchmark (^GSPC) -------------------------

def _download_gspc(first_date: Optional[str], last_date: Optional[str]) -> List[Dict[str,Any]]:
    # pad a bit
    start = (pd.to_datetime(first_date, errors="coerce") - pd.Timedelta(days=5)).date() if first_date else dt.date(2010,1,1)
    end   = (pd.to_datetime(last_date,  errors="coerce") + pd.Timedelta(days=5)).date() if last_date  else dt.date.today()
    try:
        df = yf.download("^GSPC", start=start, end=end, auto_adjust=True, progress=False, interval="1d")
    except Exception:
        df = pd.DataFrame()
    out=[]
    if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
        idx = list(df.index)
        closes = df["Close"].tolist()
        for d, c in zip(idx, closes):
            if pd.isna(c): 
                continue
            out.append({"date": _date_str(d), "close": float(c)})
    return out

def _equity_from_prices(prices: List[Dict[str,Any]]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    arr = sorted(prices, key=lambda r: r["date"])
    daily=[]
    eq=[]
    prev=None; equity=1.0
    for r in arr:
        c=r["close"]
        if prev is not None and c>0 and prev>0:
            x=(c/prev)-1.0
            daily.append({"date": r["date"], "ret": x})
            equity *= (1+x)
            eq.append({"date": r["date"], "equity": equity})
        prev=c
    return eq, daily

def _sp500_proxy_from_rets(rets: Dict[str, Dict[str, float]]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    # Equal-weight proxy from member returns if Yahoo fails
    dates=set()
    for m in rets.values(): dates |= set(m.keys())
    dates=sorted(dates)
    equity=1.0
    daily=[]; curve=[]
    for d in dates:
        xs=[m[d] for m in rets.values() if d in m]
        if not xs: continue
        r=sum(xs)/len(xs)
        daily.append({"date": d, "ret": r})
        equity*=(1+r)
        curve.append({"date": d, "equity": equity})
    return curve, daily

# ------------------------- Main -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="apps/web/public/data")
    ap.add_argument("--sp500-csv", default="data/sp500.csv")
    ap.add_argument("--top-n", type=int, default=50)
    args = ap.parse_args()

    data_dir = Path(args.data_dir); data_dir.mkdir(parents=True, exist_ok=True)
    ticker_dir = data_dir / "ticker"; ticker_dir.mkdir(parents=True, exist_ok=True)
    sp500_csv  = Path(args.sp500_csv) if args.sp500_csv else None

    # ----- index rows -----
    rows, mcaps = _index_from_tickers(ticker_dir, sp500_csv if sp500_csv and sp500_csv.exists() else None)
    syms = [r["symbol"] for r in rows]
    mcaps = _fetch_missing_mcaps(syms, mcaps)

    # ----- index EW / Cap-weighted sentiment -----
    last_date, ew, cw = _sp500_sentiment(rows, mcaps)

    # ----- write index.json -----
    with (data_dir / "index.json").open("w", encoding="utf-8") as f:
        json.dump({
            "count": len(rows),
            "sp500": {"date": last_date, "sentiment_equal": ew, "sentiment_cap": cw},
            "tickers": rows
        }, f, ensure_ascii=False)

    # ----- portfolio (long-only) -----
    preds, scores = _daily_signal_maps(ticker_dir)
    rets = _daily_returns_map(ticker_dir)
    curve, daily = _build_long_only(preds, scores, rets, top_n=args.top_n)
    p_metrics = _metrics(daily)

    # ----- benchmark (^GSPC) with robust fallback -----
    first_date = curve[0]["date"] if curve else None
    last_date  = curve[-1]["date"] if curve else None
    gspc_prices = _download_gspc(first_date, last_date)
    if not gspc_prices:
        # Fallback: SP500 equal-weight proxy from member returns
        g_eq, g_daily = _sp500_proxy_from_rets(rets)
        bench_symbol = "SPX_proxy_EW"
    else:
        g_eq, g_daily = _equity_from_prices(gspc_prices)
        bench_symbol = "^GSPC"

    with (data_dir / "benchmark_gspc.json").open("w", encoding="utf-8") as f:
        json.dump({"symbol": bench_symbol, "prices": gspc_prices}, f, ensure_ascii=False)

    g_metrics = _metrics(g_daily)

    # ----- comparison -----
    comp=[]
    pm = {x["date"]: x["equity"] for x in curve}
    gm = {x["date"]: x["equity"] for x in g_eq}
    for d in sorted(set(pm.keys()) | set(gm.keys())):
        if d in pm and d in gm:
            comp.append({"date": d, "portfolio": pm[d], "sp500": gm[d]})

    # ----- write portfolio.json -----
    with (data_dir / "portfolio.json").open("w", encoding="utf-8") as f:
        json.dump({
            "long_only": {"n": args.top_n, "rebalance": "daily",
                          "metrics": p_metrics,
                          "equity_curve": curve,
                          "daily": daily},
            "benchmark": {"symbol": bench_symbol,
                          "metrics": g_metrics,
                          "equity_curve": g_eq,
                          "daily": g_daily},
            "comparison": comp
        }, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
