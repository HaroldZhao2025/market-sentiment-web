# src/market_sentiment/cli/build_portfolio.py
from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd


# -----------------------------
# IO helpers
# -----------------------------

def _safe_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def _open_maybe_gzip(path: str, mode: str = "rt", encoding: str = "utf-8"):
    """
    Open plain text or gzip-compressed files transparently.
    mode: "rt" / "rb"
    """
    if path.endswith(".gz"):
        return gzip.open(path, mode=mode, encoding=encoding if "t" in mode else None)
    return open(path, mode=mode, encoding=encoding if "t" in mode else None)


def _read_json_any(path: str):
    with _open_maybe_gzip(path, "rt") as f:
        return json.load(f)


def _read_table_any(path: str) -> pd.DataFrame:
    """
    Read CSV/TSV (optionally gz) into a DataFrame (delimiter inferred).
    """
    with _open_maybe_gzip(path, "rt") as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        return pd.read_csv(f, delimiter=dialect.delimiter)


def _first_existing(base: str, rel_candidates: Sequence[str]) -> Optional[str]:
    for rel in rel_candidates:
        p = os.path.join(base, rel)
        if os.path.exists(p):
            return p
    return None


def _discover_tickers(data_root: str) -> List[str]:
    """
    Heuristic: ticker = any directory directly under data_root that is not hidden.
    """
    if not os.path.isdir(data_root):
        return []

    out: List[str] = []
    for name in sorted(os.listdir(data_root)):
        if name.startswith("."):
            continue
        p = os.path.join(data_root, name)
        if not os.path.isdir(p):
            continue
        if name.lower() in {"public", "apps", "src", "node_modules"}:
            continue
        out.append(name)
    return out


def _list_tickers_from_universe(universe_csv: str) -> List[str]:
    df = pd.read_csv(universe_csv)
    candidates = ["symbol", "Symbol", "ticker", "Ticker"]
    col = next((c for c in candidates if c in df.columns), None)
    if col is None:
        col = df.columns[0]
    tickers = df[col].astype(str).str.strip().replace({"": np.nan}).dropna().unique().tolist()
    return [t for t in tickers if t.lower() not in {"nan", "none"}]


# -----------------------------
# Loaders: price / sentiment
# -----------------------------

_PRICE_PATH_CANDIDATES = [
    os.path.join("price", "daily.json"),
    os.path.join("price", "daily.json.gz"),
    os.path.join("prices", "daily.json"),
    os.path.join("prices", "daily.json.gz"),
    os.path.join("price", "daily.csv"),
    os.path.join("price", "daily.csv.gz"),
    os.path.join("prices", "daily.csv"),
    os.path.join("prices", "daily.csv.gz"),
]

_SENT_DIR_CANDIDATES = [
    "sentiment",
    "sentiments",
    "news_sentiment",
    "daily_sentiment",
]


def load_price_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expected (preferred): {data_root}/{ticker}/price/daily.json
      with records like [{"date":"YYYY-MM-DD","close":123.4}, ...]

    Accepts common variants (csv, gz, alt column names).
    Returns columns: date (Timestamp), close (float), ret (float).
    """
    tdir = os.path.join(data_root, ticker)
    fpath = _first_existing(tdir, _PRICE_PATH_CANDIDATES)
    if not fpath:
        return pd.DataFrame()

    if fpath.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
        rows = _read_table_any(fpath)
    else:
        data = _read_json_any(fpath)
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                data = data["data"]
            elif "prices" in data and isinstance(data["prices"], list):
                data = data["prices"]
        if isinstance(data, list):
            rows = pd.DataFrame(data)
        elif isinstance(data, dict):
            rows = pd.DataFrame(data)
        else:
            return pd.DataFrame()

    if rows.empty:
        return pd.DataFrame()

    date_col = next((c for c in ["date", "Date", "datetime", "timestamp", "time"] if c in rows.columns), None)
    if date_col is None:
        return pd.DataFrame()

    close_col = next(
        (c for c in ["adj_close", "adjClose", "Adj Close", "adjclose", "close", "Close", "price", "Price"] if c in rows.columns),
        None,
    )
    if close_col is None:
        return pd.DataFrame()

    out = rows[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"}).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=False)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    if out.empty:
        return pd.DataFrame()

    out["ret"] = out["close"].pct_change()
    return out[["date", "close", "ret"]]


def load_sentiment_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Accepts:
      - {data_root}/{ticker}/sentiment/daily.json(.gz)  (list or dict)
      - {data_root}/{ticker}/sentiment/*.json(.gz)      (one dict per file)
      - daily.csv(.gz)

    Returns columns: date (Timestamp), score_mean (float)
    """
    tdir = os.path.join(data_root, ticker)
    sdir = None
    for cand in _SENT_DIR_CANDIDATES:
        p = os.path.join(tdir, cand)
        if os.path.isdir(p):
            sdir = p
            break
    if not sdir:
        return pd.DataFrame()

    score_keys = ["score_mean", "S", "sentiment", "score", "mean", "compound", "sentiment_mean", "avg_sentiment"]

    def extract_score(d: dict) -> Optional[float]:
        for k in score_keys:
            if k in d and d[k] is not None:
                v = d[k]
                if isinstance(v, dict):
                    for kk in ["score_mean", "mean", "compound", "score", "sentiment"]:
                        if kk in v and v[kk] is not None:
                            return _safe_float(v[kk])
                else:
                    return _safe_float(v)
        return None

    def extract_date(d: dict) -> Optional[str]:
        for k in ["date", "Date", "datetime", "timestamp", "time"]:
            if k in d and d[k]:
                return str(d[k])
        return None

    rows: List[Dict[str, object]] = []

    daily = _first_existing(sdir, ["daily.json", "daily.json.gz", "daily.csv", "daily.csv.gz"])

    if daily and daily.endswith((".csv", ".csv.gz")):
        df = _read_table_any(daily)
        if df.empty:
            return pd.DataFrame()

        date_col = next((c for c in ["date", "Date", "datetime", "timestamp"] if c in df.columns), None)
        score_col = next((c for c in score_keys if c in df.columns), None)
        if date_col is None or score_col is None:
            return pd.DataFrame()

        out = df[[date_col, score_col]].rename(columns={date_col: "date", score_col: "score_mean"}).copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=False)
        out["score_mean"] = pd.to_numeric(out["score_mean"], errors="coerce")
        out = out.dropna(subset=["date", "score_mean"]).sort_values("date")
        return out[["date", "score_mean"]]

    if daily and daily.endswith((".json", ".json.gz")):
        try:
            data = _read_json_any(daily)
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    data = data["data"]
                elif "rows" in data and isinstance(data["rows"], list):
                    data = data["rows"]

            if isinstance(data, list):
                for d in data:
                    if not isinstance(d, dict):
                        continue
                    ds = extract_date(d)
                    sc = extract_score(d)
                    if ds is None or sc is None or not np.isfinite(sc):
                        continue
                    rows.append({"date": ds, "score_mean": sc})
        except Exception:
            rows = []

    if not rows:
        # per-day files
        for fname in sorted(os.listdir(sdir)):
            if not (fname.endswith(".json") or fname.endswith(".json.gz")):
                continue
            if fname.startswith(".") or fname.startswith("daily."):
                continue
            fpath = os.path.join(sdir, fname)
            try:
                d = _read_json_any(fpath)
                if not isinstance(d, dict):
                    continue
                ds = extract_date(d)
                sc = extract_score(d)
                if ds is None or sc is None or not np.isfinite(sc):
                    continue
                rows.append({"date": ds, "score_mean": sc})
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=False)
    out["score_mean"] = pd.to_numeric(out["score_mean"], errors="coerce")
    out = out.dropna(subset=["date", "score_mean"]).sort_values("date")
    out = out.groupby("date", as_index=False)["score_mean"].mean()
    return out[["date", "score_mean"]]


# -----------------------------
# Backtest utilities
# -----------------------------

def compute_metrics(daily_ret: pd.Series, rf_annual: float = 0.0) -> Dict[str, float]:
    r = daily_ret.dropna().astype(float)
    if r.empty:
        return {}

    freq = 252.0
    T = float(len(r))

    cum_ret = float((1.0 + r).prod() - 1.0)
    ann_ret = float((1.0 + cum_ret) ** (freq / T) - 1.0)
    ann_vol = float(r.std(ddof=1) * np.sqrt(freq))

    rf_daily = (1.0 + rf_annual) ** (1.0 / freq) - 1.0
    ex = r - rf_daily
    sharpe = float((ex.mean() / ex.std(ddof=1)) * np.sqrt(freq)) if ex.std(ddof=1) > 0 else float("nan")

    wealth = (1.0 + r).cumprod()
    peak = wealth.cummax()
    dd = wealth / peak - 1.0
    max_dd = float(dd.min())

    hit_rate = float((r > 0).mean())

    return {
        "cumulative_return": cum_ret,
        "annualized_return": ann_ret,
        "annualized_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "hit_rate": hit_rate,
        "num_days": float(len(r)),
    }


def _rebalance_starts(dates: pd.DatetimeIndex, mode: str) -> List[pd.Timestamp]:
    if len(dates) == 0:
        return []
    if mode == "daily":
        return list(pd.to_datetime(dates))

    tmp = pd.DataFrame({"date": pd.to_datetime(dates)})
    tmp["week"] = tmp["date"].dt.to_period("W")
    first_dates = tmp.groupby("week")["date"].min().sort_values()
    return [pd.Timestamp(x) for x in first_dates.values]


def _compute_weekly_sentiment(df_long: pd.DataFrame, trading_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Recency-weighted weekly sentiment (matches your notebook idea):
    - Within each calendar week, later trading days have higher weights (1..m).
    - Weekly(ticker, week) = weighted avg of score_mean on days it exists.
    """
    if df_long.empty:
        return pd.DataFrame()

    cal = pd.DataFrame({"date": pd.to_datetime(trading_dates)})
    cal["week"] = cal["date"].dt.to_period("W")
    cal["pos"] = cal.groupby("week").cumcount() + 1
    cal["m"] = cal.groupby("week")["pos"].transform("max")
    cal["day_weight"] = cal["pos"] / (cal["m"] * (cal["m"] + 1) / 2.0)
    cal = cal[["date", "week", "day_weight"]]

    sent = df_long[["ticker", "date", "score_mean"]].copy()
    sent = sent.dropna(subset=["date", "score_mean"])
    sent["date"] = pd.to_datetime(sent["date"])
    sent = sent.merge(cal, on="date", how="left").dropna(subset=["day_weight"])

    if sent.empty:
        return pd.DataFrame()

    def _wavg(g: pd.DataFrame) -> float:
        return float(np.average(g["score_mean"].astype(float), weights=g["day_weight"].astype(float)))

    weekly = (
        sent.groupby(["week", "ticker"])
        .apply(_wavg)
        .rename("weekly_sent")
        .reset_index()
        .pivot(index="week", columns="ticker", values="weekly_sent")
        .sort_index()
    )
    return weekly


@dataclass
class Holding:
    date: str
    long: List[str]
    short: List[str]


def build_portfolio(
    data_root: str,
    tickers: List[str],
    k: int = 5,
    long_short: bool = True,
    gross_per_side: float = 1.0,
    rebalance: str = "weekly",
    signal: str = "weekavg",  # day | ma7 | weekavg
    lag_days: int = 1,
    benchmark_ticker: str = "SPY",
    exclude_benchmark_from_universe: bool = True,
    verbose: bool = False,
) -> Dict:
    frames: List[pd.DataFrame] = []
    loaded = 0

    for t in tickers:
        if exclude_benchmark_from_universe and t == benchmark_ticker:
            continue

        px = load_price_daily(data_root, t)
        if px.empty:
            if verbose:
                print(f"[skip] {t}: missing price")
            continue

        s = load_sentiment_daily(data_root, t)
        if s.empty:
            if verbose:
                print(f"[skip] {t}: missing sentiment")
            continue

        # IMPORTANT: keep all price dates
        merged = px.merge(s, on="date", how="left")
        merged["ticker"] = t
        frames.append(merged[["ticker", "date", "ret", "score_mean"]])
        loaded += 1

    if verbose:
        print(f"Loaded tickers with price+sentiment: {loaded} / {len(tickers)}")

    if not frames:
        # If you still see {} after this, it means data_root/path assumptions still don't match.
        return {}

    df = pd.concat(frames, ignore_index=True).sort_values(["ticker", "date"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["ret"] = pd.to_numeric(df["ret"], errors="coerce")

    ret_wide = df.pivot(index="date", columns="ticker", values="ret").sort_index()
    if ret_wide.empty:
        return {}

    # Benchmark (try load separately if not in universe)
    bench_ret = None
    if benchmark_ticker:
        bpx = load_price_daily(data_root, benchmark_ticker)
        if not bpx.empty:
            b = bpx.set_index("date")["ret"].sort_index()
            bench_ret = b.reindex(ret_wide.index).fillna(0.0)

    port_ret = pd.Series(index=ret_wide.index, data=0.0, dtype=float)
    holdings: List[Holding] = []

    if signal in {"day", "ma7"}:
        df["S_day"] = pd.to_numeric(df["score_mean"], errors="coerce")
        if signal == "ma7":
            df["S_sig"] = df.groupby("ticker")["S_day"].transform(lambda s: s.rolling(7, min_periods=7).mean())
        else:
            df["S_sig"] = df["S_day"]
        df["S_lag"] = df.groupby("ticker")["S_sig"].shift(lag_days)

        sig_wide = df.pivot(index="date", columns="ticker", values="S_lag").reindex(ret_wide.index)
        starts = _rebalance_starts(ret_wide.index, rebalance)

        for i, start in enumerate(starts):
            end = starts[i + 1] if i + 1 < len(starts) else (ret_wide.index.max() + pd.Timedelta(days=1))
            if start not in sig_wide.index:
                continue

            sig = sig_wide.loc[start].dropna()
            if exclude_benchmark_from_universe:
                sig = sig.drop(index=benchmark_ticker, errors="ignore")
            if sig.empty:
                continue

            sig_sorted = sig.sort_values(ascending=False)
            k_eff = max(1, min(k, len(sig_sorted)))

            long_names = sig_sorted.head(k_eff).index.tolist()
            short_names = sig_sorted.tail(k_eff).index.tolist() if long_short else []

            w = pd.Series(0.0, index=ret_wide.columns, dtype=float)
            w[long_names] = gross_per_side / float(len(long_names))
            if long_short and short_names:
                w[short_names] = -gross_per_side / float(len(short_names))

            mask = (ret_wide.index >= start) & (ret_wide.index < end)
            slice_ret = ret_wide.loc[mask, w.index].fillna(0.0)
            port_ret.loc[mask] = (slice_ret * w.values).sum(axis=1).values
            holdings.append(Holding(date=pd.Timestamp(start).date().isoformat(), long=long_names, short=short_names))

    elif signal == "weekavg":
        # Always weekly in this mode
        weekly_sent = _compute_weekly_sentiment(df, trading_dates=ret_wide.index)

        cal = pd.DataFrame({"date": pd.to_datetime(ret_wide.index)})
        cal["week"] = cal["date"].dt.to_period("W")
        first_dates = cal.groupby("week")["date"].min().sort_values()
        weeks = list(first_dates.index)

        for k_idx, w_cur in enumerate(weeks):
            if k_idx == 0:
                continue
            w_prev = weeks[k_idx - 1]

            start = pd.Timestamp(first_dates.loc[w_cur])
            end = pd.Timestamp(first_dates.loc[weeks[k_idx + 1]]) if k_idx + 1 < len(weeks) else (ret_wide.index.max() + pd.Timedelta(days=1))

            if w_prev not in weekly_sent.index:
                continue

            sig = weekly_sent.loc[w_prev].dropna()
            if exclude_benchmark_from_universe:
                sig = sig.drop(index=benchmark_ticker, errors="ignore")
            if sig.empty:
                continue

            sig_sorted = sig.sort_values(ascending=False)
            k_eff = max(1, min(k, len(sig_sorted)))

            long_names = sig_sorted.head(k_eff).index.tolist()
            short_names = sig_sorted.tail(k_eff).index.tolist() if long_short else []

            w = pd.Series(0.0, index=ret_wide.columns, dtype=float)
            w[long_names] = gross_per_side / float(len(long_names))
            if long_short and short_names:
                w[short_names] = -gross_per_side / float(len(short_names))

            mask = (ret_wide.index >= start) & (ret_wide.index < end)
            slice_ret = ret_wide.loc[mask, w.index].fillna(0.0)
            port_ret.loc[mask] = (slice_ret * w.values).sum(axis=1).values
            holdings.append(Holding(date=start.date().isoformat(), long=long_names, short=short_names))

    else:
        raise ValueError(f"Unknown signal='{signal}'. Use day|ma7|weekavg.")

    port_ret = port_ret.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    equity = (1.0 + port_ret).cumprod()

    out: Dict[str, object] = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rebalance": rebalance if signal != "weekavg" else "weekly",
            "signal": signal,
            "lag_days": int(lag_days),
            "k": int(k),
            "long_short": bool(long_short),
            "gross_per_side": float(gross_per_side),
            "benchmark": benchmark_ticker if bench_ret is not None else None,
            "universe_size_used": int(ret_wide.shape[1]),
        },
        "dates": [pd.Timestamp(d).date().isoformat() for d in port_ret.index],
        "portfolio_return": [float(np.round(x, 8)) for x in port_ret.values],
        "equity": [float(np.round(x, 8)) for x in equity.values],
        "holdings": [asdict(h) for h in holdings],
        "metrics": compute_metrics(port_ret, rf_annual=0.0),
    }

    if bench_ret is not None:
        bench_ret = pd.to_numeric(bench_ret, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        bench_equity = (1.0 + bench_ret).cumprod()
        out["benchmark_series"] = {
            "ticker": benchmark_ticker,
            "equity": [float(np.round(x, 8)) for x in bench_equity.values],
        }

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True, help="Path to repo data folder (e.g. data)")
    ap.add_argument("--universe", default="", help="Optional CSV with tickers (Symbol/symbol/ticker column).")
    ap.add_argument("--out", required=True, help="Output JSON path (e.g. apps/web/public/data/portfolio_strategy.json)")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--rebalance", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--signal", choices=["day", "ma7", "weekavg"], default="weekavg")
    ap.add_argument("--lag-days", type=int, default=1)
    ap.add_argument("--long-short", action="store_true")
    ap.add_argument("--gross-per-side", type=float, default=1.0)
    ap.add_argument("--benchmark", default="SPY")
    ap.add_argument("--include-benchmark-in-universe", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if args.universe:
        tickers = _list_tickers_from_universe(args.universe)
    else:
        tickers = _discover_tickers(args.data_root)

    if args.verbose:
        print(f"data_root={args.data_root}")
        print(f"tickers discovered={len(tickers)} (first 10: {tickers[:10]})")

    data = build_portfolio(
        data_root=args.data_root,
        tickers=tickers,
        k=args.k,
        long_short=args.long_short,
        gross_per_side=args.gross_per_side,
        rebalance=args.rebalance,
        signal=args.signal,
        lag_days=args.lag_days,
        benchmark_ticker=args.benchmark,
        exclude_benchmark_from_universe=not args.include_benchmark_in_universe,
        verbose=args.verbose,
    )

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"Wrote {args.out} (dates={len(data.get('dates', []))}, holdings={len(data.get('holdings', []))})")


if __name__ == "__main__":
    main()
