# src/market_sentiment/cli/build_portfolio.py
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _list_tickers_from_universe(universe_csv: str) -> List[str]:
    df = pd.read_csv(universe_csv)
    candidates = ["symbol", "Symbol", "ticker", "Ticker"]
    col = next((c for c in candidates if c in df.columns), None)
    if col is None:
        # fallback: first column
        col = df.columns[0]
    tickers = (
        df[col].astype(str).str.strip().replace({"": np.nan}).dropna().unique().tolist()
    )
    return [t for t in tickers if t.lower() not in {"nan", "none"}]


def load_price_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect: {data_root}/{ticker}/price/daily.json
    with records like [{"date":"YYYY-MM-DD","close":123.4}, ...]
    """
    fpath = os.path.join(data_root, ticker, "price", "daily.json")
    if not os.path.exists(fpath):
        return pd.DataFrame()

    rows = pd.read_json(fpath)
    if rows.empty:
        return pd.DataFrame()

    # normalize
    if "date" not in rows.columns:
        return pd.DataFrame()

    rows["date"] = pd.to_datetime(rows["date"], errors="coerce")
    # common keys: close, adj_close, price
    px_col = "close" if "close" in rows.columns else ("adj_close" if "adj_close" in rows.columns else None)
    if px_col is None:
        # last resort: try "price"
        px_col = "price" if "price" in rows.columns else None
    if px_col is None:
        return pd.DataFrame()

    out = rows[["date", px_col]].rename(columns={px_col: "close"}).dropna()
    out = out.sort_values("date")
    out["ret"] = out["close"].pct_change()
    return out


def load_sentiment_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect either:
      A) {data_root}/{ticker}/sentiment/daily.json  (list)
      B) {data_root}/{ticker}/sentiment/*.json      (one dict per file)
    Each record should contain a date and a sentiment score.

    We try these score keys in order:
      score_mean, S, sentiment, score
    """
    sdir = os.path.join(data_root, ticker, "sentiment")
    if not os.path.isdir(sdir):
        return pd.DataFrame()

    daily_path = os.path.join(sdir, "daily.json")
    rows = []

    score_keys = ["score_mean", "S", "sentiment", "score"]

    def extract_score(d: dict) -> Optional[float]:
        for k in score_keys:
            if k in d and d[k] is not None:
                return _safe_float(d[k])
        return None

    if os.path.exists(daily_path):
        try:
            data = _read_json(daily_path)
            if isinstance(data, list):
                for d in data:
                    if not isinstance(d, dict):
                        continue
                    if "date" not in d:
                        continue
                    sc = extract_score(d)
                    if sc is None:
                        continue
                    rows.append({"ticker": ticker, "date": d["date"], "score_mean": sc})
        except Exception:
            pass
    else:
        # per-day files
        for fname in os.listdir(sdir):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(sdir, fname)
            try:
                d = _read_json(fpath)
                if not isinstance(d, dict):
                    continue
                if "date" not in d:
                    continue
                sc = extract_score(d)
                if sc is None:
                    continue
                rows.append({"ticker": ticker, "date": d["date"], "score_mean": sc})
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "score_mean"])
    df = (
        df.groupby(["ticker", "date"], as_index=False)["score_mean"]
        .mean()
        .sort_values(["ticker", "date"])
    )
    return df


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
    if mode == "daily":
        return list(dates)

    # weekly: first trading day of each calendar week
    # group by ISO year-week
    tmp = pd.Series(index=dates, data=np.arange(len(dates)))
    g = tmp.groupby([dates.isocalendar().year, dates.isocalendar().week])
    starts = [pd.Timestamp(idx[0]) for idx in g.apply(lambda s: s.index.min()).tolist()]
    starts = sorted(list(set(starts)))
    return starts


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
    signal: str = "ma7",
    lag_days: int = 1,
    benchmark_ticker: str = "SPY",
) -> Dict:
    # Load & merge
    frames = []
    for t in tickers:
        px = load_price_daily(data_root, t)
        s = load_sentiment_daily(data_root, t)
        if px.empty or s.empty:
            continue

        df = pd.merge(px, s, on="date", how="inner")
        df["ticker"] = t
        frames.append(df[["ticker", "date", "ret", "score_mean"]])

    if not frames:
        return {}

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["ticker", "date"])

    # signal choice (similar spirit to notebook: S_day vs 7d rolling)
    df["S_day"] = df["score_mean"].astype(float)
    if signal == "ma7":
        df["S_sig"] = df.groupby("ticker")["S_day"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    else:
        df["S_sig"] = df["S_day"]

    # lag to avoid lookahead (use yesterday's signal for today's holding decision)
    df["S_lag"] = df.groupby("ticker")["S_sig"].shift(lag_days)

    # wide matrices
    ret_wide = df.pivot(index="date", columns="ticker", values="ret").sort_index()
    sig_wide = df.pivot(index="date", columns="ticker", values="S_lag").sort_index()

    # benchmark returns aligned (optional)
    bench_ret = None
    if benchmark_ticker in ret_wide.columns:
        bench_ret = ret_wide[benchmark_ticker].copy()

    dates = ret_wide.index
    starts = _rebalance_starts(dates, rebalance)

    weights = pd.DataFrame(0.0, index=ret_wide.index, columns=ret_wide.columns, dtype=float)
    holdings: List[Holding] = []

    for i, start in enumerate(starts):
        if start not in sig_wide.index:
            continue
        end = starts[i + 1] if i + 1 < len(starts) else (dates.max() + pd.Timedelta(days=1))

        sig = sig_wide.loc[start].dropna()
        if sig.empty:
            continue

        sig_sorted = sig.sort_values(ascending=False)

        long_names = sig_sorted.head(max(1, min(k, len(sig_sorted)))).index.tolist()
        short_names = sig_sorted.tail(max(1, min(k, len(sig_sorted)))).index.tolist() if long_short else []

        w = pd.Series(0.0, index=ret_wide.columns, dtype=float)

        # weights
        if long_names:
            w[long_names] = gross_per_side / float(len(long_names))
        if long_short and short_names:
            w[short_names] = -gross_per_side / float(len(short_names))

        mask = (weights.index >= start) & (weights.index < end)
        weights.loc[mask, :] = w.values

        holdings.append(Holding(date=start.date().isoformat(), long=long_names, short=short_names))

    # portfolio returns
    port_ret = (weights * ret_wide.fillna(0.0)).sum(axis=1)
    port_ret = port_ret.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    equity = (1.0 + port_ret).cumprod()

    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rebalance": rebalance,
            "signal": signal,
            "lag_days": lag_days,
            "k": k,
            "long_short": bool(long_short),
            "gross_per_side": gross_per_side,
            "universe_size_used": int(len([c for c in ret_wide.columns if c])),
            "benchmark": benchmark_ticker if bench_ret is not None else None,
        },
        "dates": [d.date().isoformat() for d in port_ret.index],
        "portfolio_return": [float(np.round(x, 8)) for x in port_ret.values],
        "equity": [float(np.round(x, 8)) for x in equity.values],
        "holdings": [asdict(h) for h in holdings],
        "metrics": compute_metrics(port_ret, rf_annual=0.0),
    }

    if bench_ret is not None:
        bench_equity = (1.0 + bench_ret.fillna(0.0)).cumprod()
        out["benchmark_series"] = {
            "ticker": benchmark_ticker,
            "return": [float(np.round(x, 8)) for x in bench_ret.fillna(0.0).values],
            "equity": [float(np.round(x, 8)) for x in bench_equity.values],
        }

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True, help="e.g. apps/web/public/data")
    ap.add_argument("--universe", default="", help="CSV with tickers (Symbol/symbol/ticker column). If omitted, uses folder names under data-root.")
    ap.add_argument("--out", required=True, help="output JSON file path (e.g. apps/web/public/data/portfolio_strategy.json)")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--rebalance", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--signal", choices=["day", "ma7"], default="ma7")
    ap.add_argument("--lag-days", type=int, default=1)
    ap.add_argument("--long-short", action="store_true", help="if set, long top-K and short bottom-K (market-neutral-ish)")
    ap.add_argument("--gross-per-side", type=float, default=1.0, help="gross notional per side; 1.0 => +1 long and -1 short if long-short")
    ap.add_argument("--benchmark", default="SPY")
    args = ap.parse_args()

    if args.universe:
        tickers = _list_tickers_from_universe(args.universe)
    else:
        # infer from folders under data-root
        tickers = [
            name for name in os.listdir(args.data_root)
            if os.path.isdir(os.path.join(args.data_root, name))
        ]

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
    )

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"Wrote {args.out} (dates={len(data.get('dates', []))}, holdings={len(data.get('holdings', []))})")


if __name__ == "__main__":
    main()
