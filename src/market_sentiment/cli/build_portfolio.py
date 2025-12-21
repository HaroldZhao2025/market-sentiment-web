# src/market_sentiment/cli/build_portfolio.py
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

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
        col = df.columns[0]
    tickers = df[col].astype(str).str.strip().replace({"": np.nan}).dropna().unique().tolist()
    return [t for t in tickers if t.lower() not in {"nan", "none"}]


def _infer_tickers_from_folders(data_root: str) -> List[str]:
    """
    Infer tickers as subfolders that look like:
      {data_root}/{TICKER}/price/daily.json
    (and/or have a sentiment folder).
    This avoids accidentally treating misc folders/files as tickers.
    """
    out: List[str] = []
    if not os.path.isdir(data_root):
        return out

    for name in os.listdir(data_root):
        tdir = os.path.join(data_root, name)
        if not os.path.isdir(tdir):
            continue
        px = os.path.join(tdir, "price", "daily.json")
        sdir = os.path.join(tdir, "sentiment")
        if os.path.exists(px) or os.path.isdir(sdir):
            out.append(name)
    return sorted(out)


def load_price_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect: {data_root}/{ticker}/price/daily.json
    with records like [{"date":"YYYY-MM-DD","close":123.4}, ...]
    """
    fpath = os.path.join(data_root, ticker, "price", "daily.json")
    if not os.path.exists(fpath):
        return pd.DataFrame()

    try:
        rows = pd.read_json(fpath)
    except Exception:
        return pd.DataFrame()

    if rows.empty or "date" not in rows.columns:
        return pd.DataFrame()

    rows["date"] = pd.to_datetime(rows["date"], errors="coerce")
    rows = rows.dropna(subset=["date"]).copy()

    # common keys: close, adj_close, price
    px_col = "close" if "close" in rows.columns else ("adj_close" if "adj_close" in rows.columns else None)
    if px_col is None:
        px_col = "price" if "price" in rows.columns else None
    if px_col is None:
        return pd.DataFrame()

    out = rows[["date", px_col]].rename(columns={px_col: "close"}).dropna()
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    out["close"] = out["close"].astype(float)
    out["ret"] = out["close"].pct_change()
    return out


def load_sentiment_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect either:
      A) {data_root}/{ticker}/sentiment/daily.json  (list)
      B) {data_root}/{ticker}/sentiment/*.json      (one dict per file)

    We try these score keys in order:
      score_mean, S, sentiment, score
    """
    sdir = os.path.join(data_root, ticker, "sentiment")
    if not os.path.isdir(sdir):
        return pd.DataFrame()

    daily_path = os.path.join(sdir, "daily.json")
    rows: List[Dict] = []

    score_keys = ["score_mean", "S", "sentiment", "score"]

    def extract_score(d: dict) -> Optional[float]:
        for k in score_keys:
            if k in d and d[k] is not None:
                v = _safe_float(d[k])
                if np.isfinite(v):
                    return float(v)
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
    df = df.dropna(subset=["date", "score_mean"]).copy()
    df["score_mean"] = df["score_mean"].astype(float)

    # collapse duplicates (ticker-date)
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
    ann_ret = float((1.0 + cum_ret) ** (freq / T) - 1.0) if T > 0 else float("nan")
    ann_vol = float(r.std(ddof=1) * np.sqrt(freq))

    rf_daily = (1.0 + rf_annual) ** (1.0 / freq) - 1.0
    ex = r - rf_daily
    ex_std = ex.std(ddof=1)
    sharpe = float((ex.mean() / ex_std) * np.sqrt(freq)) if ex_std and ex_std > 0 else float("nan")

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
    """
    mode:
      - daily: every trading day
      - weekly: first trading day of each ISO calendar week (Mon-based)
    """
    dates = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values().unique()
    if len(dates) == 0:
        return []

    if mode == "daily":
        return list(dates)

    iso = dates.isocalendar()
    s = pd.Series(dates, index=dates)
    starts = s.groupby([iso.year, iso.week]).min().sort_values().tolist()
    # ensure python Timestamp
    return [pd.Timestamp(x) for x in starts]


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
    """
    Strategy (matches notebook spirit):
      - Merge price returns + sentiment per ticker.
      - Signal = day sentiment OR 7-day MA sentiment.
      - Lag signal by `lag_days` trading days to avoid lookahead.
      - Rebalance daily or weekly (weekly = first trading day of ISO week).
      - Long top-K by signal.
      - Optional short bottom-K by signal (disjoint from longs when possible).
    """

    if k <= 0:
        return {}

    # Separate benchmark from tradable universe (important)
    bench_ticker = benchmark_ticker.strip() if benchmark_ticker else ""
    tradable = [t for t in tickers if t and t != bench_ticker]

    # Load & merge (tradable tickers)
    frames = []
    used_tickers: List[str] = []

    for t in tradable:
        px = load_price_daily(data_root, t)
        s = load_sentiment_daily(data_root, t)
        if px.empty or s.empty:
            continue

        m = pd.merge(px, s[["date", "score_mean"]], on="date", how="inner")
        m = m.dropna(subset=["date", "ret", "score_mean"]).copy()
        if m.empty:
            continue

        m["ticker"] = t
        frames.append(m[["ticker", "date", "ret", "score_mean"]])
        used_tickers.append(t)

    if not frames:
        return {}

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["ticker", "date"]).copy()

    # signals
    df["S_day"] = df["score_mean"].astype(float)

    if signal == "ma7":
        df["S_sig"] = df.groupby("ticker")["S_day"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    else:
        df["S_sig"] = df["S_day"]

    # lag to avoid lookahead
    df["S_lag"] = df.groupby("ticker")["S_sig"].shift(int(lag_days))

    # wide matrices
    ret_wide = df.pivot(index="date", columns="ticker", values="ret").sort_index()
    sig_wide = df.pivot(index="date", columns="ticker", values="S_lag").sort_index()

    # Align columns
    sig_wide = sig_wide.reindex(columns=ret_wide.columns)

    dates = ret_wide.index
    starts = _rebalance_starts(dates, rebalance)

    weights = pd.DataFrame(0.0, index=dates, columns=ret_wide.columns, dtype=float)
    holdings: List[Holding] = []

    for i, start in enumerate(starts):
        if start not in sig_wide.index:
            continue

        end = starts[i + 1] if i + 1 < len(starts) else (dates.max() + pd.Timedelta(days=1))

        sig = sig_wide.loc[start].dropna()
        if sig.empty:
            continue

        sig_sorted = sig.sort_values(ascending=False)
        n = len(sig_sorted)
        k_eff = int(min(k, n))

        long_names = sig_sorted.head(k_eff).index.tolist()

        short_names: List[str] = []
        if long_short:
            # choose from bottom, but keep disjoint from long if possible
            bottom = sig_sorted.tail(k_eff).index.tolist()
            short_names = [t for t in bottom if t not in set(long_names)]

        w = pd.Series(0.0, index=ret_wide.columns, dtype=float)

        # long weights
        if long_names:
            w.loc[long_names] = float(gross_per_side) / float(len(long_names))

        # short weights
        if long_short and short_names:
            w.loc[short_names] = -float(gross_per_side) / float(len(short_names))

        mask = (weights.index >= start) & (weights.index < end)
        weights.loc[mask, :] = w.values

        holdings.append(
            Holding(
                date=pd.Timestamp(start).date().isoformat(),
                long=long_names,
                short=short_names,
            )
        )

    # portfolio returns
    port_ret = (weights * ret_wide.fillna(0.0)).sum(axis=1)
    port_ret = port_ret.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    equity = (1.0 + port_ret).cumprod()

    out: Dict = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rebalance": rebalance,
            "signal": signal,
            "lag_days": int(lag_days),
            "k": int(k),
            "long_short": bool(long_short),
            "gross_per_side": float(gross_per_side),
            "benchmark": bench_ticker if bench_ticker else None,
            "universe_size_used": int(len(used_tickers)),
        },
        "dates": [d.date().isoformat() for d in dates],
        "portfolio_return": [float(np.round(x, 8)) for x in port_ret.values],
        "equity": [float(np.round(x, 8)) for x in equity.values],
        "holdings": [asdict(h) for h in holdings],
        "metrics": compute_metrics(port_ret, rf_annual=0.0),
    }

    # Benchmark series (aligned to portfolio dates)
    if bench_ticker:
        bpx = load_price_daily(data_root, bench_ticker)
        if not bpx.empty and "ret" in bpx.columns:
            b = bpx.set_index("date")[["ret"]].sort_index()
            b = b.reindex(dates)
            bret = b["ret"].astype(float).fillna(0.0)
            beq = (1.0 + bret).cumprod()

            out["benchmark_series"] = {
                "ticker": bench_ticker,
                "equity": [float(np.round(x, 8)) for x in beq.values],
            }
        else:
            out["meta"]["benchmark"] = None

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True, help="e.g. apps/web/public/data")
    ap.add_argument(
        "--universe",
        default="",
        help="CSV with tickers (Symbol/symbol/ticker column). If omitted, uses folder names under data-root.",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="output JSON file path (e.g. apps/web/public/data/portfolio_strategy.json)",
    )
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--rebalance", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--signal", choices=["day", "ma7"], default="ma7")
    ap.add_argument("--lag-days", type=int, default=1)
    ap.add_argument(
        "--long-short",
        action="store_true",
        help="if set, long top-K and short bottom-K (market-neutral-ish). Default is long-only.",
    )
    ap.add_argument(
        "--gross-per-side",
        type=float,
        default=1.0,
        help="gross notional per side; 1.0 => +1 long and -1 short if long-short",
    )
    ap.add_argument("--benchmark", default="SPY")
    args = ap.parse_args()

    if args.universe:
        tickers = _list_tickers_from_universe(args.universe)
    else:
        tickers = _infer_tickers_from_folders(args.data_root)

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
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(
        f"Wrote {args.out} "
        f"(dates={len(data.get('dates', []))}, holdings={len(data.get('holdings', []))}, "
        f"universe_used={data.get('meta', {}).get('universe_size_used', 0)})"
    )


if __name__ == "__main__":
    main()
