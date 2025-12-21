# src/market_sentiment/cli/build_portfolio.py
from __future__ import annotations

import argparse
import gzip
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# IO helpers
# -----------------------------
def _read_json(path: str) -> Any:
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _canonical_ticker_filename(t: str) -> str:
    # GitHub-pages-friendly: BRK.B -> BRK-B, BF.B -> BF-B, etc.
    # Keep caret etc as-is if you ever have those, but dot->dash is critical.
    return t.replace(".", "-")


def _load_universe_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    # try common column names; otherwise first col
    for col in ["ticker", "Ticker", "symbol", "Symbol"]:
        if col in df.columns:
            s = df[col].astype(str)
            return [x.strip() for x in s.tolist() if x and x.strip()]
    s = df.iloc[:, 0].astype(str)
    return [x.strip() for x in s.tolist() if x and x.strip()]


def _find_ticker_snapshot(data_root: str, ticker: str) -> Optional[str]:
    """
    We expect: {data_root}/ticker/{TICKER}.json
    Try both raw ticker and canonicalized filename.
    """
    t1 = ticker
    t2 = _canonical_ticker_filename(ticker)

    cand = [
        os.path.join(data_root, "ticker", f"{t1}.json"),
        os.path.join(data_root, "ticker", f"{t2}.json"),
        os.path.join(data_root, "ticker", f"{t1}.json.gz"),
        os.path.join(data_root, "ticker", f"{t2}.json.gz"),
    ]
    for p in cand:
        if os.path.exists(p):
            return p
    return None


def _snapshot_to_df(obj: Dict[str, Any]) -> pd.DataFrame:
    """
    Expected keys (based on your repo export):
      - dates: list[str]
      - price: list[number]
      - S or sentiment: list[number]
    """
    dates = obj.get("dates", [])
    price = obj.get("price", [])
    if not dates or not price:
        return pd.DataFrame()

    # prefer S if present, else sentiment
    sig = obj.get("S", obj.get("sentiment", obj.get("sentiment_score", [])))

    n = min(len(dates), len(price), len(sig) if isinstance(sig, list) else len(dates))
    if n <= 2:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(dates[:n]), errors="coerce"),
            "price": pd.to_numeric(pd.Series(price[:n]), errors="coerce"),
            "signal_raw": pd.to_numeric(pd.Series(sig[:n]), errors="coerce"),
        }
    )
    df = df.dropna(subset=["date", "price"]).sort_values("date")
    if df.empty:
        return df

    df["ret"] = df["price"].pct_change()
    df = df.set_index("date")
    return df[["price", "ret", "signal_raw"]]


# -----------------------------
# Strategy / backtest helpers
# -----------------------------
def _rebalance_mask(dates: pd.DatetimeIndex, mode: str) -> np.ndarray:
    if mode == "daily":
        return np.ones(len(dates), dtype=bool)

    # weekly: first trading day of each calendar week (Period 'W')
    week = dates.to_period("W")
    first_pos = pd.Series(np.arange(len(dates)), index=dates).groupby(week).min().values
    first_pos = set(int(x) for x in first_pos)
    return np.array([i in first_pos for i in range(len(dates))], dtype=bool)


def _compute_metrics(port_ret: pd.Series, equity: pd.Series) -> Dict[str, float]:
    # Use 252 trading days annualization
    r = port_ret.fillna(0.0).values
    eq = equity.fillna(1.0).values
    n = len(r)
    n_days = max(1, n - 1)

    cumulative_return = float(eq[-1] - 1.0)

    # annualized return via CAGR on equity curve
    annualized_return = float(eq[-1] ** (252.0 / n_days) - 1.0) if eq[-1] > 0 else float("nan")

    # annualized vol (exclude first day, which is usually 0/NaN)
    if n > 2:
        daily_std = float(np.std(r[1:], ddof=1))
    else:
        daily_std = float("nan")
    annualized_vol = float(daily_std * math.sqrt(252.0)) if np.isfinite(daily_std) else float("nan")

    sharpe = float(annualized_return / annualized_vol) if annualized_vol and annualized_vol > 0 else float("nan")

    # max drawdown
    peak = np.maximum.accumulate(eq)
    dd = eq / np.where(peak == 0, np.nan, peak) - 1.0
    max_drawdown = float(np.nanmin(dd)) if np.isfinite(dd).any() else float("nan")

    hit_rate = float(np.mean(r[1:] > 0)) if n > 2 else float("nan")

    return {
        "cumulative_return": cumulative_return,
        "annualized_return": annualized_return,
        "annualized_vol": annualized_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "hit_rate": hit_rate,
        "num_days": float(n_days),
    }


@dataclass
class BacktestConfig:
    rebalance: str
    signal: str
    lag_days: int
    k: int
    long_short: bool
    gross_per_side: float


def _build_portfolio_from_panel(
    ret_wide: pd.DataFrame,
    sig_wide: pd.DataFrame,
    dates: pd.DatetimeIndex,
    cfg: BacktestConfig,
) -> Tuple[pd.Series, pd.Series, List[Dict[str, Any]]]:
    reb_mask = _rebalance_mask(dates, cfg.rebalance)

    # weights at rebalance dates (will ffill and shift(1) later)
    weights_reb = pd.DataFrame(0.0, index=dates, columns=ret_wide.columns)
    holdings: List[Dict[str, Any]] = []

    # compute actual signal series
    if cfg.signal == "day":
        sig_used = sig_wide.copy()
    elif cfg.signal == "ma7":
        sig_used = sig_wide.rolling(window=7, min_periods=7).mean()
    else:
        raise ValueError(f"Unsupported signal: {cfg.signal}")

    for i, d in enumerate(dates):
        if not reb_mask[i]:
            continue

        j = i - int(cfg.lag_days)
        if j < 0:
            continue

        s = sig_used.iloc[j].dropna()
        if s.empty:
            continue

        s_sorted = s.sort_values(ascending=False)

        longs = list(s_sorted.head(cfg.k).index)
        shorts: List[str] = []
        if cfg.long_short:
            shorts = list(s_sorted.tail(cfg.k).index)

        w = pd.Series(0.0, index=weights_reb.columns, dtype=float)

        if longs:
            w.loc[longs] = float(cfg.gross_per_side) / float(len(longs))
        if cfg.long_short and shorts:
            w.loc[shorts] = -float(cfg.gross_per_side) / float(len(shorts))

        weights_reb.loc[d] = w.values

        holdings.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "long": [str(x) for x in longs],
                "short": [str(x) for x in shorts],
            }
        )

    # hold weights until next rebalance, and apply starting *next day* to avoid look-ahead
    weights = (
        weights_reb.replace(0.0, np.nan)
        .ffill()
        .fillna(0.0)
        .shift(1)
        .fillna(0.0)
    )

    port_ret = (weights * ret_wide.fillna(0.0)).sum(axis=1)
    equity = (1.0 + port_ret.fillna(0.0)).cumprod()

    return port_ret, equity, holdings


def build_portfolio_strategy(
    data_root: str,
    universe_csv: Optional[str],
    benchmark: Optional[str],
    out_path: str,
    cfg: BacktestConfig,
) -> Dict[str, Any]:
    ticker_dir = os.path.join(data_root, "ticker")
    if not os.path.isdir(ticker_dir):
        raise FileNotFoundError(
            f"Expected ticker snapshots at: {ticker_dir}\n"
            f"(i.e., {data_root}/ticker/<TICKER>.json)."
        )

    if universe_csv:
        universe = _load_universe_csv(universe_csv)
    else:
        universe = [os.path.splitext(x)[0] for x in os.listdir(ticker_dir) if x.endswith(".json")]

    loaded: List[str] = []
    dfs: Dict[str, pd.DataFrame] = {}
    missing: List[str] = []

    for t in universe:
        p = _find_ticker_snapshot(data_root, t)
        if not p:
            missing.append(t)
            continue

        obj = _read_json(p)
        df = _snapshot_to_df(obj)
        if df.empty:
            missing.append(t)
            continue

        # Use the ticker string stored in file if present, otherwise universe ticker
        file_ticker = obj.get("ticker") or _canonical_ticker_filename(t)
        dfs[file_ticker] = df
        loaded.append(file_ticker)

    if not loaded:
        # IMPORTANT: fail loudly instead of silently writing {}
        raise RuntimeError(
            "No tickers loaded for portfolio backtest.\n"
            f"data_root={data_root}\n"
            f"universe_csv={universe_csv}\n"
            "This almost always means your build_portfolio loader does not match the repo data format."
        )

    # Build union date index; drop dates where all returns are NaN
    all_dates = sorted(set().union(*[set(df.index) for df in dfs.values()]))
    dates = pd.DatetimeIndex(all_dates)
    if len(dates) < 5:
        raise RuntimeError("Too few dates after loading snapshots; cannot backtest.")

    ret_wide = pd.DataFrame(index=dates)
    sig_wide = pd.DataFrame(index=dates)
    for t, df in dfs.items():
        ret_wide[t] = df["ret"]
        sig_wide[t] = df["signal_raw"]

    # drop dates where there are no returns at all
    keep = ret_wide.notna().sum(axis=1) > 0
    ret_wide = ret_wide.loc[keep]
    sig_wide = sig_wide.loc[keep]
    dates = ret_wide.index

    port_ret, equity, holdings = _build_portfolio_from_panel(ret_wide, sig_wide, dates, cfg)
    metrics = _compute_metrics(port_ret, equity)

    benchmark_series = None
    if benchmark:
        p_b = _find_ticker_snapshot(data_root, benchmark)
        if p_b:
            obj_b = _read_json(p_b)
            df_b = _snapshot_to_df(obj_b)
            if not df_b.empty:
                b_ret = df_b["ret"].reindex(dates).fillna(0.0)
                b_eq = (1.0 + b_ret).cumprod()
                benchmark_series = {
                    "ticker": obj_b.get("ticker") or benchmark,
                    "equity": [float(x) for x in b_eq.values],
                }

    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rebalance": cfg.rebalance,
            "signal": cfg.signal,
            "lag_days": int(cfg.lag_days),
            "k": int(cfg.k),
            "long_short": bool(cfg.long_short),
            "gross_per_side": float(cfg.gross_per_side),
            "benchmark": benchmark,
            "universe_size_used": int(len(loaded)),
        },
        "metrics": metrics,
        "dates": [d.strftime("%Y-%m-%d") for d in dates],
        "equity": [float(x) for x in equity.values],
        "portfolio_return": [float(x) for x in port_ret.fillna(0.0).values],
        "holdings": holdings,
        "benchmark_series": benchmark_series,
    }

    _write_json(out_path, out)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="apps/web/public/data", help="Root that contains ticker/<TICKER>.json")
    ap.add_argument("--universe", default=None, help="CSV with tickers (e.g. data/sp500.csv). If omitted, use all in data-root/ticker/")
    ap.add_argument("--out", default="apps/web/public/data/portfolio_strategy.json")
    ap.add_argument("--rebalance", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--signal", choices=["day", "ma7"], default="ma7")
    ap.add_argument("--lag-days", type=int, default=1)
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--long-short", action="store_true")
    ap.add_argument("--gross-per-side", type=float, default=1.0)
    ap.add_argument("--benchmark", default="SPY", help="Ticker for benchmark equity curve; set to empty to disable")

    args = ap.parse_args()

    benchmark = args.benchmark.strip() if isinstance(args.benchmark, str) else None
    if benchmark == "":
        benchmark = None

    cfg = BacktestConfig(
        rebalance=args.rebalance,
        signal=args.signal,
        lag_days=max(0, int(args.lag_days)),
        k=max(1, int(args.k)),
        long_short=bool(args.long_short),
        gross_per_side=float(args.gross_per_side),
    )

    build_portfolio_strategy(
        data_root=args.data_root,
        universe_csv=args.universe,
        benchmark=benchmark,
        out_path=args.out,
        cfg=cfg,
    )


if __name__ == "__main__":
    main()
