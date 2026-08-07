from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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


def _json_safe(value: Any) -> Any:
    """Convert numpy values and non-finite floats into strict JSON values."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        x = float(value)
        return x if math.isfinite(x) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _write_json(path: str, obj: Any) -> None:
    """Atomically write strict JSON so a failed build cannot leave a partial file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_safe(obj)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _canonical_ticker_filename(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def _load_universe_csv(path: str) -> List[str]:
    df = pd.read_csv(path)
    for col in ["ticker", "Ticker", "symbol", "Symbol"]:
        if col in df.columns:
            vals = df[col]
            break
    else:
        if df.empty or len(df.columns) == 0:
            return []
        vals = df.iloc[:, 0]
    out: List[str] = []
    seen = set()
    for x in vals.dropna().astype(str):
        t = _canonical_ticker_filename(x)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _find_ticker_snapshot(data_root: str, ticker: str) -> Optional[str]:
    raw = str(ticker).strip()
    canonical = _canonical_ticker_filename(raw)
    candidates = [raw, canonical, raw.replace("-", "."), canonical.replace("-", ".")]
    for name in dict.fromkeys(candidates):
        for suffix in (".json", ".json.gz"):
            p = os.path.join(data_root, "ticker", f"{name}{suffix}")
            if os.path.exists(p):
                return p
    return None


def _snapshot_to_df(obj: Dict[str, Any]) -> pd.DataFrame:
    """Read the repository's {dates, price, S/sentiment} ticker snapshot safely."""
    dates = obj.get("dates") or []
    prices = obj.get("price") or []
    if not isinstance(dates, list) or not isinstance(prices, list):
        return pd.DataFrame()
    n = min(len(dates), len(prices))
    if n < 3:
        return pd.DataFrame()

    raw_signal = obj.get("S", obj.get("sentiment", obj.get("sentiment_score")))
    if isinstance(raw_signal, list):
        signal = list(raw_signal[:n]) + [None] * max(0, n - len(raw_signal))
    else:
        signal = [None] * n

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(dates[:n]), errors="coerce", utc=True),
            "price": pd.to_numeric(pd.Series(prices[:n]), errors="coerce"),
            "signal_raw": pd.to_numeric(pd.Series(signal[:n]), errors="coerce"),
        }
    )
    df = df.dropna(subset=["date", "price"])
    df = df[df["price"] > 0].sort_values("date").drop_duplicates("date", keep="last")
    if df.empty:
        return df
    df["date"] = df["date"].dt.tz_convert(None)
    df["ret"] = df["price"].pct_change(fill_method=None)
    # Obvious feed corruption should become missing rather than dominate the backtest.
    df.loc[(df["ret"] <= -0.95) | (df["ret"] >= 2.0), "ret"] = np.nan
    return df.set_index("date")[["price", "ret", "signal_raw"]]


# -----------------------------
# Strategy helpers
# -----------------------------
def _rebalance_mask(dates: pd.DatetimeIndex, mode: str) -> np.ndarray:
    if mode == "daily":
        return np.ones(len(dates), dtype=bool)
    if mode != "weekly":
        raise ValueError(f"Unsupported rebalance mode: {mode}")
    periods = dates.to_period("W-FRI")
    last_positions = pd.Series(np.arange(len(dates)), index=dates).groupby(periods).max().values
    selected = {int(x) for x in last_positions}
    return np.array([i in selected for i in range(len(dates))], dtype=bool)


def _rank_centered(s: pd.Series) -> pd.Series:
    clean = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return clean.rank(method="average", pct=True) - 0.5


def _weighted_cross_section(components: List[Tuple[pd.Series, float]]) -> pd.Series:
    names = sorted(set().union(*(set(s.index) for s, _ in components)))
    score = pd.Series(0.0, index=names, dtype=float)
    denom = pd.Series(0.0, index=names, dtype=float)
    for values, weight in components:
        ranked = _rank_centered(values).reindex(names)
        ok = ranked.notna()
        score.loc[ok] += ranked.loc[ok] * float(weight)
        denom.loc[ok] += abs(float(weight))
    result = score / denom.replace(0.0, np.nan)
    return result.replace([np.inf, -np.inf], np.nan)


def _capped_inverse_vol_weights(
    names: List[str],
    vol: pd.Series,
    gross: float,
    max_weight: float,
) -> pd.Series:
    if not names or gross <= 0:
        return pd.Series(dtype=float)
    v = pd.to_numeric(vol.reindex(names), errors="coerce").replace([np.inf, -np.inf], np.nan)
    fallback = float(v.dropna().median()) if v.notna().any() else 0.02
    fallback = fallback if fallback > 1e-8 else 0.02
    v = v.fillna(fallback).clip(lower=max(fallback * 0.10, 1e-4))
    raw = 1.0 / v
    weights = raw / raw.sum() * float(gross)

    cap = max(0.0, min(float(max_weight), float(gross)))
    if cap <= 0 or cap * len(names) + 1e-12 < gross:
        cap = float(gross) / len(names)

    # Iterative redistribution after capping.
    fixed = pd.Series(False, index=weights.index)
    for _ in range(len(names) + 1):
        over = (~fixed) & (weights > cap + 1e-12)
        if not over.any():
            break
        weights.loc[over] = cap
        fixed.loc[over] = True
        remaining = float(gross) - float(weights.loc[fixed].sum())
        free = ~fixed
        if remaining <= 0 or not free.any():
            break
        basis = raw.loc[free]
        weights.loc[free] = basis / basis.sum() * remaining
    if weights.sum() > 0:
        weights *= float(gross) / float(weights.sum())
    return weights


def _compute_metrics(
    port_ret: pd.Series,
    equity: pd.Series,
    turnover: pd.Series,
    trading_cost: pd.Series,
) -> Dict[str, float]:
    r = pd.to_numeric(port_ret, errors="coerce").fillna(0.0)
    eq = pd.to_numeric(equity, errors="coerce").ffill().fillna(1.0)
    n = max(0, len(r) - 1)
    if len(eq) == 0:
        return {}

    last_eq = float(eq.iloc[-1])
    cumulative_return = last_eq - 1.0
    annualized_return = last_eq ** (252.0 / max(1, n)) - 1.0 if last_eq > 0 else float("nan")
    sample = r.iloc[1:] if len(r) > 1 else r
    daily_std = float(sample.std(ddof=1)) if len(sample) > 1 else float("nan")
    annualized_vol = daily_std * math.sqrt(252.0) if math.isfinite(daily_std) else float("nan")
    daily_mean = float(sample.mean()) if len(sample) else float("nan")
    sharpe = daily_mean / daily_std * math.sqrt(252.0) if daily_std > 0 else float("nan")
    downside = sample[sample < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else float("nan")
    sortino = daily_mean / downside_std * math.sqrt(252.0) if downside_std > 0 else float("nan")

    peak = eq.cummax().replace(0.0, np.nan)
    drawdown = eq / peak - 1.0
    max_drawdown = float(drawdown.min()) if drawdown.notna().any() else float("nan")
    calmar = annualized_return / abs(max_drawdown) if max_drawdown < 0 else float("nan")

    return {
        "cumulative_return": cumulative_return,
        "annualized_return": annualized_return,
        "annualized_vol": annualized_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "calmar": calmar,
        "hit_rate": float((sample > 0).mean()) if len(sample) else float("nan"),
        "num_days": float(n),
        "average_daily_turnover": float(turnover.mean()) if len(turnover) else 0.0,
        "annualized_turnover": float(turnover.mean() * 252.0) if len(turnover) else 0.0,
        "total_trading_cost": float(trading_cost.sum()) if len(trading_cost) else 0.0,
    }


@dataclass(frozen=True)
class BacktestConfig:
    rebalance: str = "weekly"
    signal: str = "blend"
    lag_days: int = 1
    k: int = 10
    long_short: bool = True
    gross_per_side: float = 0.5
    transaction_cost_bps: float = 10.0
    risk_lookback: int = 20
    min_coverage: float = 0.80
    max_weight: float = 0.10
    selection_buffer: float = 1.50


def _build_portfolio_from_panel(
    ret_wide: pd.DataFrame,
    sig_wide: pd.DataFrame,
    dates: pd.DatetimeIndex,
    cfg: BacktestConfig,
) -> Tuple[pd.Series, pd.Series, List[Dict[str, Any]], pd.Series, pd.DataFrame]:
    if ret_wide.empty or sig_wide.empty:
        raise ValueError("Portfolio panel is empty")
    if not ret_wide.index.equals(sig_wide.index):
        raise ValueError("Return and signal panels must share the same date index")

    rebalance_mask = _rebalance_mask(dates, cfg.rebalance)
    sentiment_day = sig_wide.copy()
    sentiment_ma7 = sig_wide.rolling(7, min_periods=3).mean()
    sentiment_delta5 = sentiment_ma7 - sentiment_ma7.shift(5)
    momentum20 = (1.0 + ret_wide).rolling(cfg.risk_lookback, min_periods=max(5, cfg.risk_lookback // 2)).apply(np.prod, raw=True) - 1.0
    volatility20 = ret_wide.rolling(cfg.risk_lookback, min_periods=max(5, cfg.risk_lookback // 2)).std(ddof=1)
    coverage20 = ret_wide.rolling(cfg.risk_lookback, min_periods=1).count() / float(cfg.risk_lookback)

    records: List[pd.Series] = []
    rebalance_dates: List[pd.Timestamp] = []
    holdings: List[Dict[str, Any]] = []
    previous_longs: List[str] = []
    previous_shorts: List[str] = []

    for i, d in enumerate(dates):
        if not rebalance_mask[i]:
            continue
        j = i - max(0, int(cfg.lag_days))
        if j < 0 or i + 1 >= len(dates):
            continue

        if cfg.signal == "day":
            score = _rank_centered(sentiment_day.iloc[j])
        elif cfg.signal == "ma7":
            score = _rank_centered(sentiment_ma7.iloc[j])
        elif cfg.signal == "blend":
            score = _weighted_cross_section(
                [
                    (sentiment_ma7.iloc[j], 0.55),
                    (sentiment_delta5.iloc[j], 0.15),
                    (momentum20.iloc[j], 0.30),
                    (volatility20.iloc[j], -0.10),
                ]
            )
        else:
            raise ValueError(f"Unsupported signal: {cfg.signal}")

        eligible = coverage20.iloc[j].ge(float(cfg.min_coverage))
        eligible &= ret_wide.iloc[max(0, j - 1) : j + 1].notna().any(axis=0)
        score = score[eligible.reindex(score.index).fillna(False)].dropna()
        minimum_breadth = max(cfg.k * (2 if cfg.long_short else 1), 10)
        if len(score) < minimum_breadth:
            continue

        ordered = score.sort_values(ascending=False)
        buffer_k = max(cfg.k, int(math.ceil(cfg.k * max(1.0, cfg.selection_buffer))))
        long_candidates = [str(x) for x in ordered.head(buffer_k).index]
        longs = [x for x in previous_longs if x in long_candidates][: cfg.k]
        longs.extend(x for x in long_candidates if x not in longs)
        longs = longs[: cfg.k]

        shorts: List[str] = []
        if cfg.long_short:
            short_pool = ordered.drop(index=longs, errors="ignore")
            short_candidates = [str(x) for x in short_pool.tail(buffer_k).index]
            shorts = [x for x in previous_shorts if x in short_candidates and x not in longs][: cfg.k]
            # Most-negative names come from the end of ascending order.
            shorts.extend(x for x in reversed(short_candidates) if x not in shorts and x not in longs)
            shorts = shorts[: cfg.k]

        previous_longs = list(longs)
        previous_shorts = list(shorts)
        w = pd.Series(0.0, index=ret_wide.columns, dtype=float)
        long_w = _capped_inverse_vol_weights(
            longs, volatility20.iloc[j], float(cfg.gross_per_side), float(cfg.max_weight)
        )
        w.loc[long_w.index] = long_w.values
        if shorts:
            short_w = _capped_inverse_vol_weights(
                shorts, volatility20.iloc[j], float(cfg.gross_per_side), float(cfg.max_weight)
            )
            w.loc[short_w.index] = -short_w.values

        rebalance_dates.append(d)
        records.append(w)
        holdings.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "signal_date": dates[j].strftime("%Y-%m-%d"),
                "effective_date": dates[i + 1].strftime("%Y-%m-%d"),
                "long": longs,
                "short": shorts,
                "long_weights": {str(k): float(v) for k, v in long_w.items()},
                "short_weights": {str(k): -float(v) for k, v in (short_w.items() if shorts else [])},
            }
        )

    if not records:
        raise RuntimeError(
            "No portfolio rebalance could be formed. Check sentiment coverage, price history, and universe size."
        )

    # Only rebalance rows are forward-filled. Zeros inside a valid rebalance row
    # remain zeros, so exited positions do not survive as ghost holdings.
    weights_reb = pd.DataFrame(records, index=pd.DatetimeIndex(rebalance_dates), columns=ret_wide.columns)
    weights = weights_reb.reindex(dates).ffill().fillna(0.0).shift(1).fillna(0.0)

    # Missing returns put that name into cash for the day instead of pretending a stale zero return.
    tradable_weights = weights.where(ret_wide.notna(), 0.0)
    gross_return = (tradable_weights * ret_wide.fillna(0.0)).sum(axis=1)
    turnover = weights.diff().abs().sum(axis=1)
    if len(turnover):
        turnover.iloc[0] = weights.iloc[0].abs().sum()
    trading_cost = turnover * (float(cfg.transaction_cost_bps) / 10_000.0)
    portfolio_return = gross_return - trading_cost
    equity = (1.0 + portfolio_return).cumprod()
    return portfolio_return, equity, holdings, turnover, weights


def build_portfolio_strategy(
    data_root: str,
    universe_csv: Optional[str],
    benchmark: Optional[str],
    out_path: str,
    cfg: BacktestConfig,
) -> Dict[str, Any]:
    ticker_dir = os.path.join(data_root, "ticker")
    if not os.path.isdir(ticker_dir):
        raise FileNotFoundError(f"Expected ticker snapshots at: {ticker_dir}")

    if universe_csv:
        universe = _load_universe_csv(universe_csv)
    else:
        universe = sorted(
            _canonical_ticker_filename(os.path.splitext(x)[0])
            for x in os.listdir(ticker_dir)
            if x.endswith((".json", ".json.gz"))
        )

    loaded: List[str] = []
    missing: List[str] = []
    dfs: Dict[str, pd.DataFrame] = {}
    for ticker in universe:
        path = _find_ticker_snapshot(data_root, ticker)
        if not path:
            missing.append(ticker)
            continue
        try:
            obj = _read_json(path)
            df = _snapshot_to_df(obj)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            missing.append(ticker)
            continue
        if df.empty or df["signal_raw"].notna().sum() < 3:
            missing.append(ticker)
            continue
        symbol = _canonical_ticker_filename(obj.get("ticker") or ticker)
        dfs[symbol] = df
        loaded.append(symbol)

    if len(loaded) < max(10, cfg.k * (2 if cfg.long_short else 1)):
        raise RuntimeError(
            f"Too few usable ticker snapshots: loaded={len(loaded)}, missing={len(missing)}, required>="
            f"{max(10, cfg.k * (2 if cfg.long_short else 1))}."
        )

    all_dates = sorted(set().union(*(set(df.index) for df in dfs.values())))
    dates = pd.DatetimeIndex(all_dates)
    if len(dates) < max(10, cfg.risk_lookback):
        raise RuntimeError("Too few trading dates after loading snapshots")

    ret_wide = pd.DataFrame({t: df["ret"].reindex(dates) for t, df in dfs.items()}, index=dates)
    sig_wide = pd.DataFrame({t: df["signal_raw"].reindex(dates) for t, df in dfs.items()}, index=dates)
    keep = ret_wide.notna().sum(axis=1) > 0
    ret_wide = ret_wide.loc[keep]
    # Sentiment is information that can remain valid until the next publication, but cap staleness.
    sig_wide = sig_wide.loc[keep].ffill(limit=5)
    dates = ret_wide.index

    port_ret, equity, holdings, turnover, weights = _build_portfolio_from_panel(
        ret_wide, sig_wide, dates, cfg
    )
    trading_cost = turnover * (float(cfg.transaction_cost_bps) / 10_000.0)
    metrics = _compute_metrics(port_ret, equity, turnover, trading_cost)

    benchmark_series = None
    if benchmark:
        benchmark_path = _find_ticker_snapshot(data_root, benchmark)
        if benchmark_path:
            try:
                benchmark_obj = _read_json(benchmark_path)
                benchmark_df = _snapshot_to_df(benchmark_obj)
                if not benchmark_df.empty:
                    b_ret = benchmark_df["ret"].reindex(dates).fillna(0.0)
                    b_eq = (1.0 + b_ret).cumprod()
                    benchmark_series = {
                        "ticker": benchmark_obj.get("ticker") or benchmark,
                        "equity": [float(x) for x in b_eq.values],
                    }
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                benchmark_series = None

    out: Dict[str, Any] = {
        "schema_version": 2,
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rebalance": cfg.rebalance,
            "signal": cfg.signal,
            "lag_days": int(cfg.lag_days),
            "k": int(cfg.k),
            "long_short": bool(cfg.long_short),
            "gross_per_side": float(cfg.gross_per_side),
            "transaction_cost_bps": float(cfg.transaction_cost_bps),
            "risk_lookback": int(cfg.risk_lookback),
            "min_coverage": float(cfg.min_coverage),
            "max_weight": float(cfg.max_weight),
            "selection_buffer": float(cfg.selection_buffer),
            "benchmark": benchmark,
            "universe_size_requested": int(len(universe)),
            "universe_size_used": int(len(loaded)),
            "universe_size_missing": int(len(missing)),
            "methodology": "lagged sentiment+momentum blend, inverse-volatility weights, next-day execution, costs included",
        },
        "metrics": metrics,
        "dates": [d.strftime("%Y-%m-%d") for d in dates],
        "equity": [float(x) for x in equity.values],
        "portfolio_return": [float(x) for x in port_ret.fillna(0.0).values],
        "turnover": [float(x) for x in turnover.fillna(0.0).values],
        "gross_exposure": [float(x) for x in weights.abs().sum(axis=1).values],
        "net_exposure": [float(x) for x in weights.sum(axis=1).values],
        "holdings": holdings,
        "benchmark_series": benchmark_series,
    }
    _write_json(out_path, out)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a robust, lagged S&P 500 sentiment portfolio backtest")
    ap.add_argument("--data-root", default="apps/web/public/data")
    ap.add_argument("--universe", default=None)
    ap.add_argument("--out", default="apps/web/public/data/portfolio_strategy.json")
    ap.add_argument("--rebalance", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--signal", choices=["day", "ma7", "blend"], default="blend")
    ap.add_argument("--lag-days", type=int, default=1)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--long-short", action="store_true")
    ap.add_argument("--gross-per-side", type=float, default=0.5)
    ap.add_argument("--transaction-cost-bps", type=float, default=10.0)
    ap.add_argument("--risk-lookback", type=int, default=20)
    ap.add_argument("--min-coverage", type=float, default=0.80)
    ap.add_argument("--max-weight", type=float, default=0.10)
    ap.add_argument("--selection-buffer", type=float, default=1.50)
    ap.add_argument("--benchmark", default="", help="Optional ticker snapshot used as a benchmark")
    args = ap.parse_args()

    benchmark = args.benchmark.strip() or None
    cfg = BacktestConfig(
        rebalance=args.rebalance,
        signal=args.signal,
        lag_days=max(0, int(args.lag_days)),
        k=max(1, int(args.k)),
        long_short=bool(args.long_short),
        gross_per_side=max(0.0, float(args.gross_per_side)),
        transaction_cost_bps=max(0.0, float(args.transaction_cost_bps)),
        risk_lookback=max(5, int(args.risk_lookback)),
        min_coverage=min(1.0, max(0.0, float(args.min_coverage))),
        max_weight=max(0.001, float(args.max_weight)),
        selection_buffer=max(1.0, float(args.selection_buffer)),
    )
    build_portfolio_strategy(args.data_root, args.universe, benchmark, args.out, cfg)


if __name__ == "__main__":
    main()
