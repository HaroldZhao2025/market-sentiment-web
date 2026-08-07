from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from market_sentiment.cli.build_portfolio import (
    BacktestConfig,
    _build_portfolio_from_panel,
    _rebalance_mask,
    _write_json,
)


def synthetic_panel() -> tuple[pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    dates = pd.bdate_range("2026-01-05", periods=25)
    names = [f"T{i:02d}" for i in range(12)]
    returns = pd.DataFrame(0.001, index=dates, columns=names)
    returns.iloc[0] = np.nan
    signals = pd.DataFrame(0.0, index=dates, columns=names)
    signals.loc[:, "T00"] = 2.0
    signals.loc[dates[10]:, "T00"] = -2.0
    signals.loc[dates[10]:, "T01"] = 3.0
    return returns, signals, dates


def config(cost_bps: float = 0.0) -> BacktestConfig:
    return BacktestConfig(
        rebalance="weekly",
        signal="day",
        lag_days=0,
        k=1,
        long_short=False,
        gross_per_side=1.0,
        transaction_cost_bps=cost_bps,
        risk_lookback=5,
        min_coverage=0.0,
        max_weight=1.0,
    )


def test_exited_positions_do_not_survive_forward_fill() -> None:
    returns, signals, dates = synthetic_panel()
    _, _, holdings, _, weights = _build_portfolio_from_panel(returns, signals, dates, config())
    assert len(holdings) >= 3
    switch = next(h for h in holdings if h["long"] == ["T01"])
    effective = pd.Timestamp(switch["effective_date"])
    assert weights.loc[effective, "T01"] > 0.99
    assert weights.loc[effective, "T00"] == 0.0


def test_new_weights_start_on_next_trading_day() -> None:
    returns, signals, dates = synthetic_panel()
    _, _, holdings, _, weights = _build_portfolio_from_panel(returns, signals, dates, config())
    first = holdings[0]
    rebalance = pd.Timestamp(first["date"])
    effective = pd.Timestamp(first["effective_date"])
    assert weights.loc[rebalance].abs().sum() == 0.0
    assert weights.loc[effective].abs().sum() > 0.99


def test_transaction_costs_reduce_equity() -> None:
    returns, signals, dates = synthetic_panel()
    _, eq_free, _, _, _ = _build_portfolio_from_panel(returns, signals, dates, config(0.0))
    _, eq_costly, _, _, _ = _build_portfolio_from_panel(returns, signals, dates, config(25.0))
    assert eq_costly.iloc[-1] < eq_free.iloc[-1]


def test_atomic_writer_emits_strict_json(tmp_path: Path) -> None:
    out = tmp_path / "result.json"
    _write_json(str(out), {"ok": 1.0, "bad": float("nan"), "x": np.float64(2.0)})
    parsed = json.loads(out.read_text(encoding="utf-8"))
    assert parsed == {"ok": 1.0, "bad": None, "x": 2.0}


def test_weekly_rebalance_uses_last_trading_day() -> None:
    dates = pd.bdate_range("2026-01-05", periods=10)
    selected = dates[_rebalance_mask(dates, "weekly")]
    assert list(selected) == [pd.Timestamp("2026-01-09"), pd.Timestamp("2026-01-16")]
