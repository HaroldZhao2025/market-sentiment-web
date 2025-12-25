#!/usr/bin/env python3
# src/market_sentiment/cli/build_research.py

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import statsmodels.api as sm
from statsmodels.regression.quantile_regression import QuantReg


# -----------------------------
# IO helpers
# -----------------------------

def _json_safe(x: Any) -> Any:
    if isinstance(x, (np.integer, np.floating)):
        return x.item()
    if isinstance(x, (np.ndarray,)):
        return [_json_safe(v) for v in x.tolist()]
    if isinstance(x, (pd.Timestamp,)):
        return x.strftime("%Y-%m-%d")
    return x


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2, default=_json_safe), encoding="utf-8")


def safe_num(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def pick_first_key(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d:
            return k
    return None


def stars(p: Optional[float]) -> str:
    if p is None or not np.isfinite(p):
        return ""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.1:
        return "*"
    return ""


# -----------------------------
# Locate ticker json directory
# -----------------------------

def find_ticker_dir(repo_root: Path, data_root: Path) -> Optional[Path]:
    candidates = [
        data_root / "ticker",
        data_root / "data" / "ticker",
        repo_root / "data" / "ticker",
        repo_root / "public" / "data" / "ticker",
        repo_root / "apps" / "web" / "public" / "data" / "ticker",
        repo_root / "apps" / "web" / "public" / "ticker",
        repo_root / "public" / "ticker",
    ]
    for d in candidates:
        if d.exists() and d.is_dir() and any(d.glob("*.json")):
            return d
    return None


def find_sp500_index_path(repo_root: Path) -> Optional[Path]:
    candidates = [
        repo_root / "apps" / "web" / "public" / "sp500_index.json",
        repo_root / "apps" / "web" / "public" / "data" / "sp500_index.json",
        repo_root / "public" / "sp500_index.json",
        repo_root / "public" / "data" / "sp500_index.json",
        repo_root / "data" / "sp500_index.json",
    ]
    for p in candidates:
        if p.exists() and p.is_file():
            return p
    return None


# -----------------------------
# Load one ticker json into df
# -----------------------------

def load_one_ticker_series(fp: Path) -> Optional[pd.DataFrame]:
    """
    Expect keys like:
      dates: [...], price: [...], score_mean or S or sentiment: [...], optional n_total: [...]
    Returns df indexed by date with: y_ret, abs_ret, score_mean, optional n_total
    """
    try:
        obj = read_json(fp)
    except Exception:
        return None

    dates = obj.get("dates")
    prices = obj.get("price")
    if not isinstance(dates, list) or not isinstance(prices, list):
        return None
    if len(dates) < 40 or len(prices) < 40 or len(dates) != len(prices):
        return None

    s_key = pick_first_key(obj, ["score_mean", "S", "sentiment", "sent", "sentiment_score"])
    if s_key is None:
        return None
    s_arr = obj.get(s_key)
    if not isinstance(s_arr, list) or len(s_arr) != len(dates):
        return None

    n_key = pick_first_key(obj, ["n_total", "news_count", "n_news", "num_news", "count"])
    n_arr = obj.get(n_key) if n_key else None

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(dates), errors="coerce"),
            "price": pd.to_numeric(pd.Series(prices), errors="coerce"),
            "score_mean": pd.to_numeric(pd.Series(s_arr), errors="coerce"),
        }
    )
    if n_key and isinstance(n_arr, list) and len(n_arr) == len(dates):
        df["n_total"] = pd.to_numeric(pd.Series(n_arr), errors="coerce")

    df = df.dropna(subset=["date", "price", "score_mean"]).sort_values("date")
    if len(df) < 40:
        return None

    df = df.set_index("date")
    df["y_ret"] = np.log(df["price"]).diff()
    df["abs_ret"] = df["y_ret"].abs()
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["y_ret", "score_mean"])

    cols = ["y_ret", "abs_ret", "score_mean"]
    if "n_total" in df.columns:
        cols.append("n_total")
    return df[cols]


def build_panel(repo_root: Path, data_root: Path, min_obs: int) -> pd.DataFrame:
    ticker_dir = find_ticker_dir(repo_root, data_root)
    if ticker_dir is None:
        raise RuntimeError("Could not find ticker json directory (e.g., data/ticker/*.json).")

    frames: List[pd.DataFrame] = []
    for fp in sorted(ticker_dir.glob("*.json")):
        t = fp.stem
        df = load_one_ticker_series(fp)
        if df is None or len(df) < min_obs:
            continue
        tmp = df.copy()
        tmp["ticker"] = t
        tmp = tmp.reset_index(names="date")
        frames.append(tmp)

    if not frames:
        raise RuntimeError(f"No usable tickers found under {ticker_dir} with min_obs={min_obs}.")

    panel = pd.concat(frames, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward return for predictability tests
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)

    return panel


# -----------------------------
# Export series for UI
# -----------------------------

def export_series(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if "date" in df.columns:
        out["dates"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("").tolist()

    def add(col: str):
        if col in df.columns:
            out[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .replace([np.inf, -np.inf], np.nan)
                .fillna(0.0)
                .astype(float)
                .tolist()
            )

    add("y_ret")
    add("y_ret_fwd1")
    add("abs_ret")
    add("score_mean")   # always export
    add("n_total")

    return out


# -----------------------------
# Regressions
# -----------------------------

def _ols_summary(res: sm.regression.linear_model.RegressionResultsWrapper) -> Dict[str, Any]:
    keys = list(res.params.index)
    return {
        "params": {k: safe_num(res.params.get(k)) for k in keys},
        "bse": {k: safe_num(res.bse.get(k)) for k in keys},
        "tvalues": {k: safe_num(res.tvalues.get(k)) for k in keys},
        "pvalues": {k: safe_num(res.pvalues.get(k)) for k in keys},
        "nobs": int(getattr(res, "nobs", 0) or 0),
        "rsquared": safe_num(getattr(res, "rsquared", None)),
        "rsquared_adj": safe_num(getattr(res, "rsquared_adj", None)),
    }


def time_series_ols_hac(df: pd.DataFrame, y: str, x_cols: List[str], maxlags: int) -> Dict[str, Any]:
    use = df.dropna(subset=[y] + x_cols).copy()
    if len(use) < 60:
        return {"error": "too_few_obs"}
    X = sm.add_constant(use[x_cols].astype(float), has_constant="add")
    yv = use[y].astype(float)
    res = sm.OLS(yv, X).fit(cov_type="HAC", cov_kwds={"maxlags": int(maxlags)})
    out = _ols_summary(res)
    out["cov_type"] = f"HAC(maxlags={maxlags})"
    return out


def panel_within_fe_cluster(panel: pd.DataFrame, y: str, x_cols: List[str]) -> Dict[str, Any]:
    use = panel.dropna(subset=["ticker", y] + x_cols).copy()
    if len(use) < 2000:
        return {"error": "too_few_obs"}

    g = use.groupby("ticker", sort=False)
    y_dm = use[y].astype(float) - g[y].transform("mean").astype(float)

    X_dm = []
    for c in x_cols:
        X_dm.append(use[c].astype(float) - g[c].transform("mean").astype(float))
    X = pd.concat(X_dm, axis=1)
    X.columns = x_cols

    res = sm.OLS(y_dm, X).fit(cov_type="cluster", cov_kwds={"groups": use["ticker"]})
    out = _ols_summary(res)
    out["cov_type"] = "cluster(ticker)"
    return out


def quantile_reg(df: pd.DataFrame, y: str, x_cols: List[str], qs: Tuple[float, ...] = (0.1, 0.5, 0.9)) -> Dict[str, Any]:
    use = df.dropna(subset=[y] + x_cols).copy()
    if len(use) < 120:
        return {"error": "too_few_obs"}

    X = sm.add_constant(use[x_cols].astype(float), has_constant="add")
    yv = use[y].astype(float)

    out: Dict[str, Any] = {}
    for q in qs:
        try:
            res = QuantReg(yv, X).fit(q=float(q), max_iter=2000)
            out[str(q)] = {
                "params": {k: safe_num(v) for k, v in res.params.items()},
                "tvalues": {k: safe_num(v) for k, v in res.tvalues.items()},
                "pvalues": {k: safe_num(v) for k, v in res.pvalues.items()},
            }
        except Exception as e:
            out[str(q)] = {"error": str(e)}
    return out


# -----------------------------
# "Serious academic" add-ons
# -----------------------------

def fama_macbeth(panel: pd.DataFrame, y: str, x_cols: List[str], min_xs: int, nw_lags: int) -> Dict[str, Any]:
    """
    Fama–MacBeth:
      For each date t, run cross-sectional OLS y_{i,t} on x_{i,t}, collect betas
      Then compute mean beta and NW t-stat across time.
    """
    use = panel.dropna(subset=["date", "ticker", y] + x_cols).copy()
    if use.empty:
        return {"error": "no_data"}

    dates = []
    betas = []
    r2s = []

    for dt, g in use.groupby("date", sort=True):
        if len(g) < min_xs:
            continue
        X = np.column_stack([np.ones(len(g))] + [g[c].astype(float).values for c in x_cols])
        yv = g[y].astype(float).values
        b, *_ = np.linalg.lstsq(X, yv, rcond=None)
        yhat = X @ b
        sse = float(np.sum((yv - yhat) ** 2))
        sst = float(np.sum((yv - float(np.mean(yv))) ** 2))
        r2 = 1.0 - (sse / sst) if sst > 0 else np.nan

        dates.append(pd.to_datetime(dt))
        betas.append(b)
        r2s.append(r2)

    if len(betas) < 40:
        return {"error": "too_few_dates", "n_dates": int(len(betas))}

    coef_df = pd.DataFrame(betas, columns=["const"] + x_cols)
    coef_df["date"] = dates

    table_rows = []
    for c in x_cols:
        res = sm.OLS(coef_df[c].values, np.ones((len(coef_df), 1))).fit(
            cov_type="HAC", cov_kwds={"maxlags": int(nw_lags)}
        )
        mean = float(res.params[0])
        t = float(res.tvalues[0])
        p = float(res.pvalues[0])
        table_rows.append([c, mean, t, p, stars(p)])

    return {
        "series": {
            "dates": coef_df["date"].dt.strftime("%Y-%m-%d").tolist(),
            **{f"beta_{c}": coef_df[c].astype(float).tolist() for c in x_cols},
        },
        "stats": {
            "n_dates": int(len(coef_df)),
            "min_xs": int(min_xs),
            "nw_lags": int(nw_lags),
            "avg_cs_r2": float(np.nanmean(r2s)),
        },
        "table": {
            "title": "Fama–MacBeth mean slopes (Newey–West t)",
            "columns": ["Variable", "Mean beta", "t (NW)", "p", "Sig"],
            "rows": table_rows,
        },
    }


def add_lags(panel: pd.DataFrame, col: str, max_lag: int) -> pd.DataFrame:
    out = panel.copy()
    for L in range(max_lag + 1):
        out[f"{col}_lag{L}"] = out.groupby("ticker")[col].shift(L)
    return out


def distributed_lag_models(panel: pd.DataFrame, df_sample: pd.DataFrame, y: str, base_col: str, max_lag: int, controls: List[str], nw_lags: int) -> Dict[str, Any]:
    """
    y ~ sum_{L=0..K} beta_L * base_col_lagL + controls
    Returns TS (sample ticker) HAC and Panel FE results + a clean table.
    """
    s = df_sample.copy()
    for L in range(max_lag + 1):
        s[f"{base_col}_lag{L}"] = s[base_col].shift(L)

    x_lags = [f"{base_col}_lag{L}" for L in range(max_lag + 1)]
    x_ts = x_lags + controls
    ts = time_series_ols_hac(s, y, x_ts, maxlags=nw_lags)

    p = add_lags(panel, base_col, max_lag)
    x_fe = x_lags + controls
    fe = panel_within_fe_cluster(p, y, x_fe)

    rows = []
    for L in range(max_lag + 1):
        v = f"{base_col}_lag{L}"
        b = safe_num((fe.get("params") or {}).get(v))
        t = safe_num((fe.get("tvalues") or {}).get(v))
        pval = safe_num((fe.get("pvalues") or {}).get(v))
        rows.append([v, b, t, pval, stars(pval)])

    sum_beta = None
    if isinstance(fe.get("params"), dict):
        sb = 0.0
        ok = True
        for L in range(max_lag + 1):
            v = f"{base_col}_lag{L}"
            bv = safe_num(fe["params"].get(v))
            if bv is None:
                ok = False
                break
            sb += bv
        sum_beta = sb if ok else None

    return {
        "time_series": ts,
        "panel_fe": fe,
        "table": {
            "title": f"Distributed lag (panel FE): {y} on {base_col} lags 0..{max_lag}",
            "columns": ["Variable", "Coef", "t", "p", "Sig"],
            "rows": rows,
        },
        "stats": {
            "max_lag": int(max_lag),
            "sum_beta": sum_beta,
        },
    }


def placebo_shuffle(panel: pd.DataFrame, y: str, x: str, controls: List[str], seed: int) -> Dict[str, Any]:
    """
    Shuffle sentiment within ticker (destroys time ordering) then re-run panel FE.
    Deterministic with seed.
    """
    rng = np.random.default_rng(int(seed))
    use = panel.dropna(subset=["ticker", y, x] + controls).copy()

    def _shuf(s: pd.Series) -> np.ndarray:
        a = s.values.copy()
        rng.shuffle(a)
        return a

    use[x] = use.groupby("ticker")[x].transform(lambda s: _shuf(s))
    fe = panel_within_fe_cluster(use, y, [x] + controls)
    return fe


def _nw_mean_t(x: np.ndarray, maxlags: int) -> Tuple[float, float, float]:
    """
    mean, t, p from OLS on constant with HAC (NW).
    """
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 40:
        return (np.nan, np.nan, np.nan)
    res = sm.OLS(x, np.ones((len(x), 1))).fit(cov_type="HAC", cov_kwds={"maxlags": int(maxlags)})
    return (float(res.params[0]), float(res.tvalues[0]), float(res.pvalues[0]))


def portfolio_sorts(panel: pd.DataFrame, sort_col: str, ret_col: str, n_bins: int, min_n: int, nw_lags: int) -> Dict[str, Any]:
    """
    Classic asset pricing portfolio sorts:
      each date: sort tickers into n_bins by sort_col, compute mean ret_col for each bin.
      long-short = top - bottom. Report NW t-stats of portfolio means and long-short.
    """
    use = panel.dropna(subset=["date", "ticker", sort_col, ret_col]).copy()
    if use.empty:
        return {"error": "no_data"}

    # per-date bins
    rows = []
    dates = []
    ports = {f"q{i}": [] for i in range(1, n_bins + 1)}
    ls = []

    for dt, g in use.groupby("date", sort=True):
        if len(g) < max(min_n, n_bins * 10):
            continue
        try:
            # qcut can fail if too many ties; add rank fallback
            x = g[sort_col].astype(float)
            if x.nunique() < n_bins:
                rk = x.rank(method="average")
                bins = pd.qcut(rk, q=n_bins, labels=False, duplicates="drop")
            else:
                bins = pd.qcut(x, q=n_bins, labels=False, duplicates="drop")
        except Exception:
            continue

        g2 = g.copy()
        g2["_bin"] = bins
        if g2["_bin"].isna().all():
            continue

        # ensure bins 0..n_bins-1 present (some may be missing due to duplicates)
        port_means = []
        for b in range(n_bins):
            gb = g2[g2["_bin"] == b]
            if len(gb) == 0:
                port_means.append(np.nan)
            else:
                port_means.append(float(np.nanmean(gb[ret_col].astype(float).values)))

        if not np.isfinite(port_means[0]) or not np.isfinite(port_means[-1]):
            continue

        dates.append(pd.to_datetime(dt))
        for i in range(n_bins):
            ports[f"q{i+1}"].append(port_means[i])
        ls.append(port_means[-1] - port_means[0])

    if len(ls) < 80:
        return {"error": "too_few_days", "n_days": int(len(ls))}

    # NW stats for each portfolio + long-short
    table_rows = []
    for i in range(n_bins):
        arr = np.array(ports[f"q{i+1}"], dtype=float)
        m, t, p = _nw_mean_t(arr, maxlags=nw_lags)
        table_rows.append([f"Q{i+1}", m, t, p, stars(p), m * 10000, (m / (np.nanstd(arr) + 1e-12)) * np.sqrt(252)])

    arr_ls = np.array(ls, dtype=float)
    m_ls, t_ls, p_ls = _nw_mean_t(arr_ls, maxlags=nw_lags)
    table_rows.append(["Long−Short (Q{hi}−Q{lo})".format(hi=n_bins, lo=1), m_ls, t_ls, p_ls, stars(p_ls), m_ls * 10000, (m_ls / (np.nanstd(arr_ls) + 1e-12)) * np.sqrt(252)])

    # cumulative log return of LS (for a plot)
    cum_ls = np.cumsum(arr_ls).tolist()

    return {
        "series": {
            "dates": [d.strftime("%Y-%m-%d") for d in dates],
            **{k: [float(v) if np.isfinite(v) else 0.0 for v in vs] for k, vs in ports.items()},
            "ls": [float(v) for v in arr_ls.tolist()],
            "cum_ls": [float(v) for v in cum_ls],
        },
        "stats": {
            "n_days": int(len(arr_ls)),
            "n_bins": int(n_bins),
            "nw_lags": int(nw_lags),
            "ls_mean": float(m_ls),
            "ls_t": float(t_ls),
            "ls_p": float(p_ls),
            "ls_sharpe": float((m_ls / (np.nanstd(arr_ls) + 1e-12)) * np.sqrt(252)),
        },
        "table": {
            "title": f"Portfolio sorts on {sort_col}: mean({ret_col}) by quantile (NW t)",
            "columns": ["Portfolio", "Mean", "t (NW)", "p", "Sig", "Mean (bps/day)", "Ann. Sharpe"],
            "rows": table_rows,
        },
    }


def oos_predictability(df: pd.DataFrame, y: str, x_cols: List[str], min_train: int, nw_lags: int) -> Dict[str, Any]:
    """
    Expanding-window OOS forecast:
      at each t, fit OLS on past data and predict y_t.
      benchmark: historical mean of y.
    Report OOS R^2 = 1 - SSE_model / SSE_bench.
    """
    use = df.dropna(subset=[y] + x_cols).copy()
    if len(use) < (min_train + 50):
        return {"error": "too_few_obs"}

    use = use.reset_index(drop=True)
    yv = use[y].astype(float).values
    X = use[x_cols].astype(float).values

    preds = np.full_like(yv, np.nan, dtype=float)
    bench = np.full_like(yv, np.nan, dtype=float)

    for t in range(min_train, len(use)):
        y_train = yv[:t]
        X_train = X[:t, :]
        X_train2 = np.column_stack([np.ones(t), X_train])
        b, *_ = np.linalg.lstsq(X_train2, y_train, rcond=None)

        x_t = np.concatenate([[1.0], X[t, :]])
        preds[t] = float(x_t @ b)
        bench[t] = float(np.mean(y_train))

    mask = np.isfinite(preds) & np.isfinite(bench) & np.isfinite(yv)
    if mask.sum() < 40:
        return {"error": "too_few_test_points"}

    sse_m = float(np.sum((yv[mask] - preds[mask]) ** 2))
    sse_b = float(np.sum((yv[mask] - bench[mask]) ** 2))
    oos_r2 = 1.0 - (sse_m / sse_b) if sse_b > 0 else np.nan

    # NW t-stat for mean forecast error improvement of squared errors (optional, simple)
    d = (yv[mask] - bench[mask]) ** 2 - (yv[mask] - preds[mask]) ** 2
    m_d, t_d, p_d = _nw_mean_t(d, maxlags=nw_lags)

    # cumulative realized vs predicted (log units)
    real = yv[mask]
    pred = preds[mask]
    cum_real = np.cumsum(real).tolist()
    cum_pred = np.cumsum(pred).tolist()

    return {
        "stats": {
            "oos_r2": float(oos_r2),
            "n_test": int(mask.sum()),
            "min_train": int(min_train),
            "nw_lags": int(nw_lags),
            "dm_like_mean": float(m_d),
            "dm_like_t": float(t_d),
            "dm_like_p": float(p_d),
        },
        "series": {
            "real": [float(v) for v in real.tolist()],
            "pred": [float(v) for v in pred.tolist()],
            "cum_real": [float(v) for v in cum_real],
            "cum_pred": [float(v) for v in cum_pred],
        },
    }


def event_study(panel: pd.DataFrame, signal_col: str, ret_col: str, z_thr: float, window: int, min_events: int) -> Dict[str, Any]:
    """
    Panel event study:
      - compute within-ticker z-score of signal_col
      - positive events: z >= z_thr; negative: z <= -z_thr
      - compute average cumulative return path around event (tau=-W..+W)
    Returns:
      tau: [-W..W], car_pos, car_neg
    """
    use = panel.dropna(subset=["ticker", "date", signal_col, ret_col]).copy()
    if use.empty:
        return {"error": "no_data"}

    use = use.sort_values(["ticker", "date"]).reset_index(drop=True)

    # within ticker zscore
    g = use.groupby("ticker", sort=False)
    mu = g[signal_col].transform("mean")
    sd = g[signal_col].transform("std").replace(0.0, np.nan)
    use["_z"] = (use[signal_col] - mu) / sd

    pos_idx = use.index[use["_z"] >= float(z_thr)].to_numpy()
    neg_idx = use.index[use["_z"] <= -float(z_thr)].to_numpy()

    def _collect(indices: np.ndarray) -> Optional[np.ndarray]:
        if len(indices) == 0:
            return None

        out = []
        # We'll work per ticker positions to ensure contiguous index isn't mixing tickers
        # Create per-ticker arrays of returns
        by_ticker = {}
        for tkr, gg in use.groupby("ticker", sort=False):
            by_ticker[tkr] = gg.reset_index(drop=True)

        # Map global row -> (ticker, local_idx)
        # (cheap method: iterate through ticker groups and build dict of original index -> local)
        idx_map = {}
        for tkr, gg in use.groupby("ticker", sort=False):
            loc = gg.reset_index(drop=True)
            for j, orig_i in enumerate(gg.index.values.tolist()):
                idx_map[int(orig_i)] = (tkr, j)

        for orig_i in indices:
            if int(orig_i) not in idx_map:
                continue
            tkr, j = idx_map[int(orig_i)]
            gg = by_ticker[tkr]
            if j - window < 0 or j + window >= len(gg):
                continue
            r = gg[ret_col].astype(float).values

            # CAR path relative to event: tau=0 -> 0
            car = np.zeros(2 * window + 1, dtype=float)
            # tau > 0: sum r_{t+1..t+tau}
            for k in range(1, window + 1):
                car[window + k] = float(np.sum(r[(j + 1):(j + k + 1)]))
            # tau < 0: -sum r_{t+tau+1..t}
            for k in range(1, window + 1):
                car[window - k] = float(-np.sum(r[(j - k + 1):(j + 1)]))
            out.append(car)

        if len(out) == 0:
            return None
        return np.vstack(out)

    pos_mat = _collect(pos_idx)
    neg_mat = _collect(neg_idx)

    if (pos_mat is None or len(pos_mat) == 0) and (neg_mat is None or len(neg_mat) == 0):
        return {"error": "too_few_events"}

    tau = list(range(-window, window + 1))

    def _avg(mat: Optional[np.ndarray]) -> List[float]:
        if mat is None:
            return []
        return [float(v) for v in np.nanmean(mat, axis=0).tolist()]

    out = {
        "stats": {
            "z_thr": float(z_thr),
            "window": int(window),
            "n_pos": int(0 if pos_mat is None else pos_mat.shape[0]),
            "n_neg": int(0 if neg_mat is None else neg_mat.shape[0]),
        },
        "series": {
            "tau": tau,
            "car_pos": _avg(pos_mat),
            "car_neg": _avg(neg_mat),
        },
    }

    # add quick table: CAR(+1,+5)
    def _pick(series: List[float], w: int, k: int) -> Optional[float]:
        # tau=k is at index w+k
        idx = w + k
        if idx < 0 or idx >= len(series):
            return None
        return float(series[idx])

    car_pos = out["series"].get("car_pos", [])
    car_neg = out["series"].get("car_neg", [])
    w = int(window)

    rows = []
    for lab, s in [("Positive", car_pos), ("Negative", car_neg)]:
        if not s:
            continue
        rows.append([lab, _pick(s, w, 1), _pick(s, w, 5)])
    out["table"] = {
        "title": f"Event study CAR (z≥{z_thr} / z≤−{z_thr}): cumulative log return",
        "columns": ["Event type", "CAR(+1)", "CAR(+5)"],
        "rows": rows,
    }

    # guard: min_events threshold (soft)
    n_events = out["stats"]["n_pos"] + out["stats"]["n_neg"]
    if n_events < int(min_events):
        out["stats"]["warning"] = f"few_events({n_events}<{min_events})"

    return out


def try_load_market_series(repo_root: Path, data_root: Path, symbol: str = "SPY") -> Optional[pd.DataFrame]:
    ticker_dir = find_ticker_dir(repo_root, data_root)
    if ticker_dir is None:
        return None
    fp = ticker_dir / f"{symbol}.json"
    if not fp.exists():
        return None
    df = load_one_ticker_series(fp)
    if df is None or df.empty:
        return None
    out = df.reset_index(names="date").sort_values("date").copy()
    out["mkt_ret"] = out["y_ret"].astype(float)
    out["mkt_ret_fwd1"] = out["mkt_ret"].shift(-1)
    return out[["date", "mkt_ret", "mkt_ret_fwd1"]]


def sector_map_from_sp500(repo_root: Path) -> Optional[Dict[str, str]]:
    p = find_sp500_index_path(repo_root)
    if p is None:
        return None
    try:
        arr = read_json(p)
        if not isinstance(arr, list):
            return None
        out = {}
        for it in arr:
            if not isinstance(it, dict):
                continue
            sym = it.get("symbol") or it.get("ticker")
            sec = it.get("sector") or it.get("gics_sector") or it.get("Sector")
            if isinstance(sym, str) and isinstance(sec, str) and sym and sec:
                out[sym.upper()] = sec
        return out if out else None
    except Exception:
        return None


# -----------------------------
# Packaging: key stats / sections / conclusions
# -----------------------------

def key_stats_from(ts: Dict[str, Any], fe: Dict[str, Any], var: str) -> List[Dict[str, str]]:
    b_ts = safe_num((ts.get("params") or {}).get(var))
    t_ts = safe_num((ts.get("tvalues") or {}).get(var))
    p_ts = safe_num((ts.get("pvalues") or {}).get(var))
    r2_ts = safe_num(ts.get("rsquared"))

    b_fe = safe_num((fe.get("params") or {}).get(var))
    t_fe = safe_num((fe.get("tvalues") or {}).get(var))
    p_fe = safe_num((fe.get("pvalues") or {}).get(var))
    r2_fe = safe_num(fe.get("rsquared"))

    def f(x: Optional[float], nd=4) -> str:
        return "—" if x is None else f"{x:.{nd}g}"

    return [
        {"label": f"β({var}) TS", "value": f"{f(b_ts)}{stars(p_ts)}"},
        {"label": "t-stat TS", "value": f(t_ts, 3)},
        {"label": "R² TS", "value": f(r2_ts, 3)},
        {"label": f"β({var}) FE", "value": f"{f(b_fe)}{stars(p_fe)}"},
        {"label": "t-stat FE", "value": f(t_fe, 3)},
        {"label": "R² FE", "value": f(r2_fe, 3)},
    ]


def build_sections_common(universe: str, freq: str, y_def: str, s_def: str, caveats: List[str]) -> List[Dict[str, Any]]:
    return [
        {
            "title": "Data",
            "bullets": [
                f"Universe: {universe}.",
                f"Frequency: {freq}.",
                f"Returns: {y_def}.",
                f"Sentiment: {s_def}.",
            ],
        },
        {"title": "Limitations", "bullets": caveats},
        {
            "title": "References (minimal)",
            "bullets": [
                "Fama, E. F., & MacBeth, J. D. (1973). Risk, return, and equilibrium: Empirical tests.",
                "Newey, W. K., & West, K. D. (1987). A simple, positive semi-definite, heteroskedasticity and autocorrelation consistent covariance matrix.",
                "Petersen, M. A. (2009). Estimating standard errors in finance panel data sets.",
            ],
        },
    ]


def conclusion_from_models(study_title: str, ts: Dict[str, Any], fe: Dict[str, Any], var: str, y_label: str) -> List[str]:
    def get(m: Dict[str, Any], field: str) -> Optional[float]:
        return safe_num(((m.get(field) or {}) if isinstance(m.get(field), dict) else {}).get(var))

    b_ts = get(ts, "params")
    t_ts = get(ts, "tvalues")
    p_ts = get(ts, "pvalues")
    r2 = safe_num(ts.get("rsquared"))

    b_fe = get(fe, "params")
    t_fe = get(fe, "tvalues")
    p_fe = get(fe, "pvalues")
    r2_fe = safe_num(fe.get("rsquared"))

    out: List[str] = []

    if b_ts is not None:
        direction = "positive" if b_ts > 0 else "negative" if b_ts < 0 else "flat"
        out.append(
            f"{study_title}: {var} is {direction} in TS (β={b_ts:.6g}{stars(p_ts)}, t={t_ts if t_ts is not None else '—'}, R²={r2 if r2 is not None else '—'})."
        )
    else:
        out.append(f"{study_title}: TS estimate for {var} not available.")

    if b_fe is not None:
        direction = "positive" if b_fe > 0 else "negative" if b_fe < 0 else "flat"
        out.append(
            f"Panel FE: {var} is {direction} after removing ticker FE (β={b_fe:.6g}{stars(p_fe)}, t={t_fe if t_fe is not None else '—'}, R²={r2_fe if r2_fe is not None else '—'})."
        )
    else:
        out.append("Panel FE output not available (insufficient data or model error).")

    if b_ts is not None:
        out.append(f"Scale: +1.0 in {var} ≈ {(b_ts*10000):.2f} bps change in {y_label} (log-return units).")

    return out


def build_study_payload(
    *,
    slug: str,
    title: str,
    category: str,
    summary: str,
    updated_at: str,
    status: str,
    tags: List[str],
    key_stats: List[Dict[str, str]],
    methodology: List[str],
    sections: List[Dict[str, Any]],
    conclusions: List[str],
    results: Dict[str, Any],
    notes: List[str],
) -> Dict[str, Any]:
    return {
        "slug": slug,
        "title": title,
        "category": category,
        "summary": summary,
        "updated_at": updated_at,
        "status": status,
        "tags": tags,
        "key_stats": key_stats,
        "methodology": methodology,
        "sections": sections,
        "conclusions": conclusions,
        "results": results,
        "notes": notes,
    }


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=str, default="data")
    ap.add_argument("--out-dir", type=str, default="apps/web/public/research")
    ap.add_argument("--min-obs", type=int, default=80)
    ap.add_argument("--updated-at", type=str, default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    ap.add_argument("--maxlags", type=int, default=5)
    ap.add_argument("--no-quantiles", action="store_true")

    # academic add-on params
    ap.add_argument("--fm-min-xs", type=int, default=120)
    ap.add_argument("--dl-max-lag", type=int, default=5)
    ap.add_argument("--placebo-seed", type=int, default=7)

    # “paper-like” extras
    ap.add_argument("--sort-bins", type=int, default=5)
    ap.add_argument("--sort-min-n", type=int, default=120)
    ap.add_argument("--oos-min-train", type=int, default=252)
    ap.add_argument("--event-z", type=float, default=2.0)
    ap.add_argument("--event-window", type=int, default=5)
    ap.add_argument("--event-min-events", type=int, default=300)
    ap.add_argument("--sector-min-tickers", type=int, default=20)

    args = ap.parse_args()

    repo_root = Path.cwd()
    data_root = (repo_root / args.data_root).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    # export-safe: never kill pages build
    try:
        panel = build_panel(repo_root, data_root, min_obs=int(args.min_obs))
    except Exception as e:
        write_json(out_dir / "index.json", [])
        write_json(out_dir / "overview.json", {"meta": {}, "sections": []})
        print(f"[WARN] build_research failed: {e}")
        print(f"[WARN] wrote empty research artifacts to: {out_dir}")
        return

    n_tickers = int(panel["ticker"].nunique())
    n_obs_panel = int(len(panel))
    start_date = pd.to_datetime(panel["date"].min()).strftime("%Y-%m-%d")
    end_date = pd.to_datetime(panel["date"].max()).strftime("%Y-%m-%d")

    sample_ticker = panel["ticker"].value_counts().index[0]
    df_sample = panel.loc[panel["ticker"] == sample_ticker].sort_values("date").copy()

    has_news = "n_total" in panel.columns
    x_cols = ["score_mean"] + (["n_total"] if has_news else [])

    universe = "S&P 500 tickers in your snapshot pipeline"
    freq = "Daily"
    y_def = "log(P_t) − log(P_{t−1}) from ticker JSON prices"
    s_def = "score_mean from your sentiment pipeline (ticker JSON)"
    caveats = [
        "Timing: after-close articles can contaminate same-day results; predictive tests mitigate this but do not fully solve it.",
        "Causality: results are descriptive; omitted variables and measurement error remain.",
        "Trading: no transaction costs / slippage / capacity modeled.",
    ]

    studies: List[Dict[str, Any]] = []

    # -------------------- Study 1: same-day returns --------------------
    ts1 = time_series_ols_hac(df_sample, "y_ret", x_cols, maxlags=int(args.maxlags))
    fe1 = panel_within_fe_cluster(panel, "y_ret", x_cols)
    q1 = {} if args.no_quantiles else quantile_reg(df_sample, "y_ret", ["score_mean"])

    s1 = build_study_payload(
        slug="same-day-sentiment-vs-returns",
        title="Same-day sentiment vs same-day returns",
        category="Contemporaneous relationships",
        summary="TS (HAC) + ticker fixed-effects panel: y_ret(t) ~ score_mean(t) (+ n_total(t)).",
        updated_at=args.updated_at,
        status="live",
        tags=["time-series", "panel", "HAC", "fixed effects", "returns", "sentiment"],
        key_stats=key_stats_from(ts1, fe1, "score_mean"),
        methodology=[
            "Dependent variable: y_ret(t).",
            "Regressors: score_mean(t) and optional n_total(t).",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["y_ret(t) ~ score_mean(t) + n_total(t) (optional)."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=conclusion_from_models("Same-day sentiment vs same-day returns", ts1, fe1, "score_mean", "daily return")
        + ["Interpretation: strong same-day effects often reflect contemporaneous reaction rather than forecastability."],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "series": export_series(df_sample),
            "time_series": ts1,
            "panel_fe": fe1,
            "quantiles": q1,
        },
        notes=[],
    )
    write_json(out_dir / f"{s1['slug']}.json", s1)
    studies.append(s1)

    # -------------------- Study 2: next-day returns --------------------
    ts2 = time_series_ols_hac(df_sample, "y_ret_fwd1", x_cols, maxlags=int(args.maxlags))
    fe2 = panel_within_fe_cluster(panel, "y_ret_fwd1", x_cols)

    s2 = build_study_payload(
        slug="sentiment-vs-next-day-returns",
        title="Sentiment vs next-day returns",
        category="Predictability",
        summary="Predictive check: y_ret(t+1) ~ score_mean(t) (+ n_total(t)) in TS + panel FE.",
        updated_at=args.updated_at,
        status="live",
        tags=["predictive", "panel", "HAC", "fixed effects", "returns", "sentiment"],
        key_stats=key_stats_from(ts2, fe2, "score_mean"),
        methodology=[
            "Dependent variable: y_ret(t+1).",
            "Regressors: score_mean(t) and optional n_total(t).",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["y_ret(t+1) ~ score_mean(t) + n_total(t) (optional)."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=conclusion_from_models("Sentiment vs next-day returns", ts2, fe2, "score_mean", "next-day return")
        + ["If predictive effects are weak while same-day is strong, the sentiment metric likely captures reaction rather than alpha."],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "series": export_series(df_sample),
            "time_series": ts2,
            "panel_fe": fe2,
        },
        notes=[],
    )
    write_json(out_dir / f"{s2['slug']}.json", s2)
    studies.append(s2)

    # -------------------- Study 3: volatility & attention --------------------
    if has_news:
        x3 = ["n_total", "score_mean"]
        main3 = "n_total"
        slug3 = "news-volume-vs-volatility"
        title3 = "News volume vs volatility (abs returns)"
        summary3 = "abs_ret(t) ~ n_total(t) + score_mean(t)."
        tags3 = ["volatility", "news volume", "panel", "fixed effects"]
    else:
        x3 = ["score_mean"]
        main3 = "score_mean"
        slug3 = "sentiment-vs-volatility"
        title3 = "Sentiment vs volatility proxy (abs returns)"
        summary3 = "abs_ret(t) ~ score_mean(t)."
        tags3 = ["volatility", "panel", "fixed effects"]

    ts3 = time_series_ols_hac(df_sample, "abs_ret", x3, maxlags=int(args.maxlags))
    fe3 = panel_within_fe_cluster(panel, "abs_ret", x3)

    s3 = build_study_payload(
        slug=slug3,
        title=title3,
        category="Volatility & attention",
        summary=summary3,
        updated_at=args.updated_at,
        status="live",
        tags=tags3,
        key_stats=key_stats_from(ts3, fe3, main3),
        methodology=[
            "Dependent variable: abs_ret(t) = |y_ret(t)|.",
            "Regressors: n_total(t) (if available) and score_mean(t).",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["|y_ret(t)| ~ n_total(t) + score_mean(t)."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=conclusion_from_models(title3, ts3, fe3, main3, "abs return (vol proxy)")
        + ["Interpretation: attention proxies (news volume) often correlate with volatility regardless of direction."],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "series": export_series(df_sample),
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[],
    )
    write_json(out_dir / f"{s3['slug']}.json", s3)
    studies.append(s3)

    # -------------------- Study 4: Fama–MacBeth --------------------
    fm_x = ["score_mean"] + (["n_total"] if has_news else [])
    fm = fama_macbeth(panel, y="y_ret_fwd1", x_cols=fm_x, min_xs=int(args.fm_min_xs), nw_lags=int(args.maxlags))

    concl4 = []
    if "error" in fm:
        concl4 = [f"Fama–MacBeth could not be computed: {fm.get('error')}."]
    else:
        concl4 = [
            "Fama–MacBeth estimates average cross-sectional pricing of sentiment signals using daily cross-sectional regressions.",
            f"Average cross-sectional R² ≈ {fm['stats'].get('avg_cs_r2'):.4g} (dates={fm['stats'].get('n_dates')}).",
            "Interpretation: significant mean slopes suggest a systematic cross-sectional relationship rather than a single-ticker artifact.",
        ]

    s4 = build_study_payload(
        slug="fama-macbeth-next-day",
        title="Fama–MacBeth: next-day returns on sentiment (cross-sectional)",
        category="Cross-sectional pricing",
        summary="Per-day cross-sectional OLS y_ret(t+1) on score_mean(t) (+ n_total). Report mean slopes and NW t-stats.",
        updated_at=args.updated_at,
        status="live",
        tags=["Fama-MacBeth", "cross-sectional", "Newey-West", "predictive"],
        key_stats=[
            {"label": "Dates", "value": str((fm.get("stats") or {}).get("n_dates", "—"))},
            {"label": "Avg CS R²", "value": f"{(fm.get('stats') or {}).get('avg_cs_r2', float('nan')):.3g}" if isinstance((fm.get("stats") or {}).get("avg_cs_r2"), (int, float)) else "—"},
            {"label": "Min XS N", "value": str((fm.get("stats") or {}).get("min_xs", "—"))},
            {"label": "NW lags", "value": str((fm.get("stats") or {}).get("nw_lags", "—"))},
            {"label": "Tickers", "value": str(n_tickers)},
            {"label": "Obs", "value": str(n_obs_panel)},
        ],
        methodology=[
            "Each date t: cross-sectional OLS over tickers: y_ret(t+1) ~ score_mean(t) (+ n_total(t)).",
            "Collect daily slope estimates; test mean slope using Newey–West standard errors.",
        ],
        sections=[
            {"title": "Specification", "bullets": [
                "For each day t: r_{i,t+1} = a_t + b_t * score_{i,t} + c_t * n_total_{i,t} + ε_{i,t}.",
                "Then test E[b_t] using Newey–West SE."
            ]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl4,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "famamacbeth": fm,
            "tables": [fm.get("table")] if isinstance(fm.get("table"), dict) else [],
        },
        notes=[],
    )
    write_json(out_dir / f"{s4['slug']}.json", s4)
    studies.append(s4)

    # -------------------- Study 5: distributed lags --------------------
    controls5 = ["n_total"] if has_news else []
    dl = distributed_lag_models(
        panel,
        df_sample,
        y="y_ret_fwd1",
        base_col="score_mean",
        max_lag=int(args.dl_max_lag),
        controls=controls5,
        nw_lags=int(args.maxlags),
    )

    sum_beta = safe_num((dl.get("stats") or {}).get("sum_beta"))
    concl5 = [
        f"Distributed lag model estimates how sentiment impacts returns over multiple days (lags 0..{args.dl_max_lag}).",
        f"Panel FE cumulative beta (sum of lag coefficients) ≈ {sum_beta if sum_beta is not None else '—'}.",
        "Interpretation: front-loaded vs delayed coefficients distinguish immediate reaction from slow diffusion.",
    ]

    s5 = build_study_payload(
        slug="distributed-lags-next-day",
        title="Distributed lags: next-day returns on lagged sentiment",
        category="Predictability",
        summary=f"Panel FE + TS HAC: y_ret(t+1) on score_mean lags 0..{args.dl_max_lag} (+ n_total).",
        updated_at=args.updated_at,
        status="live",
        tags=["distributed lags", "panel", "fixed effects", "Newey-West"],
        key_stats=[
            {"label": "Max lag", "value": str(args.dl_max_lag)},
            {"label": "Sum beta (FE)", "value": f"{sum_beta:.3g}" if sum_beta is not None else "—"},
            {"label": "R² TS", "value": f"{safe_num((dl.get('time_series') or {}).get('rsquared')):.3g}" if safe_num((dl.get('time_series') or {}).get("rsquared")) is not None else "—"},
            {"label": "R² FE", "value": f"{safe_num((dl.get('panel_fe') or {}).get('rsquared')):.3g}" if safe_num((dl.get('panel_fe') or {}).get("rsquared")) is not None else "—"},
            {"label": "Tickers", "value": str(n_tickers)},
            {"label": "Obs", "value": str(n_obs_panel)},
        ],
        methodology=[
            f"Construct lagged sentiment variables score_mean_lag0..score_mean_lag{args.dl_max_lag}.",
            "Estimate y_ret(t+1) on lags with panel FE; SE clustered by ticker. Also fit TS HAC on sample ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": [
                f"y_ret(t+1) ~ Σ_{{L=0..{args.dl_max_lag}}} β_L score_mean(t−L) + controls."
            ]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl5,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "series": export_series(df_sample),
            "time_series": dl.get("time_series"),
            "panel_fe": dl.get("panel_fe"),
            "tables": [dl.get("table")] if isinstance(dl.get("table"), dict) else [],
            "dl_stats": dl.get("stats"),
        },
        notes=[],
    )
    write_json(out_dir / f"{s5['slug']}.json", s5)
    studies.append(s5)

    # -------------------- Study 6: placebo / shuffle test --------------------
    controls6 = ["n_total"] if has_news else []
    fe_real = fe2  # from Study 2 baseline
    fe_pl = placebo_shuffle(panel, y="y_ret_fwd1", x="score_mean", controls=controls6, seed=int(args.placebo_seed))

    b_real = safe_num((fe_real.get("params") or {}).get("score_mean"))
    p_real = safe_num((fe_real.get("pvalues") or {}).get("score_mean"))
    b_pl = safe_num((fe_pl.get("params") or {}).get("score_mean"))
    p_pl = safe_num((fe_pl.get("pvalues") or {}).get("score_mean"))

    concl6 = [
        "Placebo test: shuffle sentiment within ticker to break time structure; predictive coefficient should collapse toward zero.",
        f"Baseline (panel FE) β={b_real if b_real is not None else '—'}{stars(p_real)} vs placebo β={b_pl if b_pl is not None else '—'}{stars(p_pl)}.",
        "Interpretation: if placebo is near zero while baseline is not, the signal is less likely to be an artifact of fixed cross-sectional differences.",
    ]

    placebo_table = {
        "title": "Placebo (shuffle within ticker): panel FE comparison",
        "columns": ["Spec", "β(score_mean)", "p", "Sig"],
        "rows": [
            ["Baseline", b_real, p_real, stars(p_real)],
            [f"Placebo (seed={args.placebo_seed})", b_pl, p_pl, stars(p_pl)],
        ],
    }

    s6 = build_study_payload(
        slug="placebo-shuffle-test",
        title="Placebo test: shuffle sentiment within ticker",
        category="Robustness",
        summary="Shuffle score_mean within ticker (destroys time ordering) and re-run predictive panel FE.",
        updated_at=args.updated_at,
        status="live",
        tags=["placebo", "robustness", "panel", "fixed effects"],
        key_stats=[
            {"label": "β real", "value": f"{b_real:.3g}{stars(p_real)}" if b_real is not None else "—"},
            {"label": "β placebo", "value": f"{b_pl:.3g}{stars(p_pl)}" if b_pl is not None else "—"},
            {"label": "Seed", "value": str(args.placebo_seed)},
            {"label": "R² FE (real)", "value": f"{safe_num(fe_real.get('rsquared')):.3g}" if safe_num(fe_real.get("rsquared")) is not None else "—"},
            {"label": "Tickers", "value": str(n_tickers)},
            {"label": "Obs", "value": str(n_obs_panel)},
        ],
        methodology=[
            "Within each ticker, randomly permute the sentiment time series (deterministic seed).",
            "Re-estimate y_ret(t+1) ~ score_mean(t) (+ controls) with panel FE and ticker-clustered SE.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["Compare baseline panel FE vs placebo with permuted score_mean."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl6,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
            "panel_fe_baseline": fe_real,
            "panel_fe_placebo": fe_pl,
            "tables": [placebo_table],
        },
        notes=[],
    )
    write_json(out_dir / f"{s6['slug']}.json", s6)
    studies.append(s6)

    # -------------------- Study 7: portfolio sorts (academic staple) --------------------
    ps = portfolio_sorts(
        panel,
        sort_col="score_mean",
        ret_col="y_ret_fwd1",
        n_bins=int(args.sort_bins),
        min_n=int(args.sort_min_n),
        nw_lags=int(args.maxlags),
    )
    if "error" not in ps:
        st = ps["stats"]
        s7 = build_study_payload(
            slug="portfolio-sorts-sentiment",
            title="Portfolio sorts: next-day returns by sentiment quantile",
            category="Portfolio sorts",
            summary="Each day, sort tickers into quantiles by score_mean(t); compute mean r(t+1) and long–short spread with NW t-stats.",
            updated_at=args.updated_at,
            status="live",
            tags=["portfolio sorts", "long-short", "Newey-West", "cross-sectional"],
            key_stats=[
                {"label": "LS mean (bps/day)", "value": f"{st.get('ls_mean', float('nan'))*10000:.2f}" if np.isfinite(st.get("ls_mean", np.nan)) else "—"},
                {"label": "LS t (NW)", "value": f"{st.get('ls_t', float('nan')):.3g}" if np.isfinite(st.get("ls_t", np.nan)) else "—"},
                {"label": "LS Sharpe (ann.)", "value": f"{st.get('ls_sharpe', float('nan')):.3g}" if np.isfinite(st.get("ls_sharpe", np.nan)) else "—"},
                {"label": "Days", "value": str(st.get("n_days", "—"))},
                {"label": "Bins", "value": str(st.get("n_bins", "—"))},
                {"label": "NW lags", "value": str(st.get("nw_lags", "—"))},
            ],
            methodology=[
                f"Per day: form {args.sort_bins}-quantile portfolios by score_mean(t).",
                "Compute average y_ret(t+1) within each portfolio and long–short (top minus bottom).",
                f"Test mean returns using Newey–West HAC SE (lags={args.maxlags}).",
            ],
            sections=[
                {"title": "Specification", "bullets": ["Q-sort on score_mean(t); report E[r(t+1)|Q] and LS = Q_high − Q_low."]},
                *build_sections_common(universe, freq, y_def, s_def, caveats),
            ],
            conclusions=[
                "Portfolio sorts provide an economically interpretable view of predictability (if any) beyond regression coefficients.",
                "A significant long–short mean with robust NW t-stat is consistent with cross-sectional pricing of the signal.",
            ],
            results={
                "n_tickers": n_tickers,
                "n_obs_panel": n_obs_panel,
                "date_range": [start_date, end_date],
                "tables": [ps.get("table")],
                "series": ps.get("series"),
                "portfolio_sorts": ps,
            },
            notes=[],
        )
        write_json(out_dir / f"{s7['slug']}.json", s7)
        studies.append(s7)

    # -------------------- Study 8: out-of-sample predictability (OOS R^2) --------------------
    # Use sample ticker as a conservative OOS demo (keeps runtime light and easy to interpret).
    oos = oos_predictability(df_sample, y="y_ret_fwd1", x_cols=x_cols, min_train=int(args.oos_min_train), nw_lags=int(args.maxlags))
    if "error" not in oos:
        st = oos["stats"]
        s8 = build_study_payload(
            slug="oos-predictability-sample",
            title="Out-of-sample predictability (sample ticker): OOS R²",
            category="Predictability",
            summary="Expanding-window OOS forecast for y_ret(t+1) using score_mean(t) (+ n_total). Report OOS R² vs historical mean benchmark.",
            updated_at=args.updated_at,
            status="live",
            tags=["out-of-sample", "forecasting", "OOS R2", "predictability"],
            key_stats=[
                {"label": "OOS R²", "value": f"{st.get('oos_r2', float('nan')):.4g}" if np.isfinite(st.get("oos_r2", np.nan)) else "—"},
                {"label": "Test N", "value": str(st.get("n_test", "—"))},
                {"label": "Train min", "value": str(st.get("min_train", "—"))},
                {"label": "NW lags", "value": str(st.get("nw_lags", "—"))},
                {"label": "DM-like t", "value": f"{st.get('dm_like_t', float('nan')):.3g}" if np.isfinite(st.get("dm_like_t", np.nan)) else "—"},
                {"label": "DM-like p", "value": f"{st.get('dm_like_p', float('nan')):.3g}" if np.isfinite(st.get("dm_like_p", np.nan)) else "—"},
            ],
            methodology=[
                f"At each t, fit OLS on data up to t−1; predict y_ret(t+1).",
                "Benchmark forecast: historical mean return up to t−1.",
                "OOS R² computed from relative MSE vs benchmark.",
            ],
            sections=[
                {"title": "Specification", "bullets": ["Forecast: y_ret(t+1) = a + b*score_mean(t) + c*n_total(t) + ε."]},
                *build_sections_common(universe, freq, y_def, s_def, caveats),
            ],
            conclusions=[
                "OOS R² is a stricter criterion than in-sample fit: it penalizes unstable relationships.",
                "If OOS R² is near zero while same-day effects are strong, the metric is likely descriptive (reaction) rather than predictive (alpha).",
            ],
            results={
                "sample_ticker": sample_ticker,
                "date_range": [start_date, end_date],
                "oos": oos,
                "series": {
                    # no dates here (we don't carry them) but sparklines work without x-axis
                    "real": oos["series"]["real"],
                    "pred": oos["series"]["pred"],
                    "cum_real": oos["series"]["cum_real"],
                    "cum_pred": oos["series"]["cum_pred"],
                },
            },
            notes=[],
        )
        write_json(out_dir / f"{s8['slug']}.json", s8)
        studies.append(s8)

    # -------------------- Study 9: event study (extreme sentiment) --------------------
    ev = event_study(panel, signal_col="score_mean", ret_col="y_ret", z_thr=float(args.event_z), window=int(args.event_window), min_events=int(args.event_min_events))
    if "error" not in ev:
        st = ev["stats"]
        s9 = build_study_payload(
            slug="event-study-extreme-sentiment",
            title="Event study: return dynamics around extreme sentiment",
            category="Event studies",
            summary="Panel event study using within-ticker z-score of sentiment. Compare average CAR paths for extreme positive vs negative sentiment days.",
            updated_at=args.updated_at,
            status="live",
            tags=["event study", "CAR", "extremes", "mechanism"],
            key_stats=[
                {"label": "z threshold", "value": str(st.get("z_thr", "—"))},
                {"label": "window", "value": str(st.get("window", "—"))},
                {"label": "N pos", "value": str(st.get("n_pos", "—"))},
                {"label": "N neg", "value": str(st.get("n_neg", "—"))},
                {"label": "warning", "value": str(st.get("warning", "—")) if "warning" in st else "—"},
                {"label": "date range", "value": f"{start_date}..{end_date}"},
            ],
            methodology=[
                "Compute within-ticker z-score of score_mean.",
                "Define events: z≥threshold (positive) and z≤−threshold (negative).",
                "Compute average cumulative log-return paths (CAR) around events: tau ∈ [−W..W].",
            ],
            sections=[
                {"title": "Specification", "bullets": ["CAR(τ) computed relative to event date, using log returns."]},
                *build_sections_common(universe, freq, y_def, s_def, caveats),
            ],
            conclusions=[
                "Event-study profiles help distinguish immediate reaction vs post-event drift.",
                "Asymmetries between positive and negative events can suggest non-linear responses to sentiment extremes.",
            ],
            results={
                "n_tickers": n_tickers,
                "n_obs_panel": n_obs_panel,
                "date_range": [start_date, end_date],
                "tables": [ev.get("table")] if isinstance(ev.get("table"), dict) else [],
                "series": ev.get("series"),
                "event_study": ev,
            },
            notes=[],
        )
        write_json(out_dir / f"{s9['slug']}.json", s9)
        studies.append(s9)

    # -------------------- Study 10: market-adjusted predictability (if SPY available) --------------------
    mkt = try_load_market_series(repo_root, data_root, symbol="SPY")
    if mkt is not None and not mkt.empty:
        p2 = panel.merge(mkt, on="date", how="left")
        p2["excess_fwd1"] = p2["y_ret_fwd1"] - p2["mkt_ret_fwd1"]
        x10 = x_cols
        fe10 = panel_within_fe_cluster(p2, "excess_fwd1", x10)
        ts10 = time_series_ols_hac(df_sample.merge(mkt, on="date", how="left"), "y_ret_fwd1", x10, maxlags=int(args.maxlags))

        s10 = build_study_payload(
            slug="market-adjusted-predictability",
            title="Market-adjusted predictability: excess next-day returns",
            category="Predictability",
            summary="Predictive regression on excess returns: (r_i(t+1) − r_mkt(t+1)) ~ score_mean(t) (+ n_total). Market proxy: SPY if present in ticker JSON.",
            updated_at=args.updated_at,
            status="live",
            tags=["market-adjusted", "excess return", "panel", "robustness"],
            key_stats=key_stats_from(ts10, fe10, "score_mean"),
            methodology=[
                "Construct market next-day return using SPY (if present).",
                "Define excess_fwd1 = y_ret_fwd1 − mkt_ret_fwd1.",
                "Estimate TS HAC and panel FE predictability on excess_fwd1.",
            ],
            sections=[
                {"title": "Specification", "bullets": ["excess_fwd1(t) ~ score_mean(t) + n_total(t) (optional)."]},
                *build_sections_common(universe, freq, y_def, s_def, caveats),
            ],
            conclusions=conclusion_from_models("Market-adjusted predictability", ts10, fe10, "score_mean", "excess next-day return")
            + ["Market-adjustment reduces the chance results are driven by broad market moves."],
            results={
                "sample_ticker": sample_ticker,
                "n_tickers": n_tickers,
                "n_obs_panel": n_obs_panel,
                "date_range": [start_date, end_date],
                "time_series": ts10,
                "panel_fe": fe10,
            },
            notes=["This study appears only if SPY.json exists in the same ticker JSON directory."],
        )
        write_json(out_dir / f"{s10['slug']}.json", s10)
        studies.append(s10)

    # -------------------- Study 11: sector heterogeneity (if mapping available) --------------------
    sec_map = sector_map_from_sp500(repo_root)
    if sec_map is not None:
        psec = panel.copy()
        psec["sector"] = psec["ticker"].astype(str).str.upper().map(sec_map)
        psec = psec.dropna(subset=["sector"]).copy()

        rows = []
        for sec, g in psec.groupby("sector", sort=True):
            tickers_sec = g["ticker"].nunique()
            if tickers_sec < int(args.sector_min_tickers):
                continue
            fe_sec = panel_within_fe_cluster(g, "y_ret_fwd1", x_cols)
            b = safe_num((fe_sec.get("params") or {}).get("score_mean"))
            t = safe_num((fe_sec.get("tvalues") or {}).get("score_mean"))
            pval = safe_num((fe_sec.get("pvalues") or {}).get("score_mean"))
            r2 = safe_num(fe_sec.get("rsquared"))
            rows.append([sec, int(tickers_sec), int(len(g)), b, t, pval, stars(pval), r2])

        if rows:
            table = {
                "title": "Sector heterogeneity: panel FE slope on score_mean (predicting r(t+1))",
                "columns": ["Sector", "Tickers", "Obs", "β", "t", "p", "Sig", "R²"],
                "rows": rows,
            }

            # key stat: share significant at 10%
            sig10 = sum(1 for r in rows if isinstance(r[5], (int, float)) and np.isfinite(r[5]) and r[5] < 0.1)
            s11 = build_study_payload(
                slug="sector-heterogeneity",
                title="Heterogeneity: predictive slope by sector (panel FE)",
                category="Heterogeneity",
                summary="Run predictive panel FE y_ret(t+1) ~ score_mean(t) (+ n_total) within each sector. Report β and clustered t-stats.",
                updated_at=args.updated_at,
                status="live",
                tags=["heterogeneity", "sector", "panel", "fixed effects"],
                key_stats=[
                    {"label": "Sectors", "value": str(len(rows))},
                    {"label": "Sig @10%", "value": str(sig10)},
                    {"label": "Min tickers", "value": str(args.sector_min_tickers)},
                    {"label": "Tickers", "value": str(n_tickers)},
                    {"label": "Obs", "value": str(n_obs_panel)},
                    {"label": "date range", "value": f"{start_date}..{end_date}"},
                ],
                methodology=[
                    "Map tickers to sector using sp500_index.json (if present).",
                    "Within each sector: panel FE with ticker-clustered SE.",
                ],
                sections=[
                    {"title": "Specification", "bullets": ["Within sector s: y_ret(t+1) ~ score_mean(t) + controls, with ticker FE."]},
                    *build_sections_common(universe, freq, y_def, s_def, caveats),
                ],
                conclusions=[
                    "Heterogeneity tables help diagnose whether the signal is concentrated in a subset of industries.",
                    "Sector-level instability is common in sentiment proxies; strong broad-based results are harder to obtain.",
                ],
                results={
                    "n_tickers": n_tickers,
                    "n_obs_panel": n_obs_panel,
                    "date_range": [start_date, end_date],
                    "tables": [table],
                },
                notes=["This study appears only if sp500_index.json exists and contains a sector field."],
            )
            write_json(out_dir / f"{s11['slug']}.json", s11)
            studies.append(s11)

    # -------------------- index + overview (sectioned) --------------------
    index = []
    for s in studies:
        index.append(
            {
                "slug": s["slug"],
                "title": s["title"],
                "summary": s["summary"],
                "updated_at": s["updated_at"],
                "status": s.get("status", "draft"),
                "tags": s.get("tags", []),
                "key_stats": s.get("key_stats", []),
                "highlight": (s.get("conclusions") or [""])[0],
                "category": s.get("category", "Other"),
            }
        )
    write_json(out_dir / "index.json", index)

    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for s in studies:
        by_cat.setdefault(s.get("category", "Other"), []).append(s)

    sections = []
    for cat, ss in by_cat.items():
        ss_sorted = sorted(ss, key=lambda x: x.get("slug", ""))
        concls = []
        for st in ss_sorted[:3]:
            c0 = (st.get("conclusions") or [""])[0]
            if c0:
                concls.append(c0)
        sections.append(
            {
                "id": cat.lower().replace(" ", "-").replace("&", "and"),
                "title": cat,
                "description": "Empirical research notes built from the live Sentiment Live dataset.",
                "conclusions": concls[:3],
                "slugs": [st["slug"] for st in ss_sorted],
            }
        )

    overview = {
        "meta": {
            "updated_at": args.updated_at,
            "n_studies": len(studies),
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "date_range": [start_date, end_date],
        },
        "sections": sorted(sections, key=lambda x: x["title"]),
    }
    write_json(out_dir / "overview.json", overview)

    print(f"[OK] wrote {len(studies)} studies to {out_dir}")
    print(f"[OK] tickers={n_tickers}, panel_obs={n_obs_panel}, sample_ticker={sample_ticker}")
    print(f"[OK] date_range={start_date}..{end_date}")


if __name__ == "__main__":
    main()
