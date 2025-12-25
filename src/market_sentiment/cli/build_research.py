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

    # forward return
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)

    return panel


# -----------------------------
# Export series for UI (fixes your missing score_mean chart)
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
    add("score_mean")   # ✅ always export for sparkline
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
# Serious academic add-ons
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
        # OLS
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

    # NW t for mean slope using OLS on constant with HAC
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
    # sample ticker
    s = df_sample.copy()
    for L in range(max_lag + 1):
        s[f"{base_col}_lag{L}"] = s[base_col].shift(L)

    x_lags = [f"{base_col}_lag{L}" for L in range(max_lag + 1)]
    x_ts = x_lags + controls
    ts = time_series_ols_hac(s, y, x_ts, maxlags=nw_lags)

    # panel
    p = add_lags(panel, base_col, max_lag)
    x_fe = x_lags + controls
    fe = panel_within_fe_cluster(p, y, x_fe)

    # build a compact table: lag coefs + t + p (panel FE)
    rows = []
    for L in range(max_lag + 1):
        v = f"{base_col}_lag{L}"
        b = safe_num((fe.get("params") or {}).get(v))
        t = safe_num((fe.get("tvalues") or {}).get(v))
        pval = safe_num((fe.get("pvalues") or {}).get(v))
        rows.append([v, b, t, pval, stars(pval)])

    # cumulative effect (sum of betas)
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


# -----------------------------
# Packaging: key stats / sections / conclusions
# -----------------------------

def key_stats_from(ts: Dict[str, Any], fe: Dict[str, Any], var: str) -> List[Dict[str, str]]:
    b_ts = safe_num((ts.get("params") or {}).get(var))
    t_ts = safe_num((ts.get("tvalues") or {}).get(var))
    p_ts = safe_num((ts.get("pvalues") or {}).get(var))

    b_fe = safe_num((fe.get("params") or {}).get(var))
    t_fe = safe_num((fe.get("tvalues") or {}).get(var))
    p_fe = safe_num((fe.get("pvalues") or {}).get(var))

    def f(x: Optional[float], nd=4) -> str:
        return "—" if x is None else f"{x:.{nd}g}"

    return [
        {"label": f"β({var}) TS", "value": f"{f(b_ts)}{stars(p_ts)}"},
        {"label": "t-stat TS", "value": f(t_ts, 3)},
        {"label": f"β({var}) FE", "value": f"{f(b_fe)}{stars(p_fe)}"},
        {"label": "t-stat FE", "value": f(t_fe, 3)},
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
            f"Panel FE: {var} is {direction} after removing ticker FE (β={b_fe:.6g}{stars(p_fe)}, t={t_fe if t_fe is not None else '—'})."
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
            "series": export_series(df_sample),  # ✅ includes score_mean
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
            "series": export_series(df_sample),  # ✅ includes score_mean, so sentiment chart renders
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[],
    )
    write_json(out_dir / f"{s3['slug']}.json", s3)
    studies.append(s3)

    # -------------------- Study 4: Fama–MacBeth (serious academic) --------------------
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
        ],
        methodology=[
            "Each date t: cross-sectional OLS over tickers: y_ret(t+1) ~ score_mean(t) (+ n_total(t)).",
            "Collect daily slope estimates; test mean slope using Newey–West standard errors.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["For each day t: r_{i,t+1} = a_t + b_t * score_{i,t} + c_t * n_total_{i,t} + ε_{i,t}.", "Then test E[b_t] using NW SE."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl4,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "famamacbeth": fm,
            "tables": [fm.get("table")] if isinstance(fm.get("table"), dict) else [],
        },
        notes=[],
    )
    write_json(out_dir / f"{s4['slug']}.json", s4)
    studies.append(s4)

    # -------------------- Study 5: distributed lags (serious academic) --------------------
    controls5 = ["n_total"] if has_news else []
    dl = distributed_lag_models(panel, df_sample, y="y_ret_fwd1", base_col="score_mean", max_lag=int(args.dl_max_lag), controls=controls5, nw_lags=int(args.maxlags))

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
            {"label": "Tickers", "value": str(n_tickers)},
            {"label": "Obs", "value": str(n_obs_panel)},
        ],
        methodology=[
            f"Construct lagged sentiment variables score_mean_lag0..score_mean_lag{args.dl_max_lag}.",
            "Estimate y_ret(t+1) on lags with panel FE; SE clustered by ticker. Also fit TS HAC on sample ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": [f"y_ret(t+1) ~ Σ_{{L=0..{args.dl_max_lag}}} β_L score_mean(t-L) + controls."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl5,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
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

    # -------------------- Study 6: placebo / shuffle test (serious academic) --------------------
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
            {"label": "Tickers", "value": str(n_tickers)},
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
            "panel_fe_baseline": fe_real,
            "panel_fe_placebo": fe_pl,
            "tables": [placebo_table],
        },
        notes=[],
    )
    write_json(out_dir / f"{s6['slug']}.json", s6)
    studies.append(s6)

    # -------------------- index + overview (sectioned, academic) --------------------
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

    # overview: meta + section-level conclusions
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
