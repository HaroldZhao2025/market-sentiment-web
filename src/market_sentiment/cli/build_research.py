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
# Discovery: find ticker json dir
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
# Load a ticker JSON into df
# -----------------------------

def load_one_ticker_series(fp: Path) -> Optional[pd.DataFrame]:
    try:
        obj = read_json(fp)
    except Exception:
        return None

    dates = obj.get("dates")
    prices = obj.get("price")
    if not isinstance(dates, list) or not isinstance(prices, list):
        return None
    if len(dates) < 20 or len(prices) < 20 or len(dates) != len(prices):
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
    if len(df) < 20:
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
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)
    return panel


# -----------------------------
# Export series for UI (CRITICAL)
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
    add("score_mean")  # ✅ always
    add("n_total")
    return out


# -----------------------------
# Regression helpers
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
    if len(use) < 30:
        return {"error": "too_few_obs"}
    X = sm.add_constant(use[x_cols].astype(float), has_constant="add")
    yv = use[y].astype(float)
    res = sm.OLS(yv, X).fit(cov_type="HAC", cov_kwds={"maxlags": int(maxlags)})
    out = _ols_summary(res)
    out["cov_type"] = f"HAC(maxlags={maxlags})"
    return out


def panel_within_fe_cluster(panel: pd.DataFrame, y: str, x_cols: List[str]) -> Dict[str, Any]:
    use = panel.dropna(subset=["ticker", y] + x_cols).copy()
    if len(use) < 200:
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
    if len(use) < 100:
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
# Academic packaging helpers
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
                f"Returns definition: {y_def}.",
                f"Sentiment definition: {s_def}.",
            ],
        },
        {
            "title": "Limitations",
            "bullets": caveats,
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
            f"Panel FE check: {var} is {direction} after removing ticker FE (β={b_fe:.6g}{stars(p_fe)}, t={t_fe if t_fe is not None else '—'})."
        )
    else:
        out.append("Panel FE output not available (insufficient data or model error).")

    if b_ts is not None:
        out.append(f"Scale: +1.0 in {var} ≈ {(b_ts*10000):.2f} bps change in {y_label} (log-return units).")

    return out


# -----------------------------
# New study: sentiment-sort long-short
# -----------------------------

def build_sentiment_sort(panel: pd.DataFrame, n_bins: int = 5, min_xs: int = 120) -> Dict[str, Any]:
    """
    Each date t:
      - sort tickers by score_mean(t) into n_bins
      - compute next-day returns y_ret_fwd1(t) by bin
      - long-short = top - bottom
    """
    use = panel.dropna(subset=["date", "ticker", "score_mean", "y_ret_fwd1"]).copy()
    use = use.sort_values(["date", "ticker"])

    out_rows = []
    for dt, g in use.groupby("date", sort=True):
        if len(g) < min_xs:
            continue
        # robust binning: use ranks to avoid qcut duplicate edges
        r = g["score_mean"].rank(method="first")
        q = pd.qcut(r, q=n_bins, labels=False, duplicates="drop")
        if q.nunique() < 2:
            continue
        g = g.assign(bin=q.values)

        by = g.groupby("bin")["y_ret_fwd1"].mean()
        lo = by.min()
        hi = by.max()
        ls = hi - lo
        out_rows.append((dt, float(lo), float(hi), float(ls)))

    if not out_rows:
        return {"error": "no_dates_after_filter"}

    df = pd.DataFrame(out_rows, columns=["date", "bottom", "top", "long_short"]).sort_values("date")
    df["cum_long_short"] = np.exp(df["long_short"].cumsum()) - 1.0
    df["cum_top"] = np.exp(df["top"].cumsum()) - 1.0
    df["cum_bottom"] = np.exp(df["bottom"].cumsum()) - 1.0

    # summary stats
    mu = df["long_short"].mean()
    sd = df["long_short"].std(ddof=1)
    sharpe = (mu / sd) * np.sqrt(252) if sd > 0 else np.nan

    # HAC t-stat for mean via regression on constant
    X = np.ones((len(df), 1))
    res = sm.OLS(df["long_short"].values, X).fit(cov_type="HAC", cov_kwds={"maxlags": 5})
    t = float(res.tvalues[0]) if len(res.tvalues) else np.nan
    p = float(res.pvalues[0]) if len(res.pvalues) else np.nan

    return {
        "series": {
            "dates": df["date"].dt.strftime("%Y-%m-%d").tolist(),
            "bottom": df["bottom"].astype(float).tolist(),
            "top": df["top"].astype(float).tolist(),
            "long_short": df["long_short"].astype(float).tolist(),
            "cum_long_short": df["cum_long_short"].astype(float).tolist(),
        },
        "stats": {
            "n_days": int(len(df)),
            "mean_daily": safe_num(mu),
            "std_daily": safe_num(sd),
            "sharpe_ann": safe_num(sharpe),
            "t_hac": safe_num(t),
            "p_hac": safe_num(p),
        },
    }


# -----------------------------
# Build payload
# -----------------------------

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=str, default="data")
    ap.add_argument("--out-dir", type=str, default="apps/web/public/research")
    ap.add_argument("--min-obs", type=int, default=80)
    ap.add_argument("--updated-at", type=str, default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    ap.add_argument("--maxlags", type=int, default=5)
    ap.add_argument("--no-quantiles", action="store_true")
    args = ap.parse_args()

    repo_root = Path.cwd()
    data_root = (repo_root / args.data_root).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    # Export-safe behavior: if panel cannot be built, write empty index/overview so site won't 404
    try:
        panel = build_panel(repo_root, data_root, min_obs=int(args.min_obs))
    except Exception as e:
        write_json(out_dir / "index.json", [])
        write_json(out_dir / "overview.json", {"sections": []})
        print(f"[WARN] build_research failed: {e}")
        print(f"[WARN] wrote empty research artifacts to: {out_dir}")
        return

    n_tickers = int(panel["ticker"].nunique())
    n_obs_panel = int(len(panel))

    sample_ticker = panel["ticker"].value_counts().index[0]
    df_sample = panel.loc[panel["ticker"] == sample_ticker].sort_values("date").copy()

    x_cols = ["score_mean"] + (["n_total"] if "n_total" in panel.columns else [])

    universe = "S&P 500 constituents (as in your snapshot pipeline)"
    freq = "Daily"
    y_def = "log(P_t) - log(P_{t-1}) using the price series in ticker JSON"
    s_def = "score_mean in ticker JSON (normalized sentiment metric in your pipeline)"
    caveats = [
        "Timing: if news arrives after close, same-day sentiment may not be tradable without shifting.",
        "Survivorship: depends on how the S&P 500 universe is snapshotted in your repo.",
        "Causality: regressions are descriptive; omitted variables may remain.",
    ]

    studies: List[Dict[str, Any]] = []

    # -------------------- Study 1 --------------------
    ts1 = time_series_ols_hac(df_sample, "y_ret", x_cols, maxlags=int(args.maxlags))
    fe1 = panel_within_fe_cluster(panel, "y_ret", x_cols)
    q1 = {} if args.no_quantiles else quantile_reg(df_sample, "y_ret", ["score_mean"])

    study1 = build_study_payload(
        slug="same-day-sentiment-vs-returns",
        title="Same-day sentiment vs same-day returns",
        category="Contemporaneous relationships",
        summary="TS (HAC) + ticker fixed-effects panel: y_ret(t) ~ score_mean(t) (+ news count if available).",
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
            {"title": "Specification", "bullets": ["y_ret(t) ~ score_mean(t) + controls."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=conclusion_from_models("Same-day sentiment vs same-day returns", ts1, fe1, "score_mean", "daily return")
        + ["Interpretation: same-day results are likely contemporaneous reaction rather than predictability."],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample),
            "time_series": ts1,
            "panel_fe": fe1,
            "quantiles": q1,
        },
        notes=[],
    )
    write_json(out_dir / f"{study1['slug']}.json", study1)
    studies.append(study1)

    # -------------------- Study 2 --------------------
    ts2 = time_series_ols_hac(df_sample, "y_ret_fwd1", x_cols, maxlags=int(args.maxlags))
    fe2 = panel_within_fe_cluster(panel, "y_ret_fwd1", x_cols)

    study2 = build_study_payload(
        slug="sentiment-vs-next-day-returns",
        title="Sentiment vs next-day returns",
        category="Predictability",
        summary="Predictability check: y_ret(t+1) ~ score_mean(t) (+ news count if available).",
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
            {"title": "Specification", "bullets": ["y_ret(t+1) ~ score_mean(t) + controls."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=conclusion_from_models("Sentiment vs next-day returns", ts2, fe2, "score_mean", "next-day return")
        + ["If this is weak while same-day is strong, the metric is likely contemporaneous rather than forecasting."],
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
    write_json(out_dir / f"{study2['slug']}.json", study2)
    studies.append(study2)

    # -------------------- Study 3 --------------------
    if "n_total" in panel.columns:
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

    study3 = build_study_payload(
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
        + ["Interpretation: news volume often correlates with larger moves regardless of direction."],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample),  # includes abs_ret, n_total, score_mean
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[],
    )
    write_json(out_dir / f"{study3['slug']}.json", study3)
    studies.append(study3)

    # -------------------- Study 4 (NEW): sentiment-sort portfolio --------------------
    sort_out = build_sentiment_sort(panel, n_bins=5, min_xs=120)
    stats = sort_out.get("stats") or {}
    t_hac = safe_num(stats.get("t_hac"))
    p_hac = safe_num(stats.get("p_hac"))
    sharpe = safe_num(stats.get("sharpe_ann"))

    concl4 = [
        f"Sentiment-sorted long-short (top-minus-bottom) shows mean={safe_num(stats.get('mean_daily'))} per day, "
        f"t(HAC)={t_hac if t_hac is not None else '—'}{stars(p_hac)}, Sharpe(ann)={sharpe if sharpe is not None else '—'}.",
        "Interpretation: if statistically significant, this suggests delayed reaction / cross-sectional predictability (subject to trading frictions).",
        "This is equal-weighted and uses next-day returns; add transaction costs and turnover next for realism.",
    ]

    study4 = build_study_payload(
        slug="sentiment-sorted-long-short",
        title="Sentiment-sorted portfolios (next-day, long-short)",
        category="Predictability",
        summary="Each day: sort tickers by score_mean(t) into quintiles; evaluate next-day returns and top-minus-bottom spread.",
        updated_at=args.updated_at,
        status="live",
        tags=["cross-sectional", "portfolios", "predictive", "HAC"],
        key_stats=[
            {"label": "t(HAC) mean LS", "value": f"{t_hac:.3g}{stars(p_hac)}" if t_hac is not None else "—"},
            {"label": "Sharpe (ann)", "value": f"{sharpe:.3g}" if sharpe is not None else "—"},
            {"label": "Days", "value": str(stats.get("n_days", "—"))},
            {"label": "Bins", "value": "5"},
        ],
        methodology=[
            "Cross-sectional sort: rank tickers by score_mean(t) each date.",
            "Compute equal-weighted next-day returns by bin; long-short = top - bottom.",
            "Test mean long-short return using OLS on constant with HAC SE.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["LS(t+1) = E[r_{top}(t+1)] - E[r_{bottom}(t+1)] based on score_mean(t) sorting."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl4,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "portfolios": sort_out,  # contains series + stats
        },
        notes=[],
    )
    write_json(out_dir / f"{study4['slug']}.json", study4)
    studies.append(study4)

    # -------------------- Study 5 (NEW): asymmetry (pos vs neg sentiment) --------------------
    panel2 = panel.copy()
    panel2["score_pos"] = panel2["score_mean"].clip(lower=0.0)
    panel2["score_neg"] = panel2["score_mean"].clip(upper=0.0)

    df_sample2 = df_sample.copy()
    df_sample2["score_pos"] = df_sample2["score_mean"].clip(lower=0.0)
    df_sample2["score_neg"] = df_sample2["score_mean"].clip(upper=0.0)

    x5 = ["score_pos", "score_neg"] + (["n_total"] if "n_total" in panel2.columns else [])
    ts5 = time_series_ols_hac(df_sample2, "y_ret_fwd1", x5, maxlags=int(args.maxlags))
    fe5 = panel_within_fe_cluster(panel2, "y_ret_fwd1", x5)

    # section conclusions (explicit)
    bpos = safe_num((fe5.get("params") or {}).get("score_pos"))
    ppos = safe_num((fe5.get("pvalues") or {}).get("score_pos"))
    bneg = safe_num((fe5.get("params") or {}).get("score_neg"))
    pneg = safe_num((fe5.get("pvalues") or {}).get("score_neg"))

    concl5 = [
        f"Asymmetry test (panel FE): score_pos β={bpos if bpos is not None else '—'}{stars(ppos)}; "
        f"score_neg β={bneg if bneg is not None else '—'}{stars(pneg)}.",
        "Interpretation: different coefficients suggest sentiment impacts differ between positive vs negative tone.",
        "Use this to motivate non-linear models (splines / regime splits) later.",
    ]

    study5 = build_study_payload(
        slug="sentiment-asymmetry-next-day",
        title="Sentiment asymmetry (positive vs negative, next-day)",
        category="Predictability",
        summary="Nonlinearity check: split sentiment into positive and negative components; regress next-day returns on both.",
        updated_at=args.updated_at,
        status="live",
        tags=["predictive", "nonlinear", "panel", "fixed effects", "HAC"],
        key_stats=[
            {"label": "β(pos) FE", "value": f"{bpos:.3g}{stars(ppos)}" if bpos is not None else "—"},
            {"label": "β(neg) FE", "value": f"{bneg:.3g}{stars(pneg)}" if bneg is not None else "—"},
            {"label": "Tickers", "value": str(n_tickers)},
            {"label": "Obs", "value": str(n_obs_panel)},
        ],
        methodology=[
            "Construct score_pos = max(score_mean, 0) and score_neg = min(score_mean, 0).",
            "Estimate y_ret(t+1) ~ score_pos(t) + score_neg(t) (+ n_total).",
            "Panel FE removes time-invariant ticker differences; SE clustered by ticker.",
        ],
        sections=[
            {"title": "Specification", "bullets": ["y_ret(t+1) ~ score_pos(t) + score_neg(t) + controls."]},
            *build_sections_common(universe, freq, y_def, s_def, caveats),
        ],
        conclusions=concl5,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample2),  # still includes score_mean
            "time_series": ts5,
            "panel_fe": fe5,
        },
        notes=[],
    )
    write_json(out_dir / f"{study5['slug']}.json", study5)
    studies.append(study5)

    # -------------------- index.json + overview.json (SECTION CONCLUSIONS) --------------------

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

    # Create section-level conclusions (1–3 bullets each)
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for s in studies:
        by_cat.setdefault(s.get("category", "Other"), []).append(s)

    sections = []
    for cat, ss in by_cat.items():
        ss_sorted = sorted(ss, key=lambda x: x.get("slug", ""))
        # take first conclusion line from up to 2 studies as the section "conclusion bullets"
        concls = []
        for st in ss_sorted[:2]:
            c0 = (st.get("conclusions") or [""])[0]
            if c0:
                concls.append(c0)
        if not concls:
            concls = ["No conclusions available yet (research build missing or too few observations)."]

        sections.append(
            {
                "id": cat.lower().replace(" ", "-").replace("&", "and"),
                "title": cat,
                "description": "Empirical snapshots updated with the same pipeline powering the site.",
                "conclusions": concls,
                "slugs": [st["slug"] for st in ss_sorted],
            }
        )

    sections = sorted(sections, key=lambda x: x["title"])
    write_json(out_dir / "overview.json", {"sections": sections})

    print(f"[OK] wrote {len(studies)} studies to {out_dir}")
    print(f"[OK] tickers={n_tickers}, panel_obs={n_obs_panel}, sample_ticker={sample_ticker}")


if __name__ == "__main__":
    main()
