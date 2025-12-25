#!/usr/bin/env python3
# src/market_sentiment/cli/build_research.py

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import statsmodels.api as sm
from statsmodels.regression.quantile_regression import QuantReg


# -----------------------------
# IO helpers
# -----------------------------

def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2, default=_json_safe), encoding="utf-8")


def _json_safe(x: Any) -> Any:
    if isinstance(x, (np.integer, np.floating)):
        return x.item()
    if isinstance(x, (np.ndarray,)):
        return [_json_safe(v) for v in x.tolist()]
    if isinstance(x, (pd.Timestamp,)):
        return x.strftime("%Y-%m-%d")
    return x


# -----------------------------
# Discovery: find ticker json dir
# -----------------------------

def find_ticker_dir(repo_root: Path, data_root: Path) -> Optional[Path]:
    """
    Try to locate per-ticker JSON time series.
    Most common patterns:
      data/ticker/*.json
      public/data/ticker/*.json
      apps/web/public/data/ticker/*.json
    """
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


def pick_first_key(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d:
            return k
    return None


# -----------------------------
# Load a ticker JSON into df
# -----------------------------

def load_one_ticker_series(fp: Path) -> Optional[pd.DataFrame]:
    """
    Expects a json like:
      { dates: [...], price: [...], S or sentiment or score_mean: [...], optional n_total: [...] }
    Returns DataFrame indexed by date with:
      y_ret, abs_ret, score_mean, optional n_total
    """
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
            # normalize sentiment to score_mean (what your UI expects)
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

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["y_ret", "score_mean"])

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

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward return
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)

    return panel


# -----------------------------
# Export series for UI (CRITICAL FIX)
# -----------------------------

def export_series(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Always export everything the UI might plot.
    This fixes your 'Sentiment (sample) ... No series available' issue by ensuring score_mean exists.
    """
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
    add("score_mean")  # ✅ MUST HAVE for your UI
    add("n_total")

    return out


# -----------------------------
# Regression helpers
# -----------------------------

def safe_num(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


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
# Conclusions generation (simple but useful)
# -----------------------------

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

    lines: List[str] = []

    if b_ts is not None:
        direction = "positive" if b_ts > 0 else "negative" if b_ts < 0 else "flat"
        bps = b_ts * 10000.0
        lines.append(
            f"{study_title}: {var} is {direction} in the time-series spec "
            f"(β={b_ts:.6g}{stars(p_ts)}, t={t_ts if t_ts is not None else '—'}, R²={r2 if r2 is not None else '—'})."
        )
        lines.append(f"Approx magnitude: +1.0 in {var} ↦ {bps:.2f} bps change in {y_label} (log-return units).")
    else:
        lines.append(f"{study_title}: time-series estimate for {var} not available.")

    if b_fe is not None:
        direction = "positive" if b_fe > 0 else "negative" if b_fe < 0 else "flat"
        lines.append(
            f"Panel FE check: {var} is {direction} after removing ticker fixed effects "
            f"(β={b_fe:.6g}{stars(p_fe)}, t={t_fe if t_fe is not None else '—'})."
        )
    else:
        lines.append("Panel FE output not available (insufficient data or model error).")

    return lines


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


# -----------------------------
# Build studies
# -----------------------------

def build_study_payload(
    *,
    slug: str,
    title: str,
    summary: str,
    updated_at: str,
    status: str,
    tags: List[str],
    key_stats: List[Dict[str, str]],
    methodology: List[str],
    conclusions: List[str],
    results: Dict[str, Any],
    notes: List[str],
) -> Dict[str, Any]:
    return {
        "slug": slug,
        "title": title,
        "summary": summary,
        "updated_at": updated_at,
        "status": status,
        "tags": tags,
        "key_stats": key_stats,
        "methodology": methodology,
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
    ap.add_argument("--allow-empty", action="store_true", help="Do not fail build if no data found")
    args = ap.parse_args()

    repo_root = Path.cwd()
    data_root = (repo_root / args.data_root).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    try:
        panel = build_panel(repo_root, data_root, min_obs=int(args.min_obs))
    except Exception as e:
        # Export-safe: don't kill your whole Pages build
        if args.allow_empty or True:
            write_json(out_dir / "index.json", [])
            print(f"[WARN] build_research: {e}")
            print(f"[WARN] wrote empty index.json to {out_dir}")
            return
        raise

    n_tickers = int(panel["ticker"].nunique())
    n_obs_panel = int(len(panel))

    # sample ticker with most observations
    sample_ticker = panel["ticker"].value_counts().index[0]
    df_sample = panel.loc[panel["ticker"] == sample_ticker].sort_values("date").copy()

    # regressors
    x_cols = ["score_mean"] + (["n_total"] if "n_total" in panel.columns else [])

    studies: List[Dict[str, Any]] = []

    # ---- Study 1: Same-day returns ----
    ts1 = time_series_ols_hac(df_sample, "y_ret", x_cols, maxlags=int(args.maxlags))
    fe1 = panel_within_fe_cluster(panel, "y_ret", x_cols)
    q1 = {} if args.no_quantiles else quantile_reg(df_sample, "y_ret", ["score_mean"])

    conclusions_1 = conclusion_from_models(
        "Same-day sentiment vs same-day returns", ts1, fe1, "score_mean", "daily return"
    )

    study1 = build_study_payload(
        slug="same-day-sentiment-vs-returns",
        title="Same-day sentiment vs same-day returns",
        summary="TS (HAC) + ticker fixed-effects panel: y_ret(t) ~ score_mean(t) (+ news count if available).",
        updated_at=args.updated_at,
        status="live",
        tags=["time-series", "panel", "fixed effects", "HAC", "returns", "sentiment"],
        key_stats=key_stats_from(ts1, fe1, "score_mean"),
        methodology=[
            "Dependent variable: daily log return y_ret(t).",
            "Main regressor: same-day sentiment score_mean(t).",
            "Optional control: same-day news count n_total(t) if present.",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker fixed effects; SE clustered by ticker.",
            "Quantiles: (optional) quantile regression on the sample ticker.",
        ],
        conclusions=conclusions_1 + [
            "Interpretation note: same-day results can reflect contemporaneous information flow rather than predictability.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample),  # ✅ always includes score_mean
            "time_series": ts1,
            "panel_fe": fe1,
            "quantiles": q1,
        },
        notes=[
            "If your sentiment is based on articles after the close, consider shifting sentiment by +1 day for cleaner predictive interpretation.",
        ],
    )
    write_json(out_dir / f"{study1['slug']}.json", study1)
    studies.append(study1)

    # ---- Study 2: Next-day returns ----
    ts2 = time_series_ols_hac(df_sample, "y_ret_fwd1", x_cols, maxlags=int(args.maxlags))
    fe2 = panel_within_fe_cluster(panel, "y_ret_fwd1", x_cols)

    conclusions_2 = conclusion_from_models(
        "Sentiment vs next-day returns", ts2, fe2, "score_mean", "next-day return"
    )

    study2 = build_study_payload(
        slug="sentiment-vs-next-day-returns",
        title="Sentiment vs next-day returns",
        summary="Predictability check: y_ret(t+1) ~ score_mean(t) (+ news count if available).",
        updated_at=args.updated_at,
        status="live",
        tags=["predictive", "panel", "fixed effects", "returns", "sentiment"],
        key_stats=key_stats_from(ts2, fe2, "score_mean"),
        methodology=[
            "Dependent variable: next-day log return y_ret(t+1).",
            "Regressors: score_mean(t) and optional n_total(t).",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker fixed effects; SE clustered by ticker.",
        ],
        conclusions=conclusions_2 + [
            "If next-day effects are weak but same-day is strong, the sentiment metric may be capturing contemporaneous reaction rather than forecasting.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample),  # ✅ includes y_ret_fwd1 + score_mean
            "time_series": ts2,
            "panel_fe": fe2,
        },
        notes=[],
    )
    write_json(out_dir / f"{study2['slug']}.json", study2)
    studies.append(study2)

    # ---- Study 3: News volume vs volatility proxy (abs returns) ----
    if "n_total" in panel.columns:
        x_cols3 = ["n_total", "score_mean"]
        main_var = "n_total"
        title3 = "News volume vs volatility (abs returns)"
        slug3 = "news-volume-vs-volatility"
        summary3 = "Does more news coincide with larger moves? abs_ret(t) ~ n_total(t) + score_mean(t)."
        tags3 = ["volatility", "news volume", "panel", "fixed effects"]
    else:
        x_cols3 = ["score_mean"]
        main_var = "score_mean"
        title3 = "Sentiment vs volatility proxy (abs returns)"
        slug3 = "sentiment-vs-volatility"
        summary3 = "abs_ret(t) ~ score_mean(t). (news volume not available in ticker JSON)"
        tags3 = ["volatility", "sentiment", "panel", "fixed effects"]

    ts3 = time_series_ols_hac(df_sample, "abs_ret", x_cols3, maxlags=int(args.maxlags))
    fe3 = panel_within_fe_cluster(panel, "abs_ret", x_cols3)

    conclusions_3 = conclusion_from_models(title3, ts3, fe3, main_var, "abs return (vol proxy)")

    study3 = build_study_payload(
        slug=slug3,
        title=title3,
        summary=summary3,
        updated_at=args.updated_at,
        status="live",
        tags=tags3,
        key_stats=key_stats_from(ts3, fe3, main_var),
        methodology=[
            "Volatility proxy: absolute daily log return |y_ret(t)|.",
            "Regressors: n_total(t) (if available) and score_mean(t).",
            f"Time-series: OLS with Newey–West HAC SE (maxlags={int(args.maxlags)}).",
            "Panel: within estimator removing ticker fixed effects; SE clustered by ticker.",
        ],
        conclusions=conclusions_3 + [
            "This is a simple volatility proxy; you can later replace it with squared returns or realized volatility measures.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": export_series(df_sample),  # ✅ includes abs_ret + score_mean (+ n_total)
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[
            "A positive n_total coefficient is common: more news tends to coincide with larger moves (regardless of direction).",
        ],
    )
    write_json(out_dir / f"{study3['slug']}.json", study3)
    studies.append(study3)

    # ---- index.json ----
    index = []
    for s in studies:
        highlight = (s.get("conclusions") or [""])[0]
        index.append(
            {
                "slug": s["slug"],
                "title": s["title"],
                "summary": s["summary"],
                "updated_at": s["updated_at"],
                "status": s.get("status", "draft"),
                "tags": s.get("tags", []),
                "key_stats": s.get("key_stats", []),
                "highlight": highlight,
            }
        )

    write_json(out_dir / "index.json", index)

    print(f"[OK] wrote {len(studies)} studies to {out_dir}")
    print(f"[OK] tickers={n_tickers}, panel_obs={n_obs_panel}, sample_ticker={sample_ticker}")
    # helpful for debugging in Actions
    print(f"[INFO] out files: {', '.join([s['slug'] + '.json' for s in studies] + ['index.json'])}")


if __name__ == "__main__":
    main()
