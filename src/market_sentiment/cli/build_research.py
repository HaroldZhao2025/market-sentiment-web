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
# Utilities
# -----------------------------

def _json_safe(x: Any) -> Any:
    if isinstance(x, (np.integer, np.floating)):
        return x.item()
    if isinstance(x, (np.ndarray,)):
        return [_json_safe(v) for v in x.tolist()]
    if isinstance(x, (pd.Timestamp,)):
        return x.strftime("%Y-%m-%d")
    return x


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=_json_safe), encoding="utf-8")


def fmt_sig(p: Optional[float]) -> str:
    if p is None or not np.isfinite(p):
        return ""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.1:
        return "*"
    return ""


def classify_significance(p: Optional[float]) -> str:
    if p is None or not np.isfinite(p):
        return "unknown"
    if p < 0.01:
        return "strong"
    if p < 0.05:
        return "moderate"
    if p < 0.1:
        return "weak"
    return "none"


def human_relation(beta: Optional[float], p: Optional[float], var_name: str) -> str:
    if beta is None or not np.isfinite(beta):
        return f"No reliable estimate for {var_name}."
    sig = classify_significance(p)
    direction = "positive" if beta > 0 else "negative" if beta < 0 else "flat"
    if sig == "strong":
        strength = "statistically strong"
    elif sig == "moderate":
        strength = "statistically significant"
    elif sig == "weak":
        strength = "marginally significant"
    elif sig == "none":
        strength = "not statistically significant"
    else:
        strength = "unclear significance"
    return f"{var_name} shows a {direction} relationship ({strength})."


def bps_per_unit(beta: Optional[float]) -> Optional[float]:
    # log-return beta -> approx. return beta; convert to bps
    if beta is None or not np.isfinite(beta):
        return None
    return float(beta) * 10000.0


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


# -----------------------------
# Data discovery + loading
# -----------------------------

def find_ticker_dir(repo_root: Path, data_root: Path) -> Optional[Path]:
    """
    Try to locate per-ticker JSON series, matching how your app searches.
    We prioritize series that look like:
      { "ticker": "...", "dates": [...], "price": [...], "S" or "sentiment": [...] }
    """
    candidates = [
        data_root / "ticker",
        data_root / "data" / "ticker",
        repo_root / "public" / "data" / "ticker",
        repo_root / "apps" / "web" / "public" / "data" / "ticker",
        repo_root / "data" / "ticker",
        repo_root / "public" / "data" / "ticker",
    ]
    for c in candidates:
        try:
            if c.exists() and c.is_dir():
                # quick sanity: has at least one json
                if any(c.glob("*.json")):
                    return c
        except Exception:
            pass
    return None


def load_one_ticker_series(ticker_path: Path) -> Optional[pd.DataFrame]:
    """
    Load a ticker JSON series and return a DataFrame indexed by date with:
      y_ret, abs_ret, score_mean, (optional) n_total
    """
    try:
        obj = read_json(ticker_path)
    except Exception:
        return None

    dates = obj.get("dates")
    prices = obj.get("price")
    if not isinstance(dates, list) or not isinstance(prices, list) or len(dates) < 3 or len(prices) < 3:
        return None

    # sentiment series key can be S or sentiment or score_mean
    s_key = pick_first_key(obj, ["S", "sentiment", "score_mean", "sentiment_score", "sent"])
    if s_key is None:
        return None
    s_arr = obj.get(s_key)
    if not isinstance(s_arr, list) or len(s_arr) != len(dates):
        # allow mismatch but only if we can align by date later (rare). For now, reject.
        return None

    # optional news count
    n_key = pick_first_key(obj, ["n_total", "news_count", "num_news", "n_news", "count"])
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
    if len(df) < 10:
        return None

    df = df.set_index("date")
    df["y_ret"] = np.log(df["price"]).diff()
    df["abs_ret"] = df["y_ret"].abs()

    # clean
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["y_ret", "score_mean"])

    return df[["y_ret", "abs_ret", "score_mean"] + (["n_total"] if "n_total" in df.columns else [])]


def load_panel(repo_root: Path, data_root: Path, min_obs: int) -> pd.DataFrame:
    ticker_dir = find_ticker_dir(repo_root, data_root)
    if ticker_dir is None:
        raise RuntimeError(
            "Could not find ticker series directory. Expected something like data/ticker/*.json or public/data/ticker/*.json"
        )

    frames: List[pd.DataFrame] = []
    tickers: List[str] = []

    for fp in sorted(ticker_dir.glob("*.json")):
        # ticker name from file
        t = fp.stem
        df = load_one_ticker_series(fp)
        if df is None or len(df) < min_obs:
            continue
        tmp = df.copy()
        tmp["ticker"] = t
        tmp = tmp.reset_index(names="date")
        frames.append(tmp)
        tickers.append(t)

    if not frames:
        raise RuntimeError(f"No usable tickers found in {ticker_dir} with min_obs={min_obs}.")

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward return
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)

    return panel


# -----------------------------
# Models
# -----------------------------

def _ols_summary(res: sm.regression.linear_model.RegressionResultsWrapper) -> Dict[str, Any]:
    def _get(d: pd.Series, k: str) -> Optional[float]:
        try:
            v = float(d.get(k))
            return v if np.isfinite(v) else None
        except Exception:
            return None

    keys = list(res.params.index)
    out = {
        "params": {k: safe_num(res.params.get(k)) for k in keys},
        "bse": {k: safe_num(res.bse.get(k)) for k in keys},
        "tvalues": {k: safe_num(res.tvalues.get(k)) for k in keys},
        "pvalues": {k: safe_num(res.pvalues.get(k)) for k in keys},
        "nobs": int(getattr(res, "nobs", 0) or 0),
        "rsquared": safe_num(getattr(res, "rsquared", None)),
        "rsquared_adj": safe_num(getattr(res, "rsquared_adj", None)),
    }
    return out


def time_series_ols_hac(df: pd.DataFrame, y: str, x_cols: List[str], maxlags: int = 5) -> Dict[str, Any]:
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
# Conclusions generation
# -----------------------------

def conclusion_block(
    *,
    study_name: str,
    ts: Dict[str, Any],
    fe: Dict[str, Any],
    main_var: str,
    y_desc: str,
    extra_notes: Optional[List[str]] = None,
) -> List[str]:
    conclusions: List[str] = []

    # TS
    b_ts = safe_num((ts.get("params") or {}).get(main_var))
    p_ts = safe_num((ts.get("pvalues") or {}).get(main_var))
    t_ts = safe_num((ts.get("tvalues") or {}).get(main_var))
    r2_ts = safe_num(ts.get("rsquared"))

    # FE
    b_fe = safe_num((fe.get("params") or {}).get(main_var))
    p_fe = safe_num((fe.get("pvalues") or {}).get(main_var))
    t_fe = safe_num((fe.get("tvalues") or {}).get(main_var))

    # Sentence 1: sign + significance summary
    conclusions.append(
        f"{study_name}: {human_relation(b_ts, p_ts, main_var)} "
        f"(TS β={b_ts if b_ts is not None else '—'}{fmt_sig(p_ts)}, t={t_ts if t_ts is not None else '—'}, R²={r2_ts if r2_ts is not None else '—'})."
    )

    # Sentence 2: FE confirmation
    conclusions.append(
        f"Ticker FE check: {human_relation(b_fe, p_fe, main_var)} "
        f"(FE β={b_fe if b_fe is not None else '—'}{fmt_sig(p_fe)}, t={t_fe if t_fe is not None else '—'})."
    )

    # Sentence 3: magnitude
    bps_ts = bps_per_unit(b_ts)
    if bps_ts is not None:
        conclusions.append(
            f"Effect size (TS): +1 unit in {main_var} is associated with ~{bps_ts:.2f} bps change in {y_desc} (approx, in log-return units)."
        )

    # Notes
    if extra_notes:
        conclusions.extend(extra_notes)

    return conclusions


def key_stats_from(ts: Dict[str, Any], fe: Dict[str, Any], main_var: str) -> List[Dict[str, str]]:
    b_ts = safe_num((ts.get("params") or {}).get(main_var))
    t_ts = safe_num((ts.get("tvalues") or {}).get(main_var))
    p_ts = safe_num((ts.get("pvalues") or {}).get(main_var))

    b_fe = safe_num((fe.get("params") or {}).get(main_var))
    t_fe = safe_num((fe.get("tvalues") or {}).get(main_var))
    p_fe = safe_num((fe.get("pvalues") or {}).get(main_var))

    def f(x: Optional[float], nd=4) -> str:
        return "—" if x is None else f"{x:.{nd}g}"

    return [
        {"label": f"β({main_var}) TS", "value": f"{f(b_ts)}{fmt_sig(p_ts)}"},
        {"label": "t-stat TS", "value": f(t_ts, 3)},
        {"label": f"β({main_var}) FE", "value": f"{f(b_fe)}{fmt_sig(p_fe)}"},
        {"label": "t-stat FE", "value": f(t_fe, 3)},
    ]


# -----------------------------
# Main build
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
    ap.add_argument("--data-root", type=str, default="data", help="Root folder where ticker json lives (e.g., data/)")
    ap.add_argument("--out-dir", type=str, default="apps/web/public/research", help="Output folder for research json")
    ap.add_argument("--min-obs", type=int, default=80, help="Min observations per ticker to be included")
    ap.add_argument("--updated-at", type=str, default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    ap.add_argument("--maxlags", type=int, default=5)
    ap.add_argument("--no-quantiles", action="store_true", help="Skip quantile regressions for speed")
    args = ap.parse_args()

    repo_root = Path.cwd()
    data_root = (repo_root / args.data_root).resolve()
    out_dir = (repo_root / args.out_dir).resolve()

    panel = load_panel(repo_root=repo_root, data_root=data_root, min_obs=int(args.min_obs))

    n_tickers = int(panel["ticker"].nunique())
    n_obs_panel = int(len(panel))

    # sample ticker with most observations
    sample_ticker = panel["ticker"].value_counts().index[0]
    df_sample = panel.loc[panel["ticker"] == sample_ticker].sort_values("date").copy()

    # Decide regressors (score_mean + optional n_total)
    x_cols = ["score_mean"] + (["n_total"] if "n_total" in panel.columns else [])

    # -------- Study 1: same-day returns --------
    ts1 = time_series_ols_hac(df_sample, "y_ret", x_cols, maxlags=int(args.maxlags))
    fe1 = panel_within_fe_cluster(panel, "y_ret", x_cols)
    q1 = {} if args.no_quantiles else quantile_reg(df_sample, "y_ret", ["score_mean"])

    key_stats_1 = key_stats_from(ts1, fe1, "score_mean")
    conclusions_1 = conclusion_block(
        study_name="Same-day sentiment vs same-day returns",
        ts=ts1,
        fe=fe1,
        main_var="score_mean",
        y_desc="daily return",
        extra_notes=[
            "Interpretation note: same-day specs are contemporaneous (they may reflect same-day information flow rather than predictability)."
        ],
    )

    study1 = build_study_payload(
        slug="same-day-sentiment-vs-returns",
        title="Same-day sentiment vs same-day returns",
        summary="TS (HAC) + ticker fixed-effects panel: y_ret(t) ~ score_mean(t) (+ news count if available).",
        updated_at=args.updated_at,
        status="live",
        tags=["time-series", "panel", "fixed effects", "HAC", "returns", "sentiment"],
        key_stats=key_stats_1,
        methodology=[
            "Dependent variable: daily log return y_ret(t).",
            "Main regressor: same-day sentiment score_mean(t).",
            "Optional control: same-day news count n_total(t) if present in ticker JSON.",
            f"Time-series: OLS with HAC (Newey–West) maxlags={int(args.maxlags)}.",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
            "Quantiles: (optional) QuantReg on the sample ticker for tails.",
        ],
        conclusions=conclusions_1,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "y_ret": df_sample["y_ret"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
                "score_mean": df_sample["score_mean"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
                "n_total": df_sample["n_total"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist()
                if "n_total" in df_sample.columns
                else [],
            },
            "time_series": ts1,
            "panel_fe": fe1,
            "quantiles": q1,
        },
        notes=[
            "If your sentiment is built from articles that arrive after the market close, consider shifting the sentiment by +1 day for cleaner interpretation.",
        ],
    )

    write_json(out_dir / f"{study1['slug']}.json", study1)

    # -------- Study 2: next-day returns --------
    ts2 = time_series_ols_hac(df_sample, "y_ret_fwd1", x_cols, maxlags=int(args.maxlags))
    fe2 = panel_within_fe_cluster(panel, "y_ret_fwd1", x_cols)

    key_stats_2 = key_stats_from(ts2, fe2, "score_mean")
    conclusions_2 = conclusion_block(
        study_name="Sentiment vs next-day returns",
        ts=ts2,
        fe=fe2,
        main_var="score_mean",
        y_desc="next-day return",
        extra_notes=[
            "This spec is a basic predictability check: if significant and stable, it suggests delayed reaction / potential alpha (subject to trading frictions)."
        ],
    )

    study2 = build_study_payload(
        slug="sentiment-vs-next-day-returns",
        title="Sentiment vs next-day returns",
        summary="Predictability check: y_ret(t+1) ~ score_mean(t) (+ news count if available).",
        updated_at=args.updated_at,
        status="live",
        tags=["predictive", "panel", "fixed effects", "returns", "sentiment"],
        key_stats=key_stats_2,
        methodology=[
            "Dependent variable: next-day log return y_ret(t+1).",
            "Regressors: score_mean(t) and optional n_total(t).",
            f"Time-series: OLS with HAC (Newey–West) maxlags={int(args.maxlags)}.",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
        ],
        conclusions=conclusions_2,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "y_ret_fwd1": df_sample["y_ret_fwd1"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
                "score_mean": df_sample["score_mean"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
            },
            "time_series": ts2,
            "panel_fe": fe2,
        },
        notes=[
            "If next-day effects are weak but same-day is strong, the sentiment metric may be capturing contemporaneous information flow rather than forecasting.",
        ],
    )

    write_json(out_dir / f"{study2['slug']}.json", study2)

    # -------- Study 3: volatility proxy (abs returns) --------
    if "n_total" in panel.columns:
        x_cols_vol = ["n_total", "score_mean"]
        main_var_vol = "n_total"
        title3 = "News volume vs volatility (abs returns)"
        summary3 = "Does more news coincide with bigger moves? abs_ret(t) ~ n_total(t) + score_mean(t)."
        tags3 = ["volatility", "news volume", "panel", "fixed effects"]
    else:
        # fallback: still a volatility page using sentiment only
        x_cols_vol = ["score_mean"]
        main_var_vol = "score_mean"
        title3 = "Sentiment vs volatility proxy (abs returns)"
        summary3 = "abs_ret(t) ~ score_mean(t). (news volume not available in ticker JSON)"
        tags3 = ["volatility", "sentiment", "panel", "fixed effects"]

    ts3 = time_series_ols_hac(df_sample, "abs_ret", x_cols_vol, maxlags=int(args.maxlags))
    fe3 = panel_within_fe_cluster(panel, "abs_ret", x_cols_vol)

    key_stats_3 = key_stats_from(ts3, fe3, main_var_vol)
    conclusions_3 = conclusion_block(
        study_name=title3,
        ts=ts3,
        fe=fe3,
        main_var=main_var_vol,
        y_desc="abs return (vol proxy)",
        extra_notes=[
            "This is a simple volatility proxy; you can later replace it with squared returns or realized volatility measures.",
        ],
    )

    study3 = build_study_payload(
        slug="news-volume-vs-volatility" if "n_total" in panel.columns else "sentiment-vs-volatility",
        title=title3,
        summary=summary3,
        updated_at=args.updated_at,
        status="live",
        tags=tags3,
        key_stats=key_stats_3,
        methodology=[
            "Volatility proxy: absolute daily log return |y_ret(t)|.",
            "Regressors: n_total(t) (if available) and/or score_mean(t).",
            f"Time-series: OLS with HAC (Newey–West) maxlags={int(args.maxlags)}.",
            "Panel: within estimator removing ticker FE; SE clustered by ticker.",
        ],
        conclusions=conclusions_3,
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "abs_ret": df_sample["abs_ret"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
                "score_mean": df_sample["score_mean"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist(),
                "n_total": df_sample["n_total"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float).tolist()
                if "n_total" in df_sample.columns
                else [],
            },
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[
            "A positive n_total coefficient is common: more news tends to coincide with larger moves (regardless of direction).",
        ],
    )

    write_json(out_dir / f"{study3['slug']}.json", study3)

    # -------- index.json --------
    studies = [study1, study2, study3]
    index = [
        {
            "slug": s["slug"],
            "title": s["title"],
            "summary": s["summary"],
            "updated_at": s["updated_at"],
            "status": s.get("status", "draft"),
            "tags": s.get("tags", []),
            "key_stats": s.get("key_stats", []),
            "highlight": (s.get("conclusions") or [""])[0],
        }
        for s in studies
    ]
    write_json(out_dir / "index.json", index)

    print(f"[OK] wrote {len(studies)} studies to: {out_dir}")
    print(f"[OK] included tickers: {n_tickers}, panel obs: {n_obs_panel}, sample ticker: {sample_ticker}")


if __name__ == "__main__":
    main()
