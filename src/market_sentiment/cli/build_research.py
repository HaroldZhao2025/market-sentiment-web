#!/usr/bin/env python3
"""
Build research JSON artifacts for the Next.js Research section.

Reads:
  data/{TICKER}/price/daily.json
  data/{TICKER}/sentiment/*.json   (one JSON per day)

Writes:
  apps/web/public/research/index.json
  apps/web/public/research/{slug}.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.regression.quantile_regression import QuantReg


def _json_safe(x: Any) -> Any:
    if isinstance(x, (np.floating, np.integer)):
        return x.item()
    if isinstance(x, (np.ndarray,)):
        return [_json_safe(v) for v in x.tolist()]
    if isinstance(x, (pd.Timestamp,)):
        return x.strftime("%Y-%m-%d")
    return x


def load_price_df(data_root: Path, ticker: str) -> pd.DataFrame:
    path = data_root / ticker / "price" / "daily.json"
    df = pd.read_json(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    df["y_ret"] = np.log(df["close"]).diff()
    df["abs_ret"] = df["y_ret"].abs()
    return df[["y_ret", "abs_ret"]]


def load_sentiment_df(data_root: Path, ticker: str) -> Optional[pd.DataFrame]:
    folder = data_root / ticker / "sentiment"
    if not folder.exists():
        return None

    files = sorted(folder.glob("*.json"))
    if not files:
        return None

    records: List[Dict[str, Any]] = []
    for fp in files:
        try:
            d = json.loads(fp.read_text())
            # date can come from content or filename stem
            date = d.get("date") or fp.stem
            rec = {"date": date}
            if "score_mean" in d:
                rec["score_mean"] = d.get("score_mean")
            if "n_total" in d:
                rec["n_total"] = d.get("n_total")
            records.append(rec)
        except Exception:
            continue

    df = pd.DataFrame(records)
    if df.empty or "date" not in df.columns:
        return None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").set_index("date")

    keep = [c for c in ["score_mean", "n_total"] if c in df.columns]
    if not keep:
        return None

    for c in keep:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[keep]


def build_panel(data_root: Path, min_obs: int = 50) -> pd.DataFrame:
    tickers = [p.name for p in data_root.iterdir() if p.is_dir()]
    out = []
    for tic in tickers:
        try:
            px = load_price_df(data_root, tic)
            se = load_sentiment_df(data_root, tic)
            if se is None:
                continue
            df = px.join(se, how="inner").dropna(subset=["y_ret", "score_mean"])
            if len(df) < min_obs:
                continue
            df["ticker"] = tic
            out.append(df)
        except Exception:
            continue

    if not out:
        raise RuntimeError("No usable ticker panels constructed. Check data paths.")

    panel = pd.concat(out).reset_index(names="date")
    panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

    # next-day return
    panel["y_ret_fwd1"] = panel.groupby("ticker")["y_ret"].shift(-1)

    return panel


def _ols_summary(res: sm.regression.linear_model.RegressionResultsWrapper) -> Dict[str, Any]:
    out = {
        "params": {k: _json_safe(v) for k, v in res.params.items()},
        "bse": {k: _json_safe(v) for k, v in res.bse.items()},
        "tvalues": {k: _json_safe(v) for k, v in res.tvalues.items()},
        "pvalues": {k: _json_safe(v) for k, v in res.pvalues.items()},
        "nobs": int(res.nobs),
        "rsquared": _json_safe(getattr(res, "rsquared", None)),
    }
    return out


def time_series_reg(df: pd.DataFrame, y_col: str, x_cols: List[str]) -> Dict[str, Any]:
    use = df.dropna(subset=[y_col] + x_cols).copy()
    X = sm.add_constant(use[x_cols].astype(float))
    y = use[y_col].astype(float)
    res = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 5})
    return _ols_summary(res)


def panel_fe_within(panel: pd.DataFrame, y_col: str, x_cols: List[str]) -> Dict[str, Any]:
    use = panel.dropna(subset=["ticker", y_col] + x_cols).copy()

    # within transformation to remove ticker FE (memory-light)
    g = use.groupby("ticker", sort=False)
    y = use[y_col].astype(float) - g[y_col].transform("mean").astype(float)

    Xdm = []
    for c in x_cols:
        Xdm.append(use[c].astype(float) - g[c].transform("mean").astype(float))

    X = pd.concat(Xdm, axis=1)
    X.columns = x_cols

    # no constant (demeaned)
    res = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": use["ticker"]})
    out = _ols_summary(res)
    out["cov_type"] = "cluster(ticker)"
    return out


def quantile_betas(df: pd.DataFrame, y_col: str, x_cols: List[str], qs=(0.1, 0.5, 0.9)) -> Dict[str, Any]:
    use = df.dropna(subset=[y_col] + x_cols).copy()
    X = sm.add_constant(use[x_cols].astype(float))
    y = use[y_col].astype(float)

    out: Dict[str, Any] = {}
    for q in qs:
        res = QuantReg(y, X).fit(q=q)
        out[str(q)] = {
            "params": {k: _json_safe(v) for k, v in res.params.items()},
            "tvalues": {k: _json_safe(v) for k, v in res.tvalues.items()},
            "pvalues": {k: _json_safe(v) for k, v in res.pvalues.items()},
        }
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=_json_safe))


def build_study_payload(
    slug: str,
    title: str,
    summary: str,
    updated_at: str,
    status: str,
    tags: List[str],
    key_stats: List[Dict[str, str]],
    methodology: List[str],
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
        "results": results,
        "notes": notes,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="data", type=str)
    ap.add_argument("--out-dir", default="apps/web/public/research", type=str)
    ap.add_argument("--min-obs", default=50, type=int)
    ap.add_argument("--updated-at", default=pd.Timestamp.today().strftime("%Y-%m-%d"), type=str)
    args = ap.parse_args()

    data_root = Path(args.data_root)
    out_dir = Path(args.out_dir)

    panel = build_panel(data_root, min_obs=args.min_obs)

    # pick sample ticker with most observations
    sample_ticker = panel["ticker"].value_counts().index[0]
    df_sample = panel.loc[panel["ticker"] == sample_ticker].copy()
    df_sample = df_sample.sort_values("date")

    n_tickers = int(panel["ticker"].nunique())
    n_obs_panel = int(len(panel))

    # ---- Study 1: same-day sentiment vs same-day returns (your notebook) ----
    x_cols = ["score_mean"] + (["n_total"] if "n_total" in panel.columns else [])
    ts1 = time_series_reg(df_sample, "y_ret", x_cols)
    fe1 = panel_fe_within(panel, "y_ret", x_cols)
    q1 = quantile_betas(df_sample, "y_ret", ["score_mean"])

    key_stats_1 = [
        {"label": "β(score_mean) TS", "value": f"{ts1['params'].get('score_mean', float('nan')):.4g}"},
        {"label": "t-stat TS", "value": f"{ts1['tvalues'].get('score_mean', float('nan')):.3g}"},
        {"label": "β(score_mean) FE", "value": f"{fe1['params'].get('score_mean', float('nan')):.4g}"},
        {"label": "t-stat FE", "value": f"{fe1['tvalues'].get('score_mean', float('nan')):.3g}"},
    ]

    study1 = build_study_payload(
        slug="same-day-sentiment-vs-returns",
        title="Same-day sentiment vs same-day returns",
        summary="Time-series + (within) ticker fixed-effects panel regression: y_ret(t) ~ score_mean(t) (+ news count).",
        updated_at=args.updated_at,
        status="live",
        tags=["time-series", "panel", "fixed effects", "HAC", "returns", "sentiment"],
        key_stats=key_stats_1,
        methodology=[
            "Dependent variable: daily log return y_ret(t).",
            "Main regressor: same-day sentiment score_mean(t).",
            "Optional control: same-day news count n_total(t).",
            "Time-series OLS uses HAC (Newey–West) standard errors with maxlags=5.",
            "Panel uses within transformation to remove ticker FE; SE clustered by ticker.",
            "Quantile regression (sample ticker) reports how β(score_mean) varies across tails.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "y_ret": df_sample["y_ret"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist(),
                "score_mean": df_sample["score_mean"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist(),
                "n_total": df_sample["n_total"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist()
                if "n_total" in df_sample.columns else [],
            },
            "time_series": ts1,
            "panel_fe": fe1,
            "quantiles": q1,
        },
        notes=[
            "Same-day spec allows contemporaneous news to move prices on the same date.",
            "Interpretation depends on timestamp granularity (headline time vs close-to-close return).",
        ],
    )

    write_json(out_dir / "same-day-sentiment-vs-returns.json", study1)

    # ---- Study 2: sentiment(t) vs next-day return(t+1) ----
    ts2 = time_series_reg(df_sample, "y_ret_fwd1", x_cols)
    fe2 = panel_fe_within(panel, "y_ret_fwd1", x_cols)

    key_stats_2 = [
        {"label": "β(score_mean) TS", "value": f"{ts2['params'].get('score_mean', float('nan')):.4g}"},
        {"label": "t-stat TS", "value": f"{ts2['tvalues'].get('score_mean', float('nan')):.3g}"},
        {"label": "β(score_mean) FE", "value": f"{fe2['params'].get('score_mean', float('nan')):.4g}"},
        {"label": "t-stat FE", "value": f"{fe2['tvalues'].get('score_mean', float('nan')):.3g}"},
    ]

    study2 = build_study_payload(
        slug="sentiment-vs-next-day-returns",
        title="Sentiment vs next-day returns",
        summary="Predictive check: y_ret(t+1) ~ score_mean(t) (+ n_total(t)) in TS + ticker FE panel.",
        updated_at=args.updated_at,
        status="live",
        tags=["predictive", "panel", "fixed effects", "returns", "sentiment"],
        key_stats=key_stats_2,
        methodology=[
            "Dependent variable: next-day log return y_ret(t+1).",
            "Regressors: score_mean(t) and optional n_total(t).",
            "Time-series OLS uses HAC; panel uses within estimator with ticker-clustered SE.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "y_ret_fwd1": df_sample["y_ret_fwd1"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist(),
                "score_mean": df_sample["score_mean"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist(),
            },
            "time_series": ts2,
            "panel_fe": fe2,
        },
        notes=[
            "If this looks stronger than same-day, it suggests delayed price reaction (or news arrives after close).",
        ],
    )

    write_json(out_dir / "sentiment-vs-next-day-returns.json", study2)

    # ---- Study 3: news volume vs volatility proxy ----
    # abs_ret(t) ~ n_total(t) (+ score_mean(t))
    x_cols_vol = []
    if "n_total" in panel.columns:
        x_cols_vol.append("n_total")
    x_cols_vol.append("score_mean")

    ts3 = time_series_reg(df_sample, "abs_ret", x_cols_vol)
    fe3 = panel_fe_within(panel, "abs_ret", x_cols_vol)

    key_stats_3 = [
        {"label": "β(n_total) TS", "value": f"{ts3['params'].get('n_total', float('nan')):.4g}"},
        {"label": "t-stat TS", "value": f"{ts3['tvalues'].get('n_total', float('nan')):.3g}"},
        {"label": "β(n_total) FE", "value": f"{fe3['params'].get('n_total', float('nan')):.4g}"},
        {"label": "t-stat FE", "value": f"{fe3['tvalues'].get('n_total', float('nan')):.3g}"},
    ]

    study3 = build_study_payload(
        slug="news-volume-vs-volatility",
        title="News volume vs volatility (abs returns)",
        summary="Does more news coincide with larger moves? abs_ret(t) ~ n_total(t) + score_mean(t).",
        updated_at=args.updated_at,
        status="live",
        tags=["volatility", "news volume", "panel", "fixed effects"],
        key_stats=key_stats_3,
        methodology=[
            "Volatility proxy: absolute daily log return |y_ret(t)|.",
            "Regressors: news count n_total(t) (if available) and score_mean(t).",
            "Time-series HAC; panel within estimator with ticker-clustered SE.",
        ],
        results={
            "sample_ticker": sample_ticker,
            "n_tickers": n_tickers,
            "n_obs_panel": n_obs_panel,
            "series": {
                "dates": df_sample["date"].dt.strftime("%Y-%m-%d").tolist(),
                "abs_ret": df_sample["abs_ret"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist(),
                "n_total": df_sample["n_total"].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).tolist()
                if "n_total" in df_sample.columns else [],
            },
            "time_series": ts3,
            "panel_fe": fe3,
        },
        notes=[
            "This is a simple volatility proxy; you can swap to squared returns or realized vol later.",
        ],
    )

    write_json(out_dir / "news-volume-vs-volatility.json", study3)

    # ---- index.json (for the Research landing page + static params) ----
    index = []
    for s in [study1, study2, study3]:
        index.append({
            "slug": s["slug"],
            "title": s["title"],
            "summary": s["summary"],
            "updated_at": s["updated_at"],
            "status": s.get("status", "draft"),
            "tags": s.get("tags", []),
            "key_stats": s.get("key_stats", []),
        })

    write_json(out_dir / "index.json", index)

    print(f"[OK] wrote {len(index)} studies to {out_dir}")


if __name__ == "__main__":
    main()
