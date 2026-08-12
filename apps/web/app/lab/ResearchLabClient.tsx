"use client";

import { useMemo, useState } from "react";
import type { LabSummary } from "../../lib/intelligence";

type Props = { rows: LabSummary[] };

function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}

function pct(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(d)}%`;
}

function num(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
}

export default function ResearchLabClient({ rows }: Props) {
  const sectors = useMemo(() => Array.from(new Set(rows.map((r) => r.sector))), [rows]);
  const [signal, setSignal] = useState<LabSummary["signal"]>("sentiment");
  const [horizon, setHorizon] = useState<LabSummary["horizon"]>(5);
  const [sector, setSector] = useState("All");
  const [quantile, setQuantile] = useState<LabSummary["quantile"]>(0.25);

  const current = useMemo(
    () => rows.find((r) => r.signal === signal && r.horizon === horizon && r.sector === sector && r.quantile === quantile) ?? null,
    [rows, signal, horizon, sector, quantile]
  );

  const signalLabel = signal === "sentiment" ? "Sentiment level" : signal === "sentiment_change" ? "Sentiment change" : "Sentiment-price divergence";

  return (
    <div className="space-y-6">
      <section className="ambient-panel p-4 md:p-5">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <label className="space-y-2 text-xs text-neutral-500">
            <span>Signal</span>
            <select value={signal} onChange={(e) => setSignal(e.target.value as LabSummary["signal"])} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">
              <option value="sentiment">Sentiment level</option>
              <option value="sentiment_change">Sentiment change</option>
              <option value="divergence">Sentiment-price divergence</option>
            </select>
          </label>
          <label className="space-y-2 text-xs text-neutral-500">
            <span>Forward horizon</span>
            <select value={horizon} onChange={(e) => setHorizon(Number(e.target.value) as LabSummary["horizon"])} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">
              {[1, 3, 5, 20].map((h) => <option key={h} value={h}>{h} trading day{h === 1 ? "" : "s"}</option>)}
            </select>
          </label>
          <label className="space-y-2 text-xs text-neutral-500">
            <span>Universe</span>
            <select value={sector} onChange={(e) => setSector(e.target.value)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">
              {sectors.map((s) => <option key={s}>{s}</option>)}
            </select>
          </label>
          <label className="space-y-2 text-xs text-neutral-500">
            <span>Daily cross-sectional quantile</span>
            <select value={quantile} onChange={(e) => setQuantile(Number(e.target.value) as LabSummary["quantile"])} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">
              <option value={0.2}>Top / bottom 20%</option>
              <option value={0.25}>Top / bottom 25%</option>
              <option value={0.33}>Top / bottom 33%</option>
            </select>
          </label>
        </div>
      </section>

      {current ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <div className="kpi xl:col-span-2">
              <div className="kpi-label">Mean daily long-short spread</div>
              <div className={`kpi-value ${tone(current.spread)}`}>{pct(current.spread, 3)}</div>
              <div className="kpi-sub">Daily high-signal quantile minus daily low-signal quantile</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">t-stat</div>
              <div className={`kpi-value ${tone(current.t_stat)}`}>{num(current.t_stat)}</div>
              <div className="kpi-sub">Across daily spread observations</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Hit rate</div>
              <div className="kpi-value">{pct(current.hit_rate)}</div>
              <div className="kpi-sub">Share of daily spreads &gt; 0</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Spread Sharpe</div>
              <div className={`kpi-value ${tone(current.sharpe)}`}>{num(current.sharpe)}</div>
              <div className="kpi-sub">Annualized using 252 / horizon</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Sample</div>
              <div className="kpi-value">{current.n_dates.toLocaleString()}d</div>
              <div className="kpi-sub">{current.n.toLocaleString()} stock-day obs · {current.start ?? "—"} → {current.end ?? "—"}</div>
            </div>
          </section>

          <section className="grid gap-4 lg:grid-cols-3">
            <div className="card p-5">
              <div className="eyebrow">High-signal portfolio</div>
              <div className={`mt-3 text-3xl font-semibold ${tone(current.top_mean)}`}>{pct(current.top_mean, 3)}</div>
              <p className="mt-2 text-sm leading-6 text-neutral-500">Mean daily {horizon}D forward return of the highest {Math.round(quantile * 100)}% cross-sectional {signalLabel.toLowerCase()} group.</p>
            </div>
            <div className="card p-5">
              <div className="eyebrow">Low-signal portfolio</div>
              <div className={`mt-3 text-3xl font-semibold ${tone(current.bottom_mean)}`}>{pct(current.bottom_mean, 3)}</div>
              <p className="mt-2 text-sm leading-6 text-neutral-500">Mean daily {horizon}D forward return of the lowest {Math.round(quantile * 100)}% cross-sectional group.</p>
            </div>
            <div className="card p-5">
              <div className="eyebrow">Specification</div>
              <dl className="mt-3 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Signal</dt><dd className="text-neutral-200">{signalLabel}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Universe</dt><dd className="text-neutral-200">{sector}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Horizon</dt><dd className="text-neutral-200">{horizon}D</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Daily quantiles</dt><dd className="text-neutral-200">{Math.round(quantile * 100)} / {Math.round(quantile * 100)}</dd></div>
              </dl>
            </div>
          </section>
        </>
      ) : (
        <div className="card p-5 text-sm text-neutral-500">No valid observations for this specification.</div>
      )}

      <section className="card p-5 text-sm leading-6 text-neutral-500">
        <div className="font-semibold text-neutral-300">Research interpretation</div>
        <p className="mt-2">For each trading date, the lab ranks the available cross-section by the selected signal, forms equal-weight high- and low-signal groups, computes their forward-return spread, and then summarizes that daily spread time series. It is descriptive rather than causal.</p>
        <p className="mt-2">Overlapping forward-return horizons can induce serial dependence, so the simple t-stat and Sharpe shown here are diagnostic statistics rather than publication-grade inference. A formal study should add robust or clustered inference, transaction costs, and out-of-sample validation.</p>
        <p className="mt-2">Divergence is observed sentiment minus a clipped 1D price-return signal scaled by 5%. Missing sentiment observations are excluded rather than imputed to zero.</p>
      </section>
    </div>
  );
}
