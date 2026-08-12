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
            <span>Long / short quantile</span>
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
              <div className="kpi-label">Long-short spread</div>
              <div className={`kpi-value ${tone(current.spread)}`}>{pct(current.spread, 3)}</div>
              <div className="kpi-sub">Top signal minus bottom signal forward return</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">t-stat</div>
              <div className={`kpi-value ${tone(current.t_stat)}`}>{num(current.t_stat)}</div>
              <div className="kpi-sub">Descriptive, not causal</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Hit rate</div>
              <div className="kpi-value">{pct(current.hit_rate)}</div>
              <div className="kpi-sub">Paired spread &gt; 0</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Spread Sharpe</div>
              <div className={`kpi-value ${tone(current.sharpe)}`}>{num(current.sharpe)}</div>
              <div className="kpi-sub">Approximate annualization</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Observations</div>
              <div className="kpi-value">{current.n.toLocaleString()}</div>
              <div className="kpi-sub">{current.start ?? "—"} → {current.end ?? "—"}</div>
            </div>
          </section>

          <section className="grid gap-4 lg:grid-cols-3">
            <div className="card p-5">
              <div className="eyebrow">High-signal portfolio</div>
              <div className={`mt-3 text-3xl font-semibold ${tone(current.top_mean)}`}>{pct(current.top_mean, 3)}</div>
              <p className="mt-2 text-sm leading-6 text-neutral-500">Mean {horizon}D forward return for the highest {Math.round(quantile * 100)}% of {signalLabel.toLowerCase()} observations.</p>
            </div>
            <div className="card p-5">
              <div className="eyebrow">Low-signal portfolio</div>
              <div className={`mt-3 text-3xl font-semibold ${tone(current.bottom_mean)}`}>{pct(current.bottom_mean, 3)}</div>
              <p className="mt-2 text-sm leading-6 text-neutral-500">Mean {horizon}D forward return for the lowest {Math.round(quantile * 100)}% of observations.</p>
            </div>
            <div className="card p-5">
              <div className="eyebrow">Specification</div>
              <dl className="mt-3 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Signal</dt><dd className="text-neutral-200">{signalLabel}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Universe</dt><dd className="text-neutral-200">{sector}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Horizon</dt><dd className="text-neutral-200">{horizon}D</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-neutral-500">Quantiles</dt><dd className="text-neutral-200">{Math.round(quantile * 100)} / {Math.round(quantile * 100)}</dd></div>
              </dl>
            </div>
          </section>
        </>
      ) : (
        <div className="card p-5 text-sm text-neutral-500">No valid observations for this specification.</div>
      )}

      <section className="card p-5 text-sm leading-6 text-neutral-500">
        <div className="font-semibold text-neutral-300">Research interpretation</div>
        <p className="mt-2">This lab performs deterministic descriptive sorting on the existing ticker panel. It does not infer causality and it does not replace a formally specified backtest with transaction costs, clustered standard errors, or out-of-sample validation.</p>
        <p className="mt-2">Divergence is defined as observed sentiment minus a clipped 1D price-return signal scaled by 5%. Missing sentiment observations are excluded rather than imputed to zero.</p>
      </section>
    </div>
  );
}
