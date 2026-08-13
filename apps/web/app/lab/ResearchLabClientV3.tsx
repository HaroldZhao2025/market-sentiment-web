"use client";

import { useMemo, useState } from "react";
import type { LabV2Horizon, LabV2Quantile, LabV2Sample, LabV2Signal, LabV2Summary } from "../../lib/researchLabV2";

type Props = { rows: LabV2Summary[] };

function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}
function pct(v: number | null, d = 2, signed = true) {
  if (v == null) return "—";
  const sign = signed && v > 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(d)}%`;
}
function num(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
}
function turnover(v: number | null) {
  if (v == null) return "—";
  return `${v.toFixed(2)}×`;
}
function sampleLabel(sample: LabV2Sample) {
  return sample === "all" ? "Full sample" : sample === "in_sample" ? "First 70% (in-sample)" : "Last 30% (out-of-sample)";
}
function hashSpec(value: string) {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return `spec-${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

function MetricCard({ label, value, note, valueClass = "text-white" }: { label: string; value: string; note: string; valueClass?: string }) {
  return (
    <div className="min-w-0 rounded-2xl border border-white/10 bg-white/[0.025] p-4 md:p-5">
      <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-neutral-600">{label}</div>
      <div className={`mt-2 truncate text-[clamp(1.7rem,2.5vw,2.35rem)] font-semibold leading-none tracking-tight ${valueClass}`}>{value}</div>
      <div className="mt-2 min-h-10 text-xs leading-5 text-neutral-600">{note}</div>
    </div>
  );
}

export default function ResearchLabClientV3({ rows }: Props) {
  const sectors = useMemo(() => Array.from(new Set(rows.map((r) => r.sector))), [rows]);
  const [signal, setSignal] = useState<LabV2Signal>("sentiment");
  const [horizon, setHorizon] = useState<LabV2Horizon>(5);
  const [sector, setSector] = useState("All");
  const [quantile, setQuantile] = useState<LabV2Quantile>(0.25);
  const [sample, setSample] = useState<LabV2Sample>("all");
  const [costBps, setCostBps] = useState(0);

  const current = useMemo(
    () => rows.find((r) => r.signal === signal && r.horizon === horizon && r.sector === sector && r.quantile === quantile && r.sample === sample) ?? null,
    [rows, signal, horizon, sector, quantile, sample]
  );

  const signalLabel = signal === "sentiment" ? "Sentiment level" : signal === "sentiment_change" ? "Sentiment change" : "Sentiment-price divergence";
  const costRate = Math.max(0, costBps) / 10000;
  const estimatedCostDrag = current?.avg_turnover == null ? null : current.avg_turnover * costRate;
  const netSpread = current?.spread == null ? null : current.spread - (estimatedCostDrag ?? 0);
  const netHacT = netSpread != null && current?.hac_se != null && current.hac_se > 0 ? netSpread / current.hac_se : null;
  const specificationId = hashSpec(JSON.stringify({ signal, horizon, sector, quantile, sample, costBps }));

  function downloadSpecification() {
    if (!current) return;
    const payload = {
      schema_version: 2,
      specification_id: specificationId,
      generated_from: "Sentiment Intelligence Research Lab V3",
      specification: { signal, horizon, sector, quantile, sample, transaction_cost_bps: costBps },
      methodology: {
        cross_section: "Rank observed sentiment dates separately within each trading date; equal-weight high/low quantiles.",
        missing_sentiment: "Excluded. sentiment_observed=false is not treated as a neutral zero.",
        inference: `Newey-West/Bartlett HAC standard error for mean daily spread with lag=${current.hac_lag}.`,
        oos_split: "Chronological first 70% / last 30% of valid daily cross-sections.",
        cost_adjustment: "Observed average long+short one-way turnover multiplied by user-selected bps cost.",
      },
      result: { ...current, estimated_cost_drag: estimatedCostDrag, net_spread: netSpread, net_hac_t_stat: netHacT },
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `sentiment-intelligence-${specificationId}.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-6">
      <section className="ambient-panel p-4 md:p-5">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
          <label className="space-y-2 text-xs text-neutral-500"><span>Signal</span><select value={signal} onChange={(e) => setSignal(e.target.value as LabV2Signal)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none"><option value="sentiment">Sentiment level</option><option value="sentiment_change">Sentiment change</option><option value="divergence">Sentiment-price divergence</option></select></label>
          <label className="space-y-2 text-xs text-neutral-500"><span>Forward horizon</span><select value={horizon} onChange={(e) => setHorizon(Number(e.target.value) as LabV2Horizon)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">{[1, 3, 5, 20].map((h) => <option key={h} value={h}>{h} trading day{h === 1 ? "" : "s"}</option>)}</select></label>
          <label className="space-y-2 text-xs text-neutral-500"><span>Universe</span><select value={sector} onChange={(e) => setSector(e.target.value)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none">{sectors.map((s) => <option key={s}>{s}</option>)}</select></label>
          <label className="space-y-2 text-xs text-neutral-500"><span>Daily quantile</span><select value={quantile} onChange={(e) => setQuantile(Number(e.target.value) as LabV2Quantile)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none"><option value={0.2}>Top / bottom 20%</option><option value={0.25}>Top / bottom 25%</option><option value={0.33}>Top / bottom 33%</option></select></label>
          <label className="space-y-2 text-xs text-neutral-500"><span>Validation window</span><select value={sample} onChange={(e) => setSample(e.target.value as LabV2Sample)} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none"><option value="all">Full sample</option><option value="in_sample">First 70%</option><option value="out_of_sample">Last 30% OOS</option></select></label>
          <label className="space-y-2 text-xs text-neutral-500"><span>Transaction cost (bps)</span><input type="number" min={0} max={100} step={1} value={costBps} onChange={(e) => setCostBps(Math.max(0, Number(e.target.value) || 0))} className="w-full rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-200 outline-none" /></label>
        </div>
      </section>

      {current ? <>
        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
          <MetricCard label="Gross mean spread" value={pct(current.spread, 3)} note="High-signal minus low-signal forward return" valueClass={tone(current.spread)} />
          <MetricCard label="HAC t-stat" value={num(current.hac_t_stat)} note={`Newey-West / Bartlett · lag ${current.hac_lag}`} valueClass={tone(current.hac_t_stat)} />
          <MetricCard label="Simple t-stat" value={num(current.simple_t_stat)} note="Unadjusted comparison statistic" valueClass={tone(current.simple_t_stat)} />
          <MetricCard label="Hit rate" value={pct(current.hit_rate, 2, false)} note="Share of daily spreads above zero" />
          <MetricCard label="Avg turnover" value={turnover(current.avg_turnover)} note="Long + short one-way portfolio turnover" />
          <MetricCard label="Sample" value={`${current.n_dates.toLocaleString()} days`} note={`${current.n.toLocaleString()} stock-day observations`} />
        </section>

        <section className="grid gap-4 lg:grid-cols-4">
          <div className="card p-5"><div className="eyebrow">High-signal portfolio</div><div className={`mt-3 text-3xl font-semibold ${tone(current.top_mean)}`}>{pct(current.top_mean, 3)}</div><p className="mt-2 text-sm leading-6 text-neutral-500">Mean {horizon}D forward return of the highest {Math.round(quantile * 100)}% observed cross-section.</p></div>
          <div className="card p-5"><div className="eyebrow">Low-signal portfolio</div><div className={`mt-3 text-3xl font-semibold ${tone(current.bottom_mean)}`}>{pct(current.bottom_mean, 3)}</div><p className="mt-2 text-sm leading-6 text-neutral-500">Mean {horizon}D forward return of the lowest {Math.round(quantile * 100)}% observed cross-section.</p></div>
          <div className="card p-5"><div className="eyebrow">Cost-adjusted diagnostic</div><div className={`mt-3 text-3xl font-semibold ${tone(netSpread)}`}>{pct(netSpread, 3)}</div><div className={`mt-2 font-mono text-sm ${tone(netHacT)}`}>HAC t ≈ {num(netHacT)}</div><p className="mt-2 text-xs leading-5 text-neutral-600">Estimated drag {pct(estimatedCostDrag, 3, false)} from {costBps} bps × average turnover.</p></div>
          <div className="card p-5"><div className="eyebrow">Reproducible specification</div><div className="mt-2 font-mono text-xs text-emerald-300">{specificationId}</div><dl className="mt-3 space-y-2 text-sm"><div className="flex justify-between gap-3"><dt className="text-neutral-500">Signal</dt><dd className="text-neutral-200">{signalLabel}</dd></div><div className="flex justify-between gap-3"><dt className="text-neutral-500">Universe</dt><dd className="text-neutral-200">{sector}</dd></div><div className="flex justify-between gap-3"><dt className="text-neutral-500">Window</dt><dd className="text-neutral-200">{sampleLabel(sample)}</dd></div><div className="flex justify-between gap-3"><dt className="text-neutral-500">Range</dt><dd className="text-right text-neutral-200">{current.start ?? "—"}<br />{current.end ?? "—"}</dd></div></dl><button type="button" onClick={downloadSpecification} className="mt-4 w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-neutral-300 transition hover:bg-white/[0.08] hover:text-white">Download reproducible JSON</button></div>
        </section>
      </> : <div className="card p-5 text-sm text-neutral-500">No valid observations for this specification.</div>}

      <section className="card p-5 text-sm leading-6 text-neutral-500">
        <div className="font-semibold text-neutral-300">Research Lab V3 methodology</div>
        <p className="mt-2">Only dates explicitly marked <code>sentiment_observed=true</code> enter the cross-sectional signal sort. Carried-forward display sentiment is not treated as a fresh observation. Sentiment change uses the previous observed sentiment value.</p>
        <p className="mt-2">Each displayed configuration receives a deterministic specification ID so exported results can be traced back to the exact signal, horizon, universe, quantile, validation window, and transaction-cost assumption.</p>
        <p className="mt-2">HAC inference remains Newey-West/Bartlett with lag horizon − 1. Results remain descriptive research diagnostics rather than causal estimates or investment recommendations.</p>
      </section>
    </div>
  );
}
