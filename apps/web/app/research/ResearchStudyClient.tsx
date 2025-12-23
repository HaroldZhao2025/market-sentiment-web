// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import Sparkline from "../../components/Sparkline";
import type { ResearchStudy } from "../../lib/research";

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const series = study.results?.series;

  const Stat = ({ label, value }: { label: string; value?: string }) => (
    <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
      <div className="text-xs text-zinc-500">{label}</div>
      <div className="text-sm font-semibold">{value ?? "—"}</div>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* quick stats */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {study.key_stats?.map((s) => (
            <Stat key={s.label} label={s.label} value={s.value} />
          )) ?? null}
          <Stat label="Sample ticker" value={study.results?.sample_ticker} />
          <Stat label="Tickers (panel)" value={study.results?.n_tickers?.toString()} />
          <Stat label="Obs (panel)" value={study.results?.n_obs_panel?.toString()} />
        </div>
      </section>

      {/* methodology */}
      {study.methodology?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Methodology</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {study.methodology.map((m, i) => (
              <li key={i}>{m}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* charts */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <div className="flex items-baseline justify-between">
            <h2 className="text-lg font-semibold">Returns (sample)</h2>
            <div className="text-xs text-zinc-500">log return</div>
          </div>
          {series?.y_ret?.length ? (
            <div className="text-zinc-900">
              <Sparkline data={series.y_ret} className="text-zinc-900" />
            </div>
          ) : (
            <div className="text-sm text-zinc-500">No series available.</div>
          )}
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <div className="flex items-baseline justify-between">
            <h2 className="text-lg font-semibold">Sentiment (sample)</h2>
            <div className="text-xs text-zinc-500">score_mean</div>
          </div>
          {series?.score_mean?.length ? (
            <div className="text-zinc-900">
              <Sparkline data={series.score_mean} className="text-zinc-900" />
            </div>
          ) : (
            <div className="text-sm text-zinc-500">No series available.</div>
          )}
        </div>
      </section>

      {/* regression outputs */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Model outputs (JSON)</h2>
        <div className="text-xs text-zinc-500">
          This is intentionally compact; you can expand what you export from the Python builder anytime.
        </div>
        <pre className="text-xs overflow-auto rounded-xl bg-zinc-50 border border-zinc-100 p-4">
{JSON.stringify(
  {
    time_series: study.results?.time_series ?? null,
    panel_fe: study.results?.panel_fe ?? null,
    quantiles: study.results?.quantiles ?? null,
  },
  null,
  2
)}
        </pre>
      </section>
    </div>
  );
}
