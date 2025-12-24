// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import Sparkline from "../../components/Sparkline";
import type { ResearchStudy } from "../../lib/research";

type ModelOut = {
  params?: Record<string, number>;
  tvalues?: Record<string, number>;
  pvalues?: Record<string, number>;
  bse?: Record<string, number>;
  nobs?: number;
  rsquared?: number;
  rsquared_adj?: number;
  cov_type?: string;
};

function star(p?: number) {
  if (p == null || !Number.isFinite(p)) return "";
  if (p < 0.01) return "***";
  if (p < 0.05) return "**";
  if (p < 0.1) return "*";
  return "";
}

function fmt(x?: number, d = 4) {
  if (x == null || !Number.isFinite(x)) return "—";
  // compact formatting
  const ax = Math.abs(x);
  if (ax !== 0 && (ax < 1e-3 || ax > 1e4)) return x.toExponential(3);
  return x.toFixed(d).replace(/0+$/, "").replace(/\.$/, "");
}

function pickSeries(series: any) {
  if (!series) return null;

  if (Array.isArray(series.y_ret) && series.y_ret.length) {
    return { key: "y_ret", title: "Returns (sample)", subtitle: "log return", data: series.y_ret as number[] };
  }
  if (Array.isArray(series.y_ret_fwd1) && series.y_ret_fwd1.length) {
    return {
      key: "y_ret_fwd1",
      title: "Next-day returns (sample)",
      subtitle: "log return (t+1)",
      data: series.y_ret_fwd1 as number[],
    };
  }
  if (Array.isArray(series.abs_ret) && series.abs_ret.length) {
    return {
      key: "abs_ret",
      title: "Volatility proxy (sample)",
      subtitle: "|log return|",
      data: series.abs_ret as number[],
    };
  }
  return null;
}

function RegressionTable({
  title,
  model,
}: {
  title: string;
  model?: ModelOut | null;
}) {
  const params = model?.params ?? {};
  const keys = Object.keys(params);

  if (!model || !keys.length) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">No model output available.</div>
      </div>
    );
  }

  const rows = keys
    .filter((k) => k !== "const")
    .sort((a, b) => a.localeCompare(b));

  const meta = [
    model.cov_type ? `SE: ${model.cov_type}` : null,
    model.nobs != null ? `N: ${model.nobs}` : null,
    model.rsquared != null ? `R²: ${fmt(model.rsquared, 4)}` : null,
  ].filter(Boolean);

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-xs text-zinc-500">{meta.join(" • ")}</div>
      </div>

      <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
        <table className="w-full text-sm">
          <thead className="text-xs text-zinc-500">
            <tr className="border-b border-zinc-200">
              <th className="text-left font-medium p-3">Variable</th>
              <th className="text-right font-medium p-3">Coef</th>
              <th className="text-right font-medium p-3">t</th>
              <th className="text-right font-medium p-3">p</th>
              <th className="text-right font-medium p-3">bps / 1.0</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((k) => {
              const b = model.params?.[k];
              const t = model.tvalues?.[k];
              const p = model.pvalues?.[k];
              const bps = b != null ? b * 10000 : undefined;
              return (
                <tr key={k} className="border-b border-zinc-200 last:border-b-0">
                  <td className="p-3 font-medium text-zinc-800">{k}</td>
                  <td className="p-3 text-right">
                    {fmt(b, 6)}
                    <span className="text-zinc-500">{star(p)}</span>
                  </td>
                  <td className="p-3 text-right">{fmt(t, 3)}</td>
                  <td className="p-3 text-right">{fmt(p, 4)}</td>
                  <td className="p-3 text-right">{fmt(bps, 2)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="text-xs text-zinc-500">
        Stars: *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.10.
      </div>
    </div>
  );
}

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const series = study.results?.series;
  const mainSeries = pickSeries(series);

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

      {/* conclusions (academic-facing) */}
      {study.conclusions?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Conclusions</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {study.conclusions.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* charts */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <div className="flex items-baseline justify-between">
            <h2 className="text-lg font-semibold">{mainSeries?.title ?? "Series (sample)"}</h2>
            <div className="text-xs text-zinc-500">{mainSeries?.subtitle ?? ""}</div>
          </div>
          {mainSeries?.data?.length ? (
            <div className="text-zinc-900">
              <Sparkline data={mainSeries.data} className="text-zinc-900" />
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
          {Array.isArray(series?.score_mean) && series?.score_mean?.length ? (
            <div className="text-zinc-900">
              <Sparkline data={series.score_mean} className="text-zinc-900" />
            </div>
          ) : (
            <div className="text-sm text-zinc-500">No series available.</div>
          )}
        </div>

        {Array.isArray(series?.n_total) && series?.n_total?.length ? (
          <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2 lg:col-span-2">
            <div className="flex items-baseline justify-between">
              <h2 className="text-lg font-semibold">News volume (sample)</h2>
              <div className="text-xs text-zinc-500">n_total</div>
            </div>
            <div className="text-zinc-900">
              <Sparkline data={series.n_total} className="text-zinc-900" />
            </div>
          </div>
        ) : null}
      </section>

      {/* regression tables */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <RegressionTable title="Time-series (HAC)" model={study.results?.time_series as any} />
        <RegressionTable title="Panel FE (clustered)" model={study.results?.panel_fe as any} />
      </section>

      {/* appendix: raw JSON */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Appendix</h2>
        <div className="text-xs text-zinc-500">
          Raw exported objects (kept for reproducibility / debugging).
        </div>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">
            Model outputs (JSON)
          </summary>
          <pre className="text-xs overflow-auto mt-3">
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
        </details>
      </section>
    </div>
  );
}
