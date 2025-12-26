// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import Sparkline from "../../components/Sparkline";
import type { ResearchStudy } from "../../lib/research";

type ModelOut = {
  error?: string;
  params?: Record<string, number>;
  tvalues?: Record<string, number>;
  pvalues?: Record<string, number>;
  bse?: Record<string, number>;
  nobs?: number;
  rsquared?: number;
  rsquared_adj?: number;
  cov_type?: string;
};

type ExportedTable = {
  title?: string;
  columns?: string[];
  rows?: any[][];
  note?: string;
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
  const ax = Math.abs(x);
  if (ax !== 0 && (ax < 1e-3 || ax > 1e4)) return x.toExponential(3);
  return x.toFixed(d).replace(/0+$/, "").replace(/\.$/, "");
}

function asNumArray(x: any): number[] | null {
  if (!Array.isArray(x)) return null;
  const out: number[] = [];
  for (const v of x) {
    const n = typeof v === "number" ? v : Number(v);
    out.push(Number.isFinite(n) ? n : 0);
  }
  return out.length ? out : null;
}

function asIntArray(x: any): number[] | null {
  if (!Array.isArray(x)) return null;
  const out: number[] = [];
  for (const v of x) {
    const n = typeof v === "number" ? v : Number(v);
    out.push(Number.isFinite(n) ? Math.trunc(n) : 0);
  }
  return out.length ? out : null;
}

function pickModelMeta(model?: ModelOut | null) {
  if (!model || model.error) return [];
  const meta = [
    model.cov_type ? `SE: ${model.cov_type}` : null,
    model.nobs != null ? `N: ${model.nobs}` : null,
    model.rsquared != null ? `R²: ${fmt(model.rsquared, 4)}` : null,
  ].filter(Boolean);
  return meta as string[];
}

function RegressionTable({ title, model }: { title: string; model?: ModelOut | null }) {
  if (!model) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">No model output available.</div>
      </div>
    );
  }

  if (model.error) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">Model error: {model.error}</div>
      </div>
    );
  }

  const params = model.params ?? {};
  const keys = Object.keys(params);

  if (!keys.length) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">No model output available.</div>
      </div>
    );
  }

  const rows = keys.slice().sort((a, b) => a.localeCompare(b));
  const meta = pickModelMeta(model);

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

      <div className="text-xs text-zinc-500">Stars: *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.10.</div>
    </div>
  );
}

function DataTable({ t }: { t: ExportedTable }) {
  if (!t?.rows?.length) return null;

  const cols = t.columns ?? [];
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="space-y-1">
        <div className="text-lg font-semibold">{t.title ?? "Table"}</div>
        {t.note ? <div className="text-sm text-zinc-600">{t.note}</div> : null}
      </div>

      <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
        <table className="w-full text-sm">
          {cols.length ? (
            <thead className="text-xs text-zinc-500">
              <tr className="border-b border-zinc-200">
                {cols.map((c, i) => (
                  <th key={i} className="text-left font-medium p-3">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
          ) : null}
          <tbody>
            {t.rows.map((row, r) => (
              <tr key={r} className="border-b border-zinc-200 last:border-b-0">
                {row.map((cell, c) => {
                  const n = typeof cell === "number" ? cell : Number(cell);
                  const v =
                    typeof cell === "number" || Number.isFinite(n) ? fmt(n, 6) : String(cell ?? "—");
                  return (
                    <td key={c} className="p-3 text-zinc-800">
                      {v}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="text-xs text-zinc-500">Note: exported by the Python builder; formatting is intentionally compact.</div>
    </div>
  );
}

function MiniLineFigure({
  title,
  subtitle,
  x,
  a,
  b,
  aLabel,
  bLabel,
}: {
  title: string;
  subtitle?: string;
  x: number[];
  a: number[];
  b: number[];
  aLabel: string;
  bLabel: string;
}) {
  // Simple SVG chart (no external deps)
  const pad = 10;

  const xmin = Math.min(...x);
  const xmax = Math.max(...x);

  const ys = [...a, ...b];
  const ymin = Math.min(...ys);
  const ymax = Math.max(...ys);

  const W = 600;
  const H = 160;

  const sx = (v: number) => {
    if (xmax === xmin) return pad;
    return pad + ((v - xmin) / (xmax - xmin)) * (W - 2 * pad);
  };
  const sy = (v: number) => {
    if (ymax === ymin) return H / 2;
    // invert
    return pad + (1 - (v - ymin) / (ymax - ymin)) * (H - 2 * pad);
  };

  const path = (arr: number[]) =>
    arr
      .map((yv, i) => `${i === 0 ? "M" : "L"} ${sx(x[i])} ${sy(yv)}`)
      .join(" ");

  const x0 = x.includes(0) ? sx(0) : null;
  const y0 = ymin <= 0 && ymax >= 0 ? sy(0) : null;

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
      <div className="flex items-baseline justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">{title}</div>
          {subtitle ? <div className="text-xs text-zinc-500 mt-1">{subtitle}</div> : null}
        </div>
        <div className="text-xs text-zinc-500 text-right">
          <div>
            <span className="inline-block w-2 h-2 rounded-full bg-zinc-900 mr-2" />
            {aLabel}
          </div>
          <div>
            <span className="inline-block w-2 h-2 rounded-full bg-zinc-400 mr-2" />
            {bLabel}
          </div>
        </div>
      </div>

      <div className="rounded-xl border border-zinc-100 bg-zinc-50 p-3 overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-[170px]">
          {/* zero lines */}
          {y0 != null ? <line x1={pad} x2={W - pad} y1={y0} y2={y0} stroke="rgb(228 228 231)" strokeWidth="1" /> : null}
          {x0 != null ? <line x1={x0} x2={x0} y1={pad} y2={H - pad} stroke="rgb(228 228 231)" strokeWidth="1" /> : null}

          <path d={path(b)} fill="none" stroke="rgb(161 161 170)" strokeWidth="2" />
          <path d={path(a)} fill="none" stroke="rgb(24 24 27)" strokeWidth="2.5" />
        </svg>

        <div className="mt-2 flex items-center justify-between text-[11px] text-zinc-500">
          <div>τ: {xmin} … {xmax}</div>
          <div>y: {fmt(ymin, 4)} … {fmt(ymax, 4)}</div>
        </div>
      </div>
    </div>
  );
}

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const series = (study.results as any)?.series;
  const tablesRaw = (study.results as any)?.tables as ExportedTable[] | undefined;
  const singleTable = (study.results as any)?.table as ExportedTable | undefined;
  const tables = [
    ...(Array.isArray(tablesRaw) ? tablesRaw : []),
    ...(singleTable?.rows?.length ? [singleTable] : []),
  ];

  const ts = (study.results as any)?.time_series as ModelOut | null | undefined;
  const fe = (study.results as any)?.panel_fe as ModelOut | null | undefined;

  // EVENT STUDY DETECTION
  const tau = asIntArray(series?.tau);
  const carPos = asNumArray(series?.car_pos);
  const carNeg = asNumArray(series?.car_neg);
  const isEventStudy = !!(tau && carPos && carNeg && tau.length === carPos.length && tau.length === carNeg.length);

  // SAMPLE SERIES (for non-event studies)
  const yRet = asNumArray(series?.y_ret);
  const yRetFwd1 = asNumArray(series?.y_ret_fwd1);
  const absRet = asNumArray(series?.abs_ret);
  const scoreMean = asNumArray(series?.score_mean);
  const nTotal = asNumArray(series?.n_total);

  const mainSeries =
    (yRetFwd1 && { title: "Next-day returns (sample)", subtitle: "log return (t+1)", data: yRetFwd1 }) ||
    (yRet && { title: "Returns (sample)", subtitle: "log return", data: yRet }) ||
    (absRet && { title: "Volatility proxy (sample)", subtitle: "|log return|", data: absRet }) ||
    null;

  const Stat = ({ label, value }: { label: string; value?: string }) => (
    <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
      <div className="text-[11px] text-zinc-500">{label}</div>
      <div className="text-sm font-semibold text-zinc-900">{value ?? "—"}</div>
    </div>
  );

  const r2ts = ts?.rsquared;
  const r2fe = fe?.rsquared;

  return (
    <div className="space-y-6">
      {/* Quick stats */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-lg font-semibold">Study</div>
            <div className="text-sm text-zinc-600 mt-1">
              {study.category ? <span className="font-semibold text-zinc-700">{study.category}</span> : null}
              {study.category ? <span className="text-zinc-400"> • </span> : null}
              <span className="text-zinc-600">Status: {(study.status ?? "draft").toUpperCase()}</span>
            </div>
          </div>

          <div className="text-right text-xs text-zinc-500">
            {study.updated_at ? <div>Updated: {study.updated_at}</div> : null}
            {(study.results as any)?.date_range ? (
              <div>Date range: {(study.results as any).date_range}</div>
            ) : null}
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {(study.key_stats ?? []).map((s) => (
            <Stat key={s.label} label={s.label} value={s.value} />
          ))}

          <Stat label="Sample ticker" value={(study.results as any)?.sample_ticker ?? "—"} />
          <Stat label="Tickers (panel)" value={String((study.results as any)?.n_tickers ?? "—")} />
          <Stat label="Obs (panel)" value={String((study.results as any)?.n_obs_panel ?? "—")} />

          <Stat label="R² (TS)" value={r2ts != null ? fmt(r2ts, 4) : "—"} />
          <Stat label="R² (FE)" value={r2fe != null ? fmt(r2fe, 4) : "—"} />

          {/* event study extra stats if present */}
          {isEventStudy ? (
            <>
              <Stat label="z threshold" value={String((study.results as any)?.stats?.z_thr ?? "—")} />
              <Stat label="window" value={String((study.results as any)?.stats?.window ?? "—")} />
              <Stat label="N pos" value={String((study.results as any)?.stats?.n_pos ?? "—")} />
              <Stat label="N neg" value={String((study.results as any)?.stats?.n_neg ?? "—")} />
              <Stat label="warning" value={String((study.results as any)?.stats?.warning ?? "—")} />
            </>
          ) : null}
        </div>
      </section>

      {/* Key findings */}
      {study.conclusions?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Key findings</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {study.conclusions.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* Study design / sections (paper-like) */}
      {study.sections?.length ? (
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {study.sections.map((sec, i) => (
            <div key={i} className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="text-lg font-semibold">{sec.title}</div>
              {Array.isArray((sec as any).bullets) && (sec as any).bullets.length ? (
                <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                  {(sec as any).bullets.map((b: string, j: number) => (
                    <li key={j}>{b}</li>
                  ))}
                </ul>
              ) : null}
            </div>
          ))}
        </section>
      ) : null}

      {/* Methodology */}
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

      {/* Figures */}
      <section className="space-y-4">
        <div className="flex items-baseline justify-between">
          <h2 className="text-lg font-semibold">Figures</h2>
          <div className="text-xs text-zinc-500">
            {isEventStudy ? "Panel-average CAR paths" : "Charts are from the sample ticker (not the full panel)."}
          </div>
        </div>

        {isEventStudy ? (
          <MiniLineFigure
            title="Event study CAR path"
            subtitle="Cumulative log-return relative to event date (τ=0 set to 0)"
            x={tau!}
            a={carPos!}
            b={carNeg!}
            aLabel="Extreme positive"
            bLabel="Extreme negative"
          />
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="flex items-baseline justify-between">
                <h3 className="text-lg font-semibold">{mainSeries?.title ?? "Series (sample)"}</h3>
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
                <h3 className="text-lg font-semibold">Sentiment (sample)</h3>
                <div className="text-xs text-zinc-500">score_mean</div>
              </div>
              {scoreMean?.length ? (
                <div className="text-zinc-900">
                  <Sparkline data={scoreMean} className="text-zinc-900" />
                </div>
              ) : (
                <div className="text-sm text-zinc-500">
                  No sentiment series available. Export <code className="px-1 bg-zinc-100 rounded">results.series.score_mean</code>.
                </div>
              )}
            </div>

            {nTotal?.length ? (
              <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2 lg:col-span-2">
                <div className="flex items-baseline justify-between">
                  <h3 className="text-lg font-semibold">News volume (sample)</h3>
                  <div className="text-xs text-zinc-500">n_total</div>
                </div>
                <div className="text-zinc-900">
                  <Sparkline data={nTotal} className="text-zinc-900" />
                </div>
              </div>
            ) : null}
          </div>
        )}
      </section>

      {/* Tables (event study, FM tables, placebo summaries, etc.) */}
      {tables.length ? (
        <section className="space-y-3">
          <h2 className="text-lg font-semibold">Tables</h2>
          <div className="space-y-4">
            {tables.map((t, i) => (
              <DataTable key={i} t={t} />
            ))}
          </div>
        </section>
      ) : null}

      {/* Regression outputs (only show if present; event study shouldn’t look “broken”) */}
      {(ts && (ts.params || ts.error)) || (fe && (fe.params || fe.error)) ? (
        <section className="space-y-3">
          <h2 className="text-lg font-semibold">Models</h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <RegressionTable title="Time-series (HAC)" model={ts as any} />
            <RegressionTable title="Panel FE (clustered)" model={fe as any} />
          </div>
        </section>
      ) : (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5">
          <div className="text-lg font-semibold">Models</div>
          <div className="text-sm text-zinc-600 mt-2">
            This study is primarily descriptive (figures/tables). No regression model outputs were exported.
          </div>
        </section>
      )}

      {/* Appendix (collapsed) */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Appendix</h2>
        <div className="text-xs text-zinc-500">Raw exported objects (reproducibility / debugging).</div>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">
            Raw exported JSON (collapsed)
          </summary>
          <pre className="text-xs overflow-auto mt-3">
{JSON.stringify(
  {
    results: study.results ?? null,
    methodology: study.methodology ?? null,
    sections: study.sections ?? null,
    conclusions: study.conclusions ?? null,
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
