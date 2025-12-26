// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import { useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
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
  error?: string;
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

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function getStudyModels(study: ResearchStudy) {
  const r: any = (study as any).results ?? {};
  const ts: ModelOut | null = (r.time_series ?? r.models?.time_series ?? null) as any;
  const fe: ModelOut | null = (r.panel_fe ?? r.models?.panel_fe ?? null) as any;
  return { ts, fe, resultsAny: r };
}

function getTables(study: ResearchStudy): ExportedTable[] {
  const r: any = (study as any).results ?? {};
  const tables = r.tables ?? r.table ? [r.table] : [];
  return (Array.isArray(tables) ? tables : []).filter(Boolean);
}

function isEventStudySeries(series: any) {
  return (
    series &&
    Array.isArray(series.tau) &&
    (Array.isArray(series.car_pos) ||
      Array.isArray(series.car_neg) ||
      Array.isArray(series.aar_pos) ||
      Array.isArray(series.aar_neg))
  );
}

function pickMainSeries(series: any) {
  if (!series) return null;
  // Avoid misclassifying event-study data as a time series chart
  if (isEventStudySeries(series)) return null;

  if (Array.isArray(series.y_ret) && series.y_ret.length) {
    return { title: "Returns (sample)", subtitle: "log return", data: series.y_ret as number[] };
  }
  if (Array.isArray(series.y_ret_fwd1) && series.y_ret_fwd1.length) {
    return { title: "Next-day returns (sample)", subtitle: "log return (t+1)", data: series.y_ret_fwd1 as number[] };
  }
  if (Array.isArray(series.abs_ret) && series.abs_ret.length) {
    return { title: "Volatility proxy (sample)", subtitle: "|log return|", data: series.abs_ret as number[] };
  }
  return null;
}

function RegressionTable({ title, model }: { title: string; model?: ModelOut | null }) {
  const params = model?.params ?? {};
  const keys = Object.keys(params);

  if (!model || model.error || !keys.length) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">
          {model?.error ? `No model output (error: ${model.error}).` : "No model output available."}
        </div>
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

      <div className="text-xs text-zinc-500">Stars: *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.10.</div>
    </div>
  );
}

function StudySections({ sections }: { sections?: any[] }) {
  if (!sections?.length) return null;

  return (
    <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <h2 className="text-lg font-semibold">Study sections</h2>
      <div className="space-y-3">
        {sections.map((sec, i) => (
          <div key={i} className="rounded-xl border border-zinc-100 bg-zinc-50 p-4">
            <div className="text-sm font-semibold text-zinc-900">{sec.title ?? `Section ${i + 1}`}</div>
            {Array.isArray(sec.bullets) && sec.bullets.length ? (
              <ul className="mt-2 list-disc pl-5 text-sm text-zinc-700 space-y-1">
                {sec.bullets.map((b: string, j: number) => (
                  <li key={j}>{b}</li>
                ))}
              </ul>
            ) : null}
          </div>
        ))}
      </div>
    </section>
  );
}

function DataTable({ t }: { t: ExportedTable }) {
  const cols = Array.isArray(t.columns) ? t.columns : [];
  const rows = Array.isArray(t.rows) ? t.rows : [];

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="text-lg font-semibold">{t.title ?? "Table"}</div>

      {cols.length ? (
        <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
          <table className="w-full text-sm">
            <thead className="text-xs text-zinc-500">
              <tr className="border-b border-zinc-200">
                {cols.map((c) => (
                  <th key={c} className="text-left font-medium p-3 whitespace-nowrap">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} className="border-b border-zinc-200 last:border-b-0">
                  {r.map((cell, j) => (
                    <td key={j} className="p-3 whitespace-nowrap">
                      {typeof cell === "number" ? fmt(cell, 6) : String(cell ?? "—")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="text-sm text-zinc-500">No rows.</div>
      )}

      {t.note ? <div className="text-xs text-zinc-500">{t.note}</div> : null}
    </div>
  );
}

function deriveAARFromCAR(tau: number[], car: number[]) {
  // Consistent with your CAR definition in Python:
  //  tau>0: CAR(tau)=sum_{1..tau} r_{t+k}
  //  tau<0: CAR(tau)=-sum_{tau+1..0} r_{t+k}
  // We derive per-step increments for visualization.
  const n = Math.min(tau.length, car.length);
  const out = new Array(n).fill(0);
  for (let i = 0; i < n; i++) {
    const tt = tau[i];
    if (!Number.isFinite(tt) || !Number.isFinite(car[i])) {
      out[i] = 0;
      continue;
    }
    if (tt === 0) {
      out[i] = 0;
      continue;
    }
    if (tt > 0 && i - 1 >= 0) out[i] = car[i] - car[i - 1];
    else if (tt < 0 && i + 1 < n) out[i] = car[i] - car[i + 1];
    else out[i] = 0;
  }
  return out;
}

function EventStudyCard({ series, title }: { series: any; title?: string }) {
  const tau: number[] = (series?.tau ?? []).map((x: any) => Number(x)).filter((x: any) => Number.isFinite(x));
  const carPosRaw: number[] = (series?.car_pos ?? []).map((x: any) => Number(x));
  const carNegRaw: number[] = (series?.car_neg ?? []).map((x: any) => Number(x));

  const aarPosRaw: number[] =
    Array.isArray(series?.aar_pos) && series.aar_pos.length
      ? series.aar_pos.map((x: any) => Number(x))
      : deriveAARFromCAR(tau, carPosRaw);

  const aarNegRaw: number[] =
    Array.isArray(series?.aar_neg) && series.aar_neg.length
      ? series.aar_neg.map((x: any) => Number(x))
      : deriveAARFromCAR(tau, carNegRaw);

  const [metric, setMetric] = useState<"CAR" | "AAR">("CAR");
  const [unit, setUnit] = useState<"raw" | "bps">("bps");
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const svgRef = useRef<SVGSVGElement | null>(null);

  const { xVals, posVals, negVals } = useMemo(() => {
    const scale = unit === "bps" ? 10000 : 1;
    const pos = (metric === "CAR" ? carPosRaw : aarPosRaw).slice(0, tau.length).map((v) => (Number.isFinite(v) ? v * scale : 0));
    const neg = (metric === "CAR" ? carNegRaw : aarNegRaw).slice(0, tau.length).map((v) => (Number.isFinite(v) ? v * scale : 0));
    return { xVals: tau, posVals: pos, negVals: neg };
  }, [tau, carPosRaw, carNegRaw, aarPosRaw, aarNegRaw, metric, unit]);

  const chart = useMemo(() => {
    const n = Math.min(xVals.length, posVals.length, negVals.length);
    if (n < 3) return null;

    const xs = xVals.slice(0, n);
    const p = posVals.slice(0, n);
    const q = negVals.slice(0, n);

    const xMin = Math.min(...xs);
    const xMax = Math.max(...xs);

    let yMin = Math.min(...p, ...q);
    let yMax = Math.max(...p, ...q);
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    const pad = (yMax - yMin) * 0.12;
    yMin -= pad;
    yMax += pad;

    const W = 820;
    const H = 260;
    const L = 46;
    const R = 18;
    const T = 18;
    const B = 34;

    const xToPx = (x: number) => L + ((x - xMin) / (xMax - xMin)) * (W - L - R);
    const yToPx = (y: number) => T + (1 - (y - yMin) / (yMax - yMin)) * (H - T - B);

    const path = (ys: number[]) => {
      let d = "";
      for (let i = 0; i < n; i++) {
        const xx = xToPx(xs[i]);
        const yy = yToPx(ys[i]);
        d += i === 0 ? `M ${xx} ${yy}` : ` L ${xx} ${yy}`;
      }
      return d;
    };

    const x0 = xToPx(0);
    const y0 = yToPx(0);

    // simple tick set
    const ticks = Array.from(new Set([xMin, Math.floor(xMin / 2) * 2, 0, Math.ceil(xMax / 2) * 2, xMax]))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b);

    return {
      W,
      H,
      L,
      R,
      T,
      B,
      xMin,
      xMax,
      yMin,
      yMax,
      xToPx,
      yToPx,
      pathPos: path(p),
      pathNeg: path(q),
      x0,
      y0,
      ticks,
    };
  }, [xVals, posVals, negVals]);

  function onMove(e: ReactMouseEvent<SVGSVGElement>) {
    if (!chart || !svgRef.current) return;
    const rect = svgRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const n = Math.min(xVals.length, posVals.length, negVals.length);
    const xs = xVals.slice(0, n);
    // nearest x by pixel distance
    let best = 0;
    let bestD = Infinity;
    for (let i = 0; i < n; i++) {
      const px = chart.xToPx(xs[i]);
      const d = Math.abs(px - x);
      if (d < bestD) {
        bestD = d;
        best = i;
      }
    }
    setHoverIdx(best);
  }

  if (!chart) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title ?? "Event study"}</div>
        <div className="text-sm text-zinc-500 mt-2">No event-study series available.</div>
      </div>
    );
  }

  const n = Math.min(xVals.length, posVals.length, negVals.length);
  const hi = hoverIdx == null ? null : clamp(hoverIdx, 0, n - 1);
  const hoverX = hi == null ? null : xVals[hi];
  const hoverP = hi == null ? null : posVals[hi];
  const hoverN = hi == null ? null : negVals[hi];

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-lg font-semibold">{title ?? "Event study"}</div>
          <div className="text-sm text-zinc-600 mt-1">
            Average paths for extreme positive vs negative sentiment days.
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            className={`text-xs px-3 py-1.5 rounded-full border ${
              metric === "CAR" ? "bg-zinc-900 text-white border-zinc-900" : "bg-white border-zinc-200 text-zinc-700"
            }`}
            onClick={() => setMetric("CAR")}
          >
            CAR
          </button>
          <button
            className={`text-xs px-3 py-1.5 rounded-full border ${
              metric === "AAR" ? "bg-zinc-900 text-white border-zinc-900" : "bg-white border-zinc-200 text-zinc-700"
            }`}
            onClick={() => setMetric("AAR")}
          >
            AAR
          </button>

          <div className="w-px h-6 bg-zinc-200 mx-1" />

          <button
            className={`text-xs px-3 py-1.5 rounded-full border ${
              unit === "bps" ? "bg-white border-zinc-900 text-zinc-900" : "bg-white border-zinc-200 text-zinc-700"
            }`}
            onClick={() => setUnit(unit === "bps" ? "raw" : "bps")}
            title="Toggle units"
          >
            {unit === "bps" ? "bps" : "raw"}
          </button>
        </div>
      </div>

      <div className="text-xs text-zinc-500">
        Hover for values. τ is measured in trading days relative to the event (τ=0 normalized to 0).
      </div>

      <div className="rounded-xl border border-zinc-100 bg-zinc-50 p-3">
        <svg
          ref={svgRef}
          viewBox={`0 0 ${chart.W} ${chart.H}`}
          className="w-full h-[260px]"
          onMouseMove={onMove}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {/* axes */}
          <line x1={chart.L} y1={chart.y0} x2={chart.W - chart.R} y2={chart.y0} stroke="currentColor" strokeOpacity={0.18} />
          <line x1={chart.x0} y1={chart.T} x2={chart.x0} y2={chart.H - chart.B} stroke="currentColor" strokeOpacity={0.18} />

          {/* x ticks */}
          {chart.ticks.map((t) => {
            const x = chart.xToPx(t);
            return (
              <g key={t}>
                <line x1={x} y1={chart.H - chart.B} x2={x} y2={chart.H - chart.B + 6} stroke="currentColor" strokeOpacity={0.18} />
                <text x={x} y={chart.H - 10} textAnchor="middle" fontSize="10" fill="currentColor" opacity="0.55">
                  {t}
                </text>
              </g>
            );
          })}

          {/* paths */}
          <path d={chart.pathPos} fill="none" stroke="currentColor" strokeWidth="2.2" strokeOpacity="0.9" />
          <path d={chart.pathNeg} fill="none" stroke="currentColor" strokeWidth="2.2" strokeOpacity="0.45" />

          {/* legend */}
          <g>
            <circle cx={chart.L + 6} cy={chart.T + 8} r="4" fill="currentColor" opacity="0.9" />
            <text x={chart.L + 16} y={chart.T + 12} fontSize="10" fill="currentColor" opacity="0.75">
              Positive events
            </text>

            <circle cx={chart.L + 110} cy={chart.T + 8} r="4" fill="currentColor" opacity="0.45" />
            <text x={chart.L + 120} y={chart.T + 12} fontSize="10" fill="currentColor" opacity="0.75">
              Negative events
            </text>
          </g>

          {/* hover */}
          {hi != null ? (
            <g>
              <line
                x1={chart.xToPx(hoverX as number)}
                y1={chart.T}
                x2={chart.xToPx(hoverX as number)}
                y2={chart.H - chart.B}
                stroke="currentColor"
                strokeOpacity={0.18}
              />
              <circle cx={chart.xToPx(hoverX as number)} cy={chart.yToPx(hoverP as number)} r="4" fill="currentColor" opacity="0.95" />
              <circle cx={chart.xToPx(hoverX as number)} cy={chart.yToPx(hoverN as number)} r="4" fill="currentColor" opacity="0.55" />
            </g>
          ) : null}
        </svg>

        {hi != null ? (
          <div className="mt-2 text-xs text-zinc-700 flex flex-wrap gap-x-4 gap-y-1">
            <div>
              <span className="text-zinc-500">τ:</span> {hoverX}
            </div>
            <div>
              <span className="text-zinc-500">pos:</span> {fmt(hoverP ?? undefined, 6)} {unit === "bps" ? "bps" : ""}
            </div>
            <div>
              <span className="text-zinc-500">neg:</span> {fmt(hoverN ?? undefined, 6)} {unit === "bps" ? "bps" : ""}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const { ts, fe, resultsAny } = useMemo(() => getStudyModels(study), [study]);
  const tables = useMemo(() => getTables(study), [study]);

  const series = (study as any).results?.series ?? resultsAny?.series ?? null;
  const mainSeries = pickMainSeries(series);
  const isEvent = isEventStudySeries(series);

  const r2ts = ts?.rsquared;
  const r2fe = fe?.rsquared;

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
          {study.key_stats?.map((s) => <Stat key={s.label} label={s.label} value={s.value} />) ?? null}

          <Stat label="Sample ticker" value={(study as any).results?.sample_ticker ?? "—"} />
          <Stat label="Tickers (panel)" value={String((study as any).results?.n_tickers ?? "—")} />
          <Stat label="Obs (panel)" value={String((study as any).results?.n_obs_panel ?? "—")} />

          {/* R² surfaced explicitly (your request) */}
          <Stat label="R² (TS)" value={r2ts == null ? "—" : fmt(r2ts, 4)} />
          <Stat label="R² (FE)" value={r2fe == null ? "—" : fmt(r2fe, 4)} />
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

      {/* conclusions */}
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

      {/* study sections (spec/data/limits/references) */}
      <StudySections sections={(study as any).sections} />

      {/* charts */}
      {isEvent ? (
        <EventStudyCard series={series} title={study.title} />
      ) : (
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
      )}

      {/* regressions: only show if at least one exists */}
      {ts || fe ? (
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <RegressionTable title="Time-series (HAC)" model={ts as any} />
          <RegressionTable title="Panel FE (clustered)" model={fe as any} />
        </section>
      ) : null}

      {/* exported tables */}
      {tables.length ? (
        <section className="space-y-4">
          <h2 className="text-lg font-semibold">Tables</h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {tables.map((t, i) => (
              <DataTable key={i} t={t} />
            ))}
          </div>
        </section>
      ) : null}

      {/* appendix (kept for reproducibility; NO download buttons) */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Appendix</h2>
        <div className="text-xs text-zinc-500">Raw exported objects (reproducibility / debugging).</div>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">Raw exported JSON</summary>
          <pre className="text-xs overflow-auto mt-3">
{JSON.stringify(
  {
    series: (study as any).results?.series ?? null,
    time_series: ts ?? null,
    panel_fe: fe ?? null,
    tables: tables ?? null,
    famamacbeth: (study as any).results?.famamacbeth ?? null,
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
