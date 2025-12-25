// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import { useMemo, useState } from "react";
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
  const ax = Math.abs(x);
  if (ax !== 0 && (ax < 1e-3 || ax > 1e4)) return x.toExponential(3);
  return x.toFixed(d).replace(/0+$/, "").replace(/\.$/, "");
}

function hasAnyModel(m: any): m is ModelOut {
  if (!m) return false;
  const params = (m as ModelOut).params;
  return !!params && Object.keys(params).length > 0;
}

function getAtTau(tau: number[], ys: number[], t: number) {
  const i = tau.indexOf(t);
  if (i < 0) return undefined;
  return ys[i];
}

function DualTauChart({
  tau,
  y1,
  y2,
  label1,
  label2,
  title,
  subtitle,
}: {
  tau: number[];
  y1: number[];
  y2?: number[];
  label1: string;
  label2?: string;
  title: string;
  subtitle?: string;
}) {
  // basic SVG chart with axes
  const W = 720;
  const H = 260;
  const padL = 48;
  const padR = 18;
  const padT = 18;
  const padB = 40;

  const xs = tau.map((t) => Number(t));
  const allY = [
    ...y1.filter((v) => Number.isFinite(v)),
    ...(y2 ? y2.filter((v) => Number.isFinite(v)) : []),
    0,
  ];
  const yMin = Math.min(...allY);
  const yMax = Math.max(...allY);
  const yPad = (yMax - yMin) * 0.08 || 1e-6;

  const y0 = yMin - yPad;
  const y1m = yMax + yPad;

  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);

  const xMap = (x: number) =>
    padL + ((x - xMin) / (xMax - xMin || 1)) * (W - padL - padR);
  const yMap = (y: number) =>
    padT + (1 - (y - y0) / (y1m - y0 || 1)) * (H - padT - padB);

  const pathFrom = (ys: number[]) => {
    let d = "";
    for (let i = 0; i < tau.length; i++) {
      const x = xMap(xs[i]);
      const y = yMap(Number(ys[i] ?? 0));
      d += i === 0 ? `M ${x} ${y}` : ` L ${x} ${y}`;
    }
    return d;
  };

  const yZeroPix = yMap(0);
  const xZeroPix = xs.includes(0) ? xMap(0) : null;

  // ticks
  const yTicks = 5;
  const yTickVals = Array.from({ length: yTicks }, (_, i) => y0 + (i * (y1m - y0)) / (yTicks - 1));

  const xTickVals = tau.length <= 13 ? tau : tau.filter((_, i) => i % 2 === 0);

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
      <div className="flex items-baseline justify-between gap-4">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-xs text-zinc-500">{subtitle ?? ""}</div>
      </div>

      <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50 p-3">
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-[260px]">
          {/* grid */}
          {yTickVals.map((yv, i) => {
            const y = yMap(yv);
            return (
              <g key={i}>
                <line x1={padL} y1={y} x2={W - padR} y2={y} className="stroke-zinc-200" />
                <text x={padL - 8} y={y + 4} textAnchor="end" className="fill-zinc-500 text-[11px]">
                  {fmt(yv, 4)}
                </text>
              </g>
            );
          })}

          {/* axes */}
          <line x1={padL} y1={padT} x2={padL} y2={H - padB} className="stroke-zinc-300" />
          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} className="stroke-zinc-300" />

          {/* y=0 baseline */}
          {yZeroPix >= padT && yZeroPix <= H - padB ? (
            <line
              x1={padL}
              y1={yZeroPix}
              x2={W - padR}
              y2={yZeroPix}
              className="stroke-zinc-400"
              strokeDasharray="4 4"
            />
          ) : null}

          {/* x=0 marker */}
          {xZeroPix != null ? (
            <line
              x1={xZeroPix}
              y1={padT}
              x2={xZeroPix}
              y2={H - padB}
              className="stroke-zinc-400"
              strokeDasharray="4 4"
            />
          ) : null}

          {/* series */}
          <path d={pathFrom(y1)} className="fill-none stroke-zinc-900" strokeWidth={2} />
          {y2?.length ? (
            <path d={pathFrom(y2)} className="fill-none stroke-zinc-500" strokeWidth={2} />
          ) : null}

          {/* x ticks */}
          {xTickVals.map((t, i) => {
            const x = xMap(t);
            return (
              <g key={i}>
                <line x1={x} y1={H - padB} x2={x} y2={H - padB + 6} className="stroke-zinc-300" />
                <text x={x} y={H - padB + 20} textAnchor="middle" className="fill-zinc-500 text-[11px]">
                  {t}
                </text>
              </g>
            );
          })}

          {/* legend */}
          <g transform={`translate(${padL}, ${padT})`}>
            <rect x={0} y={0} width={210} height={42} rx={10} className="fill-white stroke-zinc-200" />
            <line x1={12} y1={14} x2={42} y2={14} className="stroke-zinc-900" strokeWidth={2} />
            <text x={52} y={18} className="fill-zinc-700 text-[12px]">
              {label1}
            </text>

            {label2 ? (
              <>
                <line x1={12} y1={30} x2={42} y2={30} className="stroke-zinc-500" strokeWidth={2} />
                <text x={52} y={34} className="fill-zinc-700 text-[12px]">
                  {label2}
                </text>
              </>
            ) : null}
          </g>

          {/* x-axis label */}
          <text x={(padL + (W - padR)) / 2} y={H - 8} textAnchor="middle" className="fill-zinc-500 text-[12px]">
            τ (days relative to event)
          </text>
        </svg>
      </div>

      <div className="text-xs text-zinc-500">
        Vertical dashed line is τ = 0. Horizontal dashed line is y = 0.
      </div>
    </div>
  );
}

function RegressionTable({ title, model }: { title: string; model?: ModelOut | null }) {
  const params = model?.params ?? {};
  const keys = Object.keys(params);

  if (!model || !keys.length) return null;

  const rows = keys.filter((k) => k !== "const").sort((a, b) => a.localeCompare(b));

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

function AcademicTable({
  table,
  idx,
}: {
  table: { title: string; columns: string[]; rows: any[][] };
  idx: number;
}) {
  const cols = table.columns ?? [];
  const rows = table.rows ?? [];
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="text-lg font-semibold">
        Table {idx}: {table.title}
      </div>
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
                    {typeof cell === "number" ? fmt(cell, 6) : (cell ?? "—").toString()}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="text-xs text-zinc-500">Notes: values are in log-return units unless labeled otherwise.</div>
    </div>
  );
}

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const series = study.results?.series ?? {};
  const tables: any[] = Array.isArray(study.results?.tables) ? study.results.tables : [];

  // ---- event-study detection ----
  const tau: number[] = Array.isArray(series?.tau) ? series.tau.map((x: any) => Number(x)) : [];
  const hasEvent =
    tau.length > 0 &&
    (Array.isArray(series?.car_pos) || Array.isArray(series?.car_neg) || Array.isArray(series?.aar_pos));

  const carPos: number[] = Array.isArray(series?.car_pos) ? series.car_pos.map((x: any) => Number(x)) : [];
  const carNeg: number[] = Array.isArray(series?.car_neg) ? series.car_neg.map((x: any) => Number(x)) : [];
  const aarPos: number[] = Array.isArray(series?.aar_pos) ? series.aar_pos.map((x: any) => Number(x)) : [];
  const aarNeg: number[] = Array.isArray(series?.aar_neg) ? series.aar_neg.map((x: any) => Number(x)) : [];

  const hasAAR = aarPos.length === tau.length && aarNeg.length === tau.length;
  const hasCAR = carPos.length === tau.length && carNeg.length === tau.length;

  const [metric, setMetric] = useState<"CAR" | "AAR">(hasAAR ? "CAR" : "CAR");

  const evtSummary = useMemo(() => {
    if (!hasEvent) return null;

    const yP = metric === "AAR" ? aarPos : carPos;
    const yN = metric === "AAR" ? aarNeg : carNeg;

    const p1 = getAtTau(tau, yP, 1);
    const p5 = getAtTau(tau, yP, 5);
    const n1 = getAtTau(tau, yN, 1);
    const n5 = getAtTau(tau, yN, 5);

    const pre5P = getAtTau(tau, yP, -5);
    const pre5N = getAtTau(tau, yN, -5);

    return {
      p1,
      p5,
      n1,
      n5,
      pre5P,
      pre5N,
    };
  }, [hasEvent, metric, tau, aarPos, aarNeg, carPos, carNeg]);

  // ---- generic “sample series” (only for non-event studies) ----
  const hasSampleRet = Array.isArray(series?.y_ret) && series.y_ret.length;
  const hasSampleFwd = Array.isArray(series?.y_ret_fwd1) && series.y_ret_fwd1.length;
  const hasSampleAbs = Array.isArray(series?.abs_ret) && series.abs_ret.length;
  const hasSampleSent = Array.isArray(series?.score_mean) && series.score_mean.length;

  const showSampleCharts = !hasEvent && (hasSampleRet || hasSampleFwd || hasSampleAbs || hasSampleSent);

  const timeSeries = study.results?.time_series as ModelOut | null | undefined;
  const panelFe = study.results?.panel_fe as ModelOut | null | undefined;
  const showReg = hasAnyModel(timeSeries) || hasAnyModel(panelFe);

  const Stat = ({ label, value }: { label: string; value?: string }) => (
    <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
      <div className="text-xs text-zinc-500">{label}</div>
      <div className="text-sm font-semibold">{value ?? "—"}</div>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Quick stats */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {(study.key_stats ?? []).map((s) => (
            <Stat key={s.label} label={s.label} value={s.value} />
          ))}

          {/* only show sample ticker if present */}
          {study.results?.sample_ticker ? <Stat label="Sample ticker" value={study.results.sample_ticker} /> : null}

          {study.results?.n_tickers != null ? (
            <Stat label="Tickers (panel)" value={String(study.results.n_tickers)} />
          ) : null}
          {study.results?.n_obs_panel != null ? (
            <Stat label="Obs (panel)" value={String(study.results.n_obs_panel)} />
          ) : null}
          {Array.isArray(study.results?.date_range) ? (
            <Stat label="Date range" value={`${study.results.date_range[0]}..${study.results.date_range[1]}`} />
          ) : null}
        </div>
      </section>

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

      {/* Study sections */}
      {study.sections?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
          <h2 className="text-lg font-semibold">Study sections</h2>
          <div className="space-y-4">
            {study.sections.map((sec, i) => (
              <div key={i} className="space-y-1">
                <div className="text-sm font-semibold text-zinc-800">{sec.title}</div>
                {sec.bullets?.length ? (
                  <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                    {sec.bullets.map((b, j) => (
                      <li key={j}>{b}</li>
                    ))}
                  </ul>
                ) : null}
              </div>
            ))}
          </div>
        </section>
      ) : null}

      {/* Conclusions */}
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

      {/* Event study block (the improved part) */}
      {hasEvent ? (
        <section className="space-y-4">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">Event study</h2>

            {hasAAR ? (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setMetric("CAR")}
                  className={`text-xs px-3 py-1.5 rounded-full border ${
                    metric === "CAR" ? "bg-zinc-900 text-white border-zinc-900" : "bg-white text-zinc-700 border-zinc-200"
                  }`}
                >
                  CAR
                </button>
                <button
                  onClick={() => setMetric("AAR")}
                  className={`text-xs px-3 py-1.5 rounded-full border ${
                    metric === "AAR" ? "bg-zinc-900 text-white border-zinc-900" : "bg-white text-zinc-700 border-zinc-200"
                  }`}
                >
                  AAR
                </button>
              </div>
            ) : (
              <div className="text-xs text-zinc-500">CAR only</div>
            )}
          </div>

          <DualTauChart
            tau={tau}
            y1={metric === "AAR" ? aarPos : carPos}
            y2={metric === "AAR" ? aarNeg : carNeg}
            label1="Positive extreme"
            label2="Negative extreme"
            title={metric === "AAR" ? "Average abnormal return around events" : "Cumulative abnormal return around events"}
            subtitle={metric === "AAR" ? "AAR(τ)" : "CAR(τ)"}
          />

          {evtSummary ? (
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
              <div className="text-lg font-semibold">Event-study summary ({metric})</div>
              <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
                <table className="w-full text-sm">
                  <thead className="text-xs text-zinc-500">
                    <tr className="border-b border-zinc-200">
                      <th className="text-left font-medium p-3">Group</th>
                      <th className="text-right font-medium p-3">{metric}(-5)</th>
                      <th className="text-right font-medium p-3">{metric}(+1)</th>
                      <th className="text-right font-medium p-3">{metric}(+5)</th>
                      <th className="text-right font-medium p-3">(+5 in bps)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      {
                        g: "Positive",
                        m5: evtSummary.pre5P,
                        p1: evtSummary.p1,
                        p5: evtSummary.p5,
                      },
                      {
                        g: "Negative",
                        m5: evtSummary.pre5N,
                        p1: evtSummary.n1,
                        p5: evtSummary.n5,
                      },
                    ].map((r) => (
                      <tr key={r.g} className="border-b border-zinc-200 last:border-b-0">
                        <td className="p-3 font-medium text-zinc-800">{r.g}</td>
                        <td className="p-3 text-right">{fmt(r.m5, 6)}</td>
                        <td className="p-3 text-right">{fmt(r.p1, 6)}</td>
                        <td className="p-3 text-right">{fmt(r.p5, 6)}</td>
                        <td className="p-3 text-right">{fmt((r.p5 ?? NaN) * 10000, 2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="text-xs text-zinc-500">
                Tip: In a “clean” event study, pre-event drift (τ&lt;0) should be relatively flat; large pre-trends can indicate
                selection/timing effects.
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      {/* Sample charts only for non-event studies */}
      {showSampleCharts ? (
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {hasSampleRet ? (
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="flex items-baseline justify-between">
                <h2 className="text-lg font-semibold">Returns (sample)</h2>
                <div className="text-xs text-zinc-500">y_ret</div>
              </div>
              <Sparkline data={series.y_ret} className="text-zinc-900" />
            </div>
          ) : null}

          {hasSampleFwd ? (
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="flex items-baseline justify-between">
                <h2 className="text-lg font-semibold">Next-day returns (sample)</h2>
                <div className="text-xs text-zinc-500">y_ret_fwd1</div>
              </div>
              <Sparkline data={series.y_ret_fwd1} className="text-zinc-900" />
            </div>
          ) : null}

          {hasSampleAbs ? (
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="flex items-baseline justify-between">
                <h2 className="text-lg font-semibold">Volatility proxy (sample)</h2>
                <div className="text-xs text-zinc-500">abs_ret</div>
              </div>
              <Sparkline data={series.abs_ret} className="text-zinc-900" />
            </div>
          ) : null}

          {hasSampleSent ? (
            <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
              <div className="flex items-baseline justify-between">
                <h2 className="text-lg font-semibold">Sentiment (sample)</h2>
                <div className="text-xs text-zinc-500">score_mean</div>
              </div>
              <Sparkline data={series.score_mean} className="text-zinc-900" />
            </div>
          ) : null}
        </section>
      ) : null}

      {/* Regressions only if present */}
      {showReg ? (
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <RegressionTable title="Time-series (HAC)" model={timeSeries ?? null} />
          <RegressionTable title="Panel FE (clustered)" model={panelFe ?? null} />
        </section>
      ) : null}

      {/* Tables */}
      {tables.length ? (
        <section className="space-y-3">
          <h2 className="text-lg font-semibold">Tables</h2>
          <div className="space-y-4">
            {tables.map((t, i) => (
              <AcademicTable key={i} table={t} idx={i + 1} />
            ))}
          </div>
        </section>
      ) : null}

      {/* Notes */}
      {study.notes?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Notes</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {study.notes.map((n, i) => (
              <li key={i}>{n}</li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}
