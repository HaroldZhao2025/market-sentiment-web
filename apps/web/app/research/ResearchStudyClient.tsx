// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import { useMemo, useState, useRef } from "react";
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
  notes?: string;
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

function asNum(x: any): number | null {
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function toBps(x?: number | null) {
  if (x == null || !Number.isFinite(x)) return null;
  return x * 10000;
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

function Stat({ label, value }: { label: string; value?: string }) {
  return (
    <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
      <div className="text-xs text-zinc-500">{label}</div>
      <div className="text-sm font-semibold">{value ?? "—"}</div>
    </div>
  );
}

function SectionCard({
  title,
  bullets,
}: {
  title: string;
  bullets?: string[];
}) {
  if (!bullets?.length) return null;
  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
      <div className="text-lg font-semibold">{title}</div>
      <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
        {bullets.map((b, i) => (
          <li key={i}>{b}</li>
        ))}
      </ul>
    </div>
  );
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
  const rows = keys.filter((k) => k !== "const").sort((a, b) => a.localeCompare(b));

  if (!rows.length) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-sm text-zinc-500 mt-2">No coefficients exported.</div>
      </div>
    );
  }

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

/**
 * A compact “paper style” table renderer for your exported builder tables.
 */
function TablesBlock({ tables }: { tables: ExportedTable[] }) {
  if (!tables?.length) return null;

  return (
    <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
      <div className="text-lg font-semibold">Tables</div>

      <div className="space-y-6">
        {tables.map((t, idx) => {
          const cols = t.columns ?? [];
          const rows = t.rows ?? [];
          return (
            <div key={idx} className="space-y-2">
              <div className="text-sm font-semibold text-zinc-800">
                Table {idx + 1}: {t.title ?? "Untitled"}
              </div>

              <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
                <table className="w-full text-sm">
                  {cols.length ? (
                    <thead className="text-xs text-zinc-500">
                      <tr className="border-b border-zinc-200">
                        {cols.map((c, j) => (
                          <th key={j} className={`p-3 font-medium ${j === 0 ? "text-left" : "text-right"}`}>
                            {c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                  ) : null}
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={i} className="border-b border-zinc-200 last:border-b-0">
                        {r.map((cell: any, j: number) => {
                          const n = asNum(cell);
                          const isNum = n != null;
                          return (
                            <td key={j} className={`p-3 ${j === 0 ? "text-left" : "text-right"}`}>
                              {isNum ? fmt(n, 6) : String(cell ?? "—")}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {t.notes ? <div className="text-xs text-zinc-500">{t.notes}</div> : null}
              {!t.notes ? (
                <div className="text-xs text-zinc-500">Notes: values are in log-return units unless labeled otherwise.</div>
              ) : null}
            </div>
          );
        })}
      </div>
    </section>
  );
}

/**
 * A more “academic” event-study figure block with:
 * - CAR/AAR toggle
 * - hover tooltip
 * - summary stats (CAR(+1), CAR(+5), mean pre AAR)
 * - optional diff line
 */
function EventStudyBlock({ series }: { series: any }) {
  const tau = (series?.tau ?? []) as number[];
  const hasCar = Array.isArray(series?.car_pos) && series.car_pos.length && Array.isArray(series?.car_neg) && series.car_neg.length;
  const hasAar = Array.isArray(series?.aar_pos) && series.aar_pos.length && Array.isArray(series?.aar_neg) && series.aar_neg.length;

  const [metric, setMetric] = useState<"CAR" | "AAR">(hasCar ? "CAR" : "AAR");
  const [showDiff, setShowDiff] = useState(false);

  const pos = useMemo(() => (metric === "CAR" ? (series?.car_pos ?? []) : (series?.aar_pos ?? [])) as number[], [metric, series]);
  const neg = useMemo(() => (metric === "CAR" ? (series?.car_neg ?? []) : (series?.aar_neg ?? [])) as number[], [metric, series]);

  // optional standard errors (if your builder exports them)
  const posSe = useMemo(() => {
    const k = metric === "CAR" ? "se_car_pos" : "se_aar_pos";
    const v = series?.[k];
    return (Array.isArray(v) ? v : []) as number[];
  }, [metric, series]);

  const negSe = useMemo(() => {
    const k = metric === "CAR" ? "se_car_neg" : "se_aar_neg";
    const v = series?.[k];
    return (Array.isArray(v) ? v : []) as number[];
  }, [metric, series]);

  const diff = useMemo(() => {
    if (!showDiff) return [];
    if (!tau.length || !pos.length || !neg.length) return [];
    return pos.map((v, i) => v - (neg[i] ?? 0));
  }, [showDiff, tau, pos, neg]);

  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const { ymin, ymax } = useMemo(() => {
    const all: number[] = [];
    for (const v of pos) if (Number.isFinite(v)) all.push(v);
    for (const v of neg) if (Number.isFinite(v)) all.push(v);
    for (const v of diff) if (Number.isFinite(v)) all.push(v);
    // include CI bounds if present
    for (let i = 0; i < pos.length; i++) {
      const se = posSe[i];
      if (Number.isFinite(se)) {
        all.push(pos[i] - 1.96 * se, pos[i] + 1.96 * se);
      }
      const se2 = negSe[i];
      if (Number.isFinite(se2)) {
        all.push(neg[i] - 1.96 * se2, neg[i] + 1.96 * se2);
      }
    }
    if (!all.length) return { ymin: -1, ymax: 1 };
    let lo = Math.min(...all);
    let hi = Math.max(...all);
    if (lo === hi) {
      lo -= 1e-4;
      hi += 1e-4;
    }
    const pad = 0.08 * (hi - lo);
    return { ymin: lo - pad, ymax: hi + pad };
  }, [pos, neg, diff, posSe, negSe]);

  const W = useMemo(() => {
    const mn = Math.min(...tau);
    const mx = Math.max(...tau);
    return Math.max(Math.abs(mn), Math.abs(mx));
  }, [tau]);

  function carAt(arr: number[], k: number): number | null {
    if (!tau.length || !arr.length) return null;
    const idx = tau.indexOf(k);
    if (idx < 0) return null;
    const v = arr[idx];
    return Number.isFinite(v) ? v : null;
  }

  function meanPre(arr: number[]): number | null {
    if (!tau.length || !arr.length) return null;
    const preIdx = tau.map((t, i) => (t < 0 ? i : -1)).filter((i) => i >= 0);
    if (!preIdx.length) return null;
    const vals = preIdx.map((i) => arr[i]).filter((v) => Number.isFinite(v));
    if (!vals.length) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  }

  // mapping functions
  const Wpx = 720;
  const Hpx = 300;
  const padL = 48;
  const padR = 18;
  const padT = 16;
  const padB = 36;

  const x = (t: number) => {
    if (!tau.length) return padL;
    const mn = Math.min(...tau);
    const mx = Math.max(...tau);
    const den = mx - mn || 1;
    return padL + ((t - mn) / den) * (Wpx - padL - padR);
  };
  const y = (v: number) => {
    const den = ymax - ymin || 1;
    return padT + ((ymax - v) / den) * (Hpx - padT - padB);
  };

  function pathOf(arr: number[]) {
    if (!tau.length || !arr.length) return "";
    let d = "";
    for (let i = 0; i < tau.length; i++) {
      const v = arr[i];
      if (!Number.isFinite(v)) continue;
      const xx = x(tau[i]);
      const yy = y(v);
      d += (d ? " L " : "M ") + `${xx} ${yy}`;
    }
    return d;
  }

  function onMove(e: React.MouseEvent<SVGSVGElement>) {
    if (!svgRef.current || !tau.length) return;
    const rect = svgRef.current.getBoundingClientRect();
    const rx = e.clientX - rect.left;
    const mn = Math.min(...tau);
    const mx = Math.max(...tau);
    const den = mx - mn || 1;
    const tFloat = mn + ((rx - padL) / (Wpx - padL - padR)) * den;
    // nearest tau
    let best = 0;
    let bestDist = Infinity;
    for (let i = 0; i < tau.length; i++) {
      const dist = Math.abs(tau[i] - tFloat);
      if (dist < bestDist) {
        bestDist = dist;
        best = i;
      }
    }
    setHoverIdx(best);
  }

  const title = metric === "CAR" ? "Cumulative abnormal return around events" : "Average abnormal return around events";
  const ylab = metric === "CAR" ? "CAR(τ)" : "AAR(τ)";

  if ((!hasCar && !hasAar) || !tau.length) {
    return (
      <div className="rounded-2xl border border-zinc-200 bg-white p-5">
        <div className="text-lg font-semibold">Event study</div>
        <div className="text-sm text-zinc-500 mt-2">
          Event-study series not found. Export <code className="px-1 py-0.5 rounded bg-zinc-100">results.series.tau</code> and
          either CAR (car_pos/car_neg) or AAR (aar_pos/aar_neg).
        </div>
      </div>
    );
  }

  const posK1 = metric === "CAR" ? carAt(pos, 1) : carAt(pos, 0);
  const negK1 = metric === "CAR" ? carAt(neg, 1) : carAt(neg, 0);
  const posK5 = metric === "CAR" ? carAt(pos, Math.min(5, W)) : carAt(pos, Math.min(5, W));
  const negK5 = metric === "CAR" ? carAt(neg, Math.min(5, W)) : carAt(neg, Math.min(5, W));

  const prePos = meanPre(metric === "CAR" ? (series?.aar_pos ?? []) : pos);
  const preNeg = meanPre(metric === "CAR" ? (series?.aar_neg ?? []) : neg);

  const showToggle = hasCar && hasAar;

  return (
    <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div className="space-y-1">
          <div className="text-lg font-semibold">Event study</div>
          <div className="text-sm text-zinc-600">{title}</div>
        </div>

        <div className="flex items-center gap-2">
          {showToggle ? (
            <div className="inline-flex rounded-xl border border-zinc-200 bg-zinc-50 p-1 text-xs">
              <button
                className={`px-3 py-1 rounded-lg ${metric === "CAR" ? "bg-white border border-zinc-200 shadow-sm" : "text-zinc-600"}`}
                onClick={() => setMetric("CAR")}
              >
                CAR
              </button>
              <button
                className={`px-3 py-1 rounded-lg ${metric === "AAR" ? "bg-white border border-zinc-200 shadow-sm" : "text-zinc-600"}`}
                onClick={() => setMetric("AAR")}
              >
                AAR
              </button>
            </div>
          ) : null}

          <label className="text-xs text-zinc-600 inline-flex items-center gap-2 select-none">
            <input
              type="checkbox"
              checked={showDiff}
              onChange={(e) => setShowDiff(e.target.checked)}
            />
            Show diff
          </label>
        </div>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <Stat label={metric === "CAR" ? "CAR(+1) pos" : "AAR(0) pos"} value={posK1 == null ? "—" : fmt(posK1, 6)} />
        <Stat label={metric === "CAR" ? "CAR(+1) neg" : "AAR(0) neg"} value={negK1 == null ? "—" : fmt(negK1, 6)} />
        <Stat label={`${metric}(+${Math.min(5, W)}) pos (bps)`} value={toBps(posK5) == null ? "—" : fmt(toBps(posK5)!, 2)} />
        <Stat label={`${metric}(+${Math.min(5, W)}) neg (bps)`} value={toBps(negK5) == null ? "—" : fmt(toBps(negK5)!, 2)} />
        {metric === "CAR" ? (
          <>
            <Stat label="Mean pre AAR (τ<0) pos" value={prePos == null ? "—" : fmt(prePos, 6)} />
            <Stat label="Mean pre AAR (τ<0) neg" value={preNeg == null ? "—" : fmt(preNeg, 6)} />
          </>
        ) : null}
      </div>

      {/* Chart */}
      <div className="relative overflow-x-auto">
        <div className="min-w-[720px]">
          <svg
            ref={svgRef}
            width={Wpx}
            height={Hpx}
            viewBox={`0 0 ${Wpx} ${Hpx}`}
            className="block"
            onMouseMove={onMove}
            onMouseLeave={() => setHoverIdx(null)}
          >
            {/* grid */}
            {(() => {
              const ticks = 6;
              const lines = [];
              for (let i = 0; i <= ticks; i++) {
                const yy = padT + (i / ticks) * (Hpx - padT - padB);
                lines.push(
                  <line
                    key={`g${i}`}
                    x1={padL}
                    x2={Wpx - padR}
                    y1={yy}
                    y2={yy}
                    stroke="rgba(0,0,0,0.06)"
                  />
                );
              }
              return lines;
            })()}

            {/* axes */}
            <line x1={padL} x2={padL} y1={padT} y2={Hpx - padB} stroke="rgba(0,0,0,0.25)" />
            <line x1={padL} x2={Wpx - padR} y1={Hpx - padB} y2={Hpx - padB} stroke="rgba(0,0,0,0.25)" />

            {/* y=0 */}
            {ymin < 0 && ymax > 0 ? (
              <line
                x1={padL}
                x2={Wpx - padR}
                y1={y(0)}
                y2={y(0)}
                stroke="rgba(0,0,0,0.25)"
                strokeDasharray="4 4"
              />
            ) : null}

            {/* tau=0 */}
            <line
              x1={x(0)}
              x2={x(0)}
              y1={padT}
              y2={Hpx - padB}
              stroke="rgba(0,0,0,0.25)"
              strokeDasharray="4 4"
            />

            {/* CI bands (if present) */}
            {posSe.length === pos.length ? (
              <path
                d={(() => {
                  // upper then lower reversed
                  const up = pos.map((v, i) => v + 1.96 * (posSe[i] ?? 0));
                  const lo = pos.map((v, i) => v - 1.96 * (posSe[i] ?? 0));
                  const p1 = pathOf(up);
                  const p2 = pathOf([...lo].reverse());
                  const xys = tau.map((t) => `${x(t)} ${y(0)}`).join(" ");
                  // build polygon path manually
                  if (!p1 || !p2) return "";
                  const upPts = tau.map((t, i) => `${x(t)} ${y(up[i] ?? 0)}`).join(" L ");
                  const loPts = [...tau].reverse().map((t, i) => {
                    const j = tau.length - 1 - i;
                    return `${x(t)} ${y(lo[j] ?? 0)}`;
                  }).join(" L ");
                  return `M ${upPts} L ${loPts} Z`;
                })()}
                fill="rgba(0,0,0,0.04)"
              />
            ) : null}

            {negSe.length === neg.length ? (
              <path
                d={(() => {
                  const up = neg.map((v, i) => v + 1.96 * (negSe[i] ?? 0));
                  const lo = neg.map((v, i) => v - 1.96 * (negSe[i] ?? 0));
                  const upPts = tau.map((t, i) => `${x(t)} ${y(up[i] ?? 0)}`).join(" L ");
                  const loPts = [...tau].reverse().map((t, i) => {
                    const j = tau.length - 1 - i;
                    return `${x(t)} ${y(lo[j] ?? 0)}`;
                  }).join(" L ");
                  return `M ${upPts} L ${loPts} Z`;
                })()}
                fill="rgba(0,0,0,0.04)"
              />
            ) : null}

            {/* lines */}
            <path d={pathOf(pos)} fill="none" stroke="rgba(0,0,0,0.9)" strokeWidth={2} />
            <path d={pathOf(neg)} fill="none" stroke="rgba(0,0,0,0.55)" strokeWidth={2} />

            {showDiff && diff.length ? (
              <path d={pathOf(diff)} fill="none" stroke="rgba(0,0,0,0.35)" strokeWidth={2} strokeDasharray="6 4" />
            ) : null}

            {/* hover marker */}
            {hoverIdx != null && hoverIdx >= 0 && hoverIdx < tau.length ? (
              <>
                <line
                  x1={x(tau[hoverIdx])}
                  x2={x(tau[hoverIdx])}
                  y1={padT}
                  y2={Hpx - padB}
                  stroke="rgba(0,0,0,0.12)"
                />
                <circle cx={x(tau[hoverIdx])} cy={y(pos[hoverIdx] ?? 0)} r={3} fill="rgba(0,0,0,0.9)" />
                <circle cx={x(tau[hoverIdx])} cy={y(neg[hoverIdx] ?? 0)} r={3} fill="rgba(0,0,0,0.55)" />
              </>
            ) : null}

            {/* labels */}
            <text x={padL} y={12} fontSize={11} fill="rgba(0,0,0,0.6)">
              {ylab}
            </text>
            <text x={Wpx - padR} y={Hpx - 10} textAnchor="end" fontSize={11} fill="rgba(0,0,0,0.6)">
              τ (days relative to event)
            </text>
          </svg>
        </div>

        {hoverIdx != null && hoverIdx >= 0 && hoverIdx < tau.length ? (
          <div className="pointer-events-none absolute top-2 right-2 rounded-xl border border-zinc-200 bg-white/95 p-3 text-xs shadow-sm">
            <div className="font-semibold text-zinc-800">τ = {tau[hoverIdx]}</div>
            <div className="text-zinc-600 mt-1">Pos: {fmt(pos[hoverIdx], 6)} ({fmt(toBps(pos[hoverIdx]) ?? undefined, 2)} bps)</div>
            <div className="text-zinc-600">Neg: {fmt(neg[hoverIdx], 6)} ({fmt(toBps(neg[hoverIdx]) ?? undefined, 2)} bps)</div>
            {showDiff && diff.length ? (
              <div className="text-zinc-600">Diff: {fmt(diff[hoverIdx], 6)}</div>
            ) : null}
          </div>
        ) : null}
      </div>

      <div className="text-xs text-zinc-500">
        Tip: In a “clean” event study, pre-event drift (τ&lt;0) should be relatively flat; large pre-trends can indicate selection/timing effects.
      </div>
    </section>
  );
}

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const results: any = study?.results ?? {};
  const series = results?.series;

  // detect event study from series signature
  const isEventStudy = useMemo(() => {
    return (
      series &&
      Array.isArray(series.tau) &&
      (Array.isArray(series.car_pos) || Array.isArray(series.aar_pos))
    );
  }, [series]);

  const mainSeries = useMemo(() => (isEventStudy ? null : pickSeries(series)), [isEventStudy, series]);

  // gather exported tables (support both results.tables and results.table)
  const exportedTables: ExportedTable[] = useMemo(() => {
    const out: ExportedTable[] = [];
    if (Array.isArray(results?.tables)) out.push(...(results.tables as any[]));
    if (results?.table && typeof results.table === "object") out.push(results.table as any);
    // sometimes nested (e.g., famamacbeth.table)
    if (results?.famamacbeth?.table && typeof results.famamacbeth.table === "object") out.push(results.famamacbeth.table as any);
    return out;
  }, [results]);

  const r2ts = asNum(results?.time_series?.rsquared);
  const r2fe = asNum(results?.panel_fe?.rsquared);

  // “paper-like” header blocks
  const keyFindings = (study as any)?.conclusions ?? (study as any)?.key_findings ?? [];

  const sections = (study as any)?.sections ?? [];
  const methodology = (study as any)?.methodology ?? [];

  return (
    <div className="space-y-6">
      {/* quick stats */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <div className="flex items-baseline justify-between gap-3">
          <div className="text-lg font-semibold">Study</div>
          <div className="text-xs text-zinc-500">
            {study?.category ? study.category : null}
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {(study as any)?.key_stats?.map((s: any) => (
            <Stat key={s.label} label={s.label} value={s.value} />
          )) ?? null}

          {/* common meta fields */}
          {results?.date_range ? (
            <Stat
              label="Date range"
              value={Array.isArray(results.date_range) ? `${results.date_range[0]}..${results.date_range[1]}` : String(results.date_range)}
            />
          ) : null}

          {results?.sample_ticker ? <Stat label="Sample ticker" value={String(results.sample_ticker)} /> : null}
          {results?.n_tickers != null ? <Stat label="Tickers (panel)" value={String(results.n_tickers)} /> : null}
          {results?.n_obs_panel != null ? <Stat label="Obs (panel)" value={String(results.n_obs_panel)} /> : null}

          {/* R² surfaced in the top grid (your request) */}
          <Stat label="R² (TS)" value={r2ts == null ? "—" : fmt(r2ts, 4)} />
          <Stat label="R² (FE)" value={r2fe == null ? "—" : fmt(r2fe, 4)} />
        </div>
      </section>

      {/* key findings */}
      {Array.isArray(keyFindings) && keyFindings.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Key findings</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {keyFindings.map((c: string, i: number) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* figures */}
      {isEventStudy ? (
        <EventStudyBlock series={series} />
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

      {/* methodology */}
      {Array.isArray(methodology) && methodology.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Methodology</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {methodology.map((m: string, i: number) => (
              <li key={i}>{m}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {/* study sections (spec/data/limitations/references etc.) */}
      {Array.isArray(sections) && sections.length ? (
        <section className="space-y-4">
          <div className="text-lg font-semibold">Study sections</div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {sections.map((sec: any, i: number) => (
              <SectionCard key={i} title={String(sec.title ?? `Section ${i + 1}`)} bullets={sec.bullets ?? []} />
            ))}
          </div>
        </section>
      ) : null}

      {/* regressions */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <RegressionTable title="Time-series (HAC)" model={results?.time_series as any} />
        <RegressionTable title="Panel FE (clustered)" model={results?.panel_fe as any} />
      </section>

      {/* exported tables */}
      <TablesBlock tables={exportedTables} />

      {/* appendix: raw JSON (collapsed, no “download” button) */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Appendix</h2>
        <div className="text-xs text-zinc-500">Raw exported objects (reproducibility / debugging).</div>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">Raw exported JSON</summary>
          <pre className="text-xs overflow-auto mt-3">
{JSON.stringify(results ?? null, null, 2)}
          </pre>
        </details>
      </section>
    </div>
  );
}
