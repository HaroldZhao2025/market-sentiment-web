"use client";

import { useMemo, useState } from "react";

type Series = {
  label: string;
  values: number[];
  strokeClassName: string;
  dotClassName?: string;
};

type Props = {
  dates: string[];
  series: Series[];
  height?: number;
  baselineValue?: number;
  valueFormat?: (v: number) => string;
  yLabel?: string;
  showMinMaxLabels?: boolean;
};

function toPath(values: number[], w = 1000, h = 320, pad = 18, min = 0, max = 1) {
  const clean = values.map((v) => (Number.isFinite(v) ? v : Number.NaN));
  const rng = max - min || 1;
  const x = (i: number) => (i / Math.max(1, clean.length - 1)) * w;
  const y = (v: number) => pad + (1 - (v - min) / rng) * (h - 2 * pad);
  let d = "";
  let started = false;
  for (let i = 0; i < clean.length; i++) {
    const v = clean[i];
    if (!Number.isFinite(v)) {
      started = false;
      continue;
    }
    d += `${started ? "L" : "M"}${x(i).toFixed(2)},${y(v).toFixed(2)} `;
    started = true;
  }
  return d.trim();
}

export default function PortfolioChart({
  dates,
  series,
  height = 520,
  baselineValue,
  valueFormat,
  yLabel,
  showMinMaxLabels = true,
}: Props) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const W = 1000;
  const H = Math.max(260, Math.floor(height));
  const pad = 28;

  const combined = useMemo(() => {
    const all = series.flatMap((s) => s.values).filter((v) => Number.isFinite(v));
    const min = all.length ? Math.min(...all) : 0;
    const max = all.length ? Math.max(...all) : 1;
    return { min, max: max === min ? min + 1 : max };
  }, [series]);

  const scaleY = (v: number) => pad + (1 - (v - combined.min) / (combined.max - combined.min)) * (H - 2 * pad);
  const scaleX = (i: number) => (i / Math.max(1, dates.length - 1)) * W;

  const paths = useMemo(() => series.map((s) => ({
    ...s,
    d: toPath(s.values, W, H, pad, combined.min, combined.max),
  })), [series, H, combined.min, combined.max]);

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const t = (e.clientX - rect.left) / Math.max(1, rect.width);
    const idx = Math.round(t * (dates.length - 1));
    setHoverIdx(Math.max(0, Math.min(dates.length - 1, idx)));
  };

  const fmt = valueFormat ?? ((v: number) => (Number.isFinite(v) ? v.toFixed(4) : "—"));
  const hDate = hoverIdx != null ? dates[hoverIdx] : null;

  return (
    <div className="space-y-3">
      <div className="flex flex-col gap-2 text-sm text-neutral-400 md:flex-row md:items-center md:gap-4">
        <div className="flex flex-wrap items-center gap-3">
          {series.map((s) => (
            <div key={s.label} className="inline-flex items-center gap-2">
              <span className={`inline-block h-1.5 w-7 rounded-full ${s.strokeClassName.replace("stroke-", "bg-")}`} />
              <span className="font-medium text-neutral-300">{s.label}</span>
            </div>
          ))}
        </div>
        <div className="tabular-nums text-neutral-500 md:ml-auto">
          {hDate ? (
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
              <span className="font-medium text-neutral-200">{hDate}</span>
              {hoverIdx != null ? series.map((s) => {
                const v = s.values[hoverIdx];
                return Number.isFinite(v) ? <span key={s.label}>{s.label}: <span className="text-neutral-200">{fmt(v)}</span></span> : null;
              }) : null}
            </div>
          ) : <span>Hover the chart for exact values</span>}
        </div>
      </div>

      <div className="overflow-hidden rounded-2xl border border-white/10 bg-black/25 p-3 shadow-inner shadow-black/30">
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full" onMouseMove={onMove} onMouseLeave={() => setHoverIdx(null)}>
          <defs>
            <linearGradient id="portfolioDarkFade" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(255,255,255,0.025)" />
              <stop offset="100%" stopColor="rgba(255,255,255,0)" />
            </linearGradient>
          </defs>
          <rect x="0" y="0" width={W} height={H} fill="url(#portfolioDarkFade)" />

          {[pad, H / 2, H - pad].map((y) => (
            <line key={y} x1="0" y1={y} x2={W} y2={y} stroke="rgba(255,255,255,0.07)" strokeWidth="1" />
          ))}

          {baselineValue != null && Number.isFinite(baselineValue) ? (
            <line x1="0" y1={scaleY(baselineValue)} x2={W} y2={scaleY(baselineValue)} stroke="rgba(255,255,255,0.16)" strokeDasharray="5 5" strokeWidth="1.25" />
          ) : null}

          {yLabel ? <text x={14} y={18} fill="rgba(212,212,216,0.62)" fontSize="12" fontFamily="ui-sans-serif, system-ui">{yLabel}</text> : null}

          {showMinMaxLabels ? (
            <>
              <text x={14} y={pad + 12} fill="rgba(161,161,170,0.62)" fontSize="11" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace">max {fmt(combined.max)}</text>
              <text x={14} y={H - pad - 7} fill="rgba(161,161,170,0.62)" fontSize="11" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace">min {fmt(combined.min)}</text>
            </>
          ) : null}

          {paths.map((p) => p.d ? <path key={p.label} d={p.d} fill="none" className={p.strokeClassName} strokeWidth="2.5" vectorEffect="non-scaling-stroke" /> : null)}

          {hoverIdx != null ? (
            <>
              <line x1={scaleX(hoverIdx)} y1={pad} x2={scaleX(hoverIdx)} y2={H - pad} stroke="rgba(255,255,255,0.2)" strokeWidth="1" />
              {paths.map((p) => {
                const v = p.values[hoverIdx];
                if (!Number.isFinite(v)) return null;
                const dot = p.dotClassName ?? p.strokeClassName.replace("stroke-", "fill-");
                return <circle key={p.label} cx={scaleX(hoverIdx)} cy={scaleY(v)} r="4.5" className={dot} stroke="rgba(10,10,10,0.8)" strokeWidth="2" />;
              })}
            </>
          ) : null}
        </svg>
      </div>
    </div>
  );
}
