"use client";

import { useMemo, useState } from "react";

type Props = {
  dates: string[];
  portfolioEquity: number[];
  benchmarkEquity?: number[];
  benchmarkLabel?: string;
  height?: number;
};

function toPath(values: number[], w = 1000, h = 320, pad = 18) {
  const clean = values.map((v) => (Number.isFinite(v) ? v : NaN));
  const finite = clean.filter((v) => Number.isFinite(v));
  if (!finite.length) return { d: "", min: 0, max: 1 };

  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const rng = max - min || 1;

  const x = (i: number) => (i / Math.max(1, clean.length - 1)) * w;
  const y = (v: number) => {
    const t = (v - min) / rng;
    return pad + (1 - t) * (h - 2 * pad);
  };

  let d = "";
  for (let i = 0; i < clean.length; i++) {
    const v = clean[i];
    if (!Number.isFinite(v)) continue;
    const cmd = d ? "L" : "M";
    d += `${cmd}${x(i).toFixed(2)},${y(v).toFixed(2)} `;
  }
  return { d: d.trim(), min, max };
}

export default function PortfolioChart({
  dates,
  portfolioEquity,
  benchmarkEquity,
  benchmarkLabel = "SPY",
  height = 420,
}: Props) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const W = 1000;
  const H = Math.max(260, Math.floor(height));
  const pad = 22;

  const combined = useMemo(() => {
    const arrs = [portfolioEquity, benchmarkEquity ?? []].filter((a) => a.length);
    const all = arrs.flat().filter((v) => Number.isFinite(v));
    const min = all.length ? Math.min(...all) : 0;
    const max = all.length ? Math.max(...all) : 1;
    return { min, max: max === min ? min + 1 : max };
  }, [portfolioEquity, benchmarkEquity]);

  const scaleY = (v: number) => {
    const t = (v - combined.min) / (combined.max - combined.min);
    return pad + (1 - t) * (H - 2 * pad);
  };
  const scaleX = (i: number) => (i / Math.max(1, dates.length - 1)) * W;

  const portPath = useMemo(() => {
    const { d } = toPath(portfolioEquity, W, H, pad);
    return d;
  }, [portfolioEquity, W, H, pad]);

  const benchPath = useMemo(() => {
    if (!benchmarkEquity?.length) return "";
    const { d } = toPath(benchmarkEquity, W, H, pad);
    return d;
  }, [benchmarkEquity, W, H, pad]);

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const t = px / rect.width;
    const idx = Math.round(t * (dates.length - 1));
    setHoverIdx(Math.max(0, Math.min(dates.length - 1, idx)));
  };

  const hDate = hoverIdx != null ? dates[hoverIdx] : null;
  const hPort = hoverIdx != null ? portfolioEquity[hoverIdx] : null;
  const hBench = hoverIdx != null && benchmarkEquity?.length ? benchmarkEquity[hoverIdx] : null;

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-4 text-sm text-neutral-600">
        <div className="inline-flex items-center gap-2">
          <span className="inline-block h-2 w-6 rounded bg-neutral-900" />
          Portfolio
        </div>
        {benchmarkEquity?.length ? (
          <div className="inline-flex items-center gap-2">
            <span className="inline-block h-2 w-6 rounded bg-blue-600" />
            {benchmarkLabel}
          </div>
        ) : null}
        <div className="ml-auto tabular-nums">
          {hDate ? (
            <>
              <span className="text-neutral-800">{hDate}</span>
              {hPort != null && Number.isFinite(hPort) ? (
                <span className="ml-3">Port: {hPort.toFixed(4)}</span>
              ) : null}
              {hBench != null && Number.isFinite(hBench) ? (
                <span className="ml-3">
                  {benchmarkLabel}: {hBench.toFixed(4)}
                </span>
              ) : null}
            </>
          ) : (
            <span>Hover the chart</span>
          )}
        </div>
      </div>

      <div className="rounded-2xl border bg-white p-3">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          className="w-full"
          onMouseMove={onMove}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {/* baseline grid */}
          <line x1="0" y1={scaleY(1)} x2={W} y2={scaleY(1)} className="stroke-neutral-200" strokeWidth="1" />
          <line x1="0" y1={pad} x2={W} y2={pad} className="stroke-neutral-100" strokeWidth="1" />
          <line x1="0" y1={H - pad} x2={W} y2={H - pad} className="stroke-neutral-100" strokeWidth="1" />

          {/* series */}
          {benchPath ? (
            <path d={benchPath} fill="none" className="stroke-blue-600" strokeWidth="2.5" />
          ) : null}
          <path d={portPath} fill="none" className="stroke-neutral-900" strokeWidth="2.5" />

          {/* hover */}
          {hoverIdx != null ? (
            <>
              <line x1={scaleX(hoverIdx)} y1={pad} x2={scaleX(hoverIdx)} y2={H - pad} className="stroke-neutral-200" strokeWidth="1" />
              {hPort != null && Number.isFinite(hPort) ? (
                <circle cx={scaleX(hoverIdx)} cy={scaleY(hPort)} r="5" className="fill-neutral-900" />
              ) : null}
              {hBench != null && Number.isFinite(hBench) ? (
                <circle cx={scaleX(hoverIdx)} cy={scaleY(hBench)} r="5" className="fill-blue-600" />
              ) : null}
            </>
          ) : null}
        </svg>
      </div>
    </div>
  );
}
