// apps/web/components/LineChart.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price?: number[];
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number;
};

type Pt = { x: number; y: number };

const COLOR_SENT = "#6c63ff";   // violet
const COLOR_MA = "#4338ca";     // indigo-700
const COLOR_PRICE = "#10b981";  // emerald-500

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [w, setW] = useState(800);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) if (e.contentRect?.width) setW(Math.max(320, Math.floor(e.contentRect.width)));
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return { ref, width: w };
}

function monthTickLabel(d: string) {
  try {
    const dt = new Date(d + "T00:00:00Z");
    return dt.toLocaleDateString(undefined, { month: "short" });
  } catch {
    return d.slice(5, 7);
  }
}

function buildRows(dates: string[], price?: number[], s?: number[], m?: number[]) {
  const n = Math.min(dates.length, (price?.length ?? dates.length), (s?.length ?? dates.length), (m?.length ?? dates.length));
  return Array.from({ length: n }, (_, i) => ({
    d: dates[i] ?? "",
    p: Number.isFinite(Number(price?.[i])) ? Number(price?.[i]) : null,
    s: Number.isFinite(Number(s?.[i])) ? Number(s?.[i]) : null,
    m: Number.isFinite(Number(m?.[i])) ? Number(m?.[i]) : null,
  }));
}

function scaleLinear(domain: [number, number], range: [number, number]) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const m = (r1 - r0) / (d1 - d0 || 1);
  return (v: number) => r0 + (v - d0) * m;
}

function pointsToPolyline(pts: Pt[]) {
  return pts.map((p) => `${Math.round(p.x)},${Math.round(p.y)}`).join(" ");
}

function ChartSVG({
  width,
  height,
  rows,
  showSent,
  showMA,
  showPrice,
}: {
  width: number;
  height: number;
  rows: { d: string; p: number | null; s: number | null; m: number | null }[];
  showSent: boolean;
  showMA: boolean;
  showPrice: boolean;
}) {
  // layout
  const pad = { t: 14, r: 56, b: 26, l: 42 };
  const w = Math.max(320, width);
  const h = Math.max(160, height);
  const W = w - pad.l - pad.r;
  const H = h - pad.t - pad.b;

  // x mapping by index
  const N = rows.length || 1;
  const x = scaleLinear([0, Math.max(0, N - 1)], [0, W]);

  // y-left: sentiment fixed [-1, 1]
  const yL = scaleLinear([1, -1], [0, H]);

  // y-right: price auto
  const pVals = rows.map((r) => r.p).filter((v): v is number => Number.isFinite(v));
  const pMin = pVals.length ? Math.min(...pVals) : 0;
  const pMax = pVals.length ? Math.max(...pVals) : 1;
  const padP = (pMax - pMin) * 0.06 || 1;
  const yR = scaleLinear([pMin - padP, pMax + padP], [H, 0]);

  // build polylines
  const sPts: Pt[] = [];
  const mPts: Pt[] = [];
  const pPts: Pt[] = [];
  rows.forEach((r, i) => {
    const xi = x(i);
    if (Number.isFinite(r.s as number)) sPts.push({ x: xi, y: yL(r.s as number) });
    if (Number.isFinite(r.m as number)) mPts.push({ x: xi, y: yL(r.m as number) });
    if (Number.isFinite(r.p as number)) pPts.push({ x: xi, y: yR(r.p as number) });
  });

  // grid & ticks
  const yTicksL = [-1, -0.5, 0, 0.5, 1];
  const yTicksR = 5;
  const monthEvery = Math.ceil(N / 8);

  return (
    <svg width={w} height={h} className="select-none">
      <g transform={`translate(${pad.l},${pad.t})`}>
        {/* grid */}
        {yTicksL.map((t, i) => (
          <line key={`gy-${i}`} x1={0} y1={yL(t)} x2={W} y2={yL(t)} stroke="currentColor" strokeOpacity={0.1} />
        ))}
        {[...Array(N)].map((_, i) =>
          i % monthEvery === 0 ? (
            <line key={`gx-${i}`} x1={x(i)} y1={0} x2={x(i)} y2={H} stroke="currentColor" strokeOpacity={0.06} />
          ) : null
        )}

        {/* axes labels */}
        {yTicksL.map((t, i) => (
          <text key={`yl-${i}`} x={-8} y={yL(t)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>
            {t.toFixed(1)}
          </text>
        ))}
        {showPrice && pVals.length
          ? [...Array(yTicksR)].map((_, i) => {
              const v = pMin - padP + ((pMax + padP - (pMin - padP)) * i) / (yTicksR - 1);
              return (
                <text key={`yr-${i}`} x={W + 6} y={yR(v)} textAnchor="start" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>
                  {Math.round(v)}
                </text>
              );
            })
          : null}

        {/* x labels */}
        {[...Array(N)].map((_, i) =>
          i % monthEvery === 0 ? (
            <text key={`xl-${i}`} x={x(i)} y={H + 16} textAnchor="middle" className="fill-neutral-500" fontSize={11}>
              {monthTickLabel(rows[i]?.d || "")}
            </text>
          ) : null
        )}

        {/* zero line for sentiment */}
        <line x1={0} y1={yL(0)} x2={W} y2={yL(0)} stroke="currentColor" strokeOpacity={0.2} />

        {/* series */}
        {showSent && sPts.length ? (
          <polyline points={pointsToPolyline(sPts)} fill="none" stroke={COLOR_SENT} strokeOpacity={0.35} strokeWidth={1.5} />
        ) : null}
        {showMA && mPts.length ? (
          <polyline points={pointsToPolyline(mPts)} fill="none" stroke={COLOR_MA} strokeWidth={2} />
        ) : null}
        {showPrice && pPts.length ? (
          <polyline points={pointsToPolyline(pPts)} fill="none" stroke={COLOR_PRICE} strokeWidth={2} />
        ) : null}
      </g>

      {/* legend */}
      <g transform={`translate(${pad.l},${pad.t - 6})`} fontSize="11" className="fill-neutral-600">
        <rect x={0} y={-10} width={10} height={2} fill={COLOR_MA} />
        <text x={14} y={-8} dominantBaseline="central">Sentiment (MA7)</text>
        {showSent && (
          <>
            <rect x={120} y={-10} width={10} height={2} fill={COLOR_SENT} />
            <text x={134} y={-8} dominantBaseline="central" >Sentiment</text>
          </>
        )}
        {showPrice && (
          <>
            <rect x={210} y={-10} width={10} height={2} fill={COLOR_PRICE} />
            <text x={224} y={-8} dominantBaseline="central" >Price</text>
          </>
        )}
      </g>
    </svg>
  );
}

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 420,
}: Props) {
  const rows = useMemo(() => buildRows(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);
  const { ref, width } = useMeasure();

  if (!rows.length) {
    return (
      <div className="w-full grid place-items-center text-neutral-400 text-sm" style={{ height }}>
        No chart data.
      </div>
    );
  }

  if (mode === "overlay") {
    return (
      <div ref={ref} className="w-full" style={{ height }}>
        <ChartSVG width={width} height={height} rows={rows} showSent showMA showPrice />
      </div>
    );
  }

  // separate -> top: Sentiment only; bottom: Price only
  const h1 = Math.max(180, Math.floor(height * 0.55));
  const h2 = Math.max(160, Math.floor(height * 0.45));
  return (
    <div ref={ref} className="w-full space-y-4">
      <ChartSVG width={width} height={h1} rows={rows} showSent showMA showPrice={false} />
      <ChartSVG width={width} height={h2} rows={rows} showSent={false} showMA={false} showPrice />
    </div>
  );
}
