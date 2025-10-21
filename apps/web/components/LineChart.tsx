"use client";

/**
 * Dual-axis time-series chart (pure SVG, dependency-free)
 * - Left Y: sentiment in [-1, 1] (zero line shown)
 * - Right Y: price (auto)
 * - Overlay / Separate modes
 * - Hover crosshair with tooltip (date / sentiment / price)
 */

import { useEffect, useMemo, useRef, useState } from "react";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];           // ISO 'YYYY-MM-DD'
  price?: number[];          // close; optional (portfolio)
  sentiment: number[];       // daily S
  sentimentMA7?: number[];   // 7d MA for S
  height?: number;           // container height (overlay); each split uses ~55%
};

type Pt = { x: number; y: number };

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [w, setW] = useState(800);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        if (e.contentRect?.width) setW(Math.max(320, Math.floor(e.contentRect.width)));
      }
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
  const n = Math.min(
    dates.length,
    price?.length ?? dates.length,
    s?.length ?? dates.length,
    m?.length ?? dates.length
  );
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

function invertLinear(domain: [number, number], range: [number, number]) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const m = (d1 - d0) / (r1 - r0 || 1);
  return (px: number) => d0 + (px - r0) * m;
}

function pointsToPolyline(pts: Pt[]) {
  return pts.map((p) => `${Math.round(p.x)},${Math.round(p.y)}`).join(" ");
}

function ChartSVG({
  width,
  height,
  rows,
  separate = false,
  showLegend = true,
}: {
  width: number;
  height: number;
  rows: { d: string; p: number | null; s: number | null; m: number | null }[];
  separate?: boolean;
  showLegend?: boolean;
}) {
  // layout
  const pad = { t: 14, r: 64, b: 28, l: 46 };
  const w = Math.max(320, width);
  const h = Math.max(220, height);
  const W = w - pad.l - pad.r;
  const H = h - pad.t - pad.b;

  // x mapping by index (dates already ordered)
  const N = rows.length || 1;
  const x = scaleLinear([0, Math.max(0, N - 1)], [0, W]);
  const xInv = invertLinear([0, Math.max(0, N - 1)], [0, W]);

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
  const monthEvery = Math.max(1, Math.ceil(N / 8));

  // hover state
  const [hover, setHover] = useState<number | null>(null);
  const onMove = (e: React.MouseEvent<SVGRectElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const mx = e.clientX - rect.left - pad.l;
    if (mx < 0 || mx > W) { setHover(null); return; }
    const idx = Math.round(xInv(mx));
    setHover(Math.max(0, Math.min(N - 1, idx)));
  };
  const onLeave = () => setHover(null);

  const hoverRow = hover != null ? rows[hover] : null;

  return (
    <svg width={w} height={h} className="select-none text-neutral-800">
      <g transform={`translate(${pad.l},${pad.t})`}>
        {/* grid */}
        {yTicksL.map((t, i) => (
          <line key={`gy-${i}`} x1={0} y1={yL(t)} x2={W} y2={yL(t)} stroke="currentColor" strokeOpacity={0.08} />
        ))}
        {[...Array(N)].map((_, i) =>
          i % monthEvery === 0 ? (
            <line key={`gx-${i}`} x1={x(i)} y1={0} x2={x(i)} y2={H} stroke="currentColor" strokeOpacity={0.05} />
          ) : null
        )}

        {/* axes labels */}
        <text x={-36} y={-6} textAnchor="start" className="fill-neutral-500 text-[11px]">Sentiment</text>
        {pVals.length ? (
          <text x={W + 6} y={-6} textAnchor="start" className="fill-neutral-500 text-[11px]">Price</text>
        ) : null}

        {/* left sentiment ticks */}
        {yTicksL.map((t, i) => (
          <text key={`yl-${i}`} x={-8} y={yL(t)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>
            {t.toFixed(1)}
          </text>
        ))}
        {/* right price ticks */}
        {pVals.length
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
        <line x1={0} y1={yL(0)} x2={W} y2={yL(0)} stroke="currentColor" strokeOpacity={0.25} />

        {/* sentiment bars (overlay mode) */}
        {!separate &&
          rows.map((r, i) =>
            Number.isFinite(r.s as number) ? (
              <line
                key={`bar-${i}`}
                x1={x(i)}
                x2={x(i)}
                y1={yL(0)}
                y2={yL(r.s as number)}
                stroke="#6F42C1"
                strokeOpacity={0.35}
                strokeWidth={1}
              />
            ) : null
          )}

        {/* sentiment MA line */}
        {mPts.length ? (
          <polyline points={pointsToPolyline(mPts)} fill="none" stroke="#6F42C1" strokeWidth={2} />
        ) : null}

        {/* price line */}
        {pPts.length ? (
          <polyline points={pointsToPolyline(pPts)} fill="none" stroke="#10B981" strokeWidth={2} />
        ) : null}

        {/* legend */}
        {showLegend ? (
          <g transform={`translate(0,${-4})`}>
            <circle cx={0} cy={0} r={4} fill="#6F42C1" />
            <text x={8} y={2} className="fill-neutral-700" fontSize={12}>Sentiment (MA7)</text>
            {pPts.length ? (
              <>
                <circle cx={150} cy={0} r={4} fill="#10B981" />
                <text x={158} y={2} className="fill-neutral-700" fontSize={12}>Stock Price</text>
              </>
            ) : null}
          </g>
        ) : null}

        {/* hover crosshair & tooltip */}
        <rect
          x={0}
          y={0}
          width={W}
          height={H}
          fill="transparent"
          onMouseMove={onMove}
          onMouseLeave={onLeave}
          style={{ cursor: "crosshair" }}
        />
        {hover != null ? (
          <>
            <line x1={x(hover)} x2={x(hover)} y1={0} y2={H} stroke="#111827" strokeOpacity={0.25} />
            {/* tooltip box */}
            <g transform={`translate(${Math.min(Math.max(8, x(hover) + 10), W - 180)},${8})`}>
              <rect width={180} height={70} rx={8} fill="#ffffff" stroke="#e5e7eb" />
              <text x={10} y={18} className="fill-neutral-900" fontSize={12} fontWeight={600}>
                {new Date((rows[hover]?.d || "") + "T00:00:00Z").toLocaleDateString()}
              </text>
              <text x={10} y={36} className="fill-neutral-700" fontSize={12}>
                Sentiment: {(rows[hover]?.s ?? 0).toFixed(2)}
              </text>
              {pPts.length ? (
                <text x={10} y={54} className="fill-neutral-700" fontSize={12}>
                  Price: {Number(rows[hover]?.p ?? 0).toFixed(2)}
                </text>
              ) : null}
            </g>
          </>
        ) : null}
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
      <div ref={ref} className="w-full overflow-visible" style={{ height }}>
        <ChartSVG width={width} height={height} rows={rows} />
      </div>
    );
  }

  // separate -> stack sentiment (with MA) then price
  const h1 = Math.max(200, Math.floor(height * 0.58));
  const h2 = Math.max(160, Math.floor(height * 0.42));
  return (
    <div ref={ref} className="w-full space-y-4 overflow-visible">
      <ChartSVG width={width} height={h1} rows={rows} separate />
      <ChartSVG width={width} height={h2} rows={rows} showLegend={false} />
    </div>
  );
}
