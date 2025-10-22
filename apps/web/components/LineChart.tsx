"use client";

/**
 * Polished dual-axis chart aligned with the mock:
 *  • Bars: sentiment ([-1,1]) centered on zero
 *  • Line: price (right axis, auto-range)
 *  • Line: MA(7) of sentiment
 *  • Crosshair tooltip (date, S, MA7, price) — S rounded to 4 dp
 *  • Clear legend and padding so nothing overlaps
 */

import { useEffect, useMemo, useRef, useState } from "react";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price?: number[];
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number;
};

type Row = { d: string; p: number | null; s: number | null; m: number | null };
type Pt = { x: number; y: number };

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [w, setW] = useState(960);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((ents) => {
      for (const e of ents) setW(Math.max(640, Math.floor(e.contentRect.width)));
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return { ref, width: w };
}

function scaleLinear(domain: [number, number], range: [number, number]) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const m = (r1 - r0) / (d1 - d0 || 1);
  const fn = (v: number) => r0 + (v - d0) * m;
  (fn as any).invert = (r: number) => d0 + (r - r0) / m;
  return fn as ((v: number) => number) & { invert: (r: number) => number };
}

function monthLabel(d: string) {
  try {
    const dt = new Date(d + "T00:00:00Z");
    return dt.toLocaleDateString(undefined, { month: "short" });
  } catch {
    return d.slice(5, 7);
  }
}

function rowsOf(d: string[], p?: number[], s?: number[], m?: number[]): Row[] {
  const n = Math.max(d.length, p?.length ?? 0, s?.length ?? 0, m?.length ?? 0);
  return Array.from({ length: n }, (_, i) => ({
    d: d[i] ?? "",
    p: Number.isFinite(Number(p?.[i])) ? Number(p?.[i]) : null,
    s: Number.isFinite(Number(s?.[i])) ? Number(s?.[i]) : null,
    m: Number.isFinite(Number(m?.[i])) ? Number(m?.[i]) : null,
  }));
}

function Legend() {
  return (
    <div className="mt-4 flex flex-wrap items-center gap-6 text-sm text-neutral-600">
      <span className="inline-flex items-center gap-2"><span className="h-2 w-2 rounded-full" style={{background:"#7C3AED"}} />Sentiment (bar)</span>
      <span className="inline-flex items-center gap-2"><span className="h-2 w-2 rounded-full" style={{background:"#059669"}} />Sentiment MA(7)</span>
      <span className="inline-flex items-center gap-2"><span className="h-2 w-2 rounded-full" style={{background:"#0284C7"}} />Stock Price</span>
    </div>
  );
}

function ChartSVG({
  width,
  height,
  rows,
  separate = false,
}: { width: number; height: number; rows: Row[]; separate?: boolean }) {
  const pad = { t: 24, r: 84, b: 44, l: 60 };
  const w = Math.max(640, width);
  const h = Math.max(280, height);
  const W = Math.max(1, w - pad.l - pad.r);
  const H = Math.max(1, h - pad.t - pad.b);

  const N = rows.length || 1;
  const x = scaleLinear([0, Math.max(0, N - 1)], [0, W]);

  // left axis: sentiment [-1,1]
  const yL = scaleLinear([1, -1], [0, H]);

  // right axis: price
  const pVals = rows.map((r) => r.p).filter((v): v is number => Number.isFinite(v));
  const pMin = pVals.length ? Math.min(...pVals) : 0;
  const pMax = pVals.length ? Math.max(...pVals) : 1;
  const padP = (pMax - pMin) * 0.08 || 1;
  const yR = scaleLinear([pMin - padP, pMax + padP], [H, 0]);

  const monthEvery = Math.max(1, Math.round(N / 8));
  const yTicksL = [-1, -0.5, 0, 0.5, 1];

  // hover
  const [hx, setHx] = useState<number | null>(null);
  const [hi, setHi] = useState<number | null>(null);
  const onMove = (ev: React.MouseEvent<SVGRectElement>) => {
    const rect = (ev.target as SVGRectElement).getBoundingClientRect();
    const rx = Math.max(0, Math.min(W, ev.clientX - rect.left));
    setHx(rx);
    setHi(Math.max(0, Math.min(N - 1, Math.round(x.invert(rx)))));
  };
  const onLeave = () => { setHx(null); setHi(null); };
  const hov = hi != null ? rows[hi] : null;

  // precompute MA7/price polyline points
  const linePts = (arr: (number | null)[], y: (v: number) => number) =>
    arr.map((v, i) => (Number.isFinite(v as number) ? `${x(i).toFixed(1)},${y(v as number).toFixed(1)}` : null))
       .filter(Boolean)
       .join(" ");

  const maPolyline = linePts(rows.map(r => r.m), yL);
  const pricePolyline = linePts(rows.map(r => r.p), yR);

  const barW = Math.max(1, W / Math.max(32, N)); // bars never overlap too strongly

  return (
    <svg width={w} height={h} className="block">
      <g transform={`translate(${pad.l},${pad.t})`}>
        {/* grid */}
        {yTicksL.map((t, i) => (
          <line key={`gy-${i}`} x1={0} y1={yL(t)} x2={W} y2={yL(t)} stroke="#000" strokeOpacity={0.08} />
        ))}
        {rows.map((_, i) =>
          i % monthEvery === 0 ? (
            <line key={`gx-${i}`} x1={x(i)} y1={0} x2={x(i)} y2={H} stroke="#000" strokeOpacity={0.05} />
          ) : null
        )}

        {/* axes */}
        {yTicksL.map((t, i) => (
          <text key={`yl-${i}`} x={-12} y={yL(t)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={12}>
            {t.toFixed(1)}
          </text>
        ))}
        {pVals.length ? [...Array(5)].map((_, i) => {
          const v = pMin - padP + (i * (pMax + padP - (pMin - padP))) / 4;
          return (
            <text key={`yr-${i}`} x={W + 12} y={yR(v)} textAnchor="start" dominantBaseline="middle" className="fill-neutral-500" fontSize={12}>
              {Math.round(v)}
            </text>
          );
        }) : null}
        {rows.map((r, i) =>
          i % monthEvery === 0 ? (
            <text key={`xl-${i}`} x={x(i)} y={H + 24} textAnchor="middle" className="fill-neutral-500" fontSize={12}>
              {monthLabel(r.d)}
            </text>
          ) : null
        )}

        {/* zero line */}
        <line x1={0} y1={yL(0)} x2={W} y2={yL(0)} stroke="#000" strokeOpacity={0.12} />

        {/* sentiment bars */}
        {!separate && rows.length ? rows.map((r, i) => {
          if (!Number.isFinite(r.s as number)) return null;
          const x0 = x(i) - barW / 2;
          const y0 = yL(0);
          const y1_ = yL(r.s as number);
          const top = Math.min(y0, y1_);
          const h_ = Math.abs(y0 - y1_);
          return <rect key={`bar-${i}`} x={x0} y={top} width={barW} height={h_} fill="#7C3AED" opacity={0.28} />;
        }) : null}

        {/* MA7 + price lines */}
        {maPolyline ? <polyline points={maPolyline} fill="none" stroke="#059669" strokeWidth={2} /> : null}
        {pricePolyline ? <polyline points={pricePolyline} fill="none" stroke="#0284C7" strokeWidth={2} /> : null}

        {/* hover */}
        <rect x={0} y={0} width={W} height={H} fill="transparent" onMouseMove={onMove} onMouseLeave={onLeave} style={{ cursor: "crosshair" }} />
        {hov && hx != null ? (
          <>
            <line x1={hx} y1={0} x2={hx} y2={H} stroke="#000" strokeOpacity={0.18} />
            {Number.isFinite(hov.m as number) ? <circle cx={hx} cy={yL(hov.m as number)} r={3} fill="#059669" /> : null}
            {Number.isFinite(hov.p as number) ? <circle cx={hx} cy={yR(hov.p as number)} r={3} fill="#0284C7" /> : null}
            <foreignObject x={Math.min(Math.max(hx + 12, 0), Math.max(0, W - 240))} y={10} width={240} height={116}>
              <div className="rounded-lg border bg-white/95 shadow-sm p-2 text-xs leading-6">
                <div className="font-semibold mb-1">
                  {(() => {
                    try {
                      const d = new Date(rows[hi!].d + "T00:00:00Z");
                      return d.toLocaleDateString(undefined, { month: "short", day: "2-digit", year: "numeric" });
                    } catch { return rows[hi!].d; }
                  })()}
                </div>
                <div className="flex items-center justify-between"><span className="text-neutral-600">Sentiment</span><span className="font-medium" style={{color:"#7C3AED"}}>{Number.isFinite(hov.s as number)?(hov.s as number).toFixed(4):"—"}</span></div>
                <div className="flex items-center justify-between"><span className="text-neutral-600">MA(7)</span><span className="font-medium" style={{color:"#059669"}}>{Number.isFinite(hov.m as number)?(hov.m as number).toFixed(4):"—"}</span></div>
                <div className="flex items-center justify-between"><span className="text-neutral-600">Price</span><span className="font-medium" style={{color:"#0284C7"}}>{Number.isFinite(hov.p as number)?(hov.p as number).toFixed(2):"—"}</span></div>
              </div>
            </foreignObject>
          </>
        ) : null}
      </g>
    </svg>
  );
}

export default function LineChart({ mode, dates, price, sentiment, sentimentMA7, height = 460 }: Props) {
  const rows = useMemo(() => rowsOf(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);
  const { ref, width } = useMeasure();

  if (!rows.length) {
    return <div className="w-full grid place-items-center text-neutral-400 text-sm" style={{ height }}>No chart data.</div>;
  }

  if (mode === "overlay") {
    return (
      <div ref={ref} className="w-full">
        <ChartSVG width={width} height={height} rows={rows} />
        <Legend />
      </div>
    );
  }

  const h1 = Math.max(280, Math.floor(height * 0.58));
  const h2 = Math.max(240, Math.floor(height * 0.42));
  return (
    <div ref={ref} className="w-full space-y-6">
      <ChartSVG width={width} height={h1} rows={rows} separate />
      <ChartSVG width={width} height={h2} rows={rows} />
      <Legend />
    </div>
  );
}
