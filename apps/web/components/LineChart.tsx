"use client";

/**
 * Dual-axis SVG chart:
 *  - Left Y: sentiment in [-1, 1] with zero line
 *  - Right Y: price (auto)
 *  - Daily sentiment bars + MA7 line + price line
 *  - Crosshair + tooltip (date, sentiment, MA7, price)
 *  - Responsive (pure SVG + ResizeObserver) — works on GH Pages
 */

import { useMemo, useRef, useState, useEffect } from "react";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price?: number[];
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number;
};

type Pt = { x: number; y: number };

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [w, setW] = useState(960);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        if (e.contentRect?.width) setW(Math.max(640, Math.floor(e.contentRect.width)));
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
  const n = Math.max(dates.length, price?.length ?? 0, s?.length ?? 0, m?.length ?? 0);
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
  const span = d1 - d0 || 1;
  const m = (r1 - r0) / span;
  const fn = (v: number) => r0 + (v - d0) * m;
  (fn as any).invert = (r: number) => d0 + (r - r0) / m;
  return fn as ((v: number) => number) & { invert: (r: number) => number };
}

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 480,
}: Props) {
  const rows = useMemo(() => buildRows(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);
  const { ref, width } = useMeasure();

  if (!rows.length) {
    return <div className="w-full grid place-items-center text-neutral-400 text-sm" style={{ height }}>No chart data.</div>;
  }

  const pad = { t: 22, r: 70, b: 36, l: 56 };
  const w = Math.max(640, width);
  const hOverlay = Math.max(260, height);
  const hTop = Math.max(220, Math.floor(height * 0.58));
  const hBottom = Math.max(200, Math.floor(height * 0.42));

  const makeChart = (h: number, separate: boolean) => {
    const W = Math.max(1, w - pad.l - pad.r);
    const H = Math.max(1, h - pad.t - pad.b);
    const N = rows.length || 1;

    const x = scaleLinear([0, Math.max(0, N - 1)], [0, W]);
    const yL = scaleLinear([1, -1], [0, H]); // sentiment
    const pVals = rows.map((r) => r.p).filter((v): v is number => Number.isFinite(v));
    const pMin = pVals.length ? Math.min(...pVals) : 0;
    const pMax = pVals.length ? Math.max(...pVals) : 1;
    const padP = (pMax - pMin) * 0.08 || 1;
    const yR = scaleLinear([pMin - padP, pMax + padP], [H, 0]);

    const monthEvery = Math.max(1, Math.floor(N / 8));
    const yTicksL = [-1, -0.5, 0, 0.5, 1];
    const yTicksR = 5;

    const [hoverX, setHoverX] = useState<number | null>(null);
    const [hoverIdx, setHoverIdx] = useState<number | null>(null);
    const onMove = (ev: React.MouseEvent<SVGRectElement>) => {
      const rect = (ev.target as SVGRectElement).getBoundingClientRect();
      const rx = ev.clientX - rect.left;
      const clamped = Math.max(0, Math.min(W, rx));
      setHoverX(clamped);
      const idx = Math.round(x.invert(clamped));
      setHoverIdx(Math.max(0, Math.min(N - 1, idx)));
    };
    const onLeave = () => { setHoverX(null); setHoverIdx(null); };
    const hover = hoverIdx != null ? rows[hoverIdx] : null;

    // daily bars for sentiment
    const bars = rows.map((r, i) => {
      if (!Number.isFinite(r.s as number)) return null;
      const xi = x(i);
      const y0 = yL(0);
      const y1 = yL(r.s as number);
      return <line key={`b-${i}`} x1={xi} y1={y0} x2={xi} y2={y1} stroke="#6B5BFF" strokeOpacity={0.28} strokeWidth={2} />;
    });

    const toPath = (pts: Pt[]) => pts.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
    const sPts: Pt[] = [];
    const mPts: Pt[] = [];
    const pPts: Pt[] = [];
    rows.forEach((r, i) => {
      const xi = x(i);
      if (Number.isFinite(r.s as number)) sPts.push({ x: xi, y: yL(r.s as number) });
      if (Number.isFinite(r.m as number)) mPts.push({ x: xi, y: yL(r.m as number) });
      if (Number.isFinite(r.p as number)) pPts.push({ x: xi, y: yR(r.p as number) });
    });

    return (
      <svg width={w} height={h} className="select-none block">
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
            <text key={`yl-${i}`} x={-10} y={yL(t)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={12}>
              {t.toFixed(1)}
            </text>
          ))}
          {pVals.length
            ? [...Array(yTicksR)].map((_, i) => {
                const v = pMin - padP + ((pMax + padP - (pMin - padP)) * i) / (yTicksR - 1);
                return (
                  <text key={`yr-${i}`} x={W + 8} y={yR(v)} textAnchor="start" dominantBaseline="middle" className="fill-neutral-500" fontSize={12}>
                    {Math.round(v)}
                  </text>
                );
              })
            : null}
          {rows.map((r, i) =>
            i % monthEvery === 0 ? (
              <text key={`xl-${i}`} x={x(i)} y={H + 18} textAnchor="middle" className="fill-neutral-500" fontSize={12}>
                {monthTickLabel(r.d)}
              </text>
            ) : null
          )}

          {/* zero line */}
          <line x1={0} y1={yL(0)} x2={W} y2={yL(0)} stroke="#000" strokeOpacity={0.12} />

          {/* series */}
          {!separate ? bars : null}
          {mPts.length ? <polyline points={toPath(mPts)} fill="none" stroke="#10B981" strokeWidth={2} /> : null}
          {pPts.length ? <polyline points={toPath(pPts)} fill="none" stroke="#0EA5E9" strokeWidth={2} /> : null}

          {/* hover */}
          <rect x={0} y={0} width={W} height={H} fill="transparent" onMouseMove={onMove} onMouseLeave={onLeave} style={{ cursor: "crosshair" }} />
          {hover && hoverX != null ? (
            <>
              <line x1={hoverX} y1={0} x2={hoverX} y2={H} stroke="#000" strokeOpacity={0.18} />
              {Number.isFinite(hover.m as number) ? <circle cx={hoverX} cy={yL(hover.m as number)} r={3} fill="#10B981" /> : null}
              {Number.isFinite(hover.p as number) ? <circle cx={hoverX} cy={yR(hover.p as number)} r={3} fill="#0EA5E9" /> : null}
              <foreignObject x={Math.min(Math.max(hoverX + 10, 0), Math.max(0, W - 240))} y={10} width={240} height={112}>
                <div className="rounded-lg border bg-white/95 shadow p-2 text-xs leading-5">
                  <div className="font-semibold mb-1">
                    {(() => {
                      try {
                        const d = new Date(rows[hoverIdx!].d + "T00:00:00Z");
                        return d.toLocaleDateString(undefined, { month: "short", day: "2-digit", year: "numeric" });
                      } catch {
                        return rows[hoverIdx!].d;
                      }
                    })()}
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-neutral-600">Sentiment</span>
                    <span className="font-medium" style={{ color: "#6B5BFF" }}>
                      {Number.isFinite(hover.s as number) ? (hover.s as number).toFixed(2) : "—"}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-neutral-600">Sentiment (MA7)</span>
                    <span className="font-medium" style={{ color: "#10B981" }}>
                      {Number.isFinite(hover.m as number) ? (hover.m as number).toFixed(2) : "—"}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-neutral-600">Stock Price</span>
                    <span className="font-medium" style={{ color: "#0EA5E9" }}>
                      {Number.isFinite(hover.p as number) ? (hover.p as number).toFixed(2) : "—"}
                    </span>
                  </div>
                </div>
              </foreignObject>
            </>
          ) : null}
        </g>
      </svg>
    );
  };

  if (mode === "overlay") {
    return (
      <div ref={ref} className="w-full space-y-4">
        {makeChart(hOverlay, false)}
        <div className="legend">
          <span className="dot" style={{ background: "#6B5BFF" }} /> Sentiment (daily bars)
          <span className="dot" style={{ background: "#10B981" }} /> Sentiment (MA7)
          <span className="dot" style={{ background: "#0EA5E9" }} /> Stock Price
        </div>
      </div>
    );
  }

  return (
    <div ref={ref} className="w-full space-y-6">
      {makeChart(hTop, true)}
      {makeChart(hBottom, false)}
      <div className="legend">
        <span className="dot" style={{ background: "#6B5BFF" }} /> Sentiment (daily bars)
        <span className="dot" style={{ background: "#10B981" }} /> Sentiment (MA7)
        <span className="dot" style={{ background: "#0EA5E9" }} /> Stock Price
      </div>
    </div>
  );
}
