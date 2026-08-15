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

type Row = { d: string; p: number | null; s: number | null; m: number | null };
type Kind = "overlay" | "sentiment" | "price";

export function ChartLegend() {
  return (
    <div className="flex flex-wrap items-center gap-4 text-xs text-neutral-400">
      <div className="flex items-center gap-2"><span className="h-2 w-2 rounded-full bg-violet-500" />Sentiment</div>
      <div className="flex items-center gap-2"><span className="h-2 w-2 rounded-full bg-emerald-500" />7D average</div>
      <div className="flex items-center gap-2"><span className="h-2 w-2 rounded-full bg-sky-500" />Price</div>
    </div>
  );
}

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(900);
  useEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver((entries) => {
      const next = entries[0]?.contentRect?.width;
      if (next) setWidth(Math.max(320, Math.floor(next)));
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);
  return { ref, width };
}

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function buildRows(dates: string[], price?: number[], sentiment?: number[], ma?: number[]): Row[] {
  return dates.map((date, index) => ({
    d: date,
    p: finite(price?.[index]),
    s: finite(sentiment?.[index]),
    m: finite(ma?.[index]),
  }));
}

function monthLabel(value: string) {
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(date.getTime()) ? value.slice(5, 7) : date.toLocaleDateString(undefined, { month: "short" });
}

function dateLabel(value: string) {
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString(undefined, { month: "short", day: "2-digit", year: "numeric" });
}

function segments(rows: Row[], field: "p" | "s" | "m", x: (index: number) => number, y: (value: number) => number) {
  const output: string[] = [];
  let current: string[] = [];
  rows.forEach((row, index) => {
    const value = row[field];
    if (value == null) {
      if (current.length > 1) output.push(current.join(" "));
      current = [];
      return;
    }
    current.push(`${x(index).toFixed(1)},${y(value).toFixed(1)}`);
  });
  if (current.length > 1) output.push(current.join(" "));
  return output;
}

function ChartPanel({ rows, width, height, kind }: { rows: Row[]; width: number; height: number; kind: Kind }) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const w = Math.max(320, width);
  const h = Math.max(240, height);
  const showSentiment = kind !== "price";
  const showPrice = kind !== "sentiment";
  const pad = { top: 18, right: showPrice ? 66 : 20, bottom: 34, left: showSentiment ? 50 : 18 };
  const innerW = Math.max(1, w - pad.left - pad.right);
  const innerH = Math.max(1, h - pad.top - pad.bottom);
  const n = Math.max(1, rows.length);
  const x = (index: number) => (n <= 1 ? 0 : (index / (n - 1)) * innerW);

  const priceValues = rows.map((row) => row.p).filter((value): value is number => value != null);
  const pMinRaw = priceValues.length ? Math.min(...priceValues) : 0;
  const pMaxRaw = priceValues.length ? Math.max(...priceValues) : 1;
  const pricePadding = Math.max((pMaxRaw - pMinRaw) * 0.08, Math.abs(pMaxRaw || 1) * 0.005, 0.01);
  const pMin = pMinRaw - pricePadding;
  const pMax = pMaxRaw + pricePadding;
  const yPrice = (value: number) => innerH - ((value - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;
  const ySent = (value: number) => innerH - ((value + 1) / 2) * innerH;

  const sentTicks = [-1, -0.5, 0, 0.5, 1];
  const priceTicks = Array.from({ length: 5 }, (_, i) => pMin + ((pMax - pMin) * i) / 4);
  const tickEvery = Math.max(1, Math.floor(rows.length / 8));
  const hover = hoverIndex == null ? null : rows[hoverIndex];
  const hoverX = hoverIndex == null ? null : x(hoverIndex);

  function onMove(event: React.MouseEvent<SVGRectElement>) {
    const svg = event.currentTarget.ownerSVGElement;
    if (!svg || !rows.length) return;
    const rect = svg.getBoundingClientRect();
    const svgX = ((event.clientX - rect.left) / Math.max(1, rect.width)) * w;
    const localX = Math.max(0, Math.min(innerW, svgX - pad.left));
    const index = n <= 1 ? 0 : Math.round((localX / innerW) * (n - 1));
    setHoverIndex(Math.max(0, Math.min(rows.length - 1, index)));
  }

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="block w-full select-none" role="img" aria-label={`${kind} market chart`}>
      <g transform={`translate(${pad.left},${pad.top})`}>
        {showSentiment ? sentTicks.map((tick) => (
          <g key={tick}>
            <line x1={0} x2={innerW} y1={ySent(tick)} y2={ySent(tick)} stroke="rgba(255,255,255,0.07)" />
            <text x={-8} y={ySent(tick)} textAnchor="end" dominantBaseline="middle" fill="#737373" fontSize={11}>{tick.toFixed(1)}</text>
          </g>
        )) : priceTicks.map((tick) => (
          <line key={tick} x1={0} x2={innerW} y1={yPrice(tick)} y2={yPrice(tick)} stroke="rgba(255,255,255,0.07)" />
        ))}

        {rows.map((row, index) => index % tickEvery === 0 ? (
          <g key={`${row.d}-${index}`}>
            <line x1={x(index)} x2={x(index)} y1={0} y2={innerH} stroke="rgba(255,255,255,0.035)" />
            <text x={x(index)} y={innerH + 22} textAnchor="middle" fill="#737373" fontSize={11}>{monthLabel(row.d)}</text>
          </g>
        ) : null)}

        {showPrice ? priceTicks.map((tick) => (
          <text key={tick} x={innerW + 8} y={yPrice(tick)} dominantBaseline="middle" fill="#737373" fontSize={11}>{tick.toFixed(0)}</text>
        )) : null}

        {showSentiment ? segments(rows, "s", x, ySent).map((points, index) => (
          <polyline key={`s-${index}`} points={points} fill="none" stroke="#8b5cf6" strokeOpacity={0.55} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
        )) : null}
        {showSentiment ? segments(rows, "m", x, ySent).map((points, index) => (
          <polyline key={`m-${index}`} points={points} fill="none" stroke="#10b981" strokeWidth={2.2} vectorEffect="non-scaling-stroke" />
        )) : null}
        {showPrice ? segments(rows, "p", x, yPrice).map((points, index) => (
          <polyline key={`p-${index}`} points={points} fill="none" stroke="#0ea5e9" strokeWidth={2.1} vectorEffect="non-scaling-stroke" />
        )) : null}

        <rect x={0} y={0} width={innerW} height={innerH} fill="transparent" onMouseMove={onMove} onMouseLeave={() => setHoverIndex(null)} />
        {hover && hoverX != null ? (
          <>
            <line x1={hoverX} x2={hoverX} y1={0} y2={innerH} stroke="rgba(255,255,255,0.22)" strokeDasharray="4 4" />
            {showSentiment && hover.s != null ? <circle cx={hoverX} cy={ySent(hover.s)} r={3.5} fill="#8b5cf6" /> : null}
            {showSentiment && hover.m != null ? <circle cx={hoverX} cy={ySent(hover.m)} r={3.5} fill="#10b981" /> : null}
            {showPrice && hover.p != null ? <circle cx={hoverX} cy={yPrice(hover.p)} r={3.5} fill="#0ea5e9" /> : null}
            <foreignObject x={Math.min(Math.max(hoverX + 10, 0), Math.max(0, innerW - 230))} y={8} width={225} height={112}>
              <div className="rounded-xl border border-white/10 bg-neutral-950/95 p-3 text-[11px] leading-5 text-neutral-400 shadow-2xl">
                <div className="mb-1 font-semibold text-neutral-200">{dateLabel(hover.d)}</div>
                {showSentiment ? <div className="flex justify-between gap-4"><span>Sentiment</span><span className="font-mono text-violet-300">{hover.s == null ? "—" : hover.s.toFixed(4)}</span></div> : null}
                {showSentiment ? <div className="flex justify-between gap-4"><span>7D average</span><span className="font-mono text-emerald-300">{hover.m == null ? "—" : hover.m.toFixed(4)}</span></div> : null}
                {showPrice ? <div className="flex justify-between gap-4"><span>Price</span><span className="font-mono text-sky-300">{hover.p == null ? "—" : hover.p.toFixed(2)}</span></div> : null}
              </div>
            </foreignObject>
          </>
        ) : null}
      </g>
    </svg>
  );
}

export default function LineChart({ mode, dates, price, sentiment, sentimentMA7, height = 520 }: Props) {
  const rows = useMemo(() => buildRows(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);
  const { ref, width } = useMeasure();

  if (!rows.length) {
    return <div className="grid w-full place-items-center text-sm text-neutral-500" style={{ height }}>No chart data.</div>;
  }

  if (mode === "overlay") {
    return <div ref={ref} className="w-full"><ChartPanel rows={rows} width={width} height={height} kind="overlay" /></div>;
  }

  return (
    <div ref={ref} className="w-full space-y-4">
      <div className="rounded-xl border border-white/[0.06] bg-black/10 p-2"><ChartPanel rows={rows} width={width} height={Math.max(260, Math.floor(height * 0.52))} kind="sentiment" /></div>
      <div className="rounded-xl border border-white/[0.06] bg-black/10 p-2"><ChartPanel rows={rows} width={width} height={Math.max(240, Math.floor(height * 0.42))} kind="price" /></div>
    </div>
  );
}
