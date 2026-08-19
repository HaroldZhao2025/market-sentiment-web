"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useLanguage } from "./LanguageProvider";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price?: number[];
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number;
};

type Row = { d: string; t: number; p: number | null; s: number | null; m: number | null };
type Pt = { x: number; y: number };
type PanelKind = "overlay" | "price" | "sentiment";
type Locale = "en" | "zh";

const MONTHS_EN = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function ChartLegend() {
  const { locale } = useLanguage();
  return (
    <div data-no-translate="true" className="flex flex-wrap items-center gap-4 text-xs text-neutral-400">
      <div className="flex items-center gap-2"><span className="inline-block h-2 w-2 rounded-full bg-violet-400" />{locale === "zh" ? "情绪" : "Sentiment"}</div>
      <div className="flex items-center gap-2"><span className="inline-block h-2 w-2 rounded-full bg-emerald-400" />{locale === "zh" ? "情绪 MA7" : "Sentiment MA7"}</div>
      <div className="flex items-center gap-2"><span className="inline-block h-2 w-2 rounded-full bg-sky-400" />{locale === "zh" ? "价格" : "Price"}</div>
    </div>
  );
}

function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(960);
  useEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry?.contentRect.width) setWidth(Math.max(320, Math.floor(entry.contentRect.width)));
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);
  return { ref, width };
}

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function rollingMean7(values: Array<number | null>): Array<number | null> {
  return values.map((_, index) => {
    if (index < 6) return null;
    const window = values.slice(index - 6, index + 1);
    if (window.some((value) => value == null)) return null;
    return window.reduce((sum, value) => sum + (value as number), 0) / 7;
  });
}

function buildRows(dates: string[], price?: number[], sentiment?: number[], ma?: number[]): Row[] {
  const sentimentValues = dates.map((_, index) => finite(sentiment?.[index]));
  const derivedMA = rollingMean7(sentimentValues);
  return dates.map((d, i) => {
    const parsed = Date.parse(`${d}T00:00:00Z`);
    return {
      d,
      t: Number.isFinite(parsed) ? parsed : i,
      p: finite(price?.[i]),
      s: sentimentValues[i],
      m: finite(ma?.[i]) ?? derivedMA[i],
    };
  }).filter((row) => row.d);
}

function scaleLinear(domain: [number, number], range: [number, number]) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const span = d1 - d0 || 1;
  const slope = (r1 - r0) / span;
  const fn = (v: number) => r0 + (v - d0) * slope;
  (fn as any).invert = (r: number) => d0 + (r - r0) / slope;
  return fn as ((v: number) => number) & { invert: (r: number) => number };
}

function extent(values: Array<number | null>, fallback: [number, number], padding = 0.06): [number, number] {
  const valid = values.filter((v): v is number => v != null && Number.isFinite(v));
  if (!valid.length) return fallback;
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const pad = (max - min) * padding || Math.max(Math.abs(max) * padding, 1);
  return [min - pad, max + pad];
}

function niceNumber(value: number, locale: Locale) {
  const abs = Math.abs(value);
  if (abs >= 1000) return new Intl.NumberFormat(locale === "zh" ? "zh-CN" : "en-US", { maximumFractionDigits: 0 }).format(value);
  if (abs >= 100) return value.toFixed(0);
  if (abs >= 10) return value.toFixed(1);
  return value.toFixed(2);
}

function parseDateParts(value: string) {
  const dt = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(dt.getTime())) return null;
  return { year: dt.getUTCFullYear(), month: dt.getUTCMonth(), day: dt.getUTCDate() };
}

function formatDate(value: string, includeYear: boolean, locale: Locale) {
  const parts = parseDateParts(value);
  if (!parts) return value;
  if (locale === "zh") return includeYear ? `${parts.year}年${parts.month + 1}月` : `${parts.month + 1}月`;
  return includeYear ? `${MONTHS_EN[parts.month]} '${String(parts.year).slice(-2)}` : MONTHS_EN[parts.month];
}

function formatFullDate(value: string, locale: Locale) {
  const parts = parseDateParts(value);
  if (!parts) return value;
  if (locale === "zh") return `${parts.year}年${parts.month + 1}月${parts.day}日`;
  return `${MONTHS_EN[parts.month]} ${parts.day}, ${parts.year}`;
}

function chooseTickIndices(rows: Row[], maxTicks: number) {
  if (!rows.length) return [];
  const count = Math.min(maxTicks, rows.length);
  if (count <= 1) return [0];
  const indices = Array.from({ length: count }, (_, i) => Math.round((i * (rows.length - 1)) / (count - 1)));
  return Array.from(new Set(indices));
}

function points(rows: Row[], x: (v: number) => number, y: (v: number) => number, key: "p" | "s" | "m") {
  return rows.filter((row) => row[key] != null).map((row) => ({ x: x(row.t), y: y(row[key] as number) }));
}

function polyline(pts: Pt[]) {
  return pts.map((pt) => `${pt.x.toFixed(1)},${pt.y.toFixed(1)}`).join(" ");
}

function ChartPanel({ width, height, rows, kind, locale }: { width: number; height: number; rows: Row[]; kind: PanelKind; locale: Locale }) {
  const pad = { t: 22, r: kind === "overlay" ? 70 : 26, b: 48, l: 58 };
  const w = Math.max(320, width);
  const h = Math.max(220, height);
  const plotW = Math.max(1, w - pad.l - pad.r);
  const plotH = Math.max(1, h - pad.t - pad.b);
  const minT = rows[0]?.t ?? 0;
  const maxT = rows.at(-1)?.t ?? minT + 1;
  const x = scaleLinear([minT, maxT === minT ? minT + 1 : maxT], [0, plotW]);
  const priceDomain = extent(rows.map((row) => row.p), [0, 1], 0.05);
  const sentimentDomain: [number, number] = [-1, 1];
  const yPrice = scaleLinear(priceDomain, [plotH, 0]);
  const ySentiment = scaleLinear(sentimentDomain, [plotH, 0]);
  const xTicks = chooseTickIndices(rows, w < 640 ? 4 : 7);
  const priceTicks = Array.from({ length: 5 }, (_, i) => priceDomain[0] + ((priceDomain[1] - priceDomain[0]) * i) / 4);
  const sentimentTicks = [-1, -0.5, 0, 0.5, 1];
  const pricePts = points(rows, x, yPrice, "p");
  const sentPts = points(rows, x, ySentiment, "s");
  const maPts = points(rows, x, ySentiment, "m");
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const onMove = (event: React.MouseEvent<SVGRectElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = Math.max(0, Math.min(plotW, event.clientX - rect.left));
    const targetTime = x.invert(px);
    let best = 0;
    let bestDistance = Infinity;
    rows.forEach((row, index) => {
      const distance = Math.abs(row.t - targetTime);
      if (distance < bestDistance) {
        best = index;
        bestDistance = distance;
      }
    });
    setHoverIdx(best);
  };

  const hover = hoverIdx == null ? null : rows[hoverIdx];
  const hoverX = hover ? x(hover.t) : null;
  const labels = locale === "zh"
    ? { sentiment: "情绪", ma7: "MA7", price: "价格" }
    : { sentiment: "Sentiment", ma7: "MA7", price: "Price" };

  return (
    <svg width={w} height={h} className="block select-none overflow-visible" data-no-translate="true">
      <g transform={`translate(${pad.l},${pad.t})`}>
        {(kind === "price" ? priceTicks : sentimentTicks).map((tick, index) => {
          const yy = kind === "price" ? yPrice(tick) : ySentiment(tick);
          return <line key={`grid-${index}`} x1={0} y1={yy} x2={plotW} y2={yy} stroke="currentColor" className="text-white" strokeOpacity={0.07} />;
        })}

        {kind !== "price" ? sentimentTicks.map((tick) => (
          <text key={`sl-${tick}`} x={-12} y={ySentiment(tick)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>{tick.toFixed(1)}</text>
        )) : priceTicks.map((tick) => (
          <text key={`pl-${tick}`} x={-12} y={yPrice(tick)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>{niceNumber(tick, locale)}</text>
        ))}

        {kind === "overlay" ? priceTicks.map((tick) => (
          <text key={`pr-${tick}`} x={plotW + 10} y={yPrice(tick)} textAnchor="start" dominantBaseline="middle" className="fill-neutral-500" fontSize={11}>{niceNumber(tick, locale)}</text>
        )) : null}

        {xTicks.map((index, pos) => {
          const row = rows[index];
          const includeYear = pos === 0 || pos === xTicks.length - 1 || new Date(`${row.d}T00:00:00Z`).getUTCMonth() === 0;
          return (
            <g key={`xt-${index}`}>
              <line x1={x(row.t)} y1={0} x2={x(row.t)} y2={plotH} stroke="currentColor" className="text-white" strokeOpacity={0.035} />
              <text x={x(row.t)} y={plotH + 24} textAnchor="middle" className="fill-neutral-500" fontSize={11}>{formatDate(row.d, includeYear, locale)}</text>
            </g>
          );
        })}

        {kind === "overlay" || kind === "sentiment" ? (
          <>
            {sentPts.length ? <polyline points={polyline(sentPts)} fill="none" stroke="#a78bfa" strokeOpacity={0.32} strokeWidth={1.2} /> : null}
            {maPts.length ? <polyline points={polyline(maPts)} fill="none" stroke="#34d399" strokeWidth={2.2} /> : null}
          </>
        ) : null}
        {kind === "overlay" || kind === "price" ? (pricePts.length ? <polyline points={polyline(pricePts)} fill="none" stroke="#38bdf8" strokeWidth={2.2} /> : null) : null}

        <rect x={0} y={0} width={plotW} height={plotH} fill="transparent" onMouseMove={onMove} onMouseLeave={() => setHoverIdx(null)} style={{ cursor: "crosshair" }} />

        {hover && hoverX != null ? (
          <>
            <line x1={hoverX} y1={0} x2={hoverX} y2={plotH} stroke="currentColor" className="text-white" strokeOpacity={0.18} />
            <foreignObject x={Math.min(Math.max(hoverX + 12, 0), Math.max(0, plotW - 220))} y={8} width={220} height={108}>
              <div className="rounded-lg border border-white/10 bg-neutral-950/95 p-2.5 text-xs shadow-2xl">
                <div className="mb-1.5 font-medium text-neutral-200">{formatFullDate(hover.d, locale)}</div>
                {kind !== "price" ? <div className="flex justify-between gap-3 text-neutral-400"><span>{labels.sentiment}</span><span className="font-mono text-violet-300">{hover.s == null ? "—" : hover.s.toFixed(4)}</span></div> : null}
                {kind !== "price" ? <div className="flex justify-between gap-3 text-neutral-400"><span>{labels.ma7}</span><span className="font-mono text-emerald-300">{hover.m == null ? "—" : hover.m.toFixed(4)}</span></div> : null}
                {kind !== "sentiment" ? <div className="flex justify-between gap-3 text-neutral-400"><span>{labels.price}</span><span className="font-mono text-sky-300">{hover.p == null ? "—" : hover.p.toFixed(2)}</span></div> : null}
              </div>
            </foreignObject>
          </>
        ) : null}
      </g>
    </svg>
  );
}

export default function LineChart({ mode, dates, price, sentiment, sentimentMA7, height = 520 }: Props) {
  const { locale } = useLanguage();
  const rows = useMemo(() => buildRows(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);
  const { ref, width } = useMeasure();

  if (!rows.length) {
    return <div data-no-translate="true" className="grid w-full place-items-center text-sm text-neutral-500" style={{ height }}>{locale === "zh" ? "暂无图表数据。" : "No chart data."}</div>;
  }

  if (mode === "overlay") {
    return <div ref={ref} data-no-translate="true" className="w-full overflow-hidden"><ChartPanel width={width} height={height} rows={rows} kind="overlay" locale={locale} /></div>;
  }

  const panelHeight = Math.max(250, Math.floor((height - 28) / 2));
  return (
    <div ref={ref} data-no-translate="true" className="w-full space-y-7 overflow-hidden">
      <div>
        <div className="mb-2 text-xs font-medium uppercase tracking-[0.12em] text-neutral-500">{locale === "zh" ? "价格" : "Price"}</div>
        <ChartPanel width={width} height={panelHeight} rows={rows} kind="price" locale={locale} />
      </div>
      <div>
        <div className="mb-2 text-xs font-medium uppercase tracking-[0.12em] text-neutral-500">{locale === "zh" ? "情绪" : "Sentiment"}</div>
        <ChartPanel width={width} height={panelHeight} rows={rows} kind="sentiment" locale={locale} />
      </div>
    </div>
  );
}
