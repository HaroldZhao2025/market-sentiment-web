"use client";

import { useMemo, useState } from "react";
import LineChart, { ChartLegend } from "../../components/LineChart";

export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type View = "overlay" | "separate";

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}
function rollingObservedMean(values: number[], window = 7) {
  const observed: number[] = [];
  return values.map((raw) => {
    const value = finite(raw);
    if (value == null) return Number.NaN;
    observed.push(value);
    const slice = observed.slice(-window);
    return slice.length >= window ? slice.reduce((sum, item) => sum + item, 0) / slice.length : Number.NaN;
  });
}

export default function Sp500Client({ series }: { series: SeriesIn }) {
  const [mode, setMode] = useState<View>("overlay");
  const n = Math.min(series.date.length, series.price.length, series.sentiment.length);
  const dates = series.date.slice(0, n);
  const price = series.price.slice(0, n);
  const sentiment = series.sentiment.slice(0, n);
  const sentimentMA7 = useMemo(() => rollingObservedMean(sentiment), [sentiment]);

  if (n < 2) return <div className="card p-5 text-sm text-neutral-500">Not enough index history to render the chart.</div>;

  return <div className="space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <ChartLegend />
      <div className="flex rounded-xl border border-white/10 bg-black/30 p-1" role="tablist" aria-label="S&P 500 chart view">
        <button type="button" role="tab" aria-selected={mode === "overlay"} onClick={() => setMode("overlay")} className={`rounded-lg px-3 py-2 text-xs ${mode === "overlay" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>Overlay</button>
        <button type="button" role="tab" aria-selected={mode === "separate"} onClick={() => setMode("separate")} className={`rounded-lg px-3 py-2 text-xs ${mode === "separate" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>Separate</button>
      </div>
    </div>
    <div className="ambient-panel p-3 md:p-5"><LineChart mode={mode} dates={dates} price={price} sentiment={sentiment} sentimentMA7={sentimentMA7} height={520} /></div>
  </div>;
}