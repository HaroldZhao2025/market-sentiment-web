"use client";

import { useMemo, useState } from "react";
import LineChart from "../../components/LineChart"; // ← moved under app/components

export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
export type NewsItem = { ts: string; title: string; url: string; text?: string };

function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] || 0);
    run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}

const label = (v: number) =>
  v >= 0.4 ? "Strong Positive" :
  v >= 0.1 ? "Positive" :
  v <= -0.4 ? "Strong Negative" :
  v <= -0.1 ? "Negative" : "Neutral";

const recommendation = (v: number) =>
  v >= 0.4 ? "Strong Buy" :
  v >= 0.1 ? "Buy" :
  v <= -0.4 ? "Strong Sell" :
  v <= -0.1 ? "Sell" : "Hold";

export default function TickerClient({
  symbol,
  series,
  news,
}: { symbol: string; series: SeriesIn; news: NewsItem[] }) {
  const [mode, setMode] = useState<"overlay" | "separate">("overlay");
  const sMA7 = useMemo(() => ma7(series.sentiment), [series.sentiment]);
  const lastS = Number(series.sentiment.at(-1) ?? 0);
  const lastMA = Number(sMA7.at(-1) ?? 0);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">Market Sentiment for {symbol}</h1>
        <div className="inline-flex items-center rounded-xl bg-neutral-100 p-1">
          <button className={`px-3 py-1.5 text-sm rounded-lg transition ${mode === "separate" ? "bg-black text-white shadow-sm" : "text-neutral-700"}`} onClick={() => setMode("separate")}>
            Separate View
          </button>
          <button className={`px-3 py-1.5 text-sm rounded-lg transition ${mode === "overlay" ? "bg-black text-white shadow-sm" : "text-neutral-700"}`} onClick={() => setMode("overlay")}>
            Overlayed View
          </button>
        </div>
      </div>

      <section className="card p-6">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        <LineChart mode={mode} dates={series.date} price={series.price} sentiment={series.sentiment} sentimentMA7={sMA7} height={480} />
      </section>

      <section className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="kpi"><div className="text-sm text-neutral-500 mb-1">Live Market Sentiment</div><div className="text-2xl md:text-3xl font-semibold">{label(lastS)} <span className="text-neutral-500 text-lg">({lastS.toFixed(2)})</span></div></div>
        <div className="kpi"><div className="text-sm text-neutral-500 mb-1">Predicted Return</div><div className="text-2xl md:text-3xl font-semibold">{(lastMA * 100).toFixed(2)}%</div></div>
        <div className="kpi"><div className="text-sm text-neutral-500 mb-1">Advisory Opinion</div><div className="text-2xl md:text-3xl font-semibold">{recommendation(lastMA)}</div></div>
        <div className="kpi"><div className="text-sm text-neutral-500 mb-1">Our Recommendation</div><div className="text-2xl md:text-3xl font-semibold">{lastMA >= 0 ? "Buy" : "Hold"}</div></div>
      </section>

      <section className="card p-6">
        <div className="flex items-baseline justify-between">
          <h3 className="font-semibold mb-1">Recent Headlines for {symbol}</h3>
          <p className="text-xs text-neutral-500">Latest 10 headlines are shown here; the chart uses all headlines across the full period.</p>
        </div>
        {news?.length ? (
          <ul className="mt-2 space-y-2">
            {news.slice(0, 10).map((n, i) => (
              <li key={i} className="text-sm leading-6 hl">
                <span className="text-neutral-500 mr-2">{new Date(n.ts).toLocaleString()}</span>
                <a href={n.url} target="_blank" rel="noreferrer">{n.title}</a>
              </li>
            ))}
          </ul>
        ) : (<div className="text-neutral-500">No recent headlines found.</div>)}
      </section>
    </div>
  );
}
