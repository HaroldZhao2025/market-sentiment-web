// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import { useMemo, useState } from "react";
import LineChart from "../../../components/LineChart";

export type SeriesIn = {
  date: string[];      // ISO dates
  price: number[];     // close prices
  sentiment: number[]; // daily S
};

export type NewsItem = { ts: string; title: string; url: string };

export default function TickerClient({
  symbol,
  series,
  news,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
}) {
  const [mode, setMode] = useState<"overlay" | "separate">("overlay");

  const ma7 = useMemo(() => {
    const s = series.sentiment || [];
    const out: number[] = [];
    let run = 0;
    for (let i = 0; i < s.length; i++) {
      const v = Number.isFinite(s[i]) ? s[i] : 0;
      run += v;
      if (i >= 7) run -= Number.isFinite(s[i - 7]) ? s[i - 7] : 0;
      out.push(i >= 6 ? run / 7 : NaN);
    }
    return out;
  }, [series.sentiment]);

  const lastS = series.sentiment?.[series.sentiment.length - 1] ?? 0;
  const lastMA = ma7?.[ma7.length - 1] ?? 0;

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">
          Market Sentiment for {symbol}
        </h1>
        <div className="inline-flex rounded-xl border overflow-hidden">
          <button
            className={`px-3 py-1 text-sm ${mode === "separate" ? "bg-black text-white" : "bg-white"}`}
            onClick={() => setMode("separate")}
          >
            Separate View
          </button>
          <button
            className={`px-3 py-1 text-sm border-l ${mode === "overlay" ? "bg-black text-white" : "bg-white"}`}
            onClick={() => setMode("overlay")}
          >
            Overlayed View
          </button>
        </div>
      </div>

      {/* Chart card */}
      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        <LineChart
          mode={mode}
          dates={series.date}
          price={series.price}
          sentiment={series.sentiment}
          sentimentMA7={ma7}
          height={420}
        />
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Live Market Sentiment</div>
          <div className={`text-2xl font-semibold mt-1 ${lastS >= 0 ? "text-emerald-600" : "text-rose-600"}`}>
            {lastS >= 0 ? "Positive" : "Negative"}
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Predicted Return</div>
          <div className="text-2xl font-semibold mt-1">
            {(lastMA * 100).toFixed(2)}%
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Our Recommendation</div>
          <div className="text-2xl font-semibold mt-1">
            {lastMA >= 0 ? "Buy" : "Hold"}
          </div>
        </div>
      </div>

      {/* News list */}
      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Recent Headlines for {symbol}</h3>
        <div className="space-y-2">
          {(news || []).slice(-20).reverse().map((n, i) => (
            <div key={i} className="text-sm">
              <span className="text-neutral-500 mr-2">
                {n.ts ? new Date(n.ts).toLocaleString() : ""}
              </span>
              <a className="underline" href={n.url} target="_blank" rel="noreferrer">
                {n.title}
              </a>
            </div>
          ))}
          {!news?.length && (
            <div className="text-neutral-500">No recent headlines found.</div>
          )}
        </div>
      </div>
    </div>
  );
}
