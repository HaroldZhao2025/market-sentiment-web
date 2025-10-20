"use client";

import { useMemo, useState } from "react";
import LineChart from "../../../components/LineChart";

export type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

export type NewsItem = { ts: string; title: string; url: string; text?: string };

function lastValid(arr: number[]) {
  for (let i = arr.length - 1; i >= 0; i--) {
    const v = arr[i];
    if (typeof v === "number" && Number.isFinite(v)) return v;
  }
  return 0;
}

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

  // 7-day MA for sentiment (client-side to keep writers simple)
  const ma7 = useMemo(() => {
    const s = series.sentiment ?? [];
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

  const sNow = lastValid(ma7.length ? ma7 : series.sentiment);
  const sLabel = sNow >= 0 ? "Positive" : "Negative";

  const rec =
    sNow >= 0.5 ? "Strong Buy" :
    sNow >= 0.1 ? "Buy" :
    sNow <= -0.5 ? "Strong Sell" :
    sNow <= -0.1 ? "Sell" : "Hold";

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <h1 className="text-3xl font-bold tracking-tight">Market Sentiment for {symbol}</h1>

      {/* toggle */}
      <div className="flex gap-2">
        <button
          className={`px-3 py-1 rounded-lg border transition ${mode === "separate" ? "bg-black text-white" : "bg-white hover:bg-neutral-50"}`}
          onClick={() => setMode("separate")}
        >
          Separate View
        </button>
        <button
          className={`px-3 py-1 rounded-lg border transition ${mode === "overlay" ? "bg-black text-white" : "bg-white hover:bg-neutral-50"}`}
          onClick={() => setMode("overlay")}
        >
          Overlayed View
        </button>
      </div>

      {/* chart card */}
      <div className="rounded-2xl p-5 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3 text-lg">Sentiment and Price Analysis</h3>
        <LineChart
          mode={mode}
          dates={series.date}
          price={series.price}
          sentiment={series.sentiment}
          sentimentMA7={ma7}
          height={400}
        />
      </div>

      {/* insights */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Live Market Sentiment</div>
          <div className="text-2xl font-semibold mt-1">
            {sLabel} <span className="text-neutral-500 text-base">({sNow.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Predicted Return</div>
          <div className="text-2xl font-semibold mt-1">
            {(sNow * 100).toFixed(2)}%
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Advisory Opinion</div>
          <div className="text-2xl font-semibold mt-1">
            {rec}
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Our Recommendation</div>
          <div className="text-2xl font-semibold mt-1">
            {sNow >= 0 ? "Buy" : "Hold"}
          </div>
        </div>
      </div>

      {/* news */}
      <div className="rounded-2xl p-5 shadow-sm border bg-white">
        <h3 className="font-semibold mb-1 text-lg">Recent Headlines for {symbol}</h3>
        <p className="text-sm text-neutral-500 mb-3">
          The table below shows the most recent headlines and the model’s aggregated sentiment.
        </p>
        {news?.length ? (
          <div className="divide-y">
            {news.slice(-30).reverse().map((n, i) => (
              <div key={i} className="py-2 flex items-start gap-3 text-sm">
                <div className="w-44 shrink-0 text-neutral-500">
                  {n.ts ? new Date(n.ts).toLocaleString() : ""}
                </div>
                <a className="underline hover:no-underline" href={n.url} target="_blank" rel="noreferrer">
                  {n.title}
                </a>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-neutral-500 text-sm">No recent headlines found.</div>
        )}
      </div>
    </div>
  );
}
