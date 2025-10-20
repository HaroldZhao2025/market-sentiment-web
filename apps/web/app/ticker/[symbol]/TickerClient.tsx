// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import dynamic from "next/dynamic";
import { useMemo, useState } from "react";

const LineChart = dynamic(() => import("../../../components/LineChart"), { ssr: false });

export type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
};

export type NewsItem = { ts: string; title: string; url: string };

function lastNumber(arr: number[] | undefined, fallback = 0): number {
  if (!arr || arr.length === 0) return fallback;
  const v = arr[arr.length - 1];
  return Number.isFinite(v) ? v : fallback;
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
  const [overlayMode, setOverlayMode] = useState<"overlay" | "separate">("overlay");

  const ma7 = useMemo(() => {
    if (series.sentiment_ma7 && series.sentiment_ma7.length) return series.sentiment_ma7;
    // compute if not provided
    const s = series.sentiment ?? [];
    const out: number[] = [];
    let run = 0;
    for (let i = 0; i < s.length; i++) {
      const v = Number.isFinite(s[i]) ? s[i] : 0;
      run += v;
      if (i >= 6) run -= Number.isFinite(s[i - 6]) ? s[i - 6] : 0;
      out.push(i >= 6 ? run / 7 : NaN);
    }
    return out;
  }, [series.sentiment, series.sentiment_ma7]);

  const live = lastNumber(ma7, lastNumber(series.sentiment, 0));
  const liveLabel = live >= 0 ? "Positive" : "Negative";

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <h1 className="text-2xl font-semibold tracking-tight">Market Sentiment for {symbol}</h1>

      <div className="flex gap-2">
        <button
          className={`px-3 py-1 rounded-lg border ${overlayMode === "separate" ? "bg-black text-white" : ""}`}
          onClick={() => setOverlayMode("separate")}
        >
          Separate View
        </button>
        <button
          className={`px-3 py-1 rounded-lg border ${overlayMode === "overlay" ? "bg-black text-white" : ""}`}
          onClick={() => setOverlayMode("overlay")}
        >
          Overlayed View
        </button>
      </div>

      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        <LineChart
          mode={overlayMode}
          dates={series.date}
          price={series.price}
          sentiment={series.sentiment}
          sentimentMA7={ma7}
          height={420}
        />
      </div>

      {/* Insight cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Live Market Sentiment</div>
          <div className="text-2xl font-semibold mt-1">
            {liveLabel} <span className="text-neutral-500 font-normal">({live.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Predicted Return</div>
          <div className="text-2xl font-semibold mt-1">{(live * 100).toFixed(2)}%</div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Our Recommendation</div>
          <div className="text-2xl font-semibold mt-1">{live >= 0 ? "Buy" : "Hold"}</div>
        </div>
      </div>

      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Recent Headlines for {symbol}</h3>
        <div className="space-y-2">
          {Array.isArray(news) && news.length > 0 ? (
            news
              .slice(-20)
              .reverse()
              .map((n, i) => (
                <div key={i} className="text-sm">
                  <span className="text-neutral-500 mr-2">
                    {(() => {
                      try {
                        return new Date(n.ts).toLocaleString();
                      } catch {
                        return n.ts;
                      }
                    })()}
                  </span>
                  <a className="underline" href={n.url} target="_blank" rel="noreferrer">
                    {n.title}
                  </a>
                </div>
              ))
          ) : (
            <div className="text-neutral-500">No recent headlines found.</div>
          )}
        </div>
      </div>
    </div>
  );
}
