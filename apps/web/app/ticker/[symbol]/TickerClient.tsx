// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import { useMemo, useState } from "react";
import LineChart from "../../../components/LineChart";

export type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

export type NewsItem = { ts: string; title: string; url: string; text?: string };

export default function TickerClient({
  symbol,
  series,
  news,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
}) {
  const [overlayMode, setOverlayMode] = useState<"overlay" | "separate">(
    "overlay"
  );

  // Smooth MA7 (client side so we’re resilient)
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

  const lastS = useMemo(() => {
    for (let i = series.sentiment.length - 1; i >= 0; i--) {
      const v = series.sentiment[i];
      if (typeof v === "number" && Number.isFinite(v)) return v;
    }
    return 0;
  }, [series.sentiment]);

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
          height={380}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Live Market Sentiment</div>
          <div className="text-2xl font-semibold mt-1">
            {lastS >= 0 ? "Positive" : "Negative"}{" "}
            <span className="text-neutral-400 text-lg">({lastS.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Predicted Return</div>
          <div className="text-2xl font-semibold mt-1">
            {(ma7.at(-1) ?? 0).toFixed(2)}%
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Our Recommendation</div>
          <div className="text-2xl font-semibold mt-1">
            {(ma7.at(-1) ?? 0) >= 0 ? "Buy" : "Hold"}
          </div>
        </div>
      </div>

      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <h3 className="font-semibold mb-2">Recent Headlines for {symbol}</h3>
        <p className="text-xs text-neutral-500 mb-3">
          The list shows the most recent headlines; the sentiment score is computed from the text we gathered.
        </p>
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
