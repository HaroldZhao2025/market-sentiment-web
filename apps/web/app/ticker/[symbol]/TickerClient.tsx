"use client";

import { useMemo, useState } from "react";
import LineChart from "../../../components/LineChart";

export type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

export type NewsItem = { ts: string; title: string; url: string; text?: string };

function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    run += Number(arr[i] || 0);
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}

function trendWord(v: number) {
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}

function recommend(v: number) {
  if (v >= 0.4) return "Strong Buy";
  if (v >= 0.1) return "Buy";
  if (v <= -0.4) return "Strong Sell";
  if (v <= -0.1) return "Sell";
  return "Hold";
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
  const sMA7 = useMemo(() => ma7(series.sentiment), [series.sentiment]);

  const lastS = Number(series.sentiment.at(-1) ?? 0);
  const lastMA = Number(sMA7.at(-1) ?? 0);

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Market Sentiment for {symbol}</h1>
        <div className="inline-flex rounded-lg border overflow-hidden">
          <button
            className={`px-3 py-1 text-sm ${mode === "separate" ? "bg-black text-white" : "bg-white"}`}
            onClick={() => setMode("separate")}
          >
            Separate View
          </button>
          <button
            className={`px-3 py-1 text-sm ${mode === "overlay" ? "bg-black text-white" : "bg-white"}`}
            onClick={() => setMode("overlay")}
          >
            Overlayed View
          </button>
        </div>
      </div>

      <div className="rounded-2xl p-5 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        <LineChart
          mode={mode}
          dates={series.date}
          price={series.price}
          sentiment={series.sentiment}
          sentimentMA7={sMA7}
          height={400}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Live Market Sentiment</div>
          <div className="text-2xl font-semibold">
            {trendWord(lastS)}{" "}
            <span className="text-neutral-500 text-lg align-middle">({lastS.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Predicted Return</div>
          <div className="text-2xl font-semibold">{(lastMA * 100).toFixed(2)}%</div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Advisory Opinion</div>
          <div className="text-2xl font-semibold">{recommend(lastMA)}</div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Our Recommendation</div>
          <div className="text-2xl font-semibold">{lastMA >= 0 ? "Buy" : "Hold"}</div>
        </div>
      </div>

      <div className="rounded-2xl p-5 shadow-sm border bg-white">
        <h3 className="font-semibold mb-2">Recent Headlines for {symbol}</h3>
        {news?.length ? (
          <ul className="space-y-2">
            {news.slice(0, 30).map((n, i) => (
              <li key={i} className="text-sm leading-6">
                <span className="text-neutral-500 mr-2">
                  {new Date(n.ts).toLocaleString()}
                </span>
                <a className="underline decoration-dotted underline-offset-2" href={n.url} target="_blank" rel="noreferrer">
                  {n.title}
                </a>
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-neutral-500">No recent headlines found.</div>
        )}
      </div>
    </div>
  );
}
