"use client";

import React, { useMemo, useState } from "react";
import LineChart, { SeriesIn } from "@/components/LineChart";
import Link from "next/link";

export type NewsItem = {
  ts: string;
  title: string;
  url: string;
  s?: number | null;
  source?: string | null;
};

type Props = {
  symbol: string;
  series: SeriesIn | null;
  news: NewsItem[];
};

export default function TickerClient({ symbol, series, news }: Props) {
  const [overlay, setOverlay] = useState(true);

  const stats = useMemo(() => {
    if (!series) return null;
    const s = series.sentiment.filter((x) => Number.isFinite(x));
    const last = s.at(-1) ?? 0;
    const mean =
      s.length ? s.reduce((a, b) => a + b, 0) / s.length : 0;
    const tone = last > 0.1 ? "Positive" : last < -0.1 ? "Negative" : "Neutral";
    return { last, mean, tone };
  }, [series]);

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">Market Sentiment for {symbol}</h1>
        <div className="space-x-2">
          <button
            className={`px-3 py-1 rounded-2xl border ${!overlay ? "bg-black text-white" : ""}`}
            onClick={() => setOverlay(false)}
          >
            Separate View
          </button>
          <button
            className={`px-3 py-1 rounded-2xl border ${overlay ? "bg-black text-white" : ""}`}
            onClick={() => setOverlay(true)}
          >
            Overlayed View
          </button>
        </div>
      </div>

      <div className="rounded-2xl p-4 shadow-sm border mb-6">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        {series ? (
          <LineChart
            series={{
              ...series,
              // simple switch: overlayed == show both; separate == still draw both but you could hide one set here
              sentimentLabel: "Sentiment Score",
              priceLabel: "Stock Price",
            }}
            height={420}
          />
        ) : (
          <div className="text-neutral-500">No data.</div>
        )}
      </div>

      {stats && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="rounded-2xl p-4 shadow-sm border">
            <div className="text-sm text-neutral-500">Live Market Sentiment</div>
            <div className="text-2xl font-semibold mt-1">{stats.tone}</div>
          </div>
          <div className="rounded-2xl p-4 shadow-sm border">
            <div className="text-sm text-neutral-500">Avg Sentiment (All)</div>
            <div className="text-2xl font-semibold mt-1">{stats.mean.toFixed(2)}</div>
          </div>
          <div className="rounded-2xl p-4 shadow-sm border">
            <div className="text-sm text-neutral-500">Latest Score</div>
            <div className="text-2xl font-semibold mt-1">{stats.last.toFixed(2)}</div>
          </div>
          <div className="rounded-2xl p-4 shadow-sm border">
            <div className="text-sm text-neutral-500">Headlines Covered</div>
            <div className="text-2xl font-semibold mt-1">{news.length}</div>
          </div>
        </div>
      )}

      <div className="rounded-2xl p-4 shadow-sm border">
        <h3 className="font-semibold mb-3">Recent Headlines for {symbol}</h3>
        <div className="text-sm text-neutral-500 mb-2">
          The table lists recent headlines with aggregated daily sentiment.
        </div>
        <div className="divide-y">
          {news.slice(0, 50).map((n, i) => (
            <div key={i} className="py-2 flex items-center justify-between">
              <div className="text-sm w-40 shrink-0 text-neutral-600">{new Date(n.ts).toLocaleString()}</div>
              <div className="px-3 text-sm grow">
                <a className="underline" href={n.url} target="_blank" rel="noreferrer">
                  {n.title}
                </a>
              </div>
              <div className="w-20 text-right text-sm">{Number.isFinite(n.s) ? n.s!.toFixed(2) : "—"}</div>
            </div>
          ))}
          {!news.length && <div className="py-6 text-neutral-500">No headlines were captured.</div>}
        </div>
      </div>

      <div className="mt-8 text-sm">
        <Link className="underline" href="/">Back to home</Link>
      </div>
    </div>
  );
}
