"use client";

import { useMemo, useState } from "react";
import LineChart, { ChartLegend } from "../../../components/LineChart";

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
    const v = Number(arr[i] || 0);
    run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}

function label(v: number) {
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}
function recommendation(v: number) {
  if (v >= 0.4) return "Strong Buy";
  if (v >= 0.1) return "Buy";
  if (v <= -0.4) return "Strong Sell";
  if (v <= -0.1) return "Sell";
  return "Hold";
}

type View = "overlay" | "sentiment" | "price";

export default function TickerClient({
  symbol,
  series,
  news,
  newsTotal = 0,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
  newsTotal?: number;
}) {
  // Three-button control:
  //   - overlay: both series on the same panel
  //   - sentiment: separate view, sentiment panel only
  //   - price: separate view, price panel only
  const [view, setView] = useState<View>("overlay");

  const sMA7 = useMemo(() => ma7(series.sentiment), [series.sentiment]);
  const lastS = Number(series.sentiment.at(-1) ?? 0);
  const lastMA = Number(sMA7.at(-1) ?? 0);

  // Derive what gets passed to the chart without changing its API.
  // We keep your LineChart "mode" contract: "overlay" | "separate".
  const chartMode = view === "overlay" ? "overlay" : "separate";

  // To hide one series in "separate", pass NaNs for the hidden one.
  const priceForChart =
    view === "sentiment" ? series.price.map(() => NaN) : series.price;
  const sentimentForChart =
    view === "price" ? series.sentiment.map(() => NaN) : series.sentiment;
  const sMA7ForChart = view === "price" ? sMA7.map(() => NaN) : sMA7;

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">
          Market Sentiment for {symbol}
        </h1>

        {/* Three-button segmented control */}
        <div className="inline-flex items-center rounded-xl bg-neutral-100 p-1">
          <button
            className={`px-3 py-1.5 text-sm rounded-lg transition ${
              view === "overlay" ? "bg-black text-white shadow-sm" : "text-neutral-700"
            }`}
            onClick={() => setView("overlay")}
          >
            Overlay
          </button>
          <button
            className={`px-3 py-1.5 text-sm rounded-lg transition ${
              view === "sentiment" ? "bg-black text-white shadow-sm" : "text-neutral-700"
            }`}
            onClick={() => setView("sentiment")}
          >
            Sentiment
          </button>
          <button
            className={`px-3 py-1.5 text-sm rounded-lg transition ${
              view === "price" ? "bg-black text-white shadow-sm" : "text-neutral-700"
            }`}
            onClick={() => setView("price")}
          >
            Price
          </button>
        </div>
      </div>

      {/* Chart Card */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white">
        <div className="flex items-center justify-between gap-4 mb-4">
          <h3 className="font-semibold">Sentiment &amp; Price</h3>
          <ChartLegend />
        </div>

        <LineChart
          mode={chartMode as "overlay" | "separate"}
          dates={series.date}
          price={priceForChart}
          sentiment={sentimentForChart}
          sentimentMA7={sMA7ForChart}
          height={520}
        />
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Live Market Sentiment</div>
          <div className="text-2xl md:text-3xl font-semibold">
            {label(lastS)}{" "}
            <span className="text-neutral-500 text-lg">({lastS.toFixed(4)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Predicted Return</div>
          <div className="text-2xl md:text-3xl font-semibold">
            {(lastMA * 100).toFixed(4)}%
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Advisory Opinion</div>
          <div className="text-2xl md:text-3xl font-semibold">
            {recommendation(lastMA)}
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">News Items (period)</div>
          <div className="text-2xl md:text-3xl font-semibold">
            {newsTotal.toLocaleString()}
          </div>
        </div>
      </div>

      {/* Headlines */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white">
        <div className="flex items-baseline justify-between">
          <h3 className="font-semibold mb-1">Recent Headlines for {symbol}</h3>
          <p className="text-xs text-neutral-500">
            The list shows up to 10 latest headlines; daily sentiment uses all news in the period (Finnhub + Yahoo Finance).
          </p>
        </div>
        {news?.length ? (
          <ul className="mt-2 space-y-2">
            {news.slice(0, 10).map((n, i) => (
              <li key={i} className="text-sm leading-6">
                <span className="text-neutral-500 mr-2">
                  {new Date(n.ts).toLocaleString()}
                </span>
                <a
                  className="underline decoration-dotted underline-offset-2"
                  href={n.url}
                  target="_blank"
                  rel="noreferrer"
                >
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
