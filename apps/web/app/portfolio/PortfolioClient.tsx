"use client";

import { useMemo, useState } from "react";
import LineChart from "../../components/LineChart";

type Props = { dates: string[]; sentiment: number[]; price?: number[] };

const ma7 = (arr: number[]) => {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] || 0);
    run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
};

const label = (v: number) =>
  v >= 0.4 ? "Strong Positive" : v >= 0.1 ? "Positive" : v <= -0.4 ? "Strong Negative" : v <= -0.1 ? "Negative" : "Neutral";

export default function PortfolioClient({ dates, sentiment, price }: Props) {
  const [mode, setMode] = useState<"overlay" | "separate">("overlay");
  const sMA7 = useMemo(() => ma7(sentiment), [sentiment]);
  const lastS = Number(sentiment.at(-1) ?? 0);
  const lastMA = Number(sMA7.at(-1) ?? 0);

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">S&amp;P 500 — Aggregate Sentiment</h1>
        <div className="inline-flex rounded-lg border overflow-hidden">
          <button className={`px-3 py-1 text-sm ${mode === "separate" ? "bg-black text-white" : "bg-white"}`} onClick={() => setMode("separate")}>
            Separate View
          </button>
          <button className={`px-3 py-1 text-sm ${mode === "overlay" ? "bg-black text-white" : "bg-white"}`} onClick={() => setMode("overlay")}>
            Overlayed View
          </button>
        </div>
      </div>

      <div className="rounded-2xl p-5 shadow-sm border bg-white">
        <h3 className="font-semibold mb-3">Sentiment and Index Price</h3>
        <LineChart mode={mode} dates={dates} price={price} sentiment={sentiment} sentimentMA7={sMA7} height={420} />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Live Market Sentiment</div>
          <div className="text-2xl font-semibold">
            {label(lastS)} <span className="text-neutral-500 text-lg">({lastS.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Predicted Return</div>
          <div className="text-2xl font-semibold">{(lastMA * 100).toFixed(2)}%</div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Our Recommendation</div>
          <div className="text-2xl font-semibold">{lastMA >= 0 ? "Buy" : "Hold"}</div>
        </div>
      </div>
    </div>
  );
}
