"use client";

import { useMemo, useState } from "react";
import LineChart from "../components/LineChart"; // ← fixed import path

type Props = { dates: string[]; sentiment: number[]; price?: number[] };

const ma7 = (arr: number[]) => {
  const out: number[] = []; let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] || 0); run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  } return out;
};

const label = (v: number) =>
  v >= 0.4 ? "Strong Positive" : v >= 0.1 ? "Positive" :
  v <= -0.4 ? "Strong Negative" : v <= -0.1 ? "Negative" : "Neutral";

export default function PortfolioClient({ dates, sentiment, price }: Props) {
  const [mode, setMode] = useState<"overlay" | "separate">("overlay");
  const sMA7 = useMemo(() => ma7(sentiment), [sentiment]);
  const lastS = Number(sentiment.at(-1) ?? 0);
  const lastMA = Number(sMA7.at(-1) ?? 0);

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">Market Sentiment — S&amp;P 500</h1>
        <div className="inline-flex items-center rounded-xl bg-neutral-100 p-1">
          <button className={`pill ${mode === "separate" ? "pill-active" : ""}`} onClick={() => setMode("separate")}>Separate View</button>
          <button className={`pill ${mode === "overlay" ? "pill-active" : ""}`} onClick={() => setMode("overlay")}>Overlayed View</button>
        </div>
      </div>

      <section className="card p-6">
        <h3 className="font-semibold mb-3">Aggregate Sentiment and Index Price</h3>
        <LineChart mode={mode} dates={dates} price={price} sentiment={sentiment} sentimentMA7={sMA7} height={480} />
      </section>

      <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="kpi">
          <div className="kpi-label">Live Market Sentiment</div>
          <div className="kpi-value">{label(lastS)} <span className="kpi-sub">({lastS.toFixed(2)})</span></div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Predicted Return</div>
          <div className="kpi-value">{(lastMA * 100).toFixed(2)}%</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Our Recommendation</div>
          <div className="kpi-value">{lastMA >= 0 ? "Buy" : "Hold"}</div>
        </div>
      </section>
    </div>
  );
}
