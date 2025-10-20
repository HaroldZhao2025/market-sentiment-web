"use client";

import { useMemo, useState } from "react";
import LineChart from "../../components/LineChart";

type Props = {
  dates: string[];
  sentiment: number[];
  price?: number[];           // optional – if you later add an index price
};

export default function PortfolioClient({ dates, sentiment, price = [] }: Props) {
  const [mode, setMode] = useState<"overlay" | "separate">("overlay");

  const ma7 = useMemo(() => {
    const s = sentiment ?? [];
    const out: number[] = [];
    let run = 0;
    for (let i = 0; i < s.length; i++) {
      const v = Number.isFinite(s[i]) ? s[i] : 0;
      run += v;
      if (i >= 7) run -= (Number.isFinite(s[i - 7]) ? s[i - 7] : 0);
      out.push(i >= 6 ? run / 7 : NaN);
    }
    return out;
  }, [sentiment]);

  const sNow = Number(sentiment.at(-1) ?? 0);
  const sNowFmt = Number.isFinite(sNow) ? sNow.toFixed(2) : "0.00";

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">S&amp;P 500 — Aggregate Sentiment</h1>
        <div className="flex gap-2">
          <button
            className={`px-3 py-1 rounded-lg border ${mode === "separate" ? "bg-black text-white" : ""}`}
            onClick={() => setMode("separate")}
          >
            Separate View
          </button>
          <button
            className={`px-3 py-1 rounded-lg border ${mode === "overlay" ? "bg-black text-white" : ""}`}
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
          dates={dates}
          price={price}
          sentiment={sentiment}
          sentimentMA7={ma7}
          height={420}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 font-medium">Live Market Sentiment</div>
          <div className={`text-3xl font-semibold mt-1 ${sNow >= 0 ? "text-emerald-600" : "text-rose-600"}`}>
            {sNow >= 0 ? "Positive" : "Negative"} <span className="text-neutral-500 text-lg">({sNowFmt})</span>
          </div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 font-medium">Predicted Return</div>
          <div className="text-3xl font-semibold mt-1">{(ma7.at(-1) ?? 0).toFixed(2)}%</div>
        </div>
        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 font-medium">Our Recommendation</div>
          <div className="text-3xl font-semibold mt-1">{(ma7.at(-1) ?? 0) >= 0 ? "Buy" : "Hold"}</div>
        </div>
      </div>
    </div>
  );
}
