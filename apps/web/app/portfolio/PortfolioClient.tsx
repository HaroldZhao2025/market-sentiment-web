// apps/web/app/portfolio/PortfolioClient.tsx
"use client";

import dynamic from "next/dynamic";
import { useMemo } from "react";

// Recharts must render client-side to avoid GH Pages SSR/hydration glitches
const OverviewChart = dynamic(() => import("../../components/OverviewChart"), {
  ssr: false,
});

type Props = {
  dates: string[];
  sentiment: number[];
  price?: number[];
};

function movingAvg(vals: number[], n = 7): number[] {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < vals.length; i++) {
    const v = Number.isFinite(vals[i]) ? vals[i] : 0;
    run += v;
    if (i >= n) run -= Number.isFinite(vals[i - n]) ? vals[i - n] : 0;
    out.push(i >= n - 1 ? run / n : NaN);
  }
  return out;
}

function advisoryText(x: number) {
  if (x >= 0.5) return "Strong Buy";
  if (x >= 0.15) return "Buy";
  if (x <= -0.5) return "Strong Sell";
  if (x <= -0.15) return "Sell";
  return "Neutral";
}

export default function PortfolioClient({ dates, sentiment, price }: Props) {
  const sMA7 = useMemo(() => movingAvg(sentiment, 7), [sentiment]);

  const live = Number.isFinite(sMA7.at(-1)!) ? (sMA7.at(-1) as number)
              : Number.isFinite(sentiment.at(-1)!) ? (sentiment.at(-1) as number)
              : 0;

  const liveLabel = live >= 0 ? "Positive" : "Negative";
  const predictedPct = (live * 100).toFixed(2);

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 space-y-6">
      <h1 className="text-2xl font-semibold tracking-tight">Market Sentiment — Portfolio</h1>

      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        <div className="flex items-center justify-between mb-3">
          <h3 className="font-semibold">Sentiment and Index Price</h3>
          <div className="flex gap-2">
            <button className="px-3 py-1 rounded-lg border">Separate View</button>
            <button className="px-3 py-1 rounded-lg border bg-black text-white">
              Overlayed View
            </button>
          </div>
        </div>

        {dates?.length ? (
          <OverviewChart dates={dates} sentiment={sentiment} price={price} />
        ) : (
          <div className="text-sm text-neutral-500">No portfolio data yet.</div>
        )}
      </div>

      {/* Insight cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Live Market Sentiment</div>
          <div className="text-2xl font-semibold mt-1">
            {liveLabel} <span className="text-neutral-500 font-normal">({live.toFixed(2)})</span>
          </div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Predicted Return</div>
          <div className="text-2xl font-semibold mt-1">{predictedPct}%</div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Advisory Opinion</div>
          <div className="text-2xl font-semibold mt-1">{advisoryText(live)}</div>
        </div>
        <div className="rounded-2xl p-4 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500">Our Recommendation</div>
          <div className="text-2xl font-semibold mt-1">
            {live >= 0.15 ? "Buy" : live <= -0.15 ? "Sell" : "Hold"}
          </div>
        </div>
      </div>

      <div className="rounded-2xl p-4 shadow-sm border bg-white text-sm text-neutral-600">
        The portfolio sentiment is bounded in [-1, 1]. If available, the chart overlays SPY (or
        ^GSPC) price on the right axis for context.
      </div>
    </div>
  );
}
