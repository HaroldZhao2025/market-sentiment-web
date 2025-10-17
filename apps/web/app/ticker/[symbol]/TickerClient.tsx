// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import { useEffect, useMemo, useState } from "react";
import { assetPath } from "../../../lib/paths";
import Link from "next/link";
import dynamic from "next/dynamic";

// Lazy-load chart (SSR off)
const Line = dynamic(() => import("react-chartjs-2").then(m => m.Line), {
  ssr: false,
});

// Register Chart.js parts at runtime in client
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
} from "chart.js";
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

type TickerJSON = {
  dates?: string[];        // ISO
  date?: string[];         // fallback
  price?: number[];        // close
  close?: number[];        // fallback
  S?: number[];            // combined sentiment
  S_news?: number[];       // optional
  S_earn?: number[];       // optional
  S_ma7?: number[];        // optional
  sentiment?: number[];    // fallback
  sentiment_ma7?: number[];// fallback
};

function coalesce<T>(...xs: (T | undefined)[]): T | undefined {
  for (const x of xs) if (x !== undefined) return x;
}

export default function TickerClient({ symbol }: { symbol: string }) {
  const [data, setData] = useState<TickerJSON | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const url = assetPath(`/data/ticker/${symbol}.json`);
    fetch(url)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((j) => setData(j))
      .catch((e) => setErr(e.message));
  }, [symbol]);

  const series = useMemo(() => {
    const dates =
      (data?.dates ?? data?.date ?? []) as string[];
    const price =
      (data?.price ?? data?.close ?? []) as number[];
    const sRaw =
      (data?.S ?? data?.sentiment ?? []) as number[];
    const sMa7 =
      (data?.S_ma7 ?? data?.sentiment_ma7 ?? []) as number[];

    const n = Math.min(dates.length, price.length, sRaw.length || Infinity);
    return {
      dates: dates.slice(0, n),
      price: price.slice(0, n),
      sRaw: sRaw.slice(0, n),
      sMa7: sMa7.length ? sMa7.slice(0, n) : undefined,
    };
  }, [data]);

  const hasData = series.dates.length > 0 && series.price.length > 0;

  const chartData = useMemo(() => {
    if (!hasData) return null;
    return {
      labels: series.dates,
      datasets: [
        {
          label: "Price",
          data: series.price,
          yAxisID: "y",
          borderWidth: 2,
          pointRadius: 0,
        },
        {
          label: "Sentiment",
          data: series.sRaw,
          yAxisID: "y1",
          borderWidth: 1,
          borderDash: [4, 3],
          pointRadius: 0,
        },
        ...(series.sMa7
          ? [
              {
                label: "Sentiment (7d MA)",
                data: series.sMa7,
                yAxisID: "y1",
                borderWidth: 2,
                pointRadius: 0,
              },
            ]
          : []),
      ],
    };
  }, [hasData, series]);

  const options = useMemo(
    () => ({
      responsive: true,
      interaction: { mode: "index" as const, intersect: false },
      plugins: { legend: { display: true } },
      scales: {
        y: {
          type: "linear" as const,
          position: "left" as const,
          ticks: { callback: (v: any) => `$${v}` },
        },
        y1: {
          type: "linear" as const,
          position: "right" as const,
          grid: { drawOnChartArea: false },
          min: -1,
          max: 1,
        },
        x: { display: true },
      },
    }),
    []
  );

  return (
    <div className="max-w-6xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">{symbol}</h1>
        <Link href="/" className="text-sm underline hover:no-underline">
          ← Back
        </Link>
      </div>

      {err && <div className="text-sm text-red-600">Failed to load: {err}</div>}

      {!err && !hasData && (
        <div className="text-sm text-neutral-500">
          No series found for this ticker.
        </div>
      )}

      {hasData && chartData && (
        <div className="rounded-2xl border border-neutral-200 p-4">
          <Line data={chartData} options={options} />
        </div>
      )}
    </div>
  );
}
