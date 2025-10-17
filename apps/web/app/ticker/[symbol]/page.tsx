"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";

const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), { ssr: false });

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

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

type TickerJSON = Record<string, any>;

function pickArr<T = number | string>(obj: TickerJSON, keys: string[], fallback: T[] = []) {
  for (const k of keys) {
    const v = obj?.[k];
    if (Array.isArray(v)) return v as T[];
  }
  return fallback;
}

export default function TickerClient({ symbol }: { symbol: string }) {
  const [data, setData] = useState<TickerJSON | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const url = `${BASE}/data/ticker/${symbol}.json`;
    fetch(url, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setErr(e.message));
  }, [symbol]);

  const series = useMemo(() => {
    const dates = pickArr<string>(data ?? {}, ["dates", "date", "DATE"], []);
    const price = pickArr<number>(data ?? {}, ["price", "close", "CLOSE"], []);
    const sRaw  = pickArr<number>(data ?? {}, ["S", "sentiment", "S_NEWS"], []);
    const sMa7  = pickArr<number>(data ?? {}, ["S_ma7", "S_MA7", "sentiment_ma7"], []);

    const n = Math.min(dates.length, price.length, sRaw.length);
    return {
      dates: dates.slice(0, n),
      price: price.slice(0, n),
      sRaw:  sRaw.slice(0, n),
      sMa7:  sMa7.length ? sMa7.slice(0, n) : undefined,
    };
  }, [data]);

  const hasData = series.dates.length > 0 && series.price.length > 0;

  const chartData = useMemo(() => {
    if (!hasData) return null;
    return {
      labels: series.dates,
      datasets: [
        { label: "Price", data: series.price, yAxisID: "y", borderWidth: 2, pointRadius: 0 },
        { label: "Sentiment", data: series.sRaw, yAxisID: "y1", borderWidth: 1, borderDash: [4, 3], pointRadius: 0 },
        ...(series.sMa7 ? [{ label: "Sentiment (7d MA)", data: series.sMa7, yAxisID: "y1", borderWidth: 2, pointRadius: 0 }] : []),
      ],
    };
  }, [hasData, series]);

  const options = useMemo(
    () => ({
      responsive: true,
      interaction: { mode: "index" as const, intersect: false },
      plugins: { legend: { display: true } },
      scales: {
        y: { type: "linear" as const, position: "left" as const, ticks: { callback: (v: any) => `$${v}` } },
        y1: { type: "linear" as const, position: "right" as const, grid: { drawOnChartArea: false }, min: -1, max: 1 },
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
          ← Home
        </Link>
      </div>

      {err && <div className="text-sm text-red-600">Failed to load: {err}</div>}

      {!err && !hasData && (
        <div className="text-sm text-neutral-500">
          No series found for this ticker (check JSON at <code>{BASE}/data/ticker/{symbol}.json</code>).
        </div>
      )}

      {hasData && chartData && (
        <>
          <div className="text-xs text-neutral-500">
            Loaded {series.dates.length} points from <code>{BASE}/data/ticker/{symbol}.json</code>
          </div>
          <div className="rounded-2xl border border-neutral-200 p-4">
            <Line data={chartData} options={options} />
          </div>
        </>
      )}
    </div>
  );
}
