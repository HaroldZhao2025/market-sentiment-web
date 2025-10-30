// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import { useEffect, useMemo, useState } from "react";

type NewsItem = {
  ts?: string | number;
  title?: string;
  url?: string;
  source?: string;
  provider?: string;
};

type TickerData = {
  ticker: string;
  dates: string[];
  sentiment?: number[]; // daily sentiment series
  S?: number[];         // alias (kept for compatibility)
  price?: number[];     // optional: close prices (if present)
  news?: NewsItem[];
  news_count?: { total?: number; finnhub?: number; yfinance?: number };
};

type ViewMode = "overlay" | "price" | "sentiment";

function basePath(): string {
  // Next will inline the basePath at build time via process.env if present
  const env = process.env.NEXT_PUBLIC_BASE_PATH;
  if (env && env !== "/") return env;
  // Fallback: detect when running from a subpath on Pages
  if (typeof window !== "undefined") {
    const m = window.location.pathname.match(/^\/([^/]+)\//);
    if (m && m[1]) return `/${m[1]}`;
  }
  return "";
}

function toDateOnly(ts?: string | number): string {
  if (ts == null) return "";
  const d = new Date(ts);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  // Fallback if provider gave a string date already
  const s = String(ts);
  // Try to extract YYYY-MM-DD
  const m = s.match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : s.slice(0, 10);
}

export default function TickerClient({ symbol }: { symbol: string }) {
  const [data, setData] = useState<TickerData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [mode, setMode] = useState<ViewMode>("overlay");

  useEffect(() => {
    let aborted = false;
    const bp = basePath();
    const url = `${bp}/data/ticker/${symbol}.json`;
    (async () => {
      try {
        const r = await fetch(url, { cache: "no-store" });
        if (!r.ok) throw new Error(`Failed to fetch ${url} (${r.status})`);
        const obj = (await r.json()) as TickerData;
        if (!aborted) setData(obj);
      } catch (e: any) {
        if (!aborted) setError(e?.message || "Failed to load data.");
      }
    })();
    return () => {
      aborted = true;
    };
  }, [symbol]);

  const series = useMemo(() => {
    if (!data) return { dates: [] as string[], sentiment: [] as number[], price: [] as number[] };
    const dates = data.dates || [];
    const sentiment = (data.sentiment && data.sentiment.length ? data.sentiment : data.S) || [];
    const price = data.price || [];
    return { dates, sentiment, price };
  }, [data]);

  const hasPrice = series.price && series.price.length === series.dates.length && series.dates.length > 0;
  const hasSent = series.sentiment && series.sentiment.length === series.dates.length && series.dates.length > 0;

  // Normalize a series to [0,1] for overlay plotting
  function normalize(xs: number[]) {
    if (!xs || xs.length === 0) return xs;
    const lo = Math.min(...xs);
    const hi = Math.max(...xs);
    if (hi === lo) return xs.map(() => 0.5);
    return xs.map((v) => (v - lo) / (hi - lo));
  }

  if (error) {
    return (
      <div className="mt-4 rounded border border-red-200 bg-red-50 p-3 text-red-700">
        {error}
      </div>
    );
  }
  if (!data) {
    return <p className="mt-4 text-gray-500">Loading…</p>;
  }

  const dates = series.dates;
  const sentN = normalize(series.sentiment || []);
  const priceN = normalize(series.price || []);

  // Decide what to draw
  const drawPrice = hasPrice && (mode === "overlay" || mode === "price");
  const drawSent = hasSent && (mode === "overlay" || mode === "sentiment");

  return (
    <div className="mt-4 flex flex-col gap-6">
      {/* View selector */}
      <div className="inline-flex overflow-hidden rounded-xl border">
        <button
          className={`px-4 py-2 ${mode === "overlay" ? "bg-gray-900 text-white" : "bg-white"}`}
          onClick={() => setMode("overlay")}
        >
          Overlay
        </button>
        {hasPrice && (
          <button
            className={`px-4 py-2 border-l ${mode === "price" ? "bg-gray-900 text-white" : "bg-white"}`}
            onClick={() => setMode("price")}
          >
            Price
          </button>
        )}
        {hasSent && (
          <button
            className={`px-4 py-2 border-l ${mode === "sentiment" ? "bg-gray-900 text-white" : "bg-white"}`}
            onClick={() => setMode("sentiment")}
          >
            Sentiment
          </button>
        )}
      </div>

      {/* Tiny SVG chart (responsive) */}
      <Chart
        dates={dates}
        price={drawPrice ? priceN : undefined}
        sentiment={drawSent ? sentN : undefined}
      />

      {/* Headlines — date only */}
      <section>
        <h2 className="text-lg font-medium mb-2">Headlines</h2>
        {(!data.news || data.news.length === 0) && (
          <p className="text-sm text-gray-500">No recent news.</p>
        )}
        <ul className="space-y-2">
          {(data.news || []).map((n, i) => (
            <li key={i} className="text-sm">
              <span className="mr-2 inline-block min-w-[7ch] font-mono text-gray-600">
                {toDateOnly(n.ts)}
              </span>
              <a href={n.url} target="_blank" rel="noreferrer" className="underline">
                {n.title || "(untitled)"}
              </a>
              {n.source ? <span className="ml-2 text-gray-500">— {n.source}</span> : null}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

function Chart({
  dates,
  price,
  sentiment,
  width = 900,
  height = 280,
  padding = 28,
}: {
  dates: string[];
  price?: number[];
  sentiment?: number[];
  width?: number;
  height?: number;
  padding?: number;
}) {
  const W = width;
  const H = height;
  const left = padding;
  const right = padding;
  const top = padding;
  const bottom = 32;

  const innerW = W - left - right;
  const innerH = H - top - bottom;
  const n = dates.length;

  function path(xs: number[]) {
    if (!xs || xs.length === 0 || n === 0) return "";
    const step = innerW / Math.max(1, n - 1);
    return xs
      .map((v, i) => {
        const x = left + i * step;
        const y = top + (1 - v) * innerH;
        return `${i === 0 ? "M" : "L"}${x},${y}`;
      })
      .join(" ");
  }

  const gridY = 4;
  const ticks = [...Array(gridY + 1)].map((_, i) => Math.round((i * 100) / gridY));

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full border rounded-xl bg-white">
      {/* Axes / frame */}
      <rect x={left} y={top} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {/* Horizontal grid */}
      {ticks.map((t, i) => {
        const y = top + (1 - t / 100) * innerH;
        return <line key={i} x1={left} x2={left + innerW} y1={y} y2={y} stroke="#f1f5f9" />;
      })}
      {/* Labels */}
      <text x={left} y={H - 8} fontSize="10" fill="#64748b">Start</text>
      <text x={left + innerW - 28} y={H - 8} fontSize="10" fill="#64748b">End</text>

      {/* Series: draw sentiment first, then price on top */}
      {sentiment && sentiment.length > 0 && (
        <path d={path(sentiment)} fill="none" stroke="#3b82f6" strokeWidth="2" />
      )}
      {price && price.length > 0 && (
        <path d={path(price)} fill="none" stroke="#10b981" strokeWidth="2" />
      )}

      {/* Legends */}
      <g transform={`translate(${left + 8}, ${top + 12})`}>
        {sentiment && sentiment.length > 0 && (
          <g>
            <line x1="0" y1="0" x2="20" y2="0" stroke="#3b82f6" strokeWidth="2" />
            <text x="26" y="3" fontSize="12" fill="#1f2937">Sentiment (normalized)</text>
          </g>
        )}
        {price && price.length > 0 && (
          <g transform="translate(0,18)">
            <line x1="0" y1="0" x2="20" y2="0" stroke="#10b981" strokeWidth="2" />
            <text x="26" y="3" fontSize="12" fill="#1f2937">Price (normalized)</text>
          </g>
        )}
      </g>
    </svg>
  );
}
