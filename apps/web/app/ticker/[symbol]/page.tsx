// apps/web/app/ticker/[symbol]/page.tsx
import path from "node:path";
import fs from "node:fs";
import LineChart from "../../../components/LineChart";
import { listTickers, loadTicker } from "../../../lib/loaders";
import { assetPath } from "../../../lib/paths";

export const dynamicParams = true;

// Pre-generate params from _tickers.json if present.
// If missing, we still export a minimal set to avoid build failure.
export async function generateStaticParams() {
  const tickers = listTickers();
  return tickers.map((t) => ({ symbol: t }));
}

export default function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const data = loadTicker(symbol); // safe, never undefined

  const priceSeries = data.series.map((p) => ({ x: p.date, y: p.close }));
  const signalSeries = data.series.map((p) => ({ x: p.date, y: p.S }));

  return (
    <main className="max-w-5xl mx-auto p-6">
      <h1 className="text-xl font-semibold mb-4">{symbol}</h1>

      <div className="mb-6">
        <LineChart left={priceSeries} right={signalSeries} height={300} />
      </div>

      <h2 className="text-lg font-semibold mb-2">Recent News</h2>
      {data.news.length === 0 ? (
        <p className="text-sm text-gray-500">No news captured for this window.</p>
      ) : (
        <ul className="space-y-2">
          {data.news.slice(0, 20).map((n, idx) => (
            <li key={idx} className="border rounded p-3">
              <div className="text-sm text-gray-500">{new Date(n.ts).toLocaleString()}</div>
              <a href={n.url} target="_blank" rel="noreferrer" className="font-medium underline">
                {n.title}
              </a>
              <div className="text-sm text-gray-600">Sentiment: {n.s.toFixed(3)} {n.source ? ` • ${n.source}` : ""}</div>
            </li>
          ))}
        </ul>
      )}

      <div className="mt-8">
        <a className="underline text-blue-600" href={assetPath("")}>
          ← Back
        </a>
      </div>
    </main>
  );
}
