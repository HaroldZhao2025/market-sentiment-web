// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import { SeriesIn, NewsItem } from "../../../lib/data";
import LineChart from "../../../components/LineChart";

export default function TickerClient({ series, news }: { series: SeriesIn; news: NewsItem[] }) {
  return (
    <div className="space-y-6">
      <div className="rounded-2xl p-4 shadow-sm border">
        <h3 className="font-semibold mb-3">Price & Daily Sentiment</h3>
        <LineChart series={series} height={380} />
      </div>

      <div className="rounded-2xl p-4 shadow-sm border">
        <h3 className="font-semibold mb-3">Recent Headlines</h3>
        {news && news.length ? (
          <ul className="divide-y">
            {news.slice(-20).reverse().map((n, i) => (
              <li key={i} className="py-2">
                <div className="text-sm text-neutral-500">{n.ts}</div>
                <a className="text-blue-600 hover:underline" href={n.url} target="_blank" rel="noreferrer">
                  {n.title || n.url}
                </a>
                {n.text ? <div className="text-sm text-neutral-600">{n.text}</div> : null}
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-neutral-500">No headlines found in the window.</div>
        )}
      </div>
    </div>
  );
}
