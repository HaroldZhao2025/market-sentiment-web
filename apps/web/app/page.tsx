/* eslint-disable @next/next/no-img-element */
import * as React from "react";
import SentimentChart from "@/components/SentimentChart";
import NewsList from "@/components/NewsList";

type TickerJson = {
  S?: number[];          // sentiment array
  sentiment?: number[];  // alternative key
  D?: string[];          // date array
  dates?: string[];      // alternative key
  P?: number[];          // price array
  prices?: number[];     // alternative key
  news?: Array<{ ts?: string; title?: string; url?: string; source?: string }>;
  // optional totals if present
  news_total?: number;
  news_period_label?: string;
};

async function fetchJson<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(path, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

function ma7(values: number[]): (number | null)[] {
  const out: (number | null)[] = Array(values.length).fill(null);
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i] ?? 0;
    sum += v;
    if (i >= 7) sum -= values[i - 7] ?? 0;
    if (i >= 6) out[i] = +(sum / 7).toFixed(4);
  }
  return out;
}

export default async function Page() {
  const tickers = await fetchJson<string[]>("/market-sentiment-web/data/_tickers.json");
  const ticker = tickers?.[0] || "AAPL";
  const raw = await fetchJson<TickerJson>(`/market-sentiment-web/data/ticker/${ticker}.json`);

  const S = raw?.S ?? raw?.sentiment ?? [];
  const D = raw?.D ?? raw?.dates ?? [];
  const P = raw?.P ?? raw?.prices ?? [];
  const Sma7 = Array.isArray(S) && S.length ? ma7(S as number[]) : [];
  const rows = (D || []).map((d, i) => ({
    date: d,
    sentiment: typeof S?.[i] === "number" ? +(+S[i]).toFixed(4) : null,
    sentiment_ma7: typeof Sma7?.[i] === "number" ? Sma7[i] : null,
    price: typeof P?.[i] === "number" ? +(+P[i]).toFixed(2) : null,
  }));

  // news handling
  const news = raw?.news || [];
  const periodLabel = raw?.news_period_label || (D?.length ? `${D[0]} → ${D[D.length - 1]}` : "");
  const totalCount = (typeof raw?.news_total === "number" ? raw?.news_total : news.length) || 0;

  // headline metrics
  const latestSent = rows.length ? rows[rows.length - 1].sentiment : null;
  const latestMa7  = rows.length ? rows[rows.length - 1].sentiment_ma7 : null;

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="max-w-6xl mx-auto px-4 py-6 space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-2xl font-bold">{ticker}</div>
            <div className="text-sm text-gray-500">Live sentiment with Yahoo Finance + Finnhub news</div>
          </div>
          <div className="flex gap-6">
            <div className="p-3 rounded-xl bg-white shadow-sm border">
              <div className="text-xs text-gray-500">Sentiment</div>
              <div className="text-lg font-semibold">
                {latestSent === null || latestSent === undefined ? "—" : latestSent.toFixed(4)}
              </div>
            </div>
            <div className="p-3 rounded-xl bg-white shadow-sm border">
              <div className="text-xs text-gray-500">Sentiment (MA7)</div>
              <div className="text-lg font-semibold">
                {latestMa7 === null || latestMa7 === undefined ? "—" : (+latestMa7).toFixed(4)}
              </div>
            </div>
            <div className="p-3 rounded-xl bg-white shadow-sm border">
              <div className="text-xs text-gray-500">News Items (period)</div>
              <div className="text-lg font-semibold">{totalCount}</div>
            </div>
          </div>
        </div>

        <SentimentChart data={rows} />

        <NewsList
          items={news.map(n => ({
            ts: n.ts,
            title: n.title,
            url: n.url,
            source: (n as any).source
          }))}
          totalCount={totalCount}
          periodLabel={periodLabel}
        />

        <div className="text-xs text-gray-400 text-right">
          Data auto-refreshes hourly via GitHub Actions; this page remains static during updates.
        </div>
      </div>
    </main>
  );
}
