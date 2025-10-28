"use client";
import React from "react";
import SentimentChart from "@/app/components/SentimentChart";
import NewsList from "@/app/components/NewsList";

type Data = {
  ticker: string;
  dates: string[];
  S: number[];
  S_ma7?: number[];
  sentiment?: number[]; // alias
  news_count?: { finnhub: number; yfinance: number; total: number };
  news: { ts: string; title: string; url: string; source?: string; provider?: string }[];
};

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

async function fetchData(symbol: string): Promise<Data | null> {
  try {
    const url = `${BASE}/data/ticker/${symbol.toUpperCase()}.json?ts=${Date.now()}`;
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as Data;
  } catch {
    return null;
  }
}

export default function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  const [data, setData] = React.useState<Data | null>(null);
  const [lastRefreshed, setLastRefreshed] = React.useState<Date | null>(null);

  const load = React.useCallback(async () => {
    const d = await fetchData(symbol);
    if (d) {
      // guard against old builds that only had `sentiment`
      if ((!d.S || d.S.length === 0) && d.sentiment && d.sentiment.length) {
        d.S = d.sentiment;
      }
      setData(d);
      setLastRefreshed(new Date());
    }
  }, [symbol]);

  React.useEffect(() => {
    load();
    const id = setInterval(load, 60 * 60 * 1000); // refresh every hour
    return () => clearInterval(id);
  }, [load]);

  if (!data) {
    return (
      <div className="max-w-5xl mx-auto p-6">
        <h1 className="text-xl font-semibold">Loading {symbol}…</h1>
      </div>
    );
  }

  const sNow = data.S?.length ? data.S[data.S.length - 1] : 0;
  const sNowText =
    typeof sNow === "number" ? (sNow > 0 ? "Positive" : sNow < 0 ? "Negative" : "Neutral") : "Neutral";

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">{data.ticker}</h1>
          <p className="text-sm text-gray-500">
            Sentiment now: <span className="font-medium">{(sNow ?? 0).toFixed(4)}</span> ({sNowText})
            {data.news_count ? (
              <> · News (window): <span className="font-medium">{data.news_count.total}</span> </> ) : null}
          </p>
        </div>
        <button
          onClick={load}
          className="px-3 py-1.5 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 shadow"
        >
          Refresh now
        </button>
      </header>

      <SentimentChart dates={data.dates || []} S={data.S || []} S_ma7={data.S_ma7 || data.sentiment || []} />

      <div className="grid md:grid-cols-2 gap-6">
        <div className="rounded-2xl shadow p-4 bg-white">
          <h3 className="text-lg font-semibold mb-3">Summary</h3>
          <ul className="text-sm text-gray-700 space-y-1">
            <li>Days with sentiment: <b>{data.dates?.length || 0}</b></li>
            <li>Finnhub items: <b>{data.news_count?.finnhub ?? 0}</b></li>
            <li>Yahoo items: <b>{data.news_count?.yfinance ?? 0}</b></li>
            <li>Total items (window): <b>{data.news_count?.total ?? 0}</b></li>
            {lastRefreshed && (
              <li>Last refreshed: <span className="text-gray-500">{lastRefreshed.toUTCString()}</span></li>
            )}
          </ul>
        </div>
        <NewsList items={data.news || []} />
      </div>
    </div>
  );
}
