// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs";
import path from "node:path";
import TickerClient from "./TickerClient";

export const dynamic = "error"; // ensure static-only for export

type TickerJSON = {
  dates?: string[];
  date?: string[];
  close?: number[];
  price?: number[];
  S?: number[];
  sentiment?: number[];
  S_MA7?: number[];
  sentiment_ma7?: number[];
  news?: Array<{
    ts?: string;
    title?: string;
    url?: string;
    text?: string;
  }>;
};

function dataDir() {
  // read from apps/web/public/data at build time
  return path.join(process.cwd(), "public", "data");
}

function readJSON<T = any>(p: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8")) as T;
  } catch {
    return null;
  }
}

export async function generateStaticParams(): Promise<{ symbol: string }[]> {
  // Pre-render only the tickers we actually have JSON for
  const base = dataDir();
  const tfile = path.join(base, "_tickers.json");
  const arr = readJSON<string[]>(tfile) || ["AAPL"]; // safe fallback
  return arr.slice(0, 1200).map((symbol) => ({ symbol }));
}

export default async function Page({
  params,
}: {
  params: { symbol: string };
}) {
  const symbol = (params.symbol || "").toUpperCase();
  const base = dataDir();
  const tfile = path.join(base, "ticker", `${symbol}.json`);

  const obj = (readJSON<TickerJSON>(tfile) || {}) as TickerJSON;

  // Be defensive with field names
  const dates = obj.dates ?? obj.date ?? [];
  const price = obj.price ?? obj.close ?? [];
  const sentiment = obj.S ?? obj.sentiment ?? [];
  const sentiment_ma7 = obj.S_MA7 ?? obj.sentiment_ma7 ?? [];

  const news = Array.isArray(obj.news) ? obj.news : [];

  const series = {
    dates,
    price,
    sentiment,
    sentiment_ma7,
    label: "Daily S",
  };

  return (
    <div className="min-h-screen">
      <TickerClient symbol={symbol} series={series} news={news} />
    </div>
  );
}
