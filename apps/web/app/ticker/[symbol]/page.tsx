// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs/promises";
import path from "path";
import TickerClient from "./TickerClient";

// If TickerClient re-exports types, import them; otherwise declare here to match it.
type SeriesIn = {
  date: string[];           // required by TickerClient
  price: number[];          // close prices
  sentiment: number[];      // daily S
  sentiment_ma7: number[];  // 7d MA of S
  label: string;
};

type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text: string;
};

export const dynamic = "error";          // force SSG
export const dynamicParams = false;      // only build known params
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

// --- helpers -------------------------------------------------------------

async function readJSON<T = any>(p: string): Promise<T> {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw) as T;
}

function coerceString(v: unknown, fallback = ""): string {
  if (v === null || v === undefined) return fallback;
  try {
    const s = String(v).trim();
    return s;
  } catch {
    return fallback;
  }
}

function coerceNumberArray(v: unknown): number[] {
  if (Array.isArray(v)) {
    return v.map((x) => (typeof x === "number" ? x : Number(x ?? 0))).map((x) =>
      Number.isFinite(x) ? x : 0
    );
  }
  return [];
}

function coerceStringArray(v: unknown): string[] {
  if (Array.isArray(v)) {
    return v.map((x) => coerceString(x, ""));
  }
  return [];
}

function buildSeries(obj: any): SeriesIn | null {
  const dates =
    coerceStringArray(obj?.date) || coerceStringArray(obj?.dates) || [];
  const price =
    coerceNumberArray(obj?.price) ||
    coerceNumberArray(obj?.close) ||
    coerceNumberArray(obj?.Close) ||
    [];
  const sentiment =
    coerceNumberArray(obj?.S) || coerceNumberArray(obj?.sentiment) || [];
  const sentiment_ma7 =
    coerceNumberArray(obj?.S_ma7) ||
    coerceNumberArray(obj?.sentiment_ma7) ||
    [];

  if (!dates.length || !price.length) return null;

  // Make sure lengths line up (truncate to shortest to be safe)
  const n = Math.min(dates.length, price.length, sentiment.length || Infinity, sentiment_ma7.length || Infinity);
  const slice = (a: any[]) => (n === Infinity ? a : a.slice(0, n));

  return {
    date: slice(dates),
    price: slice(price),
    sentiment: slice(sentiment),
    sentiment_ma7: slice(sentiment_ma7),
    label: "Daily S",
  };
}

function buildNews(obj: any): NewsItem[] {
  const raw: any[] = Array.isArray(obj?.news) ? obj.news : [];
  const rows: NewsItem[] = raw
    .map((r) => {
      const ts = coerceString(r?.ts || r?.date, "");
      const title = coerceString(r?.title, "");
      const url = coerceString(r?.url, "");
      const text = coerceString(r?.text || r?.summary || r?.title, "");
      return { ts, title, url, text };
    })
    // all required by the TickerClient NewsItem type:
    .filter((r) => r.ts && r.title);
  return rows;
}

// --- Next.js SSG hooks ---------------------------------------------------

export async function generateStaticParams() {
  // public/data/_tickers.json => ["AAPL", ...]
  let tickers: string[] = [];
  try {
    tickers = await readJSON<string[]>(
      path.join(DATA_ROOT, "_tickers.json")
    );
  } catch {
    // fallback to AAPL if missing during preview
    tickers = ["AAPL"];
  }
  // Only build pages that we actually have JSON for
  return tickers.map((symbol) => ({ symbol }));
}

export default async function Page({
  params,
}: {
  params: { symbol: string };
}) {
  const symbol = (params.symbol || "").toUpperCase();

  // public/data/ticker/SYM.json produced by writers.py
  const f = path.join(DATA_ROOT, "ticker", `${symbol}.json`);

  let obj: any = null;
  try {
    obj = await readJSON<any>(f);
  } catch {
    // If no data, render a soft empty state
    return (
      <div className="min-h-screen p-6">
        <div className="max-w-5xl mx-auto">
          <h1 className="text-2xl font-semibold mb-4">{symbol}</h1>
          <div className="text-neutral-500">No data for {symbol}.</div>
        </div>
      </div>
    );
  }

  const series = buildSeries(obj);
  const news: NewsItem[] = buildNews(obj);

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-2xl font-semibold mb-6 tracking-tight">
          {symbol}
        </h1>
        {series ? (
          <TickerClient symbol={symbol} series={series} news={news} />
        ) : (
          <div className="text-neutral-500">No time series for {symbol}.</div>
        )}
      </div>
    </div>
  );
}
