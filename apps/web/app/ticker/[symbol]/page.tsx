// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs/promises";
import path from "path";
import TickerClient from "./TickerClient";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

type NewsItem = { ts: string; title: string; url: string; text: string };

export const dynamic = "error";     // SSG only
export const dynamicParams = false; // only build known params
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T> {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw) as T;
}

function s(v: unknown, fallback = ""): string {
  if (v === null || v === undefined) return fallback;
  try { return String(v).trim(); } catch { return fallback; }
}
function ns(a: unknown[]): number[] {
  if (!Array.isArray(a)) return [];
  return a.map((x) => (typeof x === "number" ? x : Number(x ?? 0))).map((x) => (Number.isFinite(x) ? x : 0));
}
function ss(a: unknown[]): string[] {
  if (!Array.isArray(a)) return [];
  return a.map((x) => s(x, ""));
}

function buildSeries(obj: any): SeriesIn | null {
  const dates = (ss(obj?.date) || []).length ? ss(obj?.date) : ss(obj?.dates);
  const price = ns(obj?.price ?? obj?.close ?? []);
  const sentiment = ns(obj?.S ?? obj?.sentiment ?? []);
  if (!dates?.length || !price?.length) return null;

  const n = Math.min(dates.length, price.length, sentiment.length || Infinity);
  const clip = (arr: any[]) => (n === Infinity ? arr : arr.slice(0, n));

  return { date: clip(dates), price: clip(price), sentiment: clip(sentiment) };
}

function buildNews(obj: any): NewsItem[] {
  const raw: any[] = Array.isArray(obj?.news) ? obj.news : [];
  return raw
    .map((r) => ({
      ts: s(r?.ts || r?.date, ""),
      title: s(r?.title, ""),
      url: s(r?.url, ""),
      text: s(r?.text || r?.summary || r?.title, ""),
    }))
    .filter((r) => r.ts && r.title);
}

export async function generateStaticParams() {
  // public/data/_tickers.json => ["AAPL", ...]
  let tickers: string[] = [];
  try {
    tickers = await readJSON<string[]>(path.join(DATA_ROOT, "_tickers.json"));
  } catch {
    tickers = ["AAPL"];
  }
  return tickers.map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const f = path.join(DATA_ROOT, "ticker", `${symbol}.json`);

  let obj: any = null;
  try {
    obj = await readJSON<any>(f);
  } catch {
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
        <h1 className="text-2xl font-semibold mb-6 tracking-tight">{symbol}</h1>
        {series ? (
          <TickerClient symbol={symbol} series={series} news={news} />
        ) : (
          <div className="text-neutral-500">No time series for {symbol}.</div>
        )}
      </div>
    </div>
  );
}
