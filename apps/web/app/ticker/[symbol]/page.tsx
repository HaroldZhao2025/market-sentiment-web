// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs/promises";
import path from "path";
import TickerClient, { NewsItem, SeriesIn } from "./TickerClient";

export const dynamic = "error";          // SSG only
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

// small utils
async function readJSON<T = any>(p: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(p, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}
const toNumArr = (v: any) =>
  Array.isArray(v) ? v.map((x) => (typeof x === "number" ? x : Number(x ?? 0))).map((x) => (Number.isFinite(x) ? x : 0)) : [];
const toStrArr = (v: any) => (Array.isArray(v) ? v.map((x) => String(x ?? "")) : []);

function buildSeries(obj: any): SeriesIn | null {
  const date = toStrArr(obj?.date ?? obj?.dates);
  const price = toNumArr(obj?.price ?? obj?.close);
  const sentiment = toNumArr(obj?.S ?? obj?.sentiment);
  if (!date.length || !price.length) return null;
  const n = Math.min(date.length, price.length, sentiment.length || Infinity);
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

function buildNews(obj: any): NewsItem[] {
  const raw: any[] = Array.isArray(obj?.news) ? obj.news : [];
  return raw
    .map((r) => ({
      ts: String(r?.ts ?? ""),
      title: String(r?.title ?? ""),
      url: String(r?.url ?? ""),
      text: r?.text ? String(r.text) : undefined,
    }))
    .filter((r) => r.ts && r.title);
}

export async function generateStaticParams() {
  let tickers: string[] = [];
  const tickersFile = path.join(DATA_ROOT, "_tickers.json");
  try {
    const arr = await readJSON<string[]>(tickersFile);
    tickers = Array.isArray(arr) ? arr : [];
  } catch { /* noop */ }
  if (!tickers.length) tickers = ["AAPL"];
  return tickers.map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const file = path.join(DATA_ROOT, "ticker", `${symbol}.json`);
  const obj = await readJSON<any>(file);

  if (!obj) {
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
  const news = buildNews(obj);

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
