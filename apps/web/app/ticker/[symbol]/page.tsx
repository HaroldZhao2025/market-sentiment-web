// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs/promises";
import path from "path";
import TickerClient from "./TickerClient";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
  label?: string;
};

type NewsItem = { ts: string; title: string; url: string; text: string };

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T> {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw) as T;
}

function asNumArr(v: unknown): number[] {
  if (!Array.isArray(v)) return [];
  return v.map((x) => (typeof x === "number" ? x : Number(x ?? 0))).map((x) => (Number.isFinite(x) ? x : 0));
}
function asStrArr(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  return v.map((x) => (x == null ? "" : String(x)));
}

function buildSeries(obj: any): SeriesIn | null {
  const dates = asStrArr(obj?.date ?? obj?.dates);
  const price = asNumArr(obj?.price ?? obj?.close ?? obj?.Close);
  const s = asNumArr(obj?.S ?? obj?.sentiment);

  if (!dates.length) return null;
  // trim to shortest so arrays align (Recharts needs consistent row objects)
  const n = Math.min(dates.length, price.length || Infinity, s.length || Infinity);
  const take = (a: any[]) => (n === Infinity ? a : a.slice(0, n));

  return { date: take(dates), price: take(price), sentiment: take(s), label: "Daily S" };
}

function buildNews(obj: any): NewsItem[] {
  const raw: any[] = Array.isArray(obj?.news) ? obj.news : [];
  return raw
    .map((r) => ({
      ts: String(r?.ts ?? r?.date ?? ""),
      title: String(r?.title ?? ""),
      url: String(r?.url ?? ""),
      text: String(r?.text ?? r?.summary ?? r?.title ?? ""),
    }))
    .filter((r) => r.ts && r.title);
}

export async function generateStaticParams() {
  try {
    const tickers: string[] = await readJSON(path.join(DATA_ROOT, "_tickers.json"));
    return tickers.map((symbol) => ({ symbol }));
  } catch {
    return [{ symbol: "AAPL" }];
  }
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const f = path.join(DATA_ROOT, "ticker", `${symbol}.json`);

  let obj: any;
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
  const news = buildNews(obj);

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto">
        <h1 className="sr-only">{symbol}</h1>
        {series ? (
          <TickerClient symbol={symbol} series={series} news={news} />
        ) : (
          <div className="text-neutral-500">No time series for {symbol}.</div>
        )}
      </div>
    </div>
  );
}
