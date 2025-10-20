// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs/promises";
import path from "path";
import TickerClient from "./TickerClient";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
  label?: string;
};

type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;
};

export const dynamic = "error";     // force SSG
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T> {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw) as T;
}

function coerceStrings(a: unknown): string[] {
  return Array.isArray(a) ? a.map((x) => String(x ?? "")) : [];
}
function coerceNums(a: unknown): number[] {
  return Array.isArray(a)
    ? a.map((x) => (typeof x === "number" && Number.isFinite(x) ? x : Number(x ?? 0) || 0))
    : [];
}

function buildSeries(obj: any): SeriesIn | null {
  const date = coerceStrings(obj?.date ?? obj?.dates);
  const price = coerceNums(obj?.price ?? obj?.close);
  const sentiment = coerceNums(obj?.S ?? obj?.sentiment);
  const sentiment_ma7 = coerceNums(obj?.S_ma7 ?? obj?.sentiment_ma7);

  if (date.length === 0 || price.length === 0) return null;
  const n = Math.min(date.length, price.length, sentiment.length || Infinity, sentiment_ma7.length || Infinity);
  const slice = (arr: any[]) => (n === Infinity ? arr : arr.slice(0, n));

  return {
    date: slice(date),
    price: slice(price),
    sentiment: slice(sentiment),
    sentiment_ma7: slice(sentiment_ma7),
    label: "Daily S",
  };
}

function buildNews(obj: any): NewsItem[] {
  const raw: any[] = Array.isArray(obj?.news) ? obj.news : [];
  return raw
    .map((r) => ({
      ts: String(r?.ts ?? ""),
      title: String(r?.title ?? ""),
      url: String(r?.url ?? ""),
      text: String(r?.text ?? ""),
    }))
    .filter((r) => r.ts && r.title);
}

export async function generateStaticParams() {
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
  const news = buildNews(obj);

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-2xl font-semibold mb-6 tracking-tight">{symbol}</h1>
        {series ? (
          <TickerClient
            symbol={symbol}
            series={{
              date: series.date,
              price: series.price,
              sentiment: series.sentiment,
              sentiment_ma7: series.sentiment_ma7,
            }}
            news={news}
          />
        ) : (
          <div className="text-neutral-500">No time series for {symbol}.</div>
        )}
      </div>
    </div>
  );
}
