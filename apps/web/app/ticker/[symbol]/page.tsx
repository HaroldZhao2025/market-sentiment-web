// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };

type NewsItem = {
  title?: string;
  source?: string;
  url?: string;
  date?: string;          // e.g., ISO string
  publishedAt?: string;   // sometimes used instead of date
};

function pickRecentHeadlines(items: NewsItem[], max = 10): NewsItem[] {
  const msDay = 24 * 60 * 60 * 1000;
  const now = Date.now();

  // Normalize + sort by time (desc)
  const byTime = (items ?? [])
    .filter(d => d && (d.date || d.publishedAt))
    .map(d => ({ ...d, _t: Date.parse(d.publishedAt ?? d.date!) }))
    .filter(d => Number.isFinite(d._t))
    .sort((a, b) => b._t - a._t);

  // Progressive windows: same-day → 3d → 7d → 14d → 30d
  for (const days of [1, 3, 7, 14, 30]) {
    const within = byTime.filter(d => now - d._t <= days * msDay);
    // light de-dupe by normalized (source,title)
    const seen = new Set<string>();
    const dedupWithin = within.filter(d => {
      const key = `${(d.source || "").toLowerCase()}::${(d.title || "").toLowerCase()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (dedupWithin.length >= max) return dedupWithin.slice(0, max);
  }

const recent = pickRecentHeadlines(data?.news ?? [], 10);

  // FINAL FALLBACK: just take latest 10 overall (deduped)
  const seen2 = new Set<string>();
  const dedupOverall = byTime.filter(d => {
    const key = `${(d.source || "").toLowerCase()}::${(d.title || "").toLowerCase()}`;
    if (seen2.has(key)) return false;
    seen2.add(key);
    return true;
  });
  return dedupOverall.slice(0, max);
}

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; } catch { return null; }
}
const numArr = (v: unknown): number[] => (Array.isArray(v) ? v.map((x) => Number(x) || 0) : []);
const strArr = (v: unknown): string[] => (Array.isArray(v) ? v.map((x) => String(x ?? "")) : []);

function buildSeries(obj: any): SeriesIn | null {
  const date = strArr(obj?.date ?? obj?.dates);
  const price = numArr(obj?.price ?? obj?.close ?? obj?.Close);
  const sentiment = numArr(obj?.S ?? obj?.sentiment);
  const n = Math.min(date.length, price.length || Infinity, sentiment.length || Infinity);
  if (!Number.isFinite(n) || n === 0) return null;
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

function buildNews(obj: any): NewsItem[] {
  const raw = Array.isArray(obj?.news) ? obj.news : [];
  return raw
    .map((r: any) => ({
      ...r,
      ts: String(r?.ts ?? r?.date ?? ""),
      title: String(r?.title ?? ""),
      url: String(r?.url ?? ""),
      text: r?.text ? String(r.text) : undefined,
    }))
    .filter((r: NewsItem) => r.ts && r.title);
}

export async function generateStaticParams() {
  const list = (await readJSON<string[]>(path.join(DATA_ROOT, "_tickers.json"))) || ["AAPL"];
  return list.map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const f = path.join(DATA_ROOT, "ticker", `${symbol}.json`);
  const obj = await readJSON<any>(f);

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
  const news = buildNews(obj).slice(0, 10); // keep top 10 as requested

  const newsTotal =
    Number(obj?.news_total ?? obj?.newsTotal ?? obj?.news_count?.total) || news.length;

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto">
        <h1 className="sr-only">{symbol}</h1>
        {series ? (
          <TickerClient symbol={symbol} series={series} news={news} newsTotal={newsTotal} />
        ) : (
          <div className="text-neutral-500">No time series for {symbol}.</div>
        )}
      </div>
    </div>
  );
}
