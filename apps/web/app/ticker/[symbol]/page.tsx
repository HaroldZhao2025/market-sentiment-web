// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url?: string; source?: string; score?: number };

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T | null> {
  try {
    return JSON.parse(await fs.readFile(p, "utf8")) as T;
  } catch {
    return null;
  }
}

const numArr = (v: unknown): number[] =>
  Array.isArray(v) ? v.map((x) => (typeof x === "number" ? x : Number(x) || 0)) : [];
const strArr = (v: unknown): string[] => (Array.isArray(v) ? v.map((x) => String(x ?? "")) : []);

function buildSeries(obj: any): SeriesIn | null {
  if (!obj) return null;
  const date = strArr(obj.date ?? obj.dates ?? obj.time ?? []);
  const price = numArr(obj.price ?? obj.prices ?? []);
  const sentiment = numArr(obj.sentiment ?? obj.sentiments ?? []);
  if (!date.length || !price.length || !sentiment.length) return null;
  const n = Math.min(date.length, price.length, sentiment.length);
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

async function loadNewsJSON(symbol: string): Promise<NewsItem[]> {
  const candidates = [
    path.join(DATA_ROOT, "news", `${symbol}.json`),
    path.join(DATA_ROOT, "ticker", `${symbol}_news.json`),
    path.join(DATA_ROOT, `${symbol}_news.json`),
  ];
  for (const p of candidates) {
    const j = await readJSON<any>(p);
    if (j && Array.isArray(j)) {
      return j.map((x: any) => ({
        ts: String(x.ts ?? x.time ?? x.date ?? ""),
        title: String(x.title ?? x.headline ?? ""),
        url: x.url ?? x.link,
        source: x.source ?? x.provider,
        score: typeof x.score === "number" ? x.score : undefined,
      }));
    }
  }
  return [];
}

/**
 * Critical for GitHub Pages static export: pre-generate /ticker/[symbol]/ for each JSON file.
 * Reads public/data/ticker/*.json and returns [{symbol}] so Next.js exports /ticker/SYMBOL/.
 */
export async function generateStaticParams() {
  const dir = path.join(DATA_ROOT, "ticker");
  let files: string[] = [];
  try {
    files = await fs.readdir(dir);
  } catch {
    // no ticker directory = no pages
    return [];
  }
  return files
    .filter((f) => f.toLowerCase().endsWith(".json"))
    .map((f) => ({ symbol: f.replace(/\.json$/i, "").toUpperCase() }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();

  const series = buildSeries(await readJSON<any>(path.join(DATA_ROOT, "ticker", `${symbol}.json`)));
  const news = await loadNewsJSON(symbol);

  if (!series) {
    return (
      <main style={{ maxWidth: 1200, margin: "40px auto", padding: "0 16px" }}>
        <h1>Market Sentiment for {symbol}</h1>
        <p style={{ color: "#6b7280" }}>
          No data found for <code>public/data/ticker/{symbol}.json</code>.
        </p>
      </main>
    );
  }

  return (
    <main style={{ maxWidth: 1200, margin: "24px auto", padding: "0 16px" }}>
      <h1 style={{ fontSize: 28, fontWeight: 600, marginBottom: 8 }}>
        Market Sentiment for {symbol}
      </h1>
      <TickerClient symbol={symbol} series={series} news={news} />
    </main>
  );
}
