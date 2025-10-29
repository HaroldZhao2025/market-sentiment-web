// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

// App Router static export guarantees
export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url?: string; source?: string; score?: number };

// Robust path resolver (works from repo root or apps/web)
async function resolveDataRoot(): Promise<string> {
  const candidates = [
    path.join(process.cwd(), "public", "data"),                // when cwd = apps/web
    path.join(process.cwd(), "apps", "web", "public", "data"), // when cwd = repo root
  ];
  for (const p of candidates) {
    try {
      const st = await fs.stat(p);
      if (st.isDirectory()) return p;
    } catch {}
  }
  return candidates[0];
}

async function readJSON<T>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; }
  catch { return null; }
}

const nArr = (x: unknown) => (Array.isArray(x) ? x.map((v) => Number(v) || 0) : []);
const sArr = (x: unknown) => (Array.isArray(x) ? x.map((v) => String(v ?? "")) : []);

function buildSeries(obj: any): SeriesIn | null {
  if (!obj) return null;
  const date = sArr(obj.date ?? obj.dates ?? obj.time ?? []);
  const price = nArr(obj.price ?? obj.prices ?? []);
  const sentiment = nArr(obj.sentiment ?? obj.sentiments ?? []);
  const n = Math.min(date.length, price.length, sentiment.length);
  if (!n) return null;
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

async function loadNews(dataRoot: string, symbol: string): Promise<NewsItem[]> {
  const candidates = [
    path.join(dataRoot, "news", `${symbol}.json`),
    path.join(dataRoot, "ticker", `${symbol}_news.json`),
    path.join(dataRoot, `${symbol}_news.json`),
  ];
  for (const p of candidates) {
    const j = await readJSON<any[]>(p);
    if (j && Array.isArray(j)) {
      return j.map((x) => ({
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

// ------- Static prerender of /ticker/[symbol] from files your pipeline generates -------
export async function generateStaticParams() {
  const dataRoot = await resolveDataRoot();
  const tickDir = path.join(dataRoot, "ticker");
  try {
    const files = await fs.readdir(tickDir);
    return files
      .filter((f) => f.toLowerCase().endsWith(".json"))
      .map((f) => ({ symbol: f.replace(/\.json$/i, "").toUpperCase() }));
  } catch {
    return [];
  }
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const dataRoot = await resolveDataRoot();

  const series = buildSeries(
    await readJSON<any>(path.join(dataRoot, "ticker", `${symbol}.json`))
  );
  const news = await loadNews(dataRoot, symbol);

  if (!series) {
    return (
      <main style={{ maxWidth: 1200, margin: "40px auto", padding: "0 16px" }}>
        <h1>Market Sentiment for {symbol}</h1>
        <p style={{ color: "#6b7280" }}>
          No data found for <code>{`public/data/ticker/${symbol}.json`}</code>.
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
