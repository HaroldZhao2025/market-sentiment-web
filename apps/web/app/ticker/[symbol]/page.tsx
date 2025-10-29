// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url?: string; source?: string; score?: number };

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

/* ------------------------------ FS helpers ------------------------------ */
async function exists(p: string) {
  try { await fs.access(p); return true; } catch { return false; }
}
async function readJSON<T = any>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; } catch { return null; }
}

/* --------------------------- Data-root discovery --------------------------- */
/** Try both public/ and repo-level data roots so dev & CI both work. */
async function candidateTickerFiles(symbol: string): Promise<string[]> {
  const cwd = process.cwd(); // usually apps/web
  const roots = [
    path.join(cwd, "public", "data"),
    path.join(cwd, "data"),
    path.join(cwd, "..", "..", "data"), // fallback when cwd is apps/web
  ];
  const tries: string[] = [];
  for (const r of roots) {
    tries.push(path.join(r, "ticker", `${symbol}.json`));
    tries.push(path.join(r, `${symbol}.json`));
  }
  // de-dup while keeping order
  return Array.from(new Set(tries));
}

async function loadTickerJSON(symbol: string): Promise<any | null> {
  for (const p of await candidateTickerFiles(symbol)) {
    if (await exists(p)) return await readJSON<any>(p);
  }
  return null;
}

/* ----------------------------- Field pickers ----------------------------- */
const isNumArr = (v: unknown) => Array.isArray(v) && v.every(x => typeof x === "number" || !isNaN(Number(x)));
const isStrArr = (v: unknown) => Array.isArray(v) && v.every(x => typeof x === "string");
const toNumArr = (v: unknown) => Array.isArray(v) ? v.map(x => Number(x) || 0) : [];
const toStrArr = (v: unknown) => Array.isArray(v) ? v.map(x => String(x ?? "")) : [];

function pickFirst(obj: any, keys: string[]) {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k)) return obj[k];
  }
  return undefined;
}

/** Support shallow nesting like { data: { dates:[], close:[], sentiment:[] } } */
function dig(obj: any, keys: string[]): any {
  const cand = pickFirst(obj, keys);
  if (cand !== undefined) return cand;
  const nests = ["data", "series", "payload", "result"];
  for (const n of nests) {
    if (obj && typeof obj[n] === "object") {
      const v = pickFirst(obj[n], keys);
      if (v !== undefined) return v;
    }
  }
  return undefined;
}

function normalizeDates(v: any): string[] {
  if (isStrArr(v)) return toStrArr(v);
  if (isNumArr(v)) {
    // maybe epoch seconds or ms
    const arr = toNumArr(v).map(n => {
      const t = String(Math.floor(n)).length >= 13 ? Math.floor(n / 1000) : Math.floor(n);
      const d = new Date(t * 1000);
      return isNaN(d.getTime()) ? String(n) : d.toISOString().slice(0, 10);
    });
    return arr;
  }
  return [];
}

function buildSeries(obj: any): SeriesIn | null {
  if (!obj) return null;

  const dateRaw =
    dig(obj, ["date", "dates", "time", "times", "ts", "index", "d"]) ??
    [];

  const priceRaw =
    dig(obj, ["price", "prices", "close", "Close", "adj_close", "adjClose", "Adj Close", "P"]) ?? [];

  const sentRaw =
    dig(obj, ["sentiment", "sentiments", "sentiment_score", "sentimentScore", "score", "scores", "senti"]) ?? [];

  const dates = normalizeDates(dateRaw);
  const price = toNumArr(priceRaw);
  const sentiment = toNumArr(sentRaw);

  const n = Math.min(dates.length, price.length, sentiment.length);
  if (!n || n < 5) return null;

  return { date: dates.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

/* ------------------------------- News loader ------------------------------ */
function mapNewsArray(arr: any[]): NewsItem[] {
  return arr.map((x: any) => ({
    ts: String(x.ts ?? x.time ?? x.date ?? x.datetime ?? ""),
    title: String(x.title ?? x.headline ?? ""),
    url: x.url ?? x.link,
    source: x.source ?? x.provider,
    score: typeof x.score === "number" ? x.score : (typeof x.sentiment === "number" ? x.sentiment : undefined),
  }));
}

async function candidateNewsFiles(symbol: string): Promise<string[]> {
  const cwd = process.cwd();
  const roots = [
    path.join(cwd, "public", "data"),
    path.join(cwd, "data"),
    path.join(cwd, "..", "..", "data"),
  ];
  const tries: string[] = [];
  for (const r of roots) {
    tries.push(path.join(r, "news", `${symbol}.json`));
    tries.push(path.join(r, "ticker", `${symbol}_news.json`));
    tries.push(path.join(r, `${symbol}_news.json`));
  }
  return Array.from(new Set(tries));
}

async function loadNews(symbol: string, inline: any): Promise<NewsItem[]> {
  // 1) inline `news` in the same JSON
  if (Array.isArray(inline)) return mapNewsArray(inline);

  // 2) external files
  for (const p of await candidateNewsFiles(symbol)) {
    if (await exists(p)) {
      const j = await readJSON<any>(p);
      if (j && Array.isArray(j)) return mapNewsArray(j);
    }
  }
  return [];
}

/* --------------------------- Static param discovery --------------------------- */
async function listSymbolsFrom(dir: string): Promise<string[]> {
  try {
    const files = await fs.readdir(dir);
    return files
      .filter((n) => n.toLowerCase().endsWith(".json"))
      .map((n) => n.replace(/\.json$/i, "").toUpperCase());
  } catch {
    return [];
  }
}

export async function generateStaticParams() {
  const cwd = process.cwd();
  const roots = [
    path.join(cwd, "public", "data", "ticker"),
    path.join(cwd, "data", "ticker"),
    path.join(cwd, "..", "..", "data", "ticker"),
  ];
  const sets = await Promise.all(roots.map((d) => listSymbolsFrom(d)));
  const all = Array.from(new Set(sets.flat()));
  return all.map((symbol) => ({ symbol }));
}

/* --------------------------------- Page --------------------------------- */
export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();

  // load ticker JSON from any supported location
  const raw = await loadTickerJSON(symbol);
  const series = buildSeries(raw);

  // load news (inline or external files)
  const news = await loadNews(symbol, raw?.news);

  if (!series) {
    return (
      <main style={{ maxWidth: 1200, margin: "40px auto", padding: "0 16px" }}>
        <h1>Market Sentiment for {symbol}</h1>
        <p style={{ color: "#6b7280" }}>
          No compatible data found for this symbol. Ensure one of these files exists and
          contains date/price/sentiment arrays:
        </p>
        <ul style={{ color: "#6b7280", marginTop: 8 }}>
          <li>apps/web/public/data/ticker/{symbol}.json</li>
          <li>apps/web/public/data/{symbol}.json</li>
          <li>data/ticker/{symbol}.json</li>
          <li>data/{symbol}.json</li>
        </ul>
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
