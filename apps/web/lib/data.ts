// apps/web/lib/data.ts
import fs from "node:fs/promises";
import path from "node:path";

export type Series = { dates: string[]; values: number[]; label: string };
export type SeriesIn = { left: Series; right: Series; overlay?: Series };
export type NewsItem = { ts: string; title: string; url: string; text?: string };

const DATA_DIR = path.join(process.cwd(), "public", "data");

function pick<T = any>(obj: any, k: string): T | undefined {
  return obj?.[k] ?? obj?.[k.toUpperCase()];
}

async function readJSON<T = any>(p: string): Promise<T> {
  const raw = await fs.readFile(p, "utf8");
  return JSON.parse(raw) as T;
}

export async function loadTickers(): Promise<string[]> {
  try {
    const p = path.join(DATA_DIR, "_tickers.json");
    const arr = await readJSON<any[]>(p);
    // tolerate string-array or object-array
    if (Array.isArray(arr) && typeof arr[0] === "string") return arr;
    if (Array.isArray(arr)) return arr.map((x: any) => String(x).toUpperCase());
  } catch (_) {}
  return [];
}

export async function loadPortfolio(): Promise<any | null> {
  try {
    const p = path.join(DATA_DIR, "portfolio.json");
    return await readJSON<any>(p);
  } catch {
    return null;
  }
}

export async function loadTickerNews(symbol: string): Promise<NewsItem[]> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const j = await readJSON<any>(p);
    const news = pick<any[]>(j, "news") ?? [];
    return news.map((n) => ({
      ts: n.ts ?? n.date ?? "",
      title: n.title ?? "",
      url: n.url ?? "",
      text: n.text ?? "",
    }));
  } catch {
    return [];
  }
}

export async function loadTickerSeries(symbol: string): Promise<SeriesIn | null> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const j = await readJSON<any>(p);

    const dates: string[] = pick<string[]>(j, "date") ?? [];
    const close: number[] = pick<number[]>(j, "close") ?? pick<number[]>(j, "CLOSE") ?? [];
    const s: number[] = pick<number[]>(j, "S") ?? [];
    // compute 7-day MA defensively
    const ma7: number[] = [];
    for (let i = 0; i < s.length; i++) {
      const start = Math.max(0, i - 6);
      const slice = s.slice(start, i + 1);
      const m = slice.length ? slice.reduce((a, b) => a + (b ?? 0), 0) / slice.length : 0;
      ma7.push(Number.isFinite(m) ? m : 0);
    }

    if (!dates.length || !close.length) return null;

    return {
      left: { dates, values: close, label: "Close" },
      right: { dates, values: s, label: "Daily Sentiment" },
      overlay: { dates, values: ma7, label: "Sentiment (7-day MA)" },
    };
  } catch {
    return null;
  }
}
