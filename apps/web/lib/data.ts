// apps/web/lib/data.ts
import fs from "node:fs";
import path from "node:path";

const baseDir = path.join(process.cwd(), "public", "data");

function readJSON<T = any>(p: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8")) as T;
  } catch {
    return null;
  }
}

export async function loadTickers(): Promise<string[]> {
  const p = path.join(baseDir, "_tickers.json");
  return readJSON<string[]>(p) ?? [];
}

export async function loadPortfolio(): Promise<any> {
  const p = path.join(baseDir, "portfolio.json");
  return readJSON<any>(p) ?? null;
}

export async function loadTicker(symbol: string): Promise<any | null> {
  const p = path.join(baseDir, "ticker", `${symbol}.json`);
  return readJSON<any>(p);
}

export async function loadTickerNews(symbol: string): Promise<any[]> {
  const obj = await loadTicker(symbol);
  return Array.isArray(obj?.news) ? (obj!.news as any[]) : [];
}

export async function loadTickerSeries(symbol: string): Promise<{
  dates: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7: number[];
  label: string;
} | null> {
  const obj = await loadTicker(symbol);
  if (!obj) return null;
  const dates = obj.date ?? obj.dates ?? [];
  const price = obj.price ?? obj.close ?? [];
  const sentiment = obj.S ?? obj.sentiment ?? [];
  const sentiment_ma7 = obj.S_ma7 ?? obj.sentiment_ma7 ?? [];
  return { dates, price, sentiment, sentiment_ma7, label: "Daily S" };
}
