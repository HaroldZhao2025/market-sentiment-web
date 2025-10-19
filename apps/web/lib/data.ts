// apps/web/lib/data.ts
import fs from "fs/promises";
import path from "path";

const DATA_DIR = path.join(process.cwd(), "public", "data");

export async function loadTickers(): Promise<string[]> {
  try {
    const s = await fs.readFile(path.join(DATA_DIR, "_tickers.json"), "utf8");
    const arr = JSON.parse(s);
    return Array.isArray(arr) && arr.length ? arr : ["AAPL"];
  } catch {
    return ["AAPL"];
  }
}

export type Portfolio = { dates: string[]; long: number[]; short: number[]; long_short: number[] };

export async function loadPortfolio(): Promise<Portfolio | null> {
  try {
    const s = await fs.readFile(path.join(DATA_DIR, "portfolio.json"), "utf8");
    return JSON.parse(s);
  } catch {
    return null;
  }
}

export type TickerJSON = {
  date: string[];
  price: number[];
  S?: number[];
  sentiment?: number[];
  news?: { ts: string; title: string; url: string }[];
};

export async function loadTickerSeries(symbol: string): Promise<TickerJSON | null> {
  try {
    const s = await fs.readFile(path.join(DATA_DIR, "ticker", `${symbol}.json`), "utf8");
    return JSON.parse(s);
  } catch {
    return null;
  }
}

export async function loadTickerNews(symbol: string) {
  const j = await loadTickerSeries(symbol);
  return j?.news ?? [];
}
