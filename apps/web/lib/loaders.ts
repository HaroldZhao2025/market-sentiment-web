// apps/web/lib/loaders.ts
import fs from "node:fs";
import path from "node:path";

export type TickerSeriesPoint = { date: string; close: number; S: number };
export type TickerNewsItem = { ts: string; title: string; url: string; s: number; source?: string };

export type TickerJson = {
  ticker: string;
  series: TickerSeriesPoint[];
  news: TickerNewsItem[];
};

export type PortfolioJson = {
  dates: string[];
  equity: number[];
  ret: number[];
  stats?: { ann_return?: number; ann_vol?: number; sharpe?: number; max_dd?: number };
};

const DATA_DIR = path.join(process.cwd(), "public", "data");

export function safeReadJSON<T>(...segments: string[]): T | null {
  const file = path.join(DATA_DIR, ...segments);
  try {
    if (!fs.existsSync(file)) return null;
    const raw = fs.readFileSync(file, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function listTickers(): string[] {
  const arr = safeReadJSON<string[]>("_tickers.json");
  if (Array.isArray(arr) && arr.length) return arr;
  // fallback (prevents build failure)
  return ["AAPL", "MSFT", "GOOGL"];
}

export function loadTicker(symbol: string): TickerJson {
  const j = safeReadJSON<TickerJson>(`${symbol}.json`);
  if (j && Array.isArray(j.series) && Array.isArray(j.news)) return j;
  return { ticker: symbol, series: [], news: [] };
}

export function loadPortfolio(): PortfolioJson | null {
  const p = safeReadJSON<PortfolioJson>("portfolio.json");
  if (!p) return null;
  // normalize arrays
  p.dates = Array.isArray(p.dates) ? p.dates : [];
  p.equity = Array.isArray(p.equity) ? p.equity : [];
  p.ret = Array.isArray(p.ret) ? p.ret : [];
  return p;
}
