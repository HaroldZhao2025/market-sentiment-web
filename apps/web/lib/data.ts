// apps/web/lib/data.ts
import { loadJson } from "./paths";

// ---------- Types (must match Python writers) ----------
export type TickerSeries = {
  dates: string[];           // YYYY-MM-DD
  price: number[];
  sentiment: number[];       // combined S (news+earnings)
  sentiment_ma7: number[];   // 7d MA of S
  news: { ts: string; title: string; url: string; S: number }[];
};

export type PortfolioSeries = {
  dates: string[];
  long: number[];
  short: number[];
  long_short: number[];
};

// ---------- Loaders ----------
export async function loadTickers(): Promise<string[]> {
  // _tickers.json is a simple ["AAPL","MSFT",...]
  return loadJson<string[]>("_tickers.json");
}

export async function loadTicker(symbol: string): Promise<TickerSeries | null> {
  try {
    return await loadJson<TickerSeries>(`ticker/${symbol}.json`);
  } catch {
    return null;
  }
}

export async function loadEarnings(symbol: string) {
  try {
    return await loadJson<{ items: { ts: string; title: string; url: string; S: number }[] }>(
      `earnings/${symbol}.json`,
    );
  } catch {
    return { items: [] };
  }
}

export async function loadPortfolio(): Promise<PortfolioSeries | null> {
  try {
    return await loadJson<PortfolioSeries>("portfolio.json");
  } catch {
    return null;
  }
}
