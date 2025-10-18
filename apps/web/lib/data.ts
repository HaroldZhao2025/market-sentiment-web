// apps/web/lib/data.ts
import fs from "node:fs";
import path from "node:path";

const DATA_DIR = path.join(process.cwd(), "public", "data");

export async function loadTickers(): Promise<string[]> {
  try {
    const p = path.join(DATA_DIR, "_tickers.json");
    const raw = await fs.promises.readFile(p, "utf-8");
    return JSON.parse(raw) as string[];
  } catch {
    return [];
  }
}

export type Portfolio = {
  dates: string[];
  long: number[];
  short: number[];
  long_short: number[];
};

export async function loadPortfolio(): Promise<Portfolio | null> {
  try {
    const p = path.join(DATA_DIR, "portfolio.json");
    const raw = await fs.promises.readFile(p, "utf-8");
    return JSON.parse(raw) as Portfolio;
  } catch {
    return null;
  }
}

export type Series = { dates: string[]; values: number[]; label: string };

export async function loadTickerSeries(symbol: string): Promise<{
  left: Series;   // price
  right: Series;  // daily S
  overlay: Series // 7d MA
} | null> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const obj = JSON.parse(await fs.promises.readFile(p, "utf-8"));
    const dates = (obj.date ?? obj.dates ?? []) as string[];
    return {
      left:   { dates, values: (obj.price ?? []) as number[], label: "Price" },
      right:  { dates, values: (obj.S ?? obj.sentiment ?? []) as number[], label: "Daily S" },
      overlay:{ dates, values: (obj.S_ma7 ?? obj.sentiment_ma7 ?? []) as number[], label: "7d MA" },
    };
  } catch {
    return null;
  }
}

export type TickerNewsItem = { ts: string; title: string; url: string; sentiment?: number };

export async function loadTickerNews(symbol: string): Promise<TickerNewsItem[]> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const obj = JSON.parse(await fs.promises.readFile(p, "utf-8"));
    return (obj.news ?? []) as TickerNewsItem[];
  } catch {
    return [];
  }
}

// Combined loader (if you prefer a single call)
export async function loadTicker(symbol: string): Promise<{
  series: {
    date: string[];
    price: number[];
    sentiment: number[];
    sentiment_ma7: number[];
    label: string;
  } | null;
  news: TickerNewsItem[];
}> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const obj = JSON.parse(await fs.promises.readFile(p, "utf-8"));
    return {
      series: {
        date: obj.date ?? obj.dates ?? [],
        price: obj.price ?? [],
        sentiment: obj.S ?? obj.sentiment ?? [],
        sentiment_ma7: obj.S_ma7 ?? obj.sentiment_ma7 ?? [],
        label: "Daily S",
      },
      news: obj.news ?? [],
    };
  } catch {
    return { series: null, news: [] };
  }
}
