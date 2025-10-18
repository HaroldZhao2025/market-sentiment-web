// apps/web/lib/data.ts
import fs from "node:fs";
import path from "node:path";

export type Series = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7: number[];
};

export type NewsItem = {
  ts?: string;
  title?: string;
  url?: string;
  S?: number;
  [k: string]: unknown;
};

export type TickerJson = {
  // tolerate multiple historical shapes
  dates?: string[];
  date?: string[];
  price?: number[];
  open?: number[];
  close?: number[];
  sentiment?: number[];
  S?: number[];
  sentiment_ma7?: number[];
  S_ma7?: number[];
  news?: NewsItem[];
  [k: string]: unknown;
};

function dataDir(): string {
  // Build-time FS path (static export)
  return path.join(process.cwd(), "public", "data");
}

export function loadTickers(): string[] {
  try {
    const file = path.join(dataDir(), "_tickers.json");
    const raw = fs.readFileSync(file, "utf-8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export function loadTickerSeries(symbol: string): Series {
  const file = path.join(dataDir(), "ticker", `${symbol}.json`);
  let j: TickerJson = {};
  try {
    j = JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch {
    // empty
  }
  const dates = (j.dates ?? j.date ?? []) as string[];
  // prefer explicit "price", else fall back to "close"
  const price = (j.price ?? j.close ?? []) as number[];
  const sentiment = (j.sentiment ?? j.S ?? []) as number[];
  const sentiment_ma7 = (j.sentiment_ma7 ?? j.S_ma7 ?? []) as number[];

  return { date: dates, price, sentiment, sentiment_ma7 };
}

export function loadTickerNews(symbol: string): NewsItem[] {
  // merge news embedded in ticker file + earnings file (if present)
  const tf = path.join(dataDir(), "ticker", `${symbol}.json`);
  const ef = path.join(dataDir(), "earnings", `${symbol}.json`);
  let news: NewsItem[] = [];
  try {
    const tj = JSON.parse(fs.readFileSync(tf, "utf-8"));
    if (Array.isArray(tj?.news)) news = news.concat(tj.news);
  } catch {}
  try {
    const ej = JSON.parse(fs.readFileSync(ef, "utf-8"));
    if (Array.isArray(ej)) news = news.concat(ej);
  } catch {}
  return news;
}
