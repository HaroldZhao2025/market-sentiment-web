import fs from "fs/promises";
import path from "path";

const DATA_DIR = path.join(process.cwd(), "public", "data");

export async function loadTickers(): Promise<string[]> {
  try {
    const p = path.join(DATA_DIR, "_tickers.json");
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export type Series = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
};

export async function loadTickerSeries(symbol: string): Promise<Series | null> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const raw = JSON.parse(await fs.readFile(p, "utf8"));

    // Support both legacy keys and new ones
    const dates: string[] = raw.date ?? raw.dates ?? [];
    const price: number[] = raw.close ?? raw.price ?? [];
    const sentiment: number[] = raw.S ?? raw.sentiment ?? [];
    const sentiment_ma7: number[] | undefined = raw.S_ma7 ?? raw.sentiment_ma7;

    if (!dates.length || !price.length || !sentiment.length) return null;

    return {
      date: dates,
      price,
      sentiment,
      sentiment_ma7,
    };
  } catch {
    return null;
  }
}

export type NewsItem = {
  ts: string;
  title: string;
  url: string;
  s?: number | null;
  source?: string | null;
};

export async function loadTickerNews(symbol: string): Promise<NewsItem[]> {
  try {
    const p = path.join(DATA_DIR, "ticker", `${symbol}.json`);
    const raw = JSON.parse(await fs.readFile(p, "utf8"));
    const news = (raw.news ?? []) as any[];
    return news.map((r) => ({
      ts: r.ts ?? r.date ?? "",
      title: r.title ?? "",
      url: r.url ?? "",
      s: typeof r.s === "number" ? r.s : undefined,
      source: r.source ?? null,
    }));
  } catch {
    return [];
  }
}
