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

/* ---------- helpers (new) ---------- */
function num(v: any): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}
function clamp(x: number, lo = -1, hi = 1): number {
  return Math.max(lo, Math.min(hi, x));
}
function hostFromUrl(u?: string): string {
  try {
    return u ? new URL(u).host.replace(/^www\./, "") : "";
  } catch {
    return "";
  }
}

/**
 * Normalize one headline item to always include:
 *  - s: number in [-1,1] (positive - negative)
 *  - probs: {pos, neu, neg} when available
 *  - source: fallback to provider or URL host
 */
function normalizeHeadline(n: any): any {
  const out: any = { ...n };

  // Accept multiple possible shapes for probabilities
  const probsRaw =
    n?.probs ??
    n?.scores ??
    n?.probabilities ??
    n?.prob ??
    null;

  if (probsRaw && typeof probsRaw === "object") {
    const pos = num(probsRaw.pos ?? probsRaw.positive ?? probsRaw.Positive ?? probsRaw.POS);
    const neu = num(probsRaw.neu ?? probsRaw.neutral ?? probsRaw.Neutral ?? probsRaw.NEU);
    const neg = num(probsRaw.neg ?? probsRaw.negative ?? probsRaw.Negative ?? probsRaw.NEG);
    out.probs = { pos, neu, neg };
    if (out.s == null) out.s = clamp(pos - neg);
  }

  // Fallback scalar score keys
  if (out.s == null) {
    const sRaw = n?.s ?? n?.score ?? n?.sentiment ?? n?.head_score ?? null;
    if (typeof sRaw === "number" && isFinite(sRaw)) out.s = clamp(sRaw);
  }

  // Ensure a readable source
  if (!out.source) out.source = n?.provider || hostFromUrl(n?.url);

  return out;
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
  const news = Array.isArray(obj?.news) ? (obj!.news as any[]) : [];
  // *** minimal, robust normalization ***
  return news.map(normalizeHeadline);
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
