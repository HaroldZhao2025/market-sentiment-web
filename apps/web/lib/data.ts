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

/* ---------- tiny helpers (local only) ---------- */
function toNum(v: any): number | undefined {
  const n = typeof v === "string" ? Number(v) : v;
  return Number.isFinite(n) ? (n as number) : undefined;
}
function hostFromUrl(u?: string): string | undefined {
  if (!u) return undefined;
  try {
    return new URL(u).host.replace(/^www\./, "");
  } catch {
    return undefined;
  }
}

/* ---------- public loaders (unchanged signatures) ---------- */
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

/**
 * Normalize per-headline sentiment so the client always gets:
 * { ts, title, url, source?, provider?, s?: number, probs?: {pos?, neu?, neg?} }
 */
export async function loadTickerNews(symbol: string): Promise<any[]> {
  const obj = await loadTicker(symbol);
  const arr = Array.isArray(obj?.news) ? (obj!.news as any[]) : [];

  const out = arr.map((n) => {
    const ts = String(n?.ts ?? n?.time ?? n?.date ?? "");
    const title = String(n?.title ?? n?.headline ?? "");
    const url = String(n?.url ?? "");
    const provider = n?.provider;
    const source = n?.source ?? provider ?? hostFromUrl(url);

    // tolerate many shapes for probabilities
    const p = n?.probs ?? n?.scores ?? n?.probabilities;
    const pos = toNum(p?.pos ?? p?.positive ?? p?.Positive ?? p?.POS);
    const neu = toNum(p?.neu ?? p?.neutral ?? p?.Neutral ?? p?.NEU);
    const neg = toNum(p?.neg ?? p?.negative ?? p?.Negative ?? p?.NEG);

    // scalar s may come as number or numeric string
    let s = toNum(n?.s);
    if (s === undefined && pos !== undefined && neg !== undefined) {
      s = toNum((pos as number) - (neg as number));
    }

    const probs =
      pos !== undefined || neu !== undefined || neg !== undefined
        ? { pos, neu, neg }
        : undefined;

    return { ts, title, url, source, provider, s, probs };
  });

  // keep only minimally valid rows (title + url + ts)
  return out.filter((x) => x.title && x.url && x.ts);
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
