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

/* ----------------------- Helpers for news normalization ----------------------- */

function hostFrom(u?: string): string {
  try {
    return u ? new URL(u).host.replace(/^www\./, "") : "";
  } catch {
    return "";
  }
}
function num(v: any): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}
function clamp(x: number, lo = -1, hi = 1): number {
  return Math.max(lo, Math.min(hi, x));
}

function normalizeNewsItem(n: any): any {
  const out: any = { ...n };

  // unify timestamp + source
  out.ts = n.ts ?? n.time ?? n.published_at ?? n.date ?? n.pubDate ?? n.pub_time ?? "";
  out.source = n.source ?? n.provider ?? (n.url ? hostFrom(n.url) : undefined);

  // (1) probabilities-like objects under various keys
  const probCandidates = [
    n.probs,
    n.prob,
    n.p,
    n.probabilities,
    n.sentiment,
    n.finbert,
    n.headline_sentiment,
  ];
  for (const obj of probCandidates) {
    if (obj && typeof obj === "object") {
      const pos = num(obj.pos ?? obj.positive ?? obj.Positive ?? obj.POS);
      const neu = num(obj.neu ?? obj.neutral ?? obj.Neutral ?? obj.NEU);
      const neg = num(obj.neg ?? obj.negative ?? obj.Negative ?? obj.NEG);
      if (pos || neu || neg) {
        out.probs = { pos, neu, neg };
        out.s = clamp(pos - neg);
        return out;
      }
    }
  }

  // (2) FinBERT-style array: [{label, score}, ...]
  if (Array.isArray(n.scores) && n.scores.length) {
    let pos = 0, neg = 0, neu = 0;
    for (const it of n.scores) {
      const lab = String(it?.label ?? "").toLowerCase();
      const sc = num(it?.score);
      if (!Number.isFinite(sc)) continue;
      if (lab.includes("pos")) pos = sc;
      else if (lab.includes("neg")) neg = sc;
      else if (lab.includes("neu")) neu = sc;
    }
    if (pos || neg || neu) {
      out.probs = { pos, neu, neg };
      out.s = clamp(pos - neg);
      return out;
    }
  }

  // (3) { sent: {label?, score} }
  if (n.sent && typeof n.sent === "object") {
    const sc = num(n.sent.score);
    if (Number.isFinite(sc)) {
      out.s = clamp(sc);
      return out;
    }
  }

  // (4) direct numeric score fields
  const sc = n.s ?? n.sent_score ?? n.score;
  if (typeof sc === "number" && Number.isFinite(sc)) {
    out.s = clamp(sc);
    return out;
  }

  // Nothing recognized; return as-is (TickerClient will show "–")
  return out;
}

/* ----------------------- News loader (normalized) ----------------------- */

export async function loadTickerNews(symbol: string): Promise<any[]> {
  const obj = await loadTicker(symbol);
  const raw = Array.isArray(obj?.news)
    ? obj!.news
    : Array.isArray(obj?.headlines)
    ? obj!.headlines
    : [];

  // Normalize and keep the first 10 as requested
  return raw.slice(0, 10).map(normalizeNewsItem);
}

/* ----------------------- Series loader (unchanged) ----------------------- */

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
