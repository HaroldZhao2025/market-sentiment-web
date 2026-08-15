import fs from "node:fs/promises";
import path from "node:path";
import CompanyVisual from "../../../components/CompanyVisual";
import type { EarningsArtifact } from "../../earnings/[symbol]/EarningsIntelligenceClientV2";
import CompanyDetailTabs from "./CompanyDetailTabs";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url: string; text?: string; summary?: string; source?: string; provider?: string; s?: number | null; sentiment_label?: string; probs?: { pos?: number; neu?: number; neg?: number } };
type CompanyMeta = { ticker?: string; name?: string; sector?: string; industry?: string; universe?: string };

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; } catch { return null; }
}
const strArr = (value: unknown): string[] => Array.isArray(value) ? value.map((x) => String(x ?? "")) : [];
const numArr = (value: unknown): number[] => Array.isArray(value) ? value.map((x) => {
  if (x === null || x === undefined || x === "") return Number.NaN;
  const number = Number(x);
  return Number.isFinite(number) ? number : Number.NaN;
}) : [];
const boolArr = (value: unknown, length: number): boolean[] | null => Array.isArray(value)
  ? Array.from({ length }, (_, index) => value[index] === true || value[index] === 1 || value[index] === "1" || value[index] === "true")
  : null;

function buildSeries(obj: any): SeriesIn | null {
  const date = strArr(obj?.date ?? obj?.dates);
  const price = numArr(obj?.price ?? obj?.close ?? obj?.Close);
  if (!date.length || !price.length) return null;
  const n = Math.min(date.length, price.length);
  const rawSentiment = numArr(obj?.S ?? obj?.sentiment).slice(0, n);
  const observed = boolArr(obj?.sentiment_observed, n);
  const sentiment = Array.from({ length: n }, (_, index) => {
    const value = rawSentiment[index];
    if (!Number.isFinite(value)) return Number.NaN;
    if (observed && !observed[index]) return Number.NaN;
    return value;
  });
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment };
}

function buildNews(obj: any): NewsItem[] {
  const raw = Array.isArray(obj?.news) ? obj.news : Array.isArray(obj?.articles) ? obj.articles : [];
  return raw.map((row: any) => ({ ...row, ts: String(row?.ts ?? row?.date ?? ""), title: String(row?.title ?? row?.headline ?? ""), url: String(row?.url ?? ""), text: row?.text ? String(row.text) : undefined, summary: row?.summary ? String(row.summary) : undefined })).filter((row: NewsItem) => row.ts && row.title);
}

async function loadUniverse() {
  return (await readJSON<{ companies?: CompanyMeta[] }>(path.join(DATA_ROOT, "v5", "universe.json")))?.companies ?? [];
}

function legacyEarnings(symbol: string, raw: any): EarningsArtifact {
  const docs = Array.isArray(raw) ? raw : Array.isArray(raw?.docs) ? raw.docs : [];
  return {
    schema_version: 1,
    symbol,
    earnings_history: [],
    calls: [],
    call_links: [],
    filing_fallback: docs.map((doc: any) => ({ ts: String(doc?.ts ?? ""), title: String(doc?.title ?? ""), url: String(doc?.url ?? ""), source: String(doc?.source ?? "Legacy earnings source"), document_type: "legacy" })),
  };
}

async function loadEarnings(symbol: string): Promise<EarningsArtifact> {
  const current = await readJSON<EarningsArtifact>(path.join(DATA_ROOT, "v5", "earnings", `${symbol}.json`));
  if (current && typeof current === "object") return current;
  const legacy = await readJSON<any>(path.join(DATA_ROOT, "earnings", `${symbol}.json`));
  return legacy ? legacyEarnings(symbol, legacy) : { schema_version: 8, symbol, earnings_history: [], calls: [], call_links: [], filing_fallback: [] };
}

export async function generateStaticParams() {
  const core = (await readJSON<string[]>(path.join(DATA_ROOT, "_tickers.json"))) || ["AAPL"];
  const symbols = new Set(core);
  for (const company of await loadUniverse()) {
    const symbol = String(company?.ticker || "").trim().toUpperCase();
    if (symbol) symbols.add(symbol);
  }
  return Array.from(symbols).sort().map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const companies = await loadUniverse();
  const company = companies.find((row) => String(row.ticker || "").toUpperCase() === symbol);
  const [core, rich, extendedHistory, earnings] = await Promise.all([
    readJSON<any>(path.join(DATA_ROOT, "ticker", `${symbol}.json`)),
    readJSON<any>(path.join(DATA_ROOT, "v5", "news", `${symbol}.json`)),
    readJSON<any>(path.join(DATA_ROOT, "v5", "history", `${symbol}.json`)),
    loadEarnings(symbol),
  ]);
  const richNews = buildNews(rich);
  const compactNews = buildNews(core);
  const news = (richNews.length ? richNews : compactNews).slice(0, 120);
  const newsTotal = Number(rich?.article_count ?? core?.news_total ?? core?.newsTotal ?? core?.news_count?.total) || news.length;
  const series = buildSeries(core) ?? buildSeries(extendedHistory);
  const historyDays = series?.date.length ?? 0;
  const callCount = Array.isArray(earnings.calls) ? earnings.calls.length : 0;
  const callLinks = Array.isArray(earnings.call_links) ? earnings.call_links.length : 0;

  return (
    <main className="space-y-6">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="flex items-center gap-4">
          <CompanyVisual ticker={symbol} name={company?.name} sector={company?.sector} size="lg" />
          <div className="min-w-0">
            <div className="eyebrow">{company?.universe || "U.S. company"}</div>
            <h1 className="mt-1 text-3xl font-semibold tracking-tight text-white">{company?.name || symbol}</h1>
            <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-neutral-500"><span className="font-mono text-neutral-300">{symbol}</span>{company?.sector ? <span>{company.sector}</span> : null}{company?.industry && company.industry !== "Unknown" ? <span>{company.industry}</span> : null}</div>
          </div>
        </div>
        <div className="flex flex-wrap gap-2 text-xs">
          <span className="pill">{newsTotal} news</span>
          {historyDays > 0 ? <span className="pill">{historyDays} trading days</span> : null}
          {callCount > 0 ? <span className="pill text-emerald-300">{callCount} structured call{callCount === 1 ? "" : "s"}</span> : callLinks > 0 ? <span className="pill">{callLinks} call source{callLinks === 1 ? "" : "s"}</span> : <span className="pill text-neutral-600">Call search pending</span>}
        </div>
      </section>

      <CompanyDetailTabs symbol={symbol} series={series} news={news} newsTotal={newsTotal} earnings={earnings} />
    </main>
  );
}
