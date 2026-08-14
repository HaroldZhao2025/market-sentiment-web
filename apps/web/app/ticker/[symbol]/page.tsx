import fs from "node:fs/promises";
import path from "node:path";
import CompanyVisual from "../../../components/CompanyVisual";
import TickerClient from "./TickerClient";

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
const numArr = (v: unknown): number[] => (Array.isArray(v) ? v.map((x) => Number(x) || 0) : []);
const strArr = (v: unknown): string[] => (Array.isArray(v) ? v.map((x) => String(x ?? "")) : []);

function buildSeries(obj: any): SeriesIn | null {
  const date = strArr(obj?.date ?? obj?.dates);
  const price = numArr(obj?.price ?? obj?.close ?? obj?.Close);
  const sentiment = numArr(obj?.S ?? obj?.sentiment);
  const n = Math.min(date.length, price.length || Infinity, sentiment.length || Infinity);
  if (!Number.isFinite(n) || n === 0) return null;
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}

function buildNews(obj: any): NewsItem[] {
  const raw = Array.isArray(obj?.news) ? obj.news : Array.isArray(obj?.articles) ? obj.articles : [];
  return raw.map((r: any) => ({ ...r, ts: String(r?.ts ?? r?.date ?? ""), title: String(r?.title ?? r?.headline ?? ""), url: String(r?.url ?? ""), text: r?.text ? String(r.text) : undefined, summary: r?.summary ? String(r.summary) : undefined })).filter((r: NewsItem) => r.ts && r.title);
}

async function loadUniverse() {
  return (await readJSON<{ companies?: CompanyMeta[] }>(path.join(DATA_ROOT, "v5", "universe.json")))?.companies ?? [];
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
  const obj = await readJSON<any>(path.join(DATA_ROOT, "ticker", `${symbol}.json`));
  const rich = await readJSON<any>(path.join(DATA_ROOT, "v5", "news", `${symbol}.json`));
  const richNews = buildNews(rich);

  const header = (
    <section className="flex items-center gap-4">
      <CompanyVisual ticker={symbol} name={company?.name} sector={company?.sector} size="lg" />
      <div className="min-w-0">
        <div className="eyebrow">{company?.universe || "U.S. company"}</div>
        <h1 className="mt-1 text-3xl font-semibold tracking-tight text-white">{company?.name || symbol}</h1>
        <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-neutral-500"><span className="font-mono text-neutral-300">{symbol}</span>{company?.sector ? <span>{company.sector}</span> : null}{company?.industry && company.industry !== "Unknown" ? <span>{company.industry}</span> : null}</div>
      </div>
    </section>
  );

  if (!obj) {
    return (
      <main className="space-y-6">
        {header}
        <p className="max-w-2xl text-sm text-neutral-500">Price history is still building. Recent company news is available below.</p>
        {richNews.length ? <div className="table-shell overflow-x-auto"><table className="w-full min-w-[820px] text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Date</th><th className="px-4 py-3">Headline</th><th className="px-4 py-3">Source</th><th className="px-4 py-3 text-right">Sentiment</th></tr></thead><tbody>{richNews.slice(0, 60).map((item, index) => <tr key={`${item.url}-${index}`} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-mono text-xs text-neutral-600">{item.ts.slice(0, 10)}</td><td className="px-4 py-3"><a href={item.url} target="_blank" rel="noreferrer" className="font-medium text-neutral-200 hover:underline">{item.title}</a></td><td className="px-4 py-3 text-xs text-neutral-500">{item.source || item.provider || "—"}</td><td className="px-4 py-3 text-right font-mono text-xs text-neutral-400">{typeof item.s === "number" ? `${item.s > 0 ? "+" : ""}${item.s.toFixed(3)}` : "—"}</td></tr>)}</tbody></table></div> : <div className="card p-5 text-sm text-neutral-500">Recent news has not been collected for this company yet.</div>}
      </main>
    );
  }

  const series = buildSeries(obj);
  const compact = buildNews(obj);
  const news = (richNews.length ? richNews : compact).slice(0, 60);
  const newsTotal = Number(rich?.article_count ?? obj?.news_total ?? obj?.newsTotal ?? obj?.news_count?.total) || news.length;

  return (
    <main className="space-y-5">
      {header}
      {series ? <TickerClient symbol={symbol} series={series} news={news as any} newsTotal={newsTotal} /> : <div className="text-neutral-500">No time series for {symbol}.</div>}
    </main>
  );
}
