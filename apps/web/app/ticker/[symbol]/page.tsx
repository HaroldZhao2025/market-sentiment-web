import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url: string; text?: string };

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T = any>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; } catch { return null; }
}
const numArr = (v: unknown): number[] => Array.isArray(v) ? v.map((x) => Number(x) || 0) : [];
const strArr = (v: unknown): string[] => Array.isArray(v) ? v.map((x) => String(x ?? "")) : [];

function buildSeries(obj: any): SeriesIn | null {
  const date = strArr(obj?.date ?? obj?.dates);
  const price = numArr(obj?.price ?? obj?.close ?? obj?.Close);
  const sentiment = numArr(obj?.S ?? obj?.sentiment);
  const n = Math.min(date.length, (price.length || Infinity), (sentiment.length || Infinity));
  if (!Number.isFinite(n) || n === 0) return null;
  return { date: date.slice(0, n), price: price.slice(0, n), sentiment: sentiment.slice(0, n) };
}
function buildNews(obj: any): NewsItem[] {
  const raw = Array.isArray(obj?.news) ? obj.news : [];
  return raw.map((r: any) => ({
    ts: String(r?.ts ?? r?.date ?? ""),
    title: String(r?.title ?? ""),
    url: String(r?.url ?? ""),
    text: r?.text ? String(r.text) : undefined,
  })).filter((r: NewsItem) => r.ts && r.title);
}

export async function generateStaticParams() {
  const list = (await readJSON<string[]>(path.join(DATA_ROOT, "_tickers.json"))) || ["AAPL"];
  return list.map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const f = path.join(DATA_ROOT, "ticker", `${symbol}.json`);
  const obj = await readJSON<any>(f);

  if (!obj) {
    return <div className="page"><h1 className="page-title">{symbol}</h1><p className="muted">No data for {symbol}.</p></div>;
  }

  const series = buildSeries(obj);
  const news = buildNews(obj);

  return (
    <div className="page">
      <h1 className="page-title">{symbol}</h1>
      {series ? <TickerClient symbol={symbol} series={series} news={news} /> : <p className="muted">No time series for {symbol}.</p>}
    </div>
  );
}
