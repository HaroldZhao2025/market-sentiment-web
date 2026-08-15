"use client";

import { useMemo, useState } from "react";
import LineChart, { ChartLegend } from "../../../components/LineChart";

export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
export type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;
  summary?: string;
  source?: string;
  provider?: string;
  s?: number | null;
  probs?: { pos?: number; neu?: number; neg?: number };
  sentiment_label?: string;
};

type View = "overlay" | "separate";
type ThemeSummary = { name: string; count: number; score: number };

const THEME_RULES: { name: string; terms: string[] }[] = [
  { name: "Earnings & guidance", terms: ["earnings", "revenue", "profit", "eps", "guidance", "quarter", "margin", "forecast"] },
  { name: "Product & AI", terms: ["ai", "artificial intelligence", "product", "launch", "iphone", "chip", "model", "cloud", "software"] },
  { name: "Regulation & legal", terms: ["regulator", "antitrust", "lawsuit", "court", "legal", "tariff", "ban", "probe", "investigation"] },
  { name: "Deals & capital", terms: ["acquisition", "merger", "deal", "buyback", "dividend", "stake", "financing", "investment"] },
  { name: "Operations & demand", terms: ["demand", "supply", "shipment", "production", "sales", "orders", "factory", "inventory"] },
  { name: "Analyst & market", terms: ["analyst", "rating", "target", "upgrade", "downgrade", "market", "valuation", "outlook"] },
];

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function observedValues(values: number[]) {
  return values.map(finite).filter((value): value is number => value != null);
}

function rollingObservedMean(values: number[], window = 7) {
  return values.map((_, index) => {
    const slice = values.slice(Math.max(0, index - window + 1), index + 1).map(finite).filter((value): value is number => value != null);
    return slice.length ? slice.reduce((sum, value) => sum + value, 0) / slice.length : Number.NaN;
  });
}

function label(value: number | null) {
  if (value == null) return "No observation";
  if (value >= 0.4) return "Strong Positive";
  if (value >= 0.1) return "Positive";
  if (value <= -0.4) return "Strong Negative";
  if (value <= -0.1) return "Negative";
  return "Neutral";
}

function tone(value: number | null) {
  if (value == null) return "text-neutral-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-neutral-300";
}

function signed(value: number | null, digits = 4) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

function pct(value: number | null, digits = 2) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${(value * 100).toFixed(digits)}%`;
}

function sourceOf(item: NewsItem) {
  if (item.source) return item.source;
  if (item.provider) return item.provider;
  try { return new URL(item.url).host.replace(/^www\./, ""); } catch { return ""; }
}

function dateOnly(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value).slice(0, 10) : date.toISOString().slice(0, 10);
}

function classifyTheme(item: NewsItem) {
  const text = `${item.title || ""} ${item.summary || ""}`.toLowerCase();
  for (const rule of THEME_RULES) if (rule.terms.some((term) => text.includes(term))) return rule.name;
  return "Other company news";
}

function summarizeThemes(news: NewsItem[]): ThemeSummary[] {
  const groups = new Map<string, number[]>();
  for (const item of news) {
    const score = finite(item.s);
    if (score == null) continue;
    const theme = classifyTheme(item);
    groups.set(theme, [...(groups.get(theme) ?? []), score]);
  }
  return Array.from(groups.entries()).map(([name, values]) => ({
    name,
    count: values.length,
    score: values.reduce((sum, value) => sum + value, 0) / values.length,
  })).sort((a, b) => Math.abs(b.score) * Math.sqrt(b.count) - Math.abs(a.score) * Math.sqrt(a.count));
}

function DriverCard({ item, positive }: { item: NewsItem; positive: boolean }) {
  const score = finite(item.s);
  return (
    <a href={item.url} target="_blank" rel="noreferrer" className="card card-hover block p-4">
      <div className="flex items-start justify-between gap-3">
        <span className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-wider ${positive ? "bg-emerald-400/10 text-emerald-300" : "bg-rose-400/10 text-rose-300"}`}>{positive ? "Positive" : "Negative"}</span>
        <span className={`font-mono text-xs ${tone(score)}`}>{signed(score, 3)}</span>
      </div>
      <div className="mt-3 text-sm font-medium leading-6 text-neutral-200">{item.title}</div>
      <div className="mt-2 text-[11px] text-neutral-600">{sourceOf(item)} · {dateOnly(item.ts)}</div>
    </a>
  );
}

export default function TickerClientV2({ series, news, newsTotal = 0 }: { symbol: string; series: SeriesIn; news: NewsItem[]; newsTotal?: number }) {
  const [mode, setMode] = useState<View>("overlay");
  const n = Math.min(series.date.length, series.price.length, series.sentiment.length);
  const aligned = useMemo(() => ({
    date: series.date.slice(0, n),
    price: series.price.slice(0, n),
    sentiment: series.sentiment.slice(0, n),
  }), [series, n]);
  const sentimentMA7 = useMemo(() => rollingObservedMean(aligned.sentiment, 7), [aligned.sentiment]);
  const sentObserved = useMemo(() => observedValues(aligned.sentiment), [aligned.sentiment]);
  const priceObserved = useMemo(() => observedValues(aligned.price), [aligned.price]);
  const latestSent = sentObserved.at(-1) ?? null;
  const priorSent = sentObserved.at(-2) ?? null;
  const sentimentChange = latestSent != null && priorSent != null ? latestSent - priorSent : null;
  const latestMA7 = finite(sentimentMA7.at(-1));
  const latestPrice = priceObserved.at(-1) ?? null;
  const priorPrice = priceObserved.at(-2) ?? null;
  const priceReturn = latestPrice != null && priorPrice != null && priorPrice !== 0 ? latestPrice / priorPrice - 1 : null;
  const scoredNews = useMemo(() => news.filter((item) => finite(item.s) != null), [news]);
  const recentNewsMean = scoredNews.length ? scoredNews.reduce((sum, item) => sum + (finite(item.s) ?? 0), 0) / scoredNews.length : null;
  const themes = useMemo(() => summarizeThemes(scoredNews), [scoredNews]);
  const dominantTheme = themes[0] ?? null;
  const positiveDrivers = useMemo(() => scoredNews.filter((item) => (finite(item.s) ?? 0) > 0).sort((a, b) => (finite(b.s) ?? 0) - (finite(a.s) ?? 0)).slice(0, 3), [scoredNews]);
  const negativeDrivers = useMemo(() => scoredNews.filter((item) => (finite(item.s) ?? 0) < 0).sort((a, b) => (finite(a.s) ?? 0) - (finite(b.s) ?? 0)).slice(0, 3), [scoredNews]);

  return (
    <div className="space-y-8">
      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="kpi"><div className="kpi-label">Latest observed sentiment</div><div className={`kpi-value ${tone(latestSent)}`}>{label(latestSent)}</div><div className="kpi-sub font-mono">{signed(latestSent)}</div></div>
        <div className="kpi"><div className="kpi-label">Observed change</div><div className={`kpi-value ${tone(sentimentChange)}`}>{signed(sentimentChange)}</div><div className="kpi-sub">Latest two news observations</div></div>
        <div className="kpi"><div className="kpi-label">7D observed average</div><div className={`kpi-value ${tone(latestMA7)}`}>{signed(latestMA7)}</div><div className="kpi-sub">Missing-news days are ignored</div></div>
        <div className="kpi"><div className="kpi-label">1D price return</div><div className={`kpi-value ${tone(priceReturn)}`}>{pct(priceReturn)}</div><div className="kpi-sub font-mono">Close {latestPrice == null ? "—" : latestPrice.toFixed(2)}</div></div>
      </section>

      <section className="space-y-4">
        <div><div className="eyebrow">Current drivers</div><h2 className="section-title mt-1">What moved the signal</h2></div>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="card p-5"><div className="kpi-label">Retained scored news</div><div className="text-2xl font-semibold text-white">{scoredNews.length.toLocaleString()}</div><div className="mt-1 text-xs text-neutral-600">{newsTotal.toLocaleString()} retained items total</div></div>
          <div className="card p-5"><div className="kpi-label">Article mean</div><div className={`text-2xl font-semibold ${tone(recentNewsMean)}`}>{signed(recentNewsMean)}</div></div>
          <div className="card p-5"><div className="kpi-label">Leading theme</div><div className="mt-1 text-lg font-semibold text-white">{dominantTheme?.name ?? "No scored theme"}</div>{dominantTheme ? <div className={`mt-1 font-mono text-xs ${tone(dominantTheme.score)}`}>{dominantTheme.count} items · {signed(dominantTheme.score, 3)}</div> : null}</div>
          <div className="card p-5"><div className="kpi-label">Signal observations</div><div className="text-2xl font-semibold text-white">{sentObserved.length.toLocaleString()}</div><div className="mt-1 text-xs text-neutral-600">Across {aligned.date.length.toLocaleString()} trading days</div></div>
        </div>
        {themes.length ? <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">{themes.slice(0, 6).map((theme) => (
          <div key={theme.name} className="card p-4"><div className="flex items-start justify-between gap-3"><div><div className="text-sm font-semibold text-neutral-200">{theme.name}</div><div className="mt-1 text-[11px] text-neutral-600">{theme.count} scored item{theme.count === 1 ? "" : "s"}</div></div><div className={`font-mono text-xs ${tone(theme.score)}`}>{signed(theme.score, 3)}</div></div></div>
        ))}</div> : null}
        {(positiveDrivers.length || negativeDrivers.length) ? <div className="grid gap-3 lg:grid-cols-2"><div className="space-y-3"><div className="text-xs font-semibold uppercase tracking-[0.14em] text-emerald-400">Positive drivers</div>{positiveDrivers.map((item, index) => <DriverCard key={`${item.url}-${index}`} item={item} positive />)}</div><div className="space-y-3"><div className="text-xs font-semibold uppercase tracking-[0.14em] text-rose-400">Negative drivers</div>{negativeDrivers.map((item, index) => <DriverCard key={`${item.url}-${index}`} item={item} positive={false} />)}</div></div> : null}
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-end justify-between gap-4"><div><div className="eyebrow">History</div><h2 className="section-title mt-1">Price and observed sentiment</h2></div><div className="flex items-center gap-3"><ChartLegend /><div className="flex rounded-xl border border-white/10 bg-black/30 p-1"><button type="button" onClick={() => setMode("overlay")} className={`rounded-lg px-3 py-2 text-xs ${mode === "overlay" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Overlay</button><button type="button" onClick={() => setMode("separate")} className={`rounded-lg px-3 py-2 text-xs ${mode === "separate" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Separate</button></div></div></div>
        <div className="ambient-panel p-3 md:p-5"><LineChart mode={mode} dates={aligned.date} price={aligned.price} sentiment={aligned.sentiment} sentimentMA7={sentimentMA7} height={520} /></div>
      </section>
    </div>
  );
}
