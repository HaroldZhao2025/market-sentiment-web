"use client";

import { useMemo, useState } from "react";
import LineChart, { ChartLegend } from "../../../components/LineChart";

export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
export type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;
  source?: string;
  provider?: string;
  s?: number;
  probs?: { pos?: number; neu?: number; neg?: number };
  sentiment_label?: string;
};

type View = "overlay" | "separate";

type ThemeSummary = {
  name: string;
  count: number;
  score: number;
  headlines: NewsItem[];
};

const THEME_RULES: { name: string; terms: string[] }[] = [
  { name: "Earnings & guidance", terms: ["earnings", "revenue", "profit", "eps", "guidance", "quarter", "margin", "forecast"] },
  { name: "Product & AI", terms: ["ai", "artificial intelligence", "product", "launch", "iphone", "chip", "model", "cloud", "software"] },
  { name: "Regulation & legal", terms: ["regulator", "antitrust", "lawsuit", "court", "legal", "tariff", "ban", "probe", "investigation"] },
  { name: "Deals & capital", terms: ["acquisition", "merger", "deal", "buyback", "dividend", "stake", "financing", "investment"] },
  { name: "Operations & demand", terms: ["demand", "supply", "shipment", "production", "sales", "orders", "factory", "inventory"] },
  { name: "Analyst & market", terms: ["analyst", "rating", "target", "upgrade", "downgrade", "market", "valuation", "outlook"] },
];

function finite(x: unknown): number | null {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    run += arr[i];
    if (i >= 7) run -= arr[i - 7];
    out.push(i >= 6 ? run / 7 : Number.NaN);
  }
  return out;
}

function label(v: number | null) {
  if (v == null) return "No observation";
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}

function signalClass(v: number | null) {
  if (v == null) return "text-neutral-400";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}

function fmtSigned(v: number | null, digits = 4) {
  if (v == null) return "—";
  return `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
}

function fmtPct(v: number | null, digits = 2) {
  if (v == null) return "—";
  return `${v > 0 ? "+" : ""}${(v * 100).toFixed(digits)}%`;
}

function classifyTheme(item: NewsItem) {
  const text = `${item.title || ""} ${item.text || ""}`.toLowerCase();
  for (const rule of THEME_RULES) {
    if (rule.terms.some((term) => text.includes(term))) return rule.name;
  }
  return "Other company news";
}

function summarizeThemes(news: NewsItem[]): ThemeSummary[] {
  const map = new Map<string, { scores: number[]; headlines: NewsItem[] }>();
  news.forEach((item) => {
    const score = finite(item.s);
    if (score == null) return;
    const theme = classifyTheme(item);
    const current = map.get(theme) ?? { scores: [], headlines: [] };
    current.scores.push(score);
    current.headlines.push(item);
    map.set(theme, current);
  });

  return Array.from(map.entries())
    .map(([name, value]) => ({
      name,
      count: value.scores.length,
      score: value.scores.reduce((a, b) => a + b, 0) / value.scores.length,
      headlines: value.headlines,
    }))
    .sort((a, b) => Math.abs(b.score) * Math.sqrt(b.count) - Math.abs(a.score) * Math.sqrt(a.count));
}

function sourceOf(item: NewsItem) {
  if (item.source) return item.source;
  if (item.provider) return item.provider;
  try { return new URL(item.url).host.replace(/^www\./, ""); } catch { return ""; }
}

function dateOnly(value: string) {
  const d = new Date(value);
  if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
}

function DriverCard({ item, positive }: { item: NewsItem; positive: boolean }) {
  const score = finite(item.s);
  return (
    <a
      href={item.url}
      target="_blank"
      rel="noreferrer"
      className="card card-hover block p-4"
    >
      <div className="flex items-start justify-between gap-3">
        <span className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-wider ${
          positive ? "bg-emerald-400/10 text-emerald-300" : "bg-rose-400/10 text-rose-300"
        }`}>
          {positive ? "Positive driver" : "Negative driver"}
        </span>
        <span className={`font-mono text-xs ${signalClass(score)}`}>{fmtSigned(score, 3)}</span>
      </div>
      <div className="mt-3 text-sm font-medium leading-6 text-neutral-200">{item.title}</div>
      <div className="mt-2 text-[11px] text-neutral-600">{sourceOf(item)} · {dateOnly(item.ts)}</div>
    </a>
  );
}

export default function TickerClient({
  symbol,
  series,
  news,
  newsTotal = 0,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
  newsTotal?: number;
}) {
  const [mode, setMode] = useState<View>("overlay");

  const aligned = useMemo(() => {
    const n = Math.min(series.date.length, series.price.length, series.sentiment.length);
    return {
      date: series.date.slice(0, n),
      price: series.price.slice(0, n),
      sentiment: series.sentiment.slice(0, n),
    };
  }, [series]);

  const sentimentMA7 = useMemo(() => ma7(aligned.sentiment), [aligned.sentiment]);
  const latestSent = finite(aligned.sentiment.at(-1));
  const priorSent = finite(aligned.sentiment.at(-2));
  const sentimentChange = latestSent != null && priorSent != null ? latestSent - priorSent : null;
  const latestMA7 = finite(sentimentMA7.at(-1));
  const latestPrice = finite(aligned.price.at(-1));
  const priorPrice = finite(aligned.price.at(-2));
  const priceReturn = latestPrice != null && priorPrice != null && priorPrice !== 0 ? latestPrice / priorPrice - 1 : null;

  const scoredNews = useMemo(() => news.filter((item) => finite(item.s) != null), [news]);
  const recentNewsMean = scoredNews.length
    ? scoredNews.reduce((sum, item) => sum + (finite(item.s) ?? 0), 0) / scoredNews.length
    : null;
  const themes = useMemo(() => summarizeThemes(scoredNews), [scoredNews]);
  const dominantTheme = themes[0] ?? null;
  const positiveDrivers = useMemo(
    () => scoredNews.slice().sort((a, b) => (finite(b.s) ?? -Infinity) - (finite(a.s) ?? -Infinity)).filter((x) => (finite(x.s) ?? 0) > 0).slice(0, 3),
    [scoredNews]
  );
  const negativeDrivers = useMemo(
    () => scoredNews.slice().sort((a, b) => (finite(a.s) ?? Infinity) - (finite(b.s) ?? Infinity)).filter((x) => (finite(x.s) ?? 0) < 0).slice(0, 3),
    [scoredNews]
  );

  return (
    <div className="space-y-8">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Ticker intelligence</div>
          <h1 className="page-title mt-2">{symbol}</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Price reaction, observed news sentiment, article-level evidence, and deterministic event themes.
          </p>
        </div>
        <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
          <button type="button" onClick={() => setMode("overlay")} className={`rounded-lg px-3 py-2 text-xs ${mode === "overlay" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Overlay</button>
          <button type="button" onClick={() => setMode("separate")} className={`rounded-lg px-3 py-2 text-xs ${mode === "separate" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Separate</button>
        </div>
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="kpi">
          <div className="kpi-label">Latest sentiment</div>
          <div className={`kpi-value ${signalClass(latestSent)}`}>{label(latestSent)}</div>
          <div className="kpi-sub font-mono">{fmtSigned(latestSent, 4)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Sentiment change</div>
          <div className={`kpi-value ${signalClass(sentimentChange)}`}>{fmtSigned(sentimentChange, 4)}</div>
          <div className="kpi-sub">Latest observation vs previous observation</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">7D sentiment average</div>
          <div className={`kpi-value ${signalClass(latestMA7)}`}>{fmtSigned(latestMA7, 4)}</div>
          <div className="kpi-sub">Smoothed signal, not a return forecast</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">1D price return</div>
          <div className={`kpi-value ${signalClass(priceReturn)}`}>{fmtPct(priceReturn)}</div>
          <div className="kpi-sub">Latest close: {latestPrice == null ? "—" : latestPrice.toFixed(2)}</div>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <div className="eyebrow">Why sentiment changed</div>
          <h2 className="section-title mt-1">Evidence behind the current signal</h2>
          <p className="section-copy">Article scores are grouped with deterministic keyword rules. This is an auditable explanation layer, not an LLM-generated narrative.</p>
        </div>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="card p-5">
            <div className="kpi-label">Recent article mean</div>
            <div className={`text-2xl font-semibold ${signalClass(recentNewsMean)}`}>{fmtSigned(recentNewsMean, 4)}</div>
            <div className="mt-1 text-xs text-neutral-600">{scoredNews.length} scored headlines displayed</div>
          </div>
          <div className="card p-5">
            <div className="kpi-label">Dominant event theme</div>
            <div className="text-lg font-semibold text-white">{dominantTheme?.name ?? "No scored theme"}</div>
            <div className={`mt-1 font-mono text-xs ${signalClass(dominantTheme?.score ?? null)}`}>{dominantTheme ? `${dominantTheme.count} headlines · ${fmtSigned(dominantTheme.score, 3)}` : "—"}</div>
          </div>
          <div className="card p-5">
            <div className="kpi-label">Positive evidence</div>
            <div className="text-2xl font-semibold text-emerald-300">{positiveDrivers.length}</div>
            <div className="mt-1 text-xs text-neutral-600">Strongest recent positive drivers shown below</div>
          </div>
          <div className="card p-5">
            <div className="kpi-label">News evidence</div>
            <div className="text-2xl font-semibold text-white">{newsTotal.toLocaleString()}</div>
            <div className="mt-1 text-xs text-neutral-600">Period total · {scoredNews.length} recent rows scored</div>
          </div>
        </div>

        {themes.length ? (
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {themes.slice(0, 6).map((theme) => {
              const width = Math.min(100, Math.max(4, Math.abs(theme.score) * 100));
              return (
                <div key={theme.name} className="card p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-semibold text-neutral-200">{theme.name}</div>
                      <div className="mt-1 text-[11px] text-neutral-600">{theme.count} scored headline{theme.count === 1 ? "" : "s"}</div>
                    </div>
                    <div className={`font-mono text-xs ${signalClass(theme.score)}`}>{fmtSigned(theme.score, 3)}</div>
                  </div>
                  <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-white/[0.06]">
                    <div
                      className={`h-full rounded-full ${theme.score >= 0 ? "bg-emerald-400" : "bg-rose-400"}`}
                      style={{ width: `${width}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        ) : null}

        {(positiveDrivers.length || negativeDrivers.length) ? (
          <div className="grid gap-3 lg:grid-cols-2">
            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-[0.14em] text-emerald-400">Positive drivers</div>
              {positiveDrivers.map((item, i) => <DriverCard key={`${item.url}-${i}`} item={item} positive />)}
            </div>
            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-[0.14em] text-rose-400">Negative drivers</div>
              {negativeDrivers.map((item, i) => <DriverCard key={`${item.url}-${i}`} item={item} positive={false} />)}
            </div>
          </div>
        ) : null}
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="eyebrow">Signal history</div>
            <h2 className="section-title mt-1">Price and sentiment</h2>
          </div>
          <ChartLegend />
        </div>
        <div className="legacy-dark ambient-panel p-3 md:p-5">
          <LineChart
            mode={mode}
            dates={aligned.date}
            price={aligned.price}
            sentiment={aligned.sentiment}
            sentimentMA7={sentimentMA7}
            height={540}
          />
        </div>
      </section>

      <section className="space-y-3">
        <div>
          <div className="eyebrow">Article evidence</div>
          <h2 className="section-title mt-1">Recent scored headlines</h2>
          <p className="section-copy">Each score is the article-level FinBERT probability difference P(positive) − P(negative).</p>
        </div>

        {news.length ? (
          <div className="table-shell overflow-x-auto">
            <table className="w-full min-w-[760px] text-sm">
              <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.12em] text-neutral-600">
                <tr>
                  <th className="px-4 py-3">Date</th>
                  <th className="px-4 py-3">Headline</th>
                  <th className="px-4 py-3">Theme</th>
                  <th className="px-4 py-3">Source</th>
                  <th className="px-4 py-3 text-right">Sentiment</th>
                </tr>
              </thead>
              <tbody>
                {news.slice(0, 10).map((item, i) => {
                  const score = finite(item.s);
                  return (
                    <tr key={`${item.url}-${i}`} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]">
                      <td className="whitespace-nowrap px-4 py-3 font-mono text-xs text-neutral-600">{dateOnly(item.ts)}</td>
                      <td className="max-w-xl px-4 py-3">
                        <a href={item.url} target="_blank" rel="noreferrer" className="font-medium leading-6 text-neutral-200 hover:text-white hover:underline">
                          {item.title}
                        </a>
                      </td>
                      <td className="px-4 py-3 text-xs text-neutral-500">{classifyTheme(item)}</td>
                      <td className="px-4 py-3 text-xs text-neutral-500">{sourceOf(item)}</td>
                      <td className={`whitespace-nowrap px-4 py-3 text-right font-mono text-xs ${signalClass(score)}`}>
                        {item.sentiment_label || label(score)} · {fmtSigned(score, 4)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="card p-5 text-sm text-neutral-500">No recent scored headlines are available.</div>
        )}
      </section>
    </div>
  );
}
