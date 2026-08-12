import fs from "node:fs";
import path from "node:path";

export type HeatmapTile = {
  symbol: string;
  name?: string;
  sector?: string;
  industry?: string;
  market_cap?: number;
  weight?: number;
  date?: string;
  price?: number | null;
  return_1d?: number | null;
  sentiment?: number | null;
  n_total?: number | null;
};

export type ScreenerRow = HeatmapTile & {
  sentiment_change: number | null;
  divergence: number | null;
  event_theme: string | null;
  event_score: number | null;
  novelty: number | null;
  source_count: number;
  disagreement: number | null;
};

export type LabSummary = {
  signal: "sentiment" | "sentiment_change" | "divergence";
  horizon: 1 | 3 | 5 | 20;
  sector: string;
  quantile: 0.2 | 0.25 | 0.33;
  top_mean: number | null;
  bottom_mean: number | null;
  spread: number | null;
  t_stat: number | null;
  hit_rate: number | null;
  sharpe: number | null;
  n: number;
  n_dates: number;
  start: string | null;
  end: string | null;
};

export type EventThemeSummary = {
  theme: string;
  count: number;
  ticker_count: number;
  source_count: number;
  avg_sentiment: number | null;
  avg_return_1d: number | null;
  avg_return_5d: number | null;
  positive_1d_rate: number | null;
  avg_novelty: number | null;
  disagreement: number | null;
  recent_examples: Array<{
    symbol: string;
    date: string;
    title: string;
    source: string;
    sentiment: number | null;
    return_1d: number | null;
  }>;
};

export type AttributionRow = {
  key: string;
  label: string;
  level: "sector" | "industry";
  sector: string;
  weight: number;
  observed_weight: number;
  contribution: number;
  sentiment: number | null;
  observed_tickers: number;
  total_tickers: number;
  news_count: number;
};

type TickerObject = {
  date?: unknown[];
  dates?: unknown[];
  price?: unknown[];
  close?: unknown[];
  S?: unknown[];
  sentiment?: unknown[];
  news?: any[];
};

type PanelObs = {
  symbol: string;
  sector: string;
  date: string;
  signal: number;
  fwd: number;
};

type DailySpread = {
  date: string;
  top: number;
  bottom: number;
  spread: number;
  observations: number;
};

const DATA_ROOT = path.join(process.cwd(), "public", "data");
const tickerCache = new Map<string, TickerObject | null>();
let heatmapCache: HeatmapTile[] | null = null;

const EVENT_RULES: Array<{ theme: string; terms: string[] }> = [
  { theme: "Earnings beat / miss", terms: ["earnings", "eps", "revenue", "profit", "quarter", "beat", "miss"] },
  { theme: "Guidance & outlook", terms: ["guidance", "outlook", "forecast", "expects", "raises forecast", "cuts forecast"] },
  { theme: "Product & AI", terms: ["artificial intelligence", " ai ", "product", "launch", "chip", "model", "cloud", "software"] },
  { theme: "M&A & strategic deals", terms: ["acquisition", "acquire", "merger", "deal", "stake", "joint venture"] },
  { theme: "Capital return & financing", terms: ["buyback", "dividend", "debt", "offering", "financing", "capital return"] },
  { theme: "Regulation & antitrust", terms: ["regulator", "regulation", "antitrust", "ftc", "doj", "tariff", "ban", "probe"] },
  { theme: "Legal & litigation", terms: ["lawsuit", "court", "legal", "settlement", "patent", "litigation"] },
  { theme: "Management change", terms: ["ceo", "cfo", "executive", "management", "appoints", "resigns", "steps down"] },
  { theme: "Operations & demand", terms: ["demand", "supply", "shipment", "production", "orders", "factory", "inventory", "sales"] },
  { theme: "Analyst action", terms: ["analyst", "rating", "price target", "upgrade", "downgrade", "initiates"] },
];

export function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readJson<T>(file: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return null;
  }
}

function values(obj: TickerObject, keyA: keyof TickerObject, keyB: keyof TickerObject): Array<number | null> {
  const raw = (obj[keyA] ?? obj[keyB] ?? []) as unknown[];
  return Array.isArray(raw) ? raw.map(finite) : [];
}

function datesOf(obj: TickerObject): string[] {
  const raw = (obj.date ?? obj.dates ?? []) as unknown[];
  return Array.isArray(raw) ? raw.map((x) => String(x ?? "")) : [];
}

export function readHeatmapTiles(): HeatmapTile[] {
  if (heatmapCache) return heatmapCache;
  const candidates = [
    path.join(DATA_ROOT, "SPX", "sp500_heatmap.json"),
    path.resolve(process.cwd(), "apps/web/public/data/SPX/sp500_heatmap.json"),
  ];
  for (const file of candidates) {
    const parsed = readJson<{ tiles?: HeatmapTile[] }>(file);
    if (Array.isArray(parsed?.tiles)) {
      heatmapCache = parsed!.tiles;
      return heatmapCache;
    }
  }
  heatmapCache = [];
  return heatmapCache;
}

function tickerObject(symbol: string): TickerObject | null {
  if (tickerCache.has(symbol)) return tickerCache.get(symbol) ?? null;
  const obj = readJson<TickerObject>(path.join(DATA_ROOT, "ticker", `${symbol}.json`));
  tickerCache.set(symbol, obj);
  return obj;
}

function sourceOf(item: any): string {
  if (item?.source) return String(item.source);
  if (item?.provider) return String(item.provider);
  try {
    return item?.url ? new URL(String(item.url)).host.replace(/^www\./, "") : "";
  } catch {
    return "";
  }
}

function titleTokens(title: string): Set<string> {
  return new Set(
    title
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((x) => x.length > 2)
  );
}

function jaccard(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let inter = 0;
  a.forEach((x) => { if (b.has(x)) inter += 1; });
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

export function classifyEvent(title: string, text = ""): string {
  const hay = ` ${title} ${text} `.toLowerCase();
  for (const rule of EVENT_RULES) {
    if (rule.terms.some((term) => hay.includes(term))) return rule.theme;
  }
  return "Other company news";
}

function std(xs: number[]): number | null {
  if (xs.length < 2) return null;
  const m = xs.reduce((a, b) => a + b, 0) / xs.length;
  const v = xs.reduce((s, x) => s + (x - m) ** 2, 0) / (xs.length - 1);
  return Math.sqrt(v);
}

function mean(xs: number[]): number | null {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
}

function articleScore(item: any): number | null {
  const direct = finite(item?.s ?? item?.score ?? item?.sentiment);
  if (direct != null) return direct;
  const pos = finite(item?.probs?.pos ?? item?.probabilities?.positive);
  const neg = finite(item?.probs?.neg ?? item?.probabilities?.negative);
  return pos != null && neg != null ? pos - neg : null;
}

function lastTwoObservedSentiment(obj: TickerObject | null): [number | null, number | null] {
  if (!obj) return [null, null];
  const ss = values(obj, "S", "sentiment").filter((x): x is number => x != null);
  return [ss.at(-1) ?? null, ss.at(-2) ?? null];
}

function eventDiagnostics(obj: TickerObject | null) {
  const news = Array.isArray(obj?.news) ? obj!.news! : [];
  const scored = news
    .map((item) => ({ item, score: articleScore(item) }))
    .filter((x): x is { item: any; score: number } => x.score != null);
  if (!scored.length) {
    return { theme: null, eventScore: null, novelty: null, sourceCount: 0, disagreement: null };
  }

  const groups = new Map<string, number[]>();
  scored.forEach(({ item, score }) => {
    const theme = classifyEvent(String(item?.title ?? ""), String(item?.text ?? ""));
    groups.set(theme, [...(groups.get(theme) ?? []), score]);
  });
  const ranked = Array.from(groups.entries())
    .map(([theme, scores]) => ({ theme, score: mean(scores) ?? 0, count: scores.length }))
    .sort((a, b) => Math.abs(b.score) * Math.sqrt(b.count) - Math.abs(a.score) * Math.sqrt(a.count));

  const tokens = scored.map(({ item }) => titleTokens(String(item?.title ?? "")));
  const noveltyScores = tokens.map((t, i) => {
    let maxSim = 0;
    tokens.forEach((other, j) => { if (i !== j) maxSim = Math.max(maxSim, jaccard(t, other)); });
    return 1 - maxSim;
  });

  return {
    theme: ranked[0]?.theme ?? null,
    eventScore: ranked[0]?.score ?? null,
    novelty: mean(noveltyScores),
    sourceCount: new Set(scored.map(({ item }) => sourceOf(item)).filter(Boolean)).size,
    disagreement: std(scored.map((x) => x.score)),
  };
}

export function buildScreenerRows(): ScreenerRow[] {
  return readHeatmapTiles().map((tile) => {
    const obj = tickerObject(tile.symbol);
    const [latest, prior] = lastTwoObservedSentiment(obj);
    const sentiment = finite(tile.sentiment) ?? latest;
    const return1d = finite(tile.return_1d);
    const diagnostics = eventDiagnostics(obj);
    return {
      ...tile,
      sentiment: sentiment ?? null,
      sentiment_change: latest != null && prior != null ? latest - prior : null,
      divergence: sentiment != null && return1d != null
        ? sentiment - Math.max(-1, Math.min(1, return1d / 0.05))
        : null,
      event_theme: diagnostics.theme,
      event_score: diagnostics.eventScore,
      novelty: diagnostics.novelty,
      source_count: diagnostics.sourceCount,
      disagreement: diagnostics.disagreement,
    };
  });
}

function forwardReturn(prices: Array<number | null>, i: number, horizon: number): number | null {
  const p0 = prices[i];
  const p1 = prices[i + horizon];
  return p0 != null && p1 != null && p0 !== 0 ? p1 / p0 - 1 : null;
}

function buildPanel(signalName: LabSummary["signal"], horizon: LabSummary["horizon"]): PanelObs[] {
  const sectorByTicker = new Map(readHeatmapTiles().map((t) => [t.symbol, t.sector || "Unknown"]));
  const out: PanelObs[] = [];
  for (const [symbol, sector] of sectorByTicker.entries()) {
    const obj = tickerObject(symbol);
    if (!obj) continue;
    const dates = datesOf(obj);
    const prices = values(obj, "price", "close");
    const sentiments = values(obj, "S", "sentiment");
    const n = Math.min(dates.length, prices.length, sentiments.length);
    for (let i = 1; i + horizon < n; i += 1) {
      const s = sentiments[i];
      if (s == null) continue;
      let signal: number | null = null;
      if (signalName === "sentiment") signal = s;
      if (signalName === "sentiment_change") {
        const prev = sentiments[i - 1];
        signal = prev == null ? null : s - prev;
      }
      if (signalName === "divergence") {
        const p0 = prices[i - 1];
        const p1 = prices[i];
        const r1 = p0 != null && p1 != null && p0 !== 0 ? p1 / p0 - 1 : null;
        signal = r1 == null ? null : s - Math.max(-1, Math.min(1, r1 / 0.05));
      }
      const fwd = forwardReturn(prices, i, horizon);
      if (signal == null || fwd == null || !dates[i]) continue;
      out.push({ symbol, sector, date: dates[i], signal, fwd });
    }
  }
  return out;
}

function dailyCrossSectionSpreads(obs: PanelObs[], q: LabSummary["quantile"]): DailySpread[] {
  const byDate = new Map<string, PanelObs[]>();
  obs.forEach((row) => byDate.set(row.date, [...(byDate.get(row.date) ?? []), row]));

  const out: DailySpread[] = [];
  Array.from(byDate.entries()).sort((a, b) => a[0].localeCompare(b[0])).forEach(([date, rows]) => {
    if (rows.length < 6) return;
    const sorted = rows.slice().sort((a, b) => a.signal - b.signal);
    const k = Math.max(1, Math.floor(sorted.length * q));
    if (k * 2 > sorted.length) return;
    const bottom = mean(sorted.slice(0, k).map((x) => x.fwd));
    const top = mean(sorted.slice(-k).map((x) => x.fwd));
    if (top == null || bottom == null) return;
    out.push({ date, top, bottom, spread: top - bottom, observations: rows.length });
  });
  return out;
}

function quantileSummary(
  obs: PanelObs[],
  q: LabSummary["quantile"],
  horizon: LabSummary["horizon"]
): Omit<LabSummary, "signal" | "horizon" | "sector" | "quantile"> {
  const daily = dailyCrossSectionSpreads(obs, q);
  if (daily.length < 3) {
    const ds = obs.map((x) => x.date).sort();
    return {
      top_mean: null,
      bottom_mean: null,
      spread: null,
      t_stat: null,
      hit_rate: null,
      sharpe: null,
      n: obs.length,
      n_dates: daily.length,
      start: ds[0] ?? null,
      end: ds.at(-1) ?? null,
    };
  }

  const topMean = mean(daily.map((x) => x.top));
  const bottomMean = mean(daily.map((x) => x.bottom));
  const spreads = daily.map((x) => x.spread);
  const spread = mean(spreads);
  const sd = std(spreads);
  const tStat = spread != null && sd != null && sd > 0 ? spread / (sd / Math.sqrt(spreads.length)) : null;
  const hit = spreads.length ? spreads.filter((x) => x > 0).length / spreads.length : null;
  const periodsPerYear = 252 / horizon;
  const sharpe = spread != null && sd != null && sd > 0 ? (spread / sd) * Math.sqrt(periodsPerYear) : null;
  const ds = daily.map((x) => x.date).sort();

  return {
    top_mean: topMean,
    bottom_mean: bottomMean,
    spread,
    t_stat: tStat,
    hit_rate: hit,
    sharpe,
    n: obs.length,
    n_dates: daily.length,
    start: ds[0] ?? null,
    end: ds.at(-1) ?? null,
  };
}

export function buildLabSummaries(): LabSummary[] {
  const signals: LabSummary["signal"][] = ["sentiment", "sentiment_change", "divergence"];
  const horizons: LabSummary["horizon"][] = [1, 3, 5, 20];
  const quantiles: LabSummary["quantile"][] = [0.2, 0.25, 0.33];
  const sectors = ["All", ...Array.from(new Set(readHeatmapTiles().map((t) => t.sector || "Unknown"))).sort()];
  const out: LabSummary[] = [];
  const panelCache = new Map<string, PanelObs[]>();

  for (const signal of signals) {
    for (const horizon of horizons) {
      const cacheKey = `${signal}:${horizon}`;
      const panel = panelCache.get(cacheKey) ?? buildPanel(signal, horizon);
      panelCache.set(cacheKey, panel);
      for (const sector of sectors) {
        const scoped = sector === "All" ? panel : panel.filter((x) => x.sector === sector);
        for (const quantile of quantiles) {
          out.push({ signal, horizon, sector, quantile, ...quantileSummary(scoped, quantile, horizon) });
        }
      }
    }
  }
  return out;
}

function priceReaction(obj: TickerObject, articleDate: string, horizon: number): number | null {
  const dates = datesOf(obj);
  const prices = values(obj, "price", "close");
  const day = articleDate.slice(0, 10);
  let idx = dates.findIndex((d) => d.slice(0, 10) >= day);
  if (idx < 0) idx = dates.length - 1;
  return forwardReturn(prices, idx, horizon);
}

export function buildEventMemory(): EventThemeSummary[] {
  const records: Array<{
    theme: string; symbol: string; date: string; title: string; source: string;
    score: number | null; r1: number | null; r5: number | null; novelty: number;
  }> = [];

  for (const tile of readHeatmapTiles()) {
    const obj = tickerObject(tile.symbol);
    const news = Array.isArray(obj?.news) ? obj!.news! : [];
    const tokens = news.map((item) => titleTokens(String(item?.title ?? "")));
    news.forEach((item, i) => {
      const title = String(item?.title ?? "").trim();
      const rawDate = String(item?.ts ?? item?.date ?? "").trim();
      if (!title || !rawDate) return;
      const tokenSet = tokens[i];
      let maxSim = 0;
      tokens.forEach((other, j) => { if (j !== i) maxSim = Math.max(maxSim, jaccard(tokenSet, other)); });
      records.push({
        theme: classifyEvent(title, String(item?.text ?? "")),
        symbol: tile.symbol,
        date: rawDate.slice(0, 10),
        title,
        source: sourceOf(item),
        score: articleScore(item),
        r1: obj ? priceReaction(obj, rawDate, 1) : null,
        r5: obj ? priceReaction(obj, rawDate, 5) : null,
        novelty: 1 - maxSim,
      });
    });
  }

  const groups = new Map<string, typeof records>();
  records.forEach((r) => groups.set(r.theme, [...(groups.get(r.theme) ?? []), r]));
  return Array.from(groups.entries()).map(([theme, rows]) => {
    const scores = rows.map((x) => x.score).filter((x): x is number => x != null);
    const r1s = rows.map((x) => x.r1).filter((x): x is number => x != null);
    const r5s = rows.map((x) => x.r5).filter((x): x is number => x != null);
    return {
      theme,
      count: rows.length,
      ticker_count: new Set(rows.map((x) => x.symbol)).size,
      source_count: new Set(rows.map((x) => x.source).filter(Boolean)).size,
      avg_sentiment: mean(scores),
      avg_return_1d: mean(r1s),
      avg_return_5d: mean(r5s),
      positive_1d_rate: r1s.length ? r1s.filter((x) => x > 0).length / r1s.length : null,
      avg_novelty: mean(rows.map((x) => x.novelty)),
      disagreement: std(scores),
      recent_examples: rows
        .slice()
        .sort((a, b) => b.date.localeCompare(a.date))
        .slice(0, 5)
        .map((x) => ({ symbol: x.symbol, date: x.date, title: x.title, source: x.source, sentiment: x.score, return_1d: x.r1 })),
    };
  }).sort((a, b) => b.count - a.count);
}

export function buildAttributionRows(): AttributionRow[] {
  const tiles = readHeatmapTiles();
  const out: AttributionRow[] = [];
  const build = (level: AttributionRow["level"], keyFn: (t: HeatmapTile) => string, sectorFn: (t: HeatmapTile) => string) => {
    const groups = new Map<string, HeatmapTile[]>();
    tiles.forEach((t) => {
      const key = keyFn(t);
      groups.set(key, [...(groups.get(key) ?? []), t]);
    });
    groups.forEach((rows, key) => {
      const observed = rows.filter((t) => finite(t.sentiment) != null);
      const weight = rows.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);
      const observedWeight = observed.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);
      const contribution = observed.reduce((s, t) => s + (finite(t.weight) ?? 0) * (finite(t.sentiment) ?? 0), 0);
      out.push({
        key: `${level}:${key}`,
        label: key,
        level,
        sector: sectorFn(rows[0]),
        weight,
        observed_weight: observedWeight,
        contribution,
        sentiment: observedWeight > 0 ? contribution / observedWeight : null,
        observed_tickers: observed.length,
        total_tickers: rows.length,
        news_count: observed.reduce((s, t) => s + Math.max(0, finite(t.n_total) ?? 0), 0),
      });
    });
  };
  build("sector", (t) => t.sector || "Unknown", (t) => t.sector || "Unknown");
  build("industry", (t) => `${t.sector || "Unknown"} · ${t.industry || "Unknown"}`, (t) => t.sector || "Unknown");
  return out;
}
