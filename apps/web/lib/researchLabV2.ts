import fs from "node:fs";
import path from "node:path";
import { finite, readHeatmapTiles } from "./intelligence";

export type LabV2Signal = "sentiment" | "sentiment_change" | "divergence";
export type LabV2Horizon = 1 | 3 | 5 | 20;
export type LabV2Quantile = 0.2 | 0.25 | 0.33;
export type LabV2Sample = "all" | "in_sample" | "out_of_sample";
export type LabV2Summary = { signal: LabV2Signal; horizon: LabV2Horizon; sector: string; quantile: LabV2Quantile; sample: LabV2Sample; top_mean: number | null; bottom_mean: number | null; spread: number | null; simple_t_stat: number | null; hac_t_stat: number | null; hac_se: number | null; hac_lag: number; hit_rate: number | null; sharpe: number | null; avg_turnover: number | null; n: number; n_dates: number; start: string | null; end: string | null };
type TickerObject = { dates?: unknown[]; date?: unknown[]; price?: unknown[]; close?: unknown[]; S?: unknown[]; sentiment?: unknown[]; sentiment_observed?: unknown[] };
type PanelObs = { symbol: string; sector: string; date: string; signal: number; fwd: number };
type DailySpread = { date: string; top: number; bottom: number; spread: number; topSymbols: string[]; bottomSymbols: string[]; observations: number };

const DATA_ROOT = path.join(process.cwd(), "public", "data");
const tickerCache = new Map<string, TickerObject | null>();
function readJson<T>(file: string): T | null { try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; } catch { return null; } }
function tickerObject(symbol: string): TickerObject | null { if (tickerCache.has(symbol)) return tickerCache.get(symbol) ?? null; const obj = readJson<TickerObject>(path.join(DATA_ROOT, "ticker", `${symbol}.json`)); tickerCache.set(symbol, obj); return obj; }
function strArray(value: unknown): string[] { return Array.isArray(value) ? value.map((x) => String(x ?? "")) : []; }
function numArray(value: unknown): Array<number | null> { return Array.isArray(value) ? value.map(finite) : []; }
function boolArray(value: unknown, length: number): boolean[] { return Array.isArray(value) ? Array.from({ length }, (_, index) => value[index] === true || value[index] === 1 || value[index] === "1" || value[index] === "true") : Array.from({ length }, () => true); }
function mean(xs: number[]): number | null { return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null; }
function sampleStd(xs: number[]): number | null { if (xs.length < 2) return null; const m = mean(xs)!; return Math.sqrt(xs.reduce((sum, x) => sum + (x - m) ** 2, 0) / (xs.length - 1)); }
function forwardReturn(prices: Array<number | null>, i: number, horizon: number): number | null { const p0 = prices[i]; const p1 = prices[i + horizon]; return p0 != null && p1 != null && p0 !== 0 ? p1 / p0 - 1 : null; }

function buildPanel(signal: LabV2Signal, horizon: LabV2Horizon): PanelObs[] {
  const sectorByTicker = new Map(readHeatmapTiles().map((t) => [t.symbol, t.sector || "Unknown"]));
  const out: PanelObs[] = [];
  for (const [symbol, sector] of sectorByTicker.entries()) {
    const obj = tickerObject(symbol);
    if (!obj) continue;
    const dates = strArray(obj.dates ?? obj.date);
    const prices = numArray(obj.price ?? obj.close);
    const sentiments = numArray(obj.S ?? obj.sentiment);
    const n = Math.min(dates.length, prices.length, sentiments.length);
    const observed = boolArray(obj.sentiment_observed, n);
    let previousObserved: number | null = observed[0] && sentiments[0] != null ? sentiments[0] : null;
    for (let i = 1; i + horizon < n; i += 1) {
      const current = sentiments[i];
      if (!observed[i] || current == null) continue;
      let signalValue: number | null = null;
      if (signal === "sentiment") signalValue = current;
      if (signal === "sentiment_change") signalValue = previousObserved == null ? null : current - previousObserved;
      if (signal === "divergence") {
        const p0 = prices[i - 1]; const p1 = prices[i];
        const r1 = p0 != null && p1 != null && p0 !== 0 ? p1 / p0 - 1 : null;
        signalValue = r1 == null ? null : current - Math.max(-1, Math.min(1, r1 / 0.05));
      }
      const fwd = forwardReturn(prices, i, horizon);
      if (signalValue != null && fwd != null && dates[i]) out.push({ symbol, sector, date: dates[i], signal: signalValue, fwd });
      previousObserved = current;
    }
  }
  return out;
}

function dailySpreads(obs: PanelObs[], quantile: LabV2Quantile): DailySpread[] {
  const byDate = new Map<string, PanelObs[]>();
  for (const row of obs) byDate.set(row.date, [...(byDate.get(row.date) ?? []), row]);
  const out: DailySpread[] = [];
  for (const [date, rows] of Array.from(byDate.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
    if (rows.length < 6) continue;
    const sorted = rows.slice().sort((a, b) => a.signal - b.signal);
    const k = Math.max(1, Math.floor(sorted.length * quantile));
    if (2 * k > sorted.length) continue;
    const lowRows = sorted.slice(0, k); const highRows = sorted.slice(-k);
    const bottom = mean(lowRows.map((x) => x.fwd)); const top = mean(highRows.map((x) => x.fwd));
    if (top == null || bottom == null) continue;
    out.push({ date, top, bottom, spread: top - bottom, topSymbols: highRows.map((x) => x.symbol), bottomSymbols: lowRows.map((x) => x.symbol), observations: rows.length });
  }
  return out;
}
function sideTurnover(previous: string[], current: string[]): number { if (!previous.length || !current.length) return 1; const names = new Set([...previous, ...current]); let absoluteChange = 0; for (const symbol of names) { const oldWeight = previous.includes(symbol) ? 1 / previous.length : 0; const newWeight = current.includes(symbol) ? 1 / current.length : 0; absoluteChange += Math.abs(newWeight - oldWeight); } return absoluteChange / 2; }
function averageTurnover(rows: DailySpread[]): number | null { if (rows.length < 2) return null; const values: number[] = []; for (let i = 1; i < rows.length; i += 1) values.push(sideTurnover(rows[i - 1].topSymbols, rows[i].topSymbols) + sideTurnover(rows[i - 1].bottomSymbols, rows[i].bottomSymbols)); return mean(values); }
function neweyWestMeanSe(values: number[], lag: number): number | null { const n = values.length; if (n < 3) return null; const mu = mean(values)!; const residuals = values.map((x) => x - mu); let longRunVariance = residuals.reduce((sum, e) => sum + e * e, 0) / n; const maxLag = Math.min(Math.max(0, lag), n - 1); for (let ell = 1; ell <= maxLag; ell += 1) { let gamma = 0; for (let t = ell; t < n; t += 1) gamma += residuals[t] * residuals[t - ell]; gamma /= n; const bartlett = 1 - ell / (maxLag + 1); longRunVariance += 2 * bartlett * gamma; } return longRunVariance > 0 ? Math.sqrt(longRunVariance / n) : null; }
function selectSample(rows: DailySpread[], sample: LabV2Sample): DailySpread[] { if (sample === "all" || rows.length < 4) return rows; const split = Math.max(1, Math.min(rows.length - 1, Math.floor(rows.length * 0.7))); return sample === "in_sample" ? rows.slice(0, split) : rows.slice(split); }

function summarize(panel: PanelObs[], dailyAll: DailySpread[], signal: LabV2Signal, horizon: LabV2Horizon, sector: string, quantile: LabV2Quantile, sample: LabV2Sample): LabV2Summary {
  const daily = selectSample(dailyAll, sample); const allowedDates = new Set(daily.map((x) => x.date)); const scopedPanel = panel.filter((x) => allowedDates.has(x.date)); const spreads = daily.map((x) => x.spread); const spread = mean(spreads); const sd = sampleStd(spreads); const simpleT = spread != null && sd != null && sd > 0 ? spread / (sd / Math.sqrt(spreads.length)) : null; const hacLag = Math.min(Math.max(0, horizon - 1), Math.max(0, spreads.length - 1)); const hacSe = neweyWestMeanSe(spreads, hacLag); const hacT = spread != null && hacSe != null && hacSe > 0 ? spread / hacSe : null; const periodsPerYear = 252 / horizon; const sharpe = spread != null && sd != null && sd > 0 ? (spread / sd) * Math.sqrt(periodsPerYear) : null; const dates = daily.map((x) => x.date);
  return { signal, horizon, sector, quantile, sample, top_mean: mean(daily.map((x) => x.top)), bottom_mean: mean(daily.map((x) => x.bottom)), spread, simple_t_stat: simpleT, hac_t_stat: hacT, hac_se: hacSe, hac_lag: hacLag, hit_rate: spreads.length ? spreads.filter((x) => x > 0).length / spreads.length : null, sharpe, avg_turnover: averageTurnover(daily), n: scopedPanel.length, n_dates: daily.length, start: dates[0] ?? null, end: dates.at(-1) ?? null };
}

export function buildLabV2Summaries(): LabV2Summary[] {
  const signals: LabV2Signal[] = ["sentiment", "sentiment_change", "divergence"]; const horizons: LabV2Horizon[] = [1, 3, 5, 20]; const quantiles: LabV2Quantile[] = [0.2, 0.25, 0.33]; const samples: LabV2Sample[] = ["all", "in_sample", "out_of_sample"]; const sectors = ["All", ...Array.from(new Set(readHeatmapTiles().map((t) => t.sector || "Unknown"))).sort()]; const output: LabV2Summary[] = [];
  for (const signal of signals) for (const horizon of horizons) { const fullPanel = buildPanel(signal, horizon); for (const sector of sectors) { const panel = sector === "All" ? fullPanel : fullPanel.filter((x) => x.sector === sector); for (const quantile of quantiles) { const daily = dailySpreads(panel, quantile); for (const sample of samples) output.push(summarize(panel, daily, signal, horizon, sector, quantile, sample)); } } }
  return output;
}
