import type { ScreenerRow } from "./intelligence";

export type QuerySort = "weight" | "sentiment" | "sentiment_change" | "return_1d" | "divergence" | "abs_divergence" | "contribution" | "n_total" | "novelty" | "disagreement";
export type QueryPlan = {
  sector: string | null;
  eventTheme: string | null;
  symbols: string[];
  sentiment: "positive" | "negative" | null;
  sentimentChange: "positive" | "negative" | null;
  return1d: "positive" | "negative" | null;
  evidenceOnly: boolean;
  minNews: number | null;
  sort: QuerySort;
  direction: "asc" | "desc";
  limit: number;
  interpretation: string[];
};

const SECTORS: Array<[string[], string]> = [
  [["technology", "tech", "software", "semiconductor", "chip"], "Information Technology"],
  [["financial", "bank", "insurance"], "Financials"],
  [["healthcare", "health care", "biotech", "pharma"], "Health Care"],
  [["consumer discretionary", "discretionary", "retail"], "Consumer Discretionary"],
  [["consumer staples", "staples"], "Consumer Staples"],
  [["communication", "media", "telecom"], "Communication Services"],
  [["industrial"], "Industrials"], [["material"], "Materials"], [["real estate", "reit"], "Real Estate"],
  [["utility", "utilities"], "Utilities"], [["energy", "oil", "gas"], "Energy"],
];
const EVENTS: Array<[string[], string]> = [
  [["earnings", "eps", "beat", "miss"], "Earnings beat / miss"], [["guidance", "outlook", "forecast"], "Guidance & outlook"],
  [["artificial intelligence", " ai ", "product"], "Product & AI"], [["merger", "acquisition", "m&a"], "M&A & strategic deals"],
  [["buyback", "dividend", "financing"], "Capital return & financing"], [["regulation", "antitrust", "tariff"], "Regulation & antitrust"],
  [["lawsuit", "litigation", "court"], "Legal & litigation"], [["management", "ceo", "cfo"], "Management change"],
  [["demand", "supply", "orders", "inventory"], "Operations & demand"], [["analyst", "upgrade", "downgrade", "price target"], "Analyst action"],
];

const finite = (v: unknown) => {
  const n = Number(v);
  return v === null || v === undefined || v === "" || !Number.isFinite(n) ? null : n;
};
const contribution = (r: ScreenerRow) => {
  const w = finite(r.weight), s = finite(r.sentiment);
  return w == null || s == null ? null : w * s;
};
const contains = (text: string, xs: string[]) => xs.some((x) => text.includes(x));

function detectSector(text: string, rows: ScreenerRow[]) {
  const actual = new Set(rows.map((r) => r.sector || ""));
  for (const [aliases, sector] of SECTORS) if (actual.has(sector) && contains(text, aliases)) return sector;
  return Array.from(actual).find((s) => s && text.includes(s.toLowerCase())) || null;
}
function detectEvent(text: string) {
  const padded = ` ${text} `;
  for (const [aliases, theme] of EVENTS) if (contains(padded, aliases)) return theme;
  return null;
}
function detectSymbols(question: string, rows: ScreenerRow[]) {
  const available = new Set(rows.map((r) => r.symbol.toUpperCase()));
  return Array.from(new Set((question.toUpperCase().match(/\b[A-Z][A-Z0-9.-]{0,5}\b/g) || []).filter((x) => available.has(x))));
}

export function parseMarketQuestion(question: string, rows: ScreenerRow[]): QueryPlan {
  const text = question.trim().toLowerCase();
  const plan: QueryPlan = {
    sector: detectSector(text, rows), eventTheme: detectEvent(text), symbols: detectSymbols(question, rows),
    sentiment: null, sentimentChange: null, return1d: null, evidenceOnly: !contains(text, ["include missing", "include no news"]),
    minNews: null, sort: "divergence", direction: "desc", limit: 15, interpretation: [],
  };
  if (contains(text, ["positive sentiment", "bullish sentiment"])) plan.sentiment = "positive";
  if (contains(text, ["negative sentiment", "bearish sentiment"])) plan.sentiment = "negative";
  if (contains(text, ["sentiment improving", "rising sentiment"])) { plan.sentimentChange = "positive"; plan.sort = "sentiment_change"; }
  if (contains(text, ["sentiment deteriorating", "falling sentiment"])) { plan.sentimentChange = "negative"; plan.sort = "sentiment_change"; plan.direction = "asc"; }
  if (contains(text, ["price up", "gainers", "up today", "positive return"])) { plan.return1d = "positive"; plan.sort = "return_1d"; }
  if (contains(text, ["price down", "losers", "down today", "negative return"])) { plan.return1d = "negative"; plan.sort = "return_1d"; plan.direction = "asc"; }
  if (contains(text, ["largest companies", "biggest stocks", "highest weight"])) { plan.sort = "weight"; plan.direction = "desc"; }
  if (contains(text, ["most positive sentiment", "highest sentiment"])) { plan.sort = "sentiment"; plan.direction = "desc"; }
  if (contains(text, ["most negative sentiment", "lowest sentiment"])) { plan.sort = "sentiment"; plan.direction = "asc"; }
  if (contains(text, ["positive contributors", "top contributors"])) { plan.sort = "contribution"; plan.direction = "desc"; }
  if (contains(text, ["negative contributors", "worst contributors"])) { plan.sort = "contribution"; plan.direction = "asc"; }
  if (contains(text, ["most news", "most headlines", "most attention"])) plan.sort = "n_total";
  if (contains(text, ["most novel", "highest novelty"])) plan.sort = "novelty";
  if (text.includes("disagreement")) plan.sort = "disagreement";
  if ((plan.sentiment === "positive" && plan.return1d === "negative") || contains(text, ["positive sentiment but price down", "bullish news but price down"])) { plan.sentiment = "positive"; plan.return1d = "negative"; plan.sort = "divergence"; plan.direction = "desc"; }
  else if ((plan.sentiment === "negative" && plan.return1d === "positive") || contains(text, ["negative sentiment but price up", "bearish news but price up"])) { plan.sentiment = "negative"; plan.return1d = "positive"; plan.sort = "divergence"; plan.direction = "asc"; }
  else if (contains(text, ["divergence", "diverging"])) plan.sort = "abs_divergence";
  const limit = text.match(/\b(?:top|show|find)\s+(\d{1,2})\b/); if (limit) plan.limit = Math.max(1, Math.min(50, Number(limit[1])));
  const minNews = text.match(/(?:at least|min(?:imum)? of?)\s+(\d{1,3})\s+(?:news|articles|headlines)/); if (minNews) plan.minNews = Number(minNews[1]);
  if (plan.sector) plan.interpretation.push(`Sector = ${plan.sector}`);
  if (plan.eventTheme) plan.interpretation.push(`Event = ${plan.eventTheme}`);
  if (plan.symbols.length) plan.interpretation.push(`Ticker = ${plan.symbols.join(", ")}`);
  if (plan.sentiment) plan.interpretation.push(`Sentiment = ${plan.sentiment}`);
  if (plan.sentimentChange) plan.interpretation.push(`Sentiment change = ${plan.sentimentChange}`);
  if (plan.return1d) plan.interpretation.push(`1D return = ${plan.return1d}`);
  if (plan.minNews != null) plan.interpretation.push(`News evidence ≥ ${plan.minNews}`);
  plan.interpretation.push(`Rank = ${plan.sort} (${plan.direction === "desc" ? "high → low" : "low → high"})`);
  return plan;
}

function sortValue(r: ScreenerRow, key: QuerySort) {
  if (key === "contribution") return contribution(r);
  if (key === "abs_divergence") { const x = finite(r.divergence); return x == null ? null : Math.abs(x); }
  return finite(r[key as keyof ScreenerRow]);
}
export function runMarketQuestion(question: string, rows: ScreenerRow[]) {
  const plan = parseMarketQuestion(question, rows);
  const sign = (v: unknown, s: "positive" | "negative" | null) => s == null || (finite(v) != null && (s === "positive" ? finite(v)! > 0 : finite(v)! < 0));
  const filtered = rows.filter((r) =>
    (!plan.sector || r.sector === plan.sector) && (!plan.eventTheme || r.event_theme === plan.eventTheme) &&
    (!plan.symbols.length || plan.symbols.includes(r.symbol.toUpperCase())) && (!plan.evidenceOnly || finite(r.sentiment) != null) &&
    (plan.minNews == null || (finite(r.n_total) || 0) >= plan.minNews) && sign(r.sentiment, plan.sentiment) &&
    sign(r.sentiment_change, plan.sentimentChange) && sign(r.return_1d, plan.return1d)
  );
  const sorted = filtered.slice().sort((a, b) => {
    const av = sortValue(a, plan.sort), bv = sortValue(b, plan.sort);
    if (av == null) return 1; if (bv == null) return -1;
    return plan.direction === "desc" ? bv - av : av - bv;
  });
  return { question, plan, matched: sorted.length, rows: sorted.slice(0, plan.limit) };
}
export { contribution as rowContribution };
