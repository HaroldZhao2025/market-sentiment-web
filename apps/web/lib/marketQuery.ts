import type { ScreenerRow } from "./intelligence";

export type QueryDirection = "asc" | "desc";
export type SignConstraint = "positive" | "negative" | null;
export type QuerySortKey =
  | "weight"
  | "sentiment"
  | "sentiment_change"
  | "return_1d"
  | "divergence"
  | "abs_divergence"
  | "contribution"
  | "n_total"
  | "novelty"
  | "disagreement";

export type MarketQueryPlan = {
  sector: string | null;
  event_theme: string | null;
  symbols: string[];
  text: string | null;
  sentiment: SignConstraint;
  sentiment_change: SignConstraint;
  return_1d: SignConstraint;
  divergence: SignConstraint;
  evidence_only: boolean;
  min_news: number | null;
  sort: QuerySortKey;
  direction: QueryDirection;
  limit: number;
  interpretation: string[];
};

export type MarketQueryResult = {
  question: string;
  plan: MarketQueryPlan;
  rows: ScreenerRow[];
  matched: number;
};

const SECTOR_ALIASES: Array<[string[], string]> = [
  [["technology", "tech", "software", "semiconductor", "chip"], "Information Technology"],
  [["financials", "financial", "banks", "bank", "insurance"], "Financials"],
  [["health care", "healthcare", "health", "biotech", "pharma"], "Health Care"],
  [["consumer discretionary", "discretionary", "retail"], "Consumer Discretionary"],
  [["consumer staples", "staples"], "Consumer Staples"],
  [["communication services", "communications", "media", "telecom"], "Communication Services"],
  [["industrials", "industrial"], "Industrials"],
  [["materials", "material"], "Materials"],
  [["real estate", "reit", "reits"], "Real Estate"],
  [["utilities", "utility"], "Utilities"],
  [["energy", "oil", "gas"], "Energy"],
];

const EVENT_ALIASES: Array<[string[], string]> = [
  [["earnings", "eps", "quarterly results", "beat", "miss"], "Earnings beat / miss"],
  [["guidance", "outlook", "forecast"], "Guidance & outlook"],
  [["artificial intelligence", " ai ", "ai stocks", "product launch", "product"], "Product & AI"],
  [["m&a", "merger", "acquisition", "strategic deal"], "M&A & strategic deals"],
  [["buyback", "dividend", "financing", "capital return"], "Capital return & financing"],
  [["antitrust", "regulation", "regulator", "tariff"], "Regulation & antitrust"],
  [["lawsuit", "litigation", "legal", "court"], "Legal & litigation"],
  [["management", "ceo", "cfo", "executive change"], "Management change"],
  [["operations", "demand", "supply", "orders", "inventory"], "Operations & demand"],
  [["analyst", "upgrade", "downgrade", "price target"], "Analyst action"],
];

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function containsAny(text: string, terms: string[]) {
  return terms.some((term) => text.includes(term));
}

function signPass(value: unknown, constraint: SignConstraint) {
  if (!constraint) return true;
  const n = finite(value);
  if (n == null) return false;
  return constraint === "positive" ? n > 0 : n < 0;
}

function contribution(row: ScreenerRow): number | null {
  const w = finite(row.weight);
  const s = finite(row.sentiment);
  return w == null || s == null ? null : w * s;
}

function sortValue(row: ScreenerRow, key: QuerySortKey): number | null {
  if (key === "contribution") return contribution(row);
  if (key === "abs_divergence") {
    const d = finite(row.divergence);
    return d == null ? null : Math.abs(d);
  }
  return finite(row[key as keyof ScreenerRow]);
}

function displaySort(key: QuerySortKey) {
  const labels: Record<QuerySortKey, string> = {
    weight: "index weight",
    sentiment: "sentiment",
    sentiment_change: "sentiment change",
    return_1d: "1D return",
    divergence: "signed sentiment-price divergence",
    abs_divergence: "absolute sentiment-price divergence",
    contribution: "index contribution",
    n_total: "news evidence",
    novelty: "event novelty",
    disagreement: "sentiment disagreement",
  };
  return labels[key];
}

function detectSector(text: string, rows: ScreenerRow[]): string | null {
  const actual = Array.from(new Set(rows.map((r) => r.sector || "").filter(Boolean)));
  const exact = actual.find((sector) => text.includes(sector.toLowerCase()));
  if (exact) return exact;
  for (const [aliases, canonical] of SECTOR_ALIASES) {
    if (containsAny(text, aliases) && actual.includes(canonical)) return canonical;
  }
  return null;
}

function detectEventTheme(text: string): string | null {
  for (const [aliases, theme] of EVENT_ALIASES) {
    if (containsAny(` ${text} `, aliases)) return theme;
  }
  return null;
}

function detectSymbols(question: string, rows: ScreenerRow[]) {
  const available = new Set(rows.map((r) => r.symbol.toUpperCase()));
  const tokens = question.toUpperCase().match(/\b[A-Z][A-Z0-9.-]{0,5}\b/g) ?? [];
  return Array.from(new Set(tokens.filter((token) => available.has(token))));
}

function parseLimit(text: string) {
  const match = text.match(/\b(?:top|show|find|give me)\s+(\d{1,2})\b/);
  if (!match) return 15;
  return Math.max(1, Math.min(50, Number(match[1])));
}

function parseMinNews(text: string) {
  const match = text.match(/(?:at least|min(?:imum)? of?)\s+(\d{1,3})\s+(?:news|articles|headlines)/);
  return match ? Math.max(0, Number(match[1])) : null;
}

export function parseMarketQuestion(question: string, rows: ScreenerRow[]): MarketQueryPlan {
  const text = question.trim().toLowerCase();
  const padded = ` ${text} `;
  const interpretation: string[] = [];
  const sector = detectSector(text, rows);
  const eventTheme = detectEventTheme(text);
  const symbols = detectSymbols(question, rows);

  let sentiment: SignConstraint = null;
  let sentimentChange: SignConstraint = null;
  let return1d: SignConstraint = null;
  let divergence: SignConstraint = null;
  let sort: QuerySortKey = "divergence";
  let direction: QueryDirection = "desc";

  if (containsAny(text, ["positive sentiment", "bullish sentiment", "sentiment positive"])) sentiment = "positive";
  if (containsAny(text, ["negative sentiment", "bearish sentiment", "sentiment negative"])) sentiment = "negative";

  if (containsAny(text, ["sentiment improving", "improving sentiment", "rising sentiment", "sentiment rising", "sentiment up"])) {
    sentimentChange = "positive";
    sort = "sentiment_change";
    direction = "desc";
  }
  if (containsAny(text, ["sentiment deteriorating", "deteriorating sentiment", "falling sentiment", "sentiment falling", "sentiment down"])) {
    sentimentChange = "negative";
    sort = "sentiment_change";
    direction = "asc";
  }

  if (containsAny(text, ["price up", "prices up", "gainers", "positive return", "rising price", "up today", "rose today"])) {
    return1d = "positive";
    sort = "return_1d";
    direction = "desc";
  }
  if (containsAny(text, ["price down", "prices down", "decliners", "losers", "negative return", "falling price", "down today", "fell today"])) {
    return1d = "negative";
    sort = "return_1d";
    direction = "asc";
  }

  if (containsAny(text, ["largest companies", "largest stocks", "biggest companies", "biggest stocks", "largest weight", "highest weight"])) {
    sort = "weight";
    direction = "desc";
  }
  if (containsAny(text, ["most positive sentiment", "highest sentiment", "strongest sentiment"])) {
    sort = "sentiment";
    direction = "desc";
  }
  if (containsAny(text, ["most negative sentiment", "lowest sentiment", "weakest sentiment"])) {
    sort = "sentiment";
    direction = "asc";
  }
  if (containsAny(text, ["largest positive contributors", "biggest positive contributors", "top contributors", "largest contributors"])) {
    sort = "contribution";
    direction = "desc";
  }
  if (containsAny(text, ["largest negative contributors", "biggest negative contributors", "worst contributors", "negative contributors"])) {
    sort = "contribution";
    direction = "asc";
  }
  if (containsAny(text, ["most news", "most headlines", "most attention", "highest attention"])) {
    sort = "n_total";
    direction = "desc";
  }
  if (containsAny(text, ["most novel", "highest novelty", "newest event", "novel events"])) {
    sort = "novelty";
    direction = "desc";
  }
  if (containsAny(text, ["most disagreement", "highest disagreement", "disagreement"]))) {
    sort = "disagreement";
    direction = "desc";
  }

  const positiveSentimentPriceDown =
    (sentiment === "positive" && return1d === "negative") ||
    containsAny(text, ["positive sentiment but price down", "positive sentiment but fell", "bullish news but price down"]);
  const negativeSentimentPriceUp =
    (sentiment === "negative" && return1d === "positive") ||
    containsAny(text, ["negative sentiment but price up", "negative sentiment but rose", "bearish news but price up"]);

  if (positiveSentimentPriceDown) {
    sentiment = "positive";
    return1d = "negative";
    sort = "divergence";
    direction = "desc";
  } else if (negativeSentimentPriceUp) {
    sentiment = "negative";
    return1d = "positive";
    sort = "divergence";
    direction = "asc";
  } else if (text.includes("divergence") || text.includes("diverging")) {
    sort = "abs_divergence";
    direction = "desc";
  }

  if (sector) interpretation.push(`Sector = ${sector}`);
  if (eventTheme) interpretation.push(`Event theme = ${eventTheme}`);
  if (symbols.length) interpretation.push(`Ticker = ${symbols.join(", ")}`);
  if (sentiment) interpretation.push(`Sentiment is ${sentiment}`);
  if (sentimentChange) interpretation.push(`Sentiment change is ${sentimentChange}`);
  if (return1d) interpretation.push(`1D return is ${return1d}`);
  if (divergence) interpretation.push(`Divergence is ${divergence}`);

  const minNews = parseMinNews(text);
  if (minNews != null) interpretation.push(`News evidence ≥ ${minNews}`);

  const recognized =
    sector || eventTheme || symbols.length || sentiment || sentimentChange || return1d || divergence ||
    minNews != null || sort !== "divergence" || text.includes("divergence") || text.includes("diverging");
  const fallbackText = text && !recognized ? text : null;
  if (fallbackText) interpretation.push(`Full-text evidence search = “${question.trim()}”`);

  interpretation.push(`Rank by ${displaySort(sort)} (${direction === "desc" ? "high → low" : "low → high"})`);

  return {
    sector,
    event_theme: eventTheme,
    symbols,
    text: fallbackText,
    sentiment,
    sentiment_change: sentimentChange,
    return_1d: return1d,
    divergence,
    evidence_only: !containsAny(text, ["include missing", "include no news", "all stocks including missing"]),
    min_news: minNews,
    sort,
    direction,
    limit: parseLimit(text),
    interpretation,
  };
}

export function runMarketQuestion(question: string, rows: ScreenerRow[]): MarketQueryResult {
  const plan = parseMarketQuestion(question, rows);
  const text = plan.text?.toLowerCase() ?? null;

  const filtered = rows.filter((row) => {
    if (plan.sector && (row.sector || "Unknown") !== plan.sector) return false;
    if (plan.event_theme && row.event_theme !== plan.event_theme) return false;
    if (plan.symbols.length && !plan.symbols.includes(row.symbol.toUpperCase())) return false;
    if (plan.evidence_only && finite(row.sentiment) == null) return false;
    if (plan.min_news != null && (finite(row.n_total) ?? 0) < plan.min_news) return false;
    if (!signPass(row.sentiment, plan.sentiment)) return false;
    if (!signPass(row.sentiment_change, plan.sentiment_change)) return false;
    if (!signPass(row.return_1d, plan.return_1d)) return false;
    if (!signPass(row.divergence, plan.divergence)) return false;
    if (text) {
      const hay = [row.symbol, row.name, row.sector, row.industry, row.event_theme]
        .map((x) => String(x || "").toLowerCase())
        .join(" ");
      if (!hay.includes(text)) return false;
    }
    return true;
  });

  const sorted = filtered.slice().sort((a, b) => {
    const av = sortValue(a, plan.sort);
    const bv = sortValue(b, plan.sort);
    if (av == null && bv == null) return a.symbol.localeCompare(b.symbol);
    if (av == null) return 1;
    if (bv == null) return -1;
    return plan.direction === "desc" ? bv - av : av - bv;
  });

  return {
    question,
    plan,
    matched: sorted.length,
    rows: sorted.slice(0, plan.limit),
  };
}

export function rowContribution(row: ScreenerRow) {
  return contribution(row);
}
