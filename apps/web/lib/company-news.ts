/** Merge independent news feeds without letting an old archive hide new articles. */
export type CompanyNewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;
  summary?: string;
  source?: string;
  provider?: string;
  s?: number | null;
  sentiment_label?: string;
  probs?: { pos?: number; neu?: number; neg?: number };
};

type Row = Record<string, unknown>;
const object = (value: unknown): Row | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : null;
const text = (value: unknown): string => typeof value === "string" ? value.trim() : "";
const titleKey = (title: string): string => title.normalize("NFKC").toLowerCase()
  .replace(/[^\p{L}\p{N}]+/gu, " ").trim();

function publicationTime(value: unknown): string | null {
  const raw = text(value);
  // Legacy date-only and offset-less ISO values denote UTC, never build-machine time.
  if (!/^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/.test(raw)) return null;
  const day = new Date(`${raw.slice(0, 10)}T00:00:00Z`);
  if (!Number.isFinite(day.getTime()) || day.toISOString().slice(0, 10) !== raw.slice(0, 10)) return null;
  let iso = raw.replace(" ", "T");
  if (iso.length === 10) iso += "T00:00:00Z";
  else if (!/(Z|[+-]\d{2}:?\d{2})$/.test(iso)) iso += "Z";
  const stamp = Date.parse(iso);
  return Number.isFinite(stamp) ? new Date(stamp).toISOString() : null;
}

function httpUrl(value: unknown): string {
  try {
    const url = new URL(text(value));
    return ["http:", "https:"].includes(url.protocol) ? url.href : "";
  } catch { return ""; }
}

function articleUrlKey(value: string): string {
  if (!value) return "";
  const url = new URL(value);
  url.hash = "";
  // Keep identity-bearing parameters (especially Finnhub ?id=), removing only trackers.
  for (const key of Array.from(url.searchParams.keys())) {
    if (/^(utm_.+|fbclid|gclid|mc_cid|mc_eid|guccounter|guce_referrer|guce_referrer_sig)$/i.test(key)) {
      url.searchParams.delete(key);
    }
  }
  url.searchParams.sort();
  const pathname = url.pathname.replace(/\/{2,}/g, "/").replace(/\/$/, "");
  // A publisher homepage is not an article identity.
  if (!pathname && !url.search) return "";
  return `${url.host.toLowerCase().replace(/^www\./, "")}${pathname}${url.search}`;
}

function score(value: unknown): number | null {
  if ((typeof value !== "number" && typeof value !== "string") || value === "" || (typeof value === "string" && !value.trim())) return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= -1 && number <= 1 ? number : null;
}

function readNews(payload: unknown): CompanyNewsItem[] {
  const obj = object(payload);
  const rows = Array.isArray(payload) ? payload : Array.isArray(obj?.news) ? obj.news : Array.isArray(obj?.articles) ? obj.articles : [];
  const result: CompanyNewsItem[] = [];
  for (const value of rows) {
    const row = object(value);
    if (!row) continue;
    const title = text(row.title) || text(row.headline);
    const ts = publicationTime(row.ts) ?? publicationTime(row.date);
    if (!titleKey(title) || !ts) continue;
    const item: CompanyNewsItem = { ts, title, url: httpUrl(row.url) || httpUrl(row.link), s: score(row.s) };
    for (const field of ["text", "summary", "source", "provider"] as const) {
      const content = text(row[field]);
      if (content) item[field] = content;
    }
    if (item.s !== null) {
      if (text(row.sentiment_label)) item.sentiment_label = text(row.sentiment_label);
      const probs = object(row.probs);
      if (probs) {
        const values = [probs.pos, probs.neu, probs.neg];
        if (values.every((v) => typeof v === "number" && Number.isFinite(v) && v >= 0 && v <= 1)
            && Math.abs((values as number[]).reduce((a, b) => a + b, 0) - 1) <= .02) {
          item.probs = { pos: probs.pos as number, neu: probs.neu as number, neg: probs.neg as number };
        }
      }
    }
    result.push(item);
  }
  return result;
}

export function mergeCompanyNews(archive: unknown, compact: unknown, limit = 160): {
  news: CompanyNewsItem[]; total: number; latestArticleAt: string | null;
} {
  if (!Number.isInteger(limit) || limit < 1) throw new RangeError("News limit must be a positive integer");
  const rows = [...readNews(archive), ...readNews(compact)];
  const parent = rows.map((_, index) => index);
  const find = (index: number): number => {
    while (parent[index] !== index) {
      parent[index] = parent[parent[index]];
      index = parent[index];
    }
    return index;
  };
  const owners = new Map<string, number>();
  rows.forEach((row, index) => {
    // Same headline on another day can be a different recurring news article.
    const keys = [`title:${row.ts.slice(0, 10)}:${titleKey(row.title)}`];
    const url = articleUrlKey(row.url);
    if (url) keys.push(`url:${url}`);
    for (const key of keys) {
      const owner = owners.get(key);
      if (owner !== undefined) parent[find(index)] = find(owner);
      owners.set(key, index);
    }
  });
  const groups = new Map<number, CompanyNewsItem[]>();
  rows.forEach((row, index) => {
    const root = find(index);
    const group = groups.get(root) ?? [];
    group.push(row);
    groups.set(root, group);
  });
  const merged = Array.from(groups.values(), (members) => {
    members.sort((a, b) => b.ts.localeCompare(a.ts)
      || (b.summary?.length ?? 0) - (a.summary?.length ?? 0)
      || a.title.localeCompare(b.title) || a.url.localeCompare(b.url));
    const item = { ...members[0] };
    // Retain richer metadata, but never move a score onto a revised/different headline.
    const sameTitle = members.filter((row) => row.title === item.title);
    for (const field of ["summary", "text", "source", "provider", "url"] as const) {
      const fallback = sameTitle.find((row) => row[field])?.[field];
      if (!item[field] && fallback) item[field] = fallback;
    }
    item.url = item.url || "";
    const scored = sameTitle.find((row) => row.s !== null && row.s !== undefined);
    if (scored) {
      item.s = scored.s;
      item.sentiment_label = scored.sentiment_label;
      item.probs = scored.probs ? { ...scored.probs } : undefined;
    }
    return item;
  });
  merged.sort((a, b) => b.ts.localeCompare(a.ts) || a.title.localeCompare(b.title) || a.url.localeCompare(b.url));
  // Deduplicate and sort the COMPLETE union before limiting the displayed rows.
  return { news: merged.slice(0, limit), total: merged.length, latestArticleAt: merged[0]?.ts ?? null };
}
