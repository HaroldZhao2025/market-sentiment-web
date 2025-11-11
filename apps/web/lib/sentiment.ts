// apps/web/lib/sentiment.ts
export type NewsItem = {
  title?: string;
  source?: string;
  url?: string;
  // 尽量兼容你 JSON 中的时间字段
  publishedAt?: string;
  time?: string;
  date?: string;
  // 兼容你 JSON 中的情感字段
  sentiment?: number;
  score?: number;
};

function parseEpoch(s?: string): number | null {
  if (!s) return null;
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

// 取 America/New_York 的分钟偏移（含 DST）
function nyOffsetMinutes(d = new Date()): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "shortOffset",
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(d);
  const tz = parts.find(p => p.type === "timeZoneName")?.value ?? "GMT-05";
  const m = tz.match(/GMT([+-]\d{1,2})(?::?(\d{2}))?/);
  if (!m) return -300; // fallback: -5h
  const h = parseInt(m[1], 10);
  const mins = parseInt(m[2] || "0", 10);
  return h * 60 + Math.sign(h) * mins;
}

// 过去 24 小时（NYC）按“逐条新闻”算均值；返回 null 表示窗口内无新闻
export function computeLiveSentimentNYC(items: NewsItem[], now = new Date()): { value: number | null; count: number } {
  if (!Array.isArray(items) || items.length === 0) return { value: null, count: 0 };

  const nyOffsetMs = nyOffsetMinutes(now) * 60_000;
  const nowUtcMs = now.getTime();
  const nowNyMs = nowUtcMs + nyOffsetMs;
  const cutoffNyMs = nowNyMs - 24 * 60 * 60 * 1000;

  const toScore = (n: NewsItem) =>
    typeof n.sentiment === "number" ? n.sentiment :
    (typeof n.score === "number" ? n.score : NaN);

  const toEpochUtc = (n: NewsItem): number | null =>
    parseEpoch(n.publishedAt ?? n.time ?? n.date);

  const windowScores: number[] = [];
  for (const n of items) {
    const s = toScore(n);
    const tsUtc = toEpochUtc(n);
    if (!Number.isFinite(s) || tsUtc === null) continue;
    const tsNy = tsUtc + nyOffsetMs; // 把 UTC 时刻平移到纽约挂钟时间
    if (tsNy >= cutoffNyMs && tsNy <= nowNyMs) windowScores.push(Number(s));
  }

  if (windowScores.length === 0) return { value: null, count: 0 };
  const avg = windowScores.reduce((a, b) => a + b, 0) / windowScores.length;
  return { value: avg, count: windowScores.length };
}
