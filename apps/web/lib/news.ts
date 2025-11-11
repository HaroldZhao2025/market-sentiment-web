// apps/web/lib/news.ts
import type { NewsItem } from "./sentiment";

// 递进窗口：1d → 3d → 7d → 14d → 30d；仍不足则退化为“全时期最新 10 条”
export function pickRecentHeadlines(items: NewsItem[], max = 10): NewsItem[] {
  const msDay = 24 * 60 * 60 * 1000;
  const now = Date.now();

  const byTime = (items ?? [])
    .filter(d => d && (d.publishedAt || d.time || d.date))
    .map(d => ({ ...d, _t: Date.parse(String(d.publishedAt ?? d.time ?? d.date)) }))
    .filter(d => Number.isFinite(d._t))
    .sort((a, b) => b._t - a._t);

  for (const days of [1, 3, 7, 14, 30]) {
    const within = byTime.filter(d => now - d._t <= days * msDay);
    const seen = new Set<string>();
    const dedup = within.filter(d => {
      const key = `${(d.source || "").toLowerCase()}::${(d.title || "").toLowerCase()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (dedup.length >= max) return dedup.slice(0, max);
  }

  const seenAll = new Set<string>();
  const fallback = byTime.filter(d => {
    const key = `${(d.source || "").toLowerCase()}::${(d.title || "").toLowerCase()}`;
    if (seenAll.has(key)) return false;
    seenAll.add(key);
    return true;
  });
  return fallback.slice(0, max);
}
