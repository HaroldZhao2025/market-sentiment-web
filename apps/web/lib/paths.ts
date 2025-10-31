// apps/web/lib/paths.ts
// Client-safe helpers for basePath-aware routes and data files.
// Fix: avoid double base-path on <Link> by NOT prefixing internal routes here.
// Next.js basePath (from next.config.cjs) already prefixes routes at render time.

const RAW_BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

// Keep normalized BASE for static assets (public/data), not for routes.
const BASE = (() => {
  const clean = RAW_BASE.replace(/^\/+|\/+$/g, "").trim();
  return clean ? `/${clean}` : "";
})();

const isServer = typeof window === "undefined";

function ensureLeadingSlash(p: string) {
  return p.startsWith("/") ? p : `/${p}`;
}

/**
 * Internal routes for <Link> and in-app navigation.
 * IMPORTANT: Do NOT add BASE here; Next.js basePath handles route prefixing.
 * Returning clean absolute paths ("/...") prevents
 * "/market-sentiment-web/market-sentiment-web/..." on GitHub Pages.
 */
export function withBase(p: string) {
  return ensureLeadingSlash(p || "/");
}

/** Named link builders for consistency across the app */
export const hrefs = {
  home: () => withBase("/"),
  portfolio: () => withBase("/portfolio"),
  ticker: (s: string) => withBase(`/ticker/${encodeURIComponent(s)}`),
  earnings: (s: string) => withBase(`/earnings/${encodeURIComponent(s)}`),
};

/**
 * Browser-safe URL to a data file under /public/data.
 * For static assets we DO need the BASE, because browsers resolve "/data"
 * to domain root; on GitHub Pages the site root is "/market-sentiment-web".
 */
export function dataUrl(rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  const prefix = BASE ? `${BASE}/data` : `/data`;
  return `${prefix}/${clean}`;
}

/** Filesystem path to a data file for server-only usage (lazy, no node: imports) */
export function dataFsPath(rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  const cwd =
    typeof process !== "undefined" && typeof process.cwd === "function"
      ? process.cwd().replace(/\/+$/, "")
      : "";
  return `${cwd}/public/data/${clean}`;
}

/** Read JSON with a client-safe default (fetch first, server-only fs fallback) */
export async function loadJson<T = unknown>(rel: string): Promise<T> {
  const url = dataUrl(rel);

  try {
    const res = await fetch(url, { cache: "no-cache" });
    if (res.ok) return (await res.json()) as T;
  } catch {
    /* fall through */
  }

  if (isServer) {
    try {
      // eslint-disable-next-line no-new-func
      const importFS = new Function("m", "return import(m)");
      const fs = (await importFS("fs/promises")) as typeof import("fs/promises");
      const p = dataFsPath(rel);
      const buf = await fs.readFile(p, "utf8");
      return JSON.parse(buf) as T;
    } catch {
      /* fall through */
    }
  }

  throw new Error(`Failed to load JSON: ${rel}`);
}
