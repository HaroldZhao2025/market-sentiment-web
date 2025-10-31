// apps/web/lib/paths.ts
// Client-safe helpers for basePath-aware routes and data files.
// Fixes double base-path by normalizing and de-duplicating the prefix.

const RAW_BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

// Normalize the configured base:
// - strip all leading/trailing slashes
// - if non-empty, add exactly one leading slash
const BASE = (() => {
  const clean = RAW_BASE.replace(/^\/+|\/+$/g, "").trim();
  return clean ? `/${clean}` : "";
})();

const isServer = typeof window === "undefined";

function ensureLeadingSlash(p: string) {
  return p.startsWith("/") ? p : `/${p}`;
}

/**
 * Prefix an internal route with the GitHub Pages base path (if any),
 * while preventing double prefixing when the input is already base-prefixed.
 */
export function withBase(p: string) {
  let route = ensureLeadingSlash(p || "/");

  // If the route already begins with the normalized BASE, return as-is
  // e.g. BASE="/market-sentiment-web", route="/market-sentiment-web/ticker/AAPL"
  if (BASE && (route === BASE || route.startsWith(`${BASE}/`))) {
    return route;
  }
  return `${BASE}${route}`;
}

/** Named link builders for consistency across the app */
export const hrefs = {
  home: () => withBase("/"),
  portfolio: () => withBase("/portfolio"),
  ticker: (s: string) => withBase(`/ticker/${encodeURIComponent(s)}`),
  earnings: (s: string) => withBase(`/earnings/${encodeURIComponent(s)}`),
};

/** Browser-safe URL to a data file under /public/data */
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
