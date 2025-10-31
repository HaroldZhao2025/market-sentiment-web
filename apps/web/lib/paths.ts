// apps/web/lib/paths.ts
// Client-safe helpers for routes and data file URLs.

// NOTE:
// - For internal app routes (used by <Link>, router.push, etc.), DO NOT prefix the base path here.
//   Next.js already applies the configured basePath at runtime, so adding it again causes
//   "/<base>/<base>/..." double-prefix issues on GitHub Pages.
// - For public data files under /public/data (served at /<base>/data/*), we DO prefix with BASE.

const RAW_BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";
const BASE = RAW_BASE.endsWith("/") ? RAW_BASE.slice(0, -1) : RAW_BASE;
const isServer = typeof window === "undefined";

function ensureLeadingSlash(p: string) {
  return p.startsWith("/") ? p : `/${p}`;
}

/** Internal app routes handled by Next.js. Never add BASE here. */
export function route(p: string) {
  return ensureLeadingSlash(p);
}

/** Back-compat alias kept for existing imports. Safe for routes now. */
export function withBase(p: string) {
  // Intentionally identical to route(): avoid double basePath on Links
  return route(p);
}

/** Named link builders used across the app */
export const hrefs = {
  home: () => route("/"),
  portfolio: () => route("/portfolio"),
  ticker: (s: string) => route(`/ticker/${encodeURIComponent(s)}`),
  earnings: (s: string) => route(`/earnings/${encodeURIComponent(s)}`),
};

/** Browser-safe URL to a data file under /public/data (served at /<BASE>/data/...) */
export function dataUrl(rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  const prefix = BASE ? `${BASE}/data` : `/data`;
  return `${prefix}/${clean}`;
}

/** Filesystem path to a data file for server-only usage (no node: imports at top level) */
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

  // Try HTTP first (works in both client and server)
  try {
    const res = await fetch(url, { cache: "no-cache" });
    if (res.ok) return (await res.json()) as T;
  } catch {
    /* fall through */
  }

  // Server fallback: read from filesystem during build/SSR
  if (isServer) {
    try {
      // Avoid bundling fs on the client
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
