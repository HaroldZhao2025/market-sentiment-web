// apps/web/lib/paths.ts
// Utilities to construct correct URLs AND filesystem paths for data files,
// plus helpers to build internal links that respect the GitHub Pages basePath.

import path from "node:path";

// Normalize base path once. In CI it's "/market-sentiment-web", locally it's "".
const RAW_BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";
const BASE = RAW_BASE.endsWith("/") ? RAW_BASE.slice(0, -1) : RAW_BASE;

const isServer = typeof window === "undefined";

function ensureLeadingSlash(p: string) {
  return p.startsWith("/") ? p : `/${p}`;
}

/** Prefix an internal route with the GitHub Pages base path */
export function withBase(p: string) {
  return `${BASE}${ensureLeadingSlash(p)}`;
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

/** Filesystem path to a data file (used at build-time / server) */
export function dataFsPath(rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  // process.cwd() will be apps/web during build steps
  return path.join(process.cwd(), "public", "data", clean);
}

/** Read JSON at build-time (server) with fallback to runtime fetch */
export async function loadJson<T = unknown>(rel: string): Promise<T> {
  const url = dataUrl(rel);

  if (isServer) {
    // Import fs only on the server to avoid bundling it for the client.
    try {
      const fs = await import("node:fs/promises");
      const p = dataFsPath(rel);
      const buf = await fs.readFile(p, "utf8");
      return JSON.parse(buf) as T;
    } catch {
      // Fall through to fetch if the file isn't present (e.g., preview)
    }
  }

  const res = await fetch(url, { cache: "no-cache" });
  if (!res.ok) throw new Error(`Failed to fetch ${rel}: ${res.status}`);
  return (await res.json()) as T;
}
