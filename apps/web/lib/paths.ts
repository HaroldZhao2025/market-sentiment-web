// apps/web/lib/paths.ts
// Utilities to construct correct URLs AND filesystem paths for data files,
// plus helpers to build internal links that respect the GitHub Pages basePath.
//
// IMPORTANT: This module must be safe to import from client components.
// Do NOT import Node built-ins at the top level (no `node:path`, no `fs`).

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

/**
 * Filesystem path to a data file (used at build-time / server).
 * Implemented without importing 'node:path' so this file stays client-safe.
 */
export function dataFsPath(rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  // process.cwd() resolves to apps/web during build
  const cwd =
    typeof process !== "undefined" && typeof process.cwd === "function"
      ? process.cwd().replace(/\/+$/, "")
      : "";
  return `${cwd}/public/data/${clean}`;
}

/**
 * Read JSON with a client-safe default.
 *
 * - On the client (or if fetch works), use fetch against /data/** (base-path aware).
 * - On the server, if fetch is unavailable (e.g., during certain build steps),
 *   we *optionally* fall back to fs, but we import it lazily and only on server.
 *
 * NOTE: We avoid any top-level `node:` imports. The lazy import is hidden from
 * bundlers and only runs when actually needed on the server.
 */
export async function loadJson<T = unknown>(rel: string): Promise<T> {
  const url = dataUrl(rel);

  // Try network fetch first (works both client-side and most server contexts).
  try {
    const res = await fetch(url, { cache: "no-cache" });
    if (res.ok) {
      return (await res.json()) as T;
    }
  } catch {
    /* ignore and fall back below if server */
  }

  // Server-only fallback to fs if fetch path isn't available during build/SSG.
  if (isServer) {
    try {
      // Lazy, string-based dynamic import so bundlers don't follow it in client builds.
      // eslint-disable-next-line no-new-func
      const importFS = new Function("m", "return import(m)");
      const fs = (await importFS("fs/promises")) as typeof import("fs/promises");
      const p = dataFsPath(rel);
      const buf = await fs.readFile(p, "utf8");
      return JSON.parse(buf) as T;
    } catch {
      // If even fs fails, fall through to a hard error.
    }
  }

  throw new Error(`Failed to load JSON: ${rel}`);
}
