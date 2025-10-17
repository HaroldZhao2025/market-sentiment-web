// apps/web/lib/paths.ts
// Utilities to construct correct URLs AND filesystem paths for data files
import path from "node:path";
import fs from "node:fs";

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

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
  if (typeof window === "undefined") {
    const p = dataFsPath(rel);
    const buf = fs.readFileSync(p, "utf8");
    return JSON.parse(buf) as T;
  } else {
    const res = await fetch(dataUrl(rel), { cache: "no-cache" });
    if (!res.ok) throw new Error(`Failed to fetch ${rel}: ${res.status}`);
    return (await res.json()) as T;
  }
}
