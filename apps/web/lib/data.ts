// apps/web/lib/data.ts
import fs from "node:fs/promises";
import path from "node:path";

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";
const PUBLIC_DIR = path.join(process.cwd(), "public");
const DATA_DIR = path.join(PUBLIC_DIR, "data");

async function readFromFS<T>(rel: string): Promise<T | null> {
  try {
    const p = path.join(DATA_DIR, rel);
    const raw = await fs.readFile(p, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

async function readFromHTTP<T>(rel: string): Promise<T | null> {
  try {
    const url = `${BASE}/data/${rel}`; // works on GitHub Pages subpath
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

export async function loadTickers(): Promise<string[]> {
  // Prefer FS (build time); fallback to HTTP (client/runtime)
  const fsVal = await readFromFS<string[]>("_tickers.json");
  if (fsVal && Array.isArray(fsVal)) return fsVal;
  const httpVal = await readFromHTTP<string[]>("_tickers.json");
  return Array.isArray(httpVal) ? httpVal : [];
}

export async function loadTicker(symbol: string): Promise<any | null> {
  const rel = `ticker/${symbol.toUpperCase()}.json`;
  return (await readFromFS<any>(rel)) ?? (await readFromHTTP<any>(rel));
}

export async function loadEarnings(symbol: string): Promise<any | null> {
  const rel = `earnings/${symbol.toUpperCase()}.json`;
  return (await readFromFS<any>(rel)) ?? (await readFromHTTP<any>(rel));
}

export async function loadPortfolio(): Promise<any | null> {
  return (await readFromFS<any>("portfolio.json")) ?? (await readFromHTTP<any>("portfolio.json"));
}
