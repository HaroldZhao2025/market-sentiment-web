// apps/web/app/ticker/[symbol]/page.tsx
// Server component: declare routes and render a minimal shell.
// All heavy data loading is done client-side from /data to work on GitHub Pages.

import path from "node:path";
import fs from "node:fs/promises";
import TickerClient from "./TickerClient";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

// ---- helpers (server-only) ----
async function fileExists(p: string) {
  try {
    await fs.stat(p);
    return true;
  } catch {
    return false;
  }
}

async function readTextIf(p: string): Promise<string | null> {
  try {
    return await fs.readFile(p, "utf8");
  } catch {
    return null;
  }
}

/**
 * We generate static params from either:
 * 1) built JSON directory (if present), or
 * 2) the committed universe CSV under /data/sp500.csv (repo root), which exists
 *    before the CI builds JSON. This avoids a race between JSON build and Next build.
 *
 * See README + workflow which build JSON into apps/web/public/data. :contentReference[oaicite:0]{index=0}
 */
export async function generateStaticParams() {
  const cwd = process.cwd();

  // try #1: JSON directory after build_json step
  const candJsonDirs = [
    path.join(cwd, "apps", "web", "public", "data", "ticker"),
    path.join(cwd, "public", "data", "ticker"),
  ];
  for (const d of candJsonDirs) {
    if (await fileExists(d)) {
      try {
        const files = await fs.readdir(d);
        const syms = files
          .filter((n) => n.toLowerCase().endsWith(".json"))
          .map((n) => n.replace(/\.json$/i, "").toUpperCase());
        if (syms.length) return syms.map((symbol) => ({ symbol }));
      } catch {
        /* ignore and fall through */
      }
    }
  }

  // try #2: committed universe CSV in /data/sp500.csv (repo root)
  const candCSVs = [
    path.join(cwd, "data", "sp500.csv"),
    path.join(cwd, "..", "data", "sp500.csv"),
    path.join(cwd, "..", "..", "data", "sp500.csv"),
  ];
  for (const p of candCSVs) {
    const txt = await readTextIf(p);
    if (!txt) continue;
    const lines = txt.split(/\r?\n/).filter(Boolean);
    if (!lines.length) continue;
    const header = lines[0].toLowerCase();
    const idx =
      header.split(",").findIndex((h) =>
        ["symbol", "ticker"].includes(h.trim())
      ) ?? -1;
    if (idx < 0) continue;

    const syms = lines.slice(1).map((ln) => {
      const cols = ln.split(",");
      return (cols[idx] || "").trim().toUpperCase();
    });
    const out = syms.filter(Boolean).slice(0, 600).map((symbol) => ({ symbol }));
    if (out.length) return out;
  }

  // Worst case: fall back to a tiny set so export doesn't fail.
  return [{ symbol: "AAPL" }, { symbol: "MSFT" }, { symbol: "NVDA" }];
}

export default function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  return (
    <main style={{ maxWidth: 1200, margin: "24px auto", padding: "0 16px" }}>
      <h1 style={{ fontSize: 28, fontWeight: 600, marginBottom: 8 }}>
        Market Sentiment for {symbol}
      </h1>
      {/* Client component fetches from /data at runtime (works on GH Pages). */}
      <TickerClient symbol={symbol} />
    </main>
  );
}
