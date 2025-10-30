// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

// Fully static export; enumerate routes at build
export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

// Find data roots whether build runs at repo root or apps/web
async function resolveDataRoots(): Promise<string[]> {
  const roots = [
    path.join(process.cwd(), "public", "data"),                // cwd = apps/web
    path.join(process.cwd(), "apps", "web", "public", "data"), // cwd = repo root
  ];
  const ok: string[] = [];
  for (const p of roots) {
    try {
      const st = await fs.stat(p);
      if (st.isDirectory()) ok.push(p);
    } catch {}
  }
  return ok.length ? ok : [roots[0]];
}

// Pre-generate /ticker/* so GitHub Pages never 404s these URLs
export async function generateStaticParams() {
  const roots = await resolveDataRoots();
  const seen = new Set<string>();
  for (const r of roots) {
    try {
      const dir = path.join(r, "ticker");
      for (const f of await fs.readdir(dir)) {
        if (f.toLowerCase().endsWith(".json")) seen.add(f.replace(/\.json$/i, "").toUpperCase());
      }
    } catch {}
  }
  // Always include AAPL as a fallback route even if data scanning fails
  if (!seen.size) seen.add("AAPL");
  return [...seen].map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  // Do NOT read JSON on the server—let the client fetch so we never bake “no data”.
  return (
    <main style={{ maxWidth: 1200, margin: "24px auto", padding: "0 16px" }}>
      <h1 style={{ fontSize: 28, fontWeight: 600, marginBottom: 8 }}>
        Market Sentiment for {symbol}
      </h1>
      <TickerClient symbol={symbol} />
    </main>
  );
}
