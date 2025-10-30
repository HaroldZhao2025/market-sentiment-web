// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import TickerClient from "./TickerClient";

// Static export constraints
export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

// Detect data roots whether build runs at repo root or apps/web
async function resolveDataRoots(): Promise<string[]> {
  const roots = [
    path.join(process.cwd(), "public", "data"),                // when cwd = apps/web
    path.join(process.cwd(), "apps", "web", "public", "data"), // when cwd = repo root
  ];
  const ok: string[] = [];
  for (const p of roots) {
    try { const st = await fs.stat(p); if (st.isDirectory()) ok.push(p); } catch {}
  }
  return ok.length ? ok : [roots[0]];
}

export async function generateStaticParams() {
  const roots = await resolveDataRoots();
  const seen = new Set<string>();
  for (const r of roots) {
    try {
      const dir = path.join(r, "ticker");
      const files = await fs.readdir(dir);
      for (const f of files) {
        if (f.toLowerCase().endsWith(".json")) {
          seen.add(f.replace(/\.json$/i, "").toUpperCase());
        }
      }
    } catch {}
  }
  return [...seen].map((symbol) => ({ symbol }));
}

export default async function Page({ params }: { params: { symbol: string } }) {
  // IMPORTANT: do NOT read JSON server-side — we’ll let the client fetch so
  // the page still works even if the file wasn’t present during the export.
  const symbol = (params.symbol || "").toUpperCase();
  return (
    <main style={{ maxWidth: 1200, margin: "24px auto", padding: "0 16px" }}>
      <h1 style={{ fontSize: 28, fontWeight: 600, marginBottom: 8 }}>
        Market Sentiment for {symbol}
      </h1>
      <TickerClient symbol={symbol} />
    </main>
  );
}
