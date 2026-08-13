import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";
import EarningsIntelligenceClient, { type EarningsArtifact } from "./EarningsIntelligenceClient";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

async function readJson<T = any>(filePath: string): Promise<T | null> {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8")) as T;
  } catch {
    return null;
  }
}

async function loadSymbols(): Promise<string[]> {
  const publicData = path.join(process.cwd(), "public", "data");
  const core = await readJson<string[]>(path.join(publicData, "_tickers.json"));
  const v5 = await readJson<{ companies?: Array<{ ticker?: string }> }>(path.join(publicData, "v5", "universe.json"));
  const symbols = new Set<string>(Array.isArray(core) ? core : ["A"]);
  for (const company of v5?.companies ?? []) {
    const ticker = String(company?.ticker || "").trim().toUpperCase();
    if (ticker) symbols.add(ticker);
  }
  return Array.from(symbols).sort();
}

function legacyToV5(symbol: string, raw: any): EarningsArtifact {
  const docs = Array.isArray(raw) ? raw : Array.isArray(raw?.docs) ? raw.docs : [];
  return {
    schema_version: 1,
    symbol,
    earnings_history: [],
    calls: [],
    filing_fallback: docs.map((doc: any) => ({
      ts: String(doc?.ts ?? ""),
      title: String(doc?.title ?? ""),
      url: String(doc?.url ?? ""),
      source: String(doc?.source ?? "Legacy earnings evidence"),
      document_type: "legacy",
      S: typeof doc?.S === "number" ? doc.S : undefined,
    })),
    methodology: { compatibility: "Legacy earnings artifact mapped into the V5 filing fallback surface." },
  };
}

async function loadEarnings(symbol: string): Promise<EarningsArtifact> {
  const publicData = path.join(process.cwd(), "public", "data");
  const v5 = await readJson<EarningsArtifact>(path.join(publicData, "v5", "earnings", `${symbol}.json`));
  if (v5 && typeof v5 === "object") return v5;
  const legacy = await readJson<any>(path.join(publicData, "earnings", `${symbol}.json`));
  if (legacy) return legacyToV5(symbol, legacy);
  return { schema_version: 2, symbol, earnings_history: [], calls: [], filing_fallback: [] };
}

export async function generateStaticParams() {
  const symbols = await loadSymbols();
  return symbols.map((symbol) => ({ symbol }));
}

export default async function EarningsPage({ params }: { params: { symbol: string } }) {
  const symbol = String(params.symbol || "").toUpperCase();
  const data = await loadEarnings(symbol);
  const callCount = Array.isArray(data.calls) ? data.calls.length : 0;
  const filingCount = Array.isArray(data.filing_fallback) ? data.filing_fallback.length : 0;
  const historyCount = Array.isArray(data.earnings_history) ? data.earnings_history.length : 0;

  return (
    <main className="space-y-7">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Phase 5 · earnings intelligence</div>
          <h1 className="page-title mt-2">{symbol} Earnings Intelligence</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Earnings surprise, call tone, prepared-versus-Q&amp;A shift, deterministic topic diagnostics, transcript evidence, and regulatory filing fallback.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="pill">{callCount} structured call{callCount === 1 ? "" : "s"}</span>
          <span className="pill">{historyCount} earnings observations</span>
          <span className="pill">{filingCount} filing fallback{filingCount === 1 ? "" : "s"}</span>
          <Link href={`/ticker/${symbol}`} className="pill">← Ticker</Link>
        </div>
      </section>
      <EarningsIntelligenceClient symbol={symbol} data={data} />
    </main>
  );
}
