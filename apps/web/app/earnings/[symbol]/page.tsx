import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";
import CompanyVisual from "../../../components/CompanyVisual";
import EarningsIntelligenceClientV2, { type EarningsArtifact } from "./EarningsIntelligenceClientV2";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type CompanyMeta = { ticker?: string; name?: string; sector?: string; industry?: string; universe?: string };

async function readJson<T = any>(filePath: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(filePath, "utf8")) as T; } catch { return null; }
}

async function loadCompanies(): Promise<CompanyMeta[]> {
  const publicData = path.join(process.cwd(), "public", "data");
  const v5 = await readJson<{ companies?: CompanyMeta[] }>(path.join(publicData, "v5", "universe.json"));
  return Array.isArray(v5?.companies) ? v5.companies : [];
}

async function loadSymbols(): Promise<string[]> {
  const publicData = path.join(process.cwd(), "public", "data");
  const core = await readJson<string[]>(path.join(publicData, "_tickers.json"));
  const symbols = new Set<string>(Array.isArray(core) ? core : ["AAPL"]);
  for (const company of await loadCompanies()) {
    const ticker = String(company?.ticker || "").trim().toUpperCase();
    if (ticker) symbols.add(ticker);
  }
  return Array.from(symbols).sort();
}

function legacyToCurrent(symbol: string, raw: any): EarningsArtifact {
  const docs = Array.isArray(raw) ? raw : Array.isArray(raw?.docs) ? raw.docs : [];
  return {
    schema_version: 1,
    symbol,
    earnings_history: [],
    calls: [],
    call_links: [],
    filing_fallback: docs.map((doc: any) => ({ ts: String(doc?.ts ?? ""), title: String(doc?.title ?? ""), url: String(doc?.url ?? ""), source: String(doc?.source ?? "Legacy earnings source"), document_type: "legacy", S: typeof doc?.S === "number" ? doc.S : undefined })),
  };
}

async function loadEarnings(symbol: string): Promise<EarningsArtifact> {
  const publicData = path.join(process.cwd(), "public", "data");
  const current = await readJson<EarningsArtifact>(path.join(publicData, "v5", "earnings", `${symbol}.json`));
  if (current && typeof current === "object") return current;
  const legacy = await readJson<any>(path.join(publicData, "earnings", `${symbol}.json`));
  return legacy ? legacyToCurrent(symbol, legacy) : { schema_version: 8, symbol, earnings_history: [], calls: [], call_links: [], filing_fallback: [] };
}

export async function generateStaticParams() {
  return (await loadSymbols()).map((symbol) => ({ symbol }));
}

export default async function EarningsPage({ params }: { params: { symbol: string } }) {
  const symbol = String(params.symbol || "").toUpperCase();
  const [data, companies] = await Promise.all([loadEarnings(symbol), loadCompanies()]);
  const company = companies.find((row) => String(row.ticker || "").toUpperCase() === symbol);
  const callCount = Array.isArray(data.calls) ? data.calls.length : 0;
  const sourceCount = Array.isArray(data.call_links) ? data.call_links.length : 0;

  return (
    <main className="space-y-7">
      <section className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="flex items-center gap-4"><CompanyVisual ticker={symbol} name={company?.name} sector={company?.sector} size="lg" /><div><div className="eyebrow">Earnings</div><h1 className="mt-1 text-3xl font-semibold tracking-tight text-white">{company?.name || symbol}</h1><div className="mt-1 flex flex-wrap gap-x-3 text-xs text-neutral-500"><span className="font-mono text-neutral-300">{symbol}</span>{company?.sector ? <span>{company.sector}</span> : null}</div></div></div>
        <div className="flex flex-wrap gap-2"><span className="pill">{callCount} structured call{callCount === 1 ? "" : "s"}</span>{sourceCount > 0 ? <span className="pill">{sourceCount} public source{sourceCount === 1 ? "" : "s"}</span> : null}<Link href={`/ticker/${symbol}#earnings`} className="pill">Open company workspace →</Link></div>
      </section>
      <EarningsIntelligenceClientV2 symbol={symbol} data={data} />
    </main>
  );
}
