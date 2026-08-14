import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";
import CompanyVisual from "../../../components/CompanyVisual";
import EarningsIntelligenceClient, { type EarningsArtifact } from "./EarningsIntelligenceClient";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type CompanyMeta = { ticker?: string; name?: string; sector?: string; industry?: string; universe?: string };
type CallLink = { title?: string; url?: string; ts?: string; source?: string; provider?: string };
type EarningsArtifactWithLinks = EarningsArtifact & { call_links?: CallLink[] };

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
  const symbols = new Set<string>(Array.isArray(core) ? core : ["A"]);
  for (const company of await loadCompanies()) {
    const ticker = String(company?.ticker || "").trim().toUpperCase();
    if (ticker) symbols.add(ticker);
  }
  return Array.from(symbols).sort();
}

function legacyToV5(symbol: string, raw: any): EarningsArtifactWithLinks {
  const docs = Array.isArray(raw) ? raw : Array.isArray(raw?.docs) ? raw.docs : [];
  return {
    schema_version: 1,
    symbol,
    earnings_history: [],
    calls: [],
    call_links: [],
    filing_fallback: docs.map((doc: any) => ({
      ts: String(doc?.ts ?? ""),
      title: String(doc?.title ?? ""),
      url: String(doc?.url ?? ""),
      source: String(doc?.source ?? "Legacy earnings source"),
      document_type: "legacy",
      S: typeof doc?.S === "number" ? doc.S : undefined,
    })),
    methodology: { compatibility: "Legacy artifact mapped into the current filing view." },
  };
}

async function loadEarnings(symbol: string): Promise<EarningsArtifactWithLinks> {
  const publicData = path.join(process.cwd(), "public", "data");
  const v5 = await readJson<EarningsArtifactWithLinks>(path.join(publicData, "v5", "earnings", `${symbol}.json`));
  if (v5 && typeof v5 === "object") return v5;
  const legacy = await readJson<any>(path.join(publicData, "earnings", `${symbol}.json`));
  if (legacy) return legacyToV5(symbol, legacy);
  return { schema_version: 3, symbol, earnings_history: [], calls: [], call_links: [], filing_fallback: [] };
}

export async function generateStaticParams() {
  const symbols = await loadSymbols();
  return symbols.map((symbol) => ({ symbol }));
}

export default async function EarningsPage({ params }: { params: { symbol: string } }) {
  const symbol = String(params.symbol || "").toUpperCase();
  const [data, companies] = await Promise.all([loadEarnings(symbol), loadCompanies()]);
  const company = companies.find((row) => String(row.ticker || "").toUpperCase() === symbol);
  const callCount = Array.isArray(data.calls) ? data.calls.length : 0;
  const callLinks = Array.isArray(data.call_links) ? data.call_links.filter((row) => row?.url && row?.title) : [];
  const filingCount = Array.isArray(data.filing_fallback) ? data.filing_fallback.length : 0;
  const historyCount = Array.isArray(data.earnings_history) ? data.earnings_history.length : 0;
  const transcriptState = callCount > 0 ? "Structured transcript" : callLinks.length > 0 ? "Transcript links" : "Filing-only";

  return (
    <main className="space-y-7">
      <section className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="flex items-center gap-4">
          <CompanyVisual ticker={symbol} name={company?.name} sector={company?.sector} size="lg" />
          <div>
            <div className="eyebrow">Earnings</div>
            <h1 className="mt-1 text-3xl font-semibold tracking-tight text-white">{company?.name || symbol}</h1>
            <div className="mt-1 flex flex-wrap gap-x-3 text-xs text-neutral-500"><span className="font-mono text-neutral-300">{symbol}</span>{company?.sector ? <span>{company.sector}</span> : null}</div>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="pill">{transcriptState}</span>
          <span className="pill">{historyCount} results</span>
          <span className="pill">{filingCount} filings</span>
          <Link href={`/ticker/${symbol}`} className="pill">← Company</Link>
        </div>
      </section>

      {callLinks.length > 0 ? (
        <section className="space-y-3">
          <div>
            <div className="eyebrow">Call sources</div>
            <h2 className="section-title mt-1">Earnings-call transcript links</h2>
            <p className="section-copy">These are source links detected from retained company news. They are shown separately from structured transcript analysis.</p>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            {callLinks.slice(0, 6).map((item, index) => (
              <a key={`${item.url}-${index}`} href={item.url} target="_blank" rel="noreferrer" className="card card-hover p-4">
                <div className="text-[11px] text-neutral-600">{item.ts ? item.ts.slice(0, 10) : "—"} · {item.source || item.provider || "Transcript source"}</div>
                <div className="mt-2 text-sm font-medium leading-6 text-neutral-200">{item.title}</div>
              </a>
            ))}
          </div>
        </section>
      ) : null}

      <EarningsIntelligenceClient symbol={symbol} data={data} />
    </main>
  );
}
