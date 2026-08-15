import fs from "node:fs";
import path from "node:path";
import CompaniesClientV2, { type CompanyRowV2 } from "./CompaniesClientV2";
import CompanyMetrics from "./CompanyMetrics";

export const dynamic = "force-static";
export const metadata = { title: "Companies" };

type UniverseFile = { generated_at_utc?: string; companies?: CompanyRowV2[] };
type EarningsCoverageRow = { ticker?: string; status?: "complete" | "partial" | "link_only" | "no_structured_call"; complete_calls?: number; structured_calls?: number; call_links?: number; source?: string };
type EarningsCoverageFile = { generated_at_utc?: string; complete_company_count?: number; complete_coverage_rate?: number; companies?: EarningsCoverageRow[] };
type CompanyDataCoverageFile = { generated_at_utc?: string; company_count?: number; news_ready_count?: number; history_ready_count?: number; attempted_count?: number; news_coverage_rate?: number; history_coverage_rate?: number };

function readJson<T>(relativePath: string): T | null {
  try {
    const file = path.join(process.cwd(), "public", "data", "v5", relativePath);
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return null;
  }
}

export default function CompaniesPage() {
  const data = readJson<UniverseFile>("universe.json") ?? {};
  const earningsCoverage = readJson<EarningsCoverageFile>("earnings_coverage.json") ?? {};
  const companyCoverage = readJson<CompanyDataCoverageFile>("company_data_coverage.json") ?? {};
  const baseRows = Array.isArray(data.companies) ? data.companies : [];
  const coverageMap = new Map(
    (Array.isArray(earningsCoverage.companies) ? earningsCoverage.companies : [])
      .filter((row) => row?.ticker)
      .map((row) => [String(row.ticker).toUpperCase(), row] as const),
  );
  const rows: CompanyRowV2[] = baseRows.map((row) => {
    const call = coverageMap.get(String(row.ticker || "").toUpperCase());
    return {
      ...row,
      call_status: call?.status ?? "no_structured_call",
      complete_calls: Number(call?.complete_calls || 0),
      structured_calls: Number(call?.structured_calls || 0),
      call_links: Number(call?.call_links || 0),
      call_source: call?.source || "",
    };
  });

  const sp500 = rows.filter((row) => row.universe === "S&P 500").length;
  const midcap = rows.filter((row) => row.universe === "S&P MidCap 400").length;
  const smallcap = rows.filter((row) => row.universe === "S&P SmallCap 600").length;
  const completeCalls = Number(earningsCoverage.complete_company_count ?? rows.filter((row) => row.call_status === "complete").length);
  const callCoverage = Number(earningsCoverage.complete_coverage_rate ?? (rows.length ? completeCalls / rows.length : 0));
  const newsReady = Number(companyCoverage.news_ready_count ?? 0);
  const historyReady = Number(companyCoverage.history_ready_count ?? 0);
  const newsRate = Number(companyCoverage.news_coverage_rate ?? (rows.length ? newsReady / rows.length : 0));
  const historyRate = Number(companyCoverage.history_coverage_rate ?? (rows.length ? historyReady / rows.length : 0));

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">U.S. company universe</div>
        <h1 className="page-title mt-2">Companies</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Search large, mid and small-cap companies. S&amp;P 500 index calculations remain a separate core universe.</p>
      </section>

      <CompanyMetrics total={rows.length} sp500={sp500} midcap={midcap} smallcap={smallcap} />

      {rows.length ? (
        <>
          <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <div className="kpi"><div className="kpi-label">Structured calls</div><div className="kpi-value text-white">{completeCalls.toLocaleString()}</div><div className="kpi-sub">{(callCoverage * 100).toFixed(1)}% company coverage</div></div>
            <div className="kpi"><div className="kpi-label">News ready</div><div className="kpi-value text-white">{newsReady.toLocaleString()}</div><div className="kpi-sub">{(newsRate * 100).toFixed(1)}% with retained free-public news</div></div>
            <div className="kpi"><div className="kpi-label">Price history ready</div><div className="kpi-value text-white">{historyReady.toLocaleString()}</div><div className="kpi-sub">{(historyRate * 100).toFixed(1)}% with extended daily history</div></div>
            <div className="kpi"><div className="kpi-label">Data policy</div><div className="mt-2 text-xl font-semibold text-emerald-300">Free public only</div><div className="kpi-sub">News, market data and call sources</div></div>
          </section>
          <CompaniesClientV2 rows={rows} generatedAt={data.generated_at_utc ?? null} coverageGeneratedAt={earningsCoverage.generated_at_utc ?? companyCoverage.generated_at_utc ?? null} />
        </>
      ) : (
        <section className="rounded-2xl border border-white/10 bg-white/[0.025] p-6"><div className="text-sm font-semibold text-neutral-200">Company data is refreshing.</div><p className="mt-2 text-sm text-neutral-500">The core S&amp;P 500 pages remain available while the broader universe is rebuilt.</p></section>
      )}
    </main>
  );
}
