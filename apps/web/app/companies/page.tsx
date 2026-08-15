import fs from "node:fs";
import path from "node:path";
import CompaniesClientV2, { type CompanyRowV2 } from "./CompaniesClientV2";
import CompanyMetrics from "./CompanyMetrics";

export const dynamic = "force-static";
export const metadata = { title: "Companies" };

type UniverseFile = { generated_at_utc?: string; companies?: CompanyRowV2[] };
type EarningsCoverageRow = {
  ticker?: string;
  status?: "complete" | "partial" | "link_only" | "no_structured_call";
  complete_calls?: number;
  structured_calls?: number;
  call_links?: number;
  source?: string;
};
type EarningsCoverageFile = {
  generated_at_utc?: string;
  complete_company_count?: number;
  complete_coverage_rate?: number;
  companies?: EarningsCoverageRow[];
};
type CompanyDataCoverageFile = {
  generated_at_utc?: string;
  company_count?: number;
  news_ready_count?: number;
  history_ready_count?: number;
  attempted_count?: number;
  news_coverage_rate?: number;
  history_coverage_rate?: number;
};

function readJson<T>(relativePath: string): T | null {
  try {
    const file = path.join(process.cwd(), "public", "data", "v5", relativePath);
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return null;
  }
}

function loadUniverse(): UniverseFile {
  const parsed = readJson<UniverseFile>("universe.json");
  return parsed && typeof parsed === "object" ? parsed : {};
}

function loadEarningsCoverage(): EarningsCoverageFile {
  const parsed = readJson<EarningsCoverageFile>("earnings_coverage.json");
  return parsed && typeof parsed === "object" ? parsed : {};
}

function loadCompanyDataCoverage(): CompanyDataCoverageFile {
  const parsed = readJson<CompanyDataCoverageFile>("company_data_coverage.json");
  return parsed && typeof parsed === "object" ? parsed : {};
}

function pct(value: number | undefined) {
  return Number.isFinite(value) ? `${((value as number) * 100).toFixed(1)}%` : "—";
}

export default function CompaniesPage() {
  const data = loadUniverse();
  const earningsCoverage = loadEarningsCoverage();
  const companyCoverage = loadCompanyDataCoverage();
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
  const completeCalls = rows.filter((row) => row.call_status === "complete").length;
  const callCoverage = rows.length ? completeCalls / rows.length : 0;
  const newsReady = Number(companyCoverage.news_ready_count || 0);
  const historyReady = Number(companyCoverage.history_ready_count || 0);
  const attempted = Number(companyCoverage.attempted_count || 0);

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">U.S. company universe</div>
        <h1 className="page-title mt-2">Companies</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">
          Search large, mid and small-cap names with company news, price history and earnings-call availability. S&amp;P 500 index calculations remain separate.
        </p>
      </section>

      <CompanyMetrics total={rows.length} sp500={sp500} midcap={midcap} smallcap={smallcap} />

      {rows.length ? (
        <>
          <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <div className="kpi"><div className="kpi-label">News ready</div><div className="kpi-value text-white">{newsReady.toLocaleString()}</div><div className="kpi-sub">{pct(companyCoverage.news_coverage_rate)} of companies</div></div>
            <div className="kpi"><div className="kpi-label">Price history ready</div><div className="kpi-value text-white">{historyReady.toLocaleString()}</div><div className="kpi-sub">{pct(companyCoverage.history_coverage_rate)} of companies</div></div>
            <div className="kpi"><div className="kpi-label">Structured calls</div><div className="kpi-value text-white">{completeCalls.toLocaleString()}</div><div className="kpi-sub">{(callCoverage * 100).toFixed(1)}% complete-call coverage</div></div>
            <div className="kpi"><div className="kpi-label">Company data searched</div><div className="kpi-value text-emerald-300">{attempted.toLocaleString()}</div><div className="kpi-sub">{rows.length ? `${Math.min(100, (attempted / rows.length) * 100).toFixed(1)}% attempted` : "—"}</div></div>
          </section>
          <CompaniesClientV2
            rows={rows}
            generatedAt={data.generated_at_utc ?? null}
            coverageGeneratedAt={earningsCoverage.generated_at_utc ?? companyCoverage.generated_at_utc ?? null}
          />
        </>
      ) : (
        <section className="rounded-2xl border border-white/10 bg-white/[0.025] p-6">
          <div className="text-sm font-semibold text-neutral-200">Company data is refreshing.</div>
          <p className="mt-2 text-sm text-neutral-500">The core S&amp;P 500 pages remain available while the broader universe is rebuilt.</p>
        </section>
      )}
    </main>
  );
}
