import fs from "node:fs";
import path from "node:path";
import CompaniesClientV2, { type CompanyRowV2 } from "./CompaniesClientV2";
import CompanyMetrics from "./CompanyMetrics";

export const dynamic = "force-static";
export const metadata = { title: "Companies" };

type UniverseFile = { generated_at_utc?: string; companies?: CompanyRowV2[] };
type CoverageRow = {
  ticker?: string;
  status?: "complete" | "partial" | "link_only" | "no_structured_call";
  complete_calls?: number;
  structured_calls?: number;
  call_links?: number;
  source?: string;
};
type CoverageFile = {
  generated_at_utc?: string;
  complete_company_count?: number;
  complete_coverage_rate?: number;
  companies?: CoverageRow[];
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

function loadCoverage(): CoverageFile {
  const parsed = readJson<CoverageFile>("earnings_coverage.json");
  return parsed && typeof parsed === "object" ? parsed : {};
}

export default function CompaniesPage() {
  const data = loadUniverse();
  const coverage = loadCoverage();
  const baseRows = Array.isArray(data.companies) ? data.companies : [];
  const coverageMap = new Map(
    (Array.isArray(coverage.companies) ? coverage.companies : [])
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

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">U.S. company universe</div>
        <h1 className="page-title mt-2">Companies</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">
          Search large, mid and small-cap names in one market view. S&amp;P 500 index calculations remain separate.
        </p>
      </section>

      <CompanyMetrics total={rows.length} sp500={sp500} midcap={midcap} smallcap={smallcap} />

      {rows.length ? (
        <>
          <section className="grid gap-3 md:grid-cols-2">
            <div className="kpi">
              <div className="kpi-label">Complete earnings calls</div>
              <div className="kpi-value text-white">{completeCalls.toLocaleString()}</div>
              <div className="kpi-sub">Structured call analytics available</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Call coverage</div>
              <div className="kpi-value text-emerald-300">{(callCoverage * 100).toFixed(1)}%</div>
              <div className="kpi-sub">Of the current company universe</div>
            </div>
          </section>
          <CompaniesClientV2
            rows={rows}
            generatedAt={data.generated_at_utc ?? null}
            coverageGeneratedAt={coverage.generated_at_utc ?? null}
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
