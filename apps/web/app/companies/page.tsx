import fs from "node:fs";
import path from "node:path";
import CompaniesClientV2, { type CompanyRowV2 } from "./CompaniesClientV2";
import CompanyMetrics from "./CompanyMetrics";

export const dynamic = "force-static";
export const metadata = { title: "Companies" };

type UniverseFile = { generated_at_utc?: string; companies?: CompanyRowV2[] };

function loadUniverse(): UniverseFile {
  try {
    const file = path.join(process.cwd(), "public", "data", "v5", "universe.json");
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

export default function CompaniesPage() {
  const data = loadUniverse();
  const rows = Array.isArray(data.companies) ? data.companies : [];
  const sp500 = rows.filter((row) => row.universe === "S&P 500").length;
  const midcap = rows.filter((row) => row.universe === "S&P MidCap 400").length;
  const smallcap = rows.filter((row) => row.universe === "S&P SmallCap 600").length;

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
        <CompaniesClientV2 rows={rows} generatedAt={data.generated_at_utc ?? null} />
      ) : (
        <section className="rounded-2xl border border-white/10 bg-white/[0.025] p-6">
          <div className="text-sm font-semibold text-neutral-200">Company data is refreshing.</div>
          <p className="mt-2 text-sm text-neutral-500">The core S&amp;P 500 pages remain available while the broader universe is rebuilt.</p>
        </section>
      )}
    </main>
  );
}
