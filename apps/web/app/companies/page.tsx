import fs from "node:fs";
import path from "node:path";
import CompaniesClient, { type CompanyRow } from "./CompaniesClient";
import CompanyMetrics from "./CompanyMetrics";

export const dynamic = "force-static";
export const metadata = { title: "Companies" };

type UniverseFile = { generated_at_utc?: string; companies?: CompanyRow[] };

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

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">Company intelligence</div>
        <h1 className="page-title mt-2">Companies</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Extended U.S. company intelligence beyond the S&amp;P 500. The broader universe remains physically separated from SPX weights, attribution, and portfolio calculations.
        </p>
      </section>

      <CompanyMetrics total={rows.length} sp500={sp500} midcap={midcap} />

      {rows.length ? (
        <CompaniesClient rows={rows} generatedAt={data.generated_at_utc ?? null} />
      ) : (
        <section className="rounded-2xl border border-white/10 bg-white/[0.025] p-6 md:p-7">
          <div className="text-sm font-semibold text-neutral-200">Extended market data is waiting for its first Phase 6 refresh.</div>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-neutral-500">
            This route remains build-safe. S&amp;P 500 market, attribution, portfolio, and research surfaces continue to use their validated core artifacts while the broader company universe is generated independently.
          </p>
        </section>
      )}
    </main>
  );
}
