import fs from "node:fs";
import path from "node:path";
import CompaniesClient, { type CompanyRow } from "./CompaniesClient";

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
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Phase 5 market expansion</div>
          <h1 className="page-title mt-2">Companies</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Extended U.S. company intelligence beyond the S&amp;P 500. The broader universe is physically separated from SPX weights, attribution, and portfolio calculations.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="pill">{rows.length || "—"} extended companies</span>
          <span className="pill">{sp500 || "—"} S&amp;P 500</span>
          <span className="pill">{midcap || "—"} MidCap 400</span>
        </div>
      </section>
      {rows.length ? <CompaniesClient rows={rows} generatedAt={data.generated_at_utc ?? null} /> : <div className="card p-5 text-sm leading-6 text-neutral-500">The V5 extended-universe artifact has not been generated in this clean build yet. This route is intentionally build-safe; the core S&amp;P 500 surfaces remain unaffected.</div>}
    </main>
  );
}
