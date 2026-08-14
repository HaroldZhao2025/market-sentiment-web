import AttributionExplorer from "../sp500/AttributionExplorer";
import { buildAttributionRows } from "../../lib/intelligence";

export const dynamic = "force-static";
export const metadata = { title: "S&P Attribution" };

export default function AttributionPage() {
  const rows = buildAttributionRows();
  const sectors = rows.filter((row) => row.level === "sector");
  const net = sectors.reduce((sum, row) => sum + row.contribution, 0);
  const observedWeight = sectors.reduce((sum, row) => sum + row.observed_weight, 0);

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">S&amp;P 500</div>
        <h1 className="page-title mt-2">Signal Attribution</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Trace company contributions through industries and sectors to the index.</p>
        <div className="mt-4 flex flex-wrap gap-2"><span className="pill">Net {(net * 10000).toFixed(2)} bps</span><span className="pill">Observed weight {(observedWeight * 100).toFixed(1)}%</span><span className="pill">Company → sector → index</span></div>
      </section>
      <AttributionExplorer rows={rows} />
      <section className="card p-4 text-xs leading-5 text-neutral-500">Contribution = constituent weight × observed sentiment. Group sentiment uses observed weight only.</section>
    </main>
  );
}
