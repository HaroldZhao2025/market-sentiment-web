import AttributionExplorer from "../sp500/AttributionExplorer";
import { buildAttributionRows } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "S&P Attribution",
};

export default function AttributionPage() {
  const rows = buildAttributionRows();
  const sectors = rows.filter((r) => r.level === "sector");
  const net = sectors.reduce((s, r) => s + r.contribution, 0);
  const observedWeight = sectors.reduce((s, r) => s + r.observed_weight, 0);

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">S&amp;P 500 attribution</div>
        <h1 className="page-title mt-2">Hierarchical Signal Attribution</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Decompose the observed index sentiment contribution from constituent companies into industries and sectors without treating unobserved sentiment as neutral.
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <span className="pill">Net raw contribution {(net * 10000).toFixed(2)} bps</span>
          <span className="pill">Observed constituent weight {(observedWeight * 100).toFixed(1)}%</span>
          <span className="pill">Company → industry → sector → index</span>
        </div>
      </section>
      <AttributionExplorer rows={rows} />
      <section className="card p-5 text-sm leading-6 text-neutral-500">
        Group contribution is the sum of constituent weight × observed sentiment. Group sentiment renormalizes only across observed constituent weight inside that group.
      </section>
    </main>
  );
}
