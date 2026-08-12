import ScreenerClient from "./ScreenerClient";
import { buildScreenerRows } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "Market Screener",
};

export default function ScreenerPage() {
  const rows = buildScreenerRows();
  const observed = rows.filter((r) => r.sentiment !== null && r.sentiment !== undefined).length;

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">Intelligence engine</div>
        <h1 className="page-title mt-2">Market Screener</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Deterministic cross-sectional screening across S&amp;P 500 price, observed sentiment, sentiment change, evidence, event diagnostics, and sentiment-price divergence.
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <span className="pill">{rows.length} constituents</span>
          <span className="pill">{observed} with observed sentiment</span>
          <span className="pill">Auditable inputs only</span>
        </div>
      </section>
      <ScreenerClient rows={rows} />
    </main>
  );
}
