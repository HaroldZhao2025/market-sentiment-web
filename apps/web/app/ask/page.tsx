import AskMarketClient from "./AskMarketClient";
import { buildScreenerRows } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "Ask the Market",
  description: "Natural-language access to deterministic Sentiment Intelligence screens and evidence.",
};

export default function AskMarketPage() {
  const rows = buildScreenerRows();
  const observed = rows.filter((r) => r.sentiment !== null && r.sentiment !== undefined).length;

  return (
    <main className="space-y-7">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Phase 4 query layer</div>
          <h1 className="page-title mt-2">Ask the Market</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Ask a natural-language market question. The interface translates it into explicit filters and rankings over the deterministic intelligence engine, then exposes the query plan and underlying evidence.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="pill">{rows.length} constituents</span>
          <span className="pill">{observed} observed</span>
          <span className="pill">No generated finance prose</span>
        </div>
      </section>
      <AskMarketClient rows={rows} />
    </main>
  );
}
