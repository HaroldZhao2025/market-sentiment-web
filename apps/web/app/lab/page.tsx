import ResearchLabClient from "./ResearchLabClient";
import { buildLabSummaries } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "Research Lab",
};

export default function ResearchLabPage() {
  const rows = buildLabSummaries();
  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">Interactive research</div>
        <h1 className="page-title mt-2">Research Lab</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Test deterministic sentiment signals against forward returns across horizons, sectors, and long-short quantiles using the same generated data that powers the site.
        </p>
      </section>
      <ResearchLabClient rows={rows} />
    </main>
  );
}
