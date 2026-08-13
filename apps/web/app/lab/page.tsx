import ResearchLabClient from "./ResearchLabClient";
import { buildLabV2Summaries } from "../../lib/researchLabV2";

export const dynamic = "force-static";

export const metadata = {
  title: "Research Lab V2",
};

export default function ResearchLabPage() {
  const rows = buildLabV2Summaries();
  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">Interactive research · V2</div>
        <h1 className="page-title mt-2">Research Lab</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Test observed sentiment signals with daily cross-sectional sorts, Newey-West inference, chronological out-of-sample checks, turnover diagnostics, transaction-cost sensitivity, and downloadable specifications.
        </p>
      </section>
      <ResearchLabClient rows={rows} />
    </main>
  );
}
