import ResearchLabClientV3 from "./ResearchLabClientV3";
import { buildLabV2Summaries } from "../../lib/researchLabV2";

export const dynamic = "force-static";

export const metadata = {
  title: "Research Lab V3",
};

export default function ResearchLabPage() {
  const rows = buildLabV2Summaries();
  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">Interactive research · V3</div>
        <h1 className="page-title mt-2">Research Lab</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Test observed sentiment signals with balanced diagnostics, Newey-West inference, chronological out-of-sample checks, turnover and cost sensitivity, plus reproducible specification IDs.
        </p>
      </section>
      <ResearchLabClientV3 rows={rows} />
    </main>
  );
}
