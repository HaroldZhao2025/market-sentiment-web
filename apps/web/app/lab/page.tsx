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
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">S&amp;P 500 research · V3</div>
          <h1 className="page-title mt-2">Research Lab</h1>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">
            Compare signals, horizons, samples and trading-cost assumptions with reproducible specification IDs. The extended Composite company layer is not silently added to this historical research panel.
          </p>
        </div>
        <span className="pill">S&amp;P 500 research scope</span>
      </section>
      <ResearchLabClientV3 rows={rows} />
    </main>
  );
}
