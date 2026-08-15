import AskMarketClient from "./AskMarketClient";
import { buildScreenerRows } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "Ask the Market",
  description: "Natural-language access to explicit S&P 500 market screens and rankings.",
};

export default function AskMarketPage() {
  const rows = buildScreenerRows();
  const observed = rows.filter((row) => row.sentiment !== null && row.sentiment !== undefined).length;

  return (
    <main className="space-y-7">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">S&amp;P 500 natural-language screen</div>
          <h1 className="page-title mt-2">Ask the Market</h1>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Describe a screen or ranking; the page translates it into explicit filters over the current S&amp;P 500 screener fields.</p>
        </div>
        <div className="flex flex-wrap gap-2"><span className="pill">S&amp;P 500 scope</span><span className="pill">{rows.length} constituents</span><span className="pill">{observed} observed</span></div>
      </section>
      <AskMarketClient rows={rows} />
    </main>
  );
}
