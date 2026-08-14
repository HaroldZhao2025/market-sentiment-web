import ScreenerClient from "./ScreenerClient";
import { buildScreenerRows } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = { title: "Market Screener" };

export default function ScreenerPage() {
  const rows = buildScreenerRows();
  const observed = rows.filter((r) => r.sentiment !== null && r.sentiment !== undefined).length;

  return (
    <main className="space-y-7">
      <section>
        <div className="eyebrow">S&P 500</div>
        <h1 className="page-title mt-2">Market Screener</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Rank price, sentiment, divergence, events and news attention across constituents.</p>
        <div className="mt-4 flex flex-wrap gap-2"><span className="pill">{rows.length} constituents</span><span className="pill">{observed} observed today</span></div>
      </section>
      <ScreenerClient rows={rows} />
    </main>
  );
}
