// apps/web/app/page.tsx
import Link from "next/link";
import { loadTickers, loadPortfolio, loadTicker } from "../lib/data";
import { hrefs } from "../lib/paths";
import dynamic from "next/dynamic";

const OverviewChart = dynamic(() => import("../components/OverviewChart"), { ssr: false });

export default async function Home() {
  const [tickers, portfolio, spy, spx] = await Promise.all([
    loadTickers(),
    loadPortfolio(),
    loadTicker("SPY"),
    loadTicker("^GSPC"),
  ]);

  const priceSrc = spy?.price?.length ? spy : spx?.price?.length ? spx : null;
  const price = priceSrc?.price ?? [];
  const dates = portfolio?.dates ?? [];
  const sentiment = portfolio?.S ?? portfolio?.sentiment ?? [];

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      <h1 className="text-3xl font-bold">Market Sentiment — S&amp;P 500</h1>

      {/* Overview */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <h2 className="text-xl font-semibold mb-3">S&amp;P 500 Overview</h2>
        {dates.length ? (
          <OverviewChart dates={dates} sentiment={sentiment} price={price} />
        ) : (
          <p className="text-sm text-neutral-500">No portfolio data yet.</p>
        )}
      </section>

      {/* Portfolio quick stats */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <h2 className="text-xl font-semibold mb-2">Portfolio</h2>
        {!portfolio ? (
          <p className="text-sm text-neutral-500">No portfolio yet.</p>
        ) : (
          <p className="text-sm text-neutral-600">Points: {portfolio.dates?.length ?? 0}</p>
        )}
      </section>

      {/* Tickers */}
      <section>
        <h2 className="text-xl font-semibold mb-3">Tickers</h2>
        {tickers.length === 0 ? (
          <p className="text-sm text-neutral-500">No data generated yet.</p>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {tickers.map((t) => (
              <Link
                key={t}
                href={hrefs.ticker(t)}
                className="px-3 py-2 rounded-lg bg-white hover:bg-neutral-100 border text-sm"
              >
                {t}
              </Link>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
