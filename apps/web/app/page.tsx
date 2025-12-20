// apps/web/app/page.tsx
import Link from "next/link";
import dynamic from "next/dynamic";
import { loadTickers, loadPortfolio, loadSp500Index } from "../lib/data";
import { hrefs } from "../lib/paths";

const OverviewChart = dynamic(() => import("../components/OverviewChart"), { ssr: false });

export default async function Home() {
  const [tickers, portfolio, sp500] = await Promise.all([
    loadTickers(),
    loadPortfolio(),        // future use; keep it separate
    loadSp500Index(),       // SPX index series for the overview
  ]);

  const dates = sp500?.dates ?? [];
  const sentiment = sp500?.sentiment ?? [];
  const price = sp500?.price ?? [];

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-8">
      <h1 className="text-3xl font-bold">Market Sentiment — S&amp;P 500</h1>

      {/* S&P 500 Overview (SPX index series, NOT portfolio) */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <h2 className="text-xl font-semibold">S&amp;P 500 Overview</h2>
          <Link href="/sp500" className="text-sm underline text-neutral-700">
            Open SPX page →
          </Link>
        </div>

        <div className="mt-3">
          {dates.length ? (
            <OverviewChart dates={dates} sentiment={sentiment} price={price} />
          ) : (
            <p className="text-sm text-neutral-500">
              No SPX index data yet. Expected: <code className="px-1">data/SPX/sp500_index.json</code>
            </p>
          )}
        </div>
      </section>

      {/* Portfolio (separate; future use) */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <h2 className="text-xl font-semibold">Portfolio (Coming soon)</h2>
          <Link href="/portfolio" className="text-sm underline text-neutral-700">
            Go to Portfolio →
          </Link>
        </div>

        {!portfolio ? (
          <p className="text-sm text-neutral-500 mt-2">No portfolio data yet.</p>
        ) : (
          <p className="text-sm text-neutral-600 mt-2">
            Points: {portfolio.dates?.length ?? 0}
          </p>
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
    </div>
  );
}
