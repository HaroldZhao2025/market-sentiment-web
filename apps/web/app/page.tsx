// apps/web/app/page.tsx
import Link from "next/link";
import { loadTickers, loadPortfolio } from "../lib/data";

export default async function HomePage() {
  const [tickers, portfolio] = await Promise.all([loadTickers(), loadPortfolio()]);

  return (
    <main className="mx-auto max-w-6xl px-4 py-8">
      <h1 className="text-2xl font-semibold mb-6">Market Sentiment — S&amp;P 500</h1>

      <section className="mb-10">
        <h2 className="text-lg font-medium mb-3">Portfolio (equal-weight long–short)</h2>
        {portfolio && portfolio.dates.length > 0 ? (
          <div className="text-sm text-gray-700">
            Data points: {portfolio.dates.length}. Latest date:{" "}
            <span className="font-mono">{portfolio.dates[portfolio.dates.length - 1]}</span>
          </div>
        ) : (
          <div className="text-sm text-gray-500">No portfolio data generated yet.</div>
        )}
      </section>

      <section>
        <h2 className="text-lg font-medium mb-3">Browse tickers</h2>
        {tickers.length === 0 ? (
          <div className="text-sm text-gray-500">No data generated yet.</div>
        ) : (
          <ul className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
            {tickers.map((t) => (
              <li key={t}>
                <Link
                  href={`/ticker/${encodeURIComponent(t)}`}
                  className="block rounded-md border px-3 py-1 text-sm hover:bg-gray-50"
                >
                  {t}
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
