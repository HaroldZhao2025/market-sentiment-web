// apps/web/app/page.tsx
import Link from "next/link";
import { loadTickers, loadPortfolio } from "../lib/data";

export default async function Home() {
  const [tickers, portfolio] = await Promise.all([
    loadTickers(),
    loadPortfolio(),
  ]);

  return (
    <main className="max-w-6xl mx-auto p-6">
      <h1 className="text-3xl font-bold mb-4">Market Sentiment — S&amp;P 500</h1>

      <section className="mb-8">
        <h2 className="text-xl font-semibold mb-2">Portfolio</h2>
        {!portfolio ? (
          <p className="text-sm text-neutral-500">No portfolio yet.</p>
        ) : (
          <p className="text-sm text-neutral-600">
            Points: {portfolio.dates?.length ?? 0}
          </p>
        )}
      </section>

      <section>
        <h2 className="text-xl font-semibold mb-3">Tickers</h2>
        {tickers.length === 0 ? (
          <p className="text-sm text-neutral-500">No data generated yet.</p>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {tickers.map((t) => (
              <Link
                key={t}
                href={`/ticker/${t}`}
                className="px-3 py-2 rounded-lg bg-neutral-50 hover:bg-neutral-100 border text-sm"
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
