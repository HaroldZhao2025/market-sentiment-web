// apps/web/app/page.tsx
import Link from "next/link";
import { listTickers, loadPortfolio } from "../lib/loaders";
import { assetPath } from "../lib/paths";

export default function HomePage() {
  const tickers = listTickers();
  const portfolio = loadPortfolio();

  return (
    <main className="max-w-6xl mx-auto p-6">
      <h1 className="text-2xl font-semibold mb-4">Market Sentiment — S&amp;P 500</h1>

      <section className="mb-8">
        <Link className="underline text-blue-600" href={assetPath("portfolio")}>
          Portfolio
        </Link>
        {!portfolio ? (
          <p className="mt-2 text-sm text-gray-500">No portfolio data generated yet.</p>
        ) : (
          <p className="mt-2 text-sm text-gray-600">Last {portfolio.dates.length} days.</p>
        )}
      </section>

      <h2 className="text-lg font-semibold mb-2">Browse Tickers</h2>
      <ul className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 gap-2">
        {tickers.map((t) => (
          <li key={t}>
            <Link className="block border rounded px-2 py-1 hover:bg-gray-50" href={assetPath(`ticker/${t}`)}>
              {t}
            </Link>
          </li>
        ))}
      </ul>
    </main>
  );
}
