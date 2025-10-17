// apps/web/app/page.tsx
import Link from "next/link";
import { loadTickers, loadPortfolio } from "../lib/data";

export default async function Home() {
  const tickers = await loadTickers();
  const portfolio = (await loadPortfolio()) || { dates: [], long_short: [] };

  return (
    <main className="max-w-6xl mx-auto p-6">
      <h1 className="text-3xl font-bold mb-6">Market Sentiment — S&amp;P 500</h1>

      <div className="mb-6 grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="rounded-2xl p-4 shadow bg-white">
          <div className="text-sm text-gray-500">Coverage</div>
          <div className="text-2xl font-semibold">{tickers.length} tickers</div>
        </div>
        <div className="rounded-2xl p-4 shadow bg-white">
          <div className="text-sm text-gray-500">Portfolio series</div>
          <div className="text-2xl font-semibold">{portfolio.dates?.length ?? 0} days</div>
        </div>
        <div className="rounded-2xl p-4 shadow bg-white">
          <div className="text-sm text-gray-500">Status</div>
          <div className="text-2xl font-semibold">OK</div>
        </div>
      </div>

      <h2 className="text-xl font-semibold mb-3">Tickers</h2>
      {tickers.length === 0 ? (
        <p className="text-gray-600">No data generated yet.</p>
      ) : (
        <div className="flex flex-wrap gap-2">
          {tickers.map((t) => (
            <Link
              key={t}
              href={`/ticker/${t}`}
              className="px-2 py-1 rounded border hover:bg-gray-50 text-sm"
            >
              {t}
            </Link>
          ))}
        </div>
      )}
    </main>
  );
}
