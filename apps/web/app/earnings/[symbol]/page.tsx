// apps/web/app/earnings/[symbol]/page.tsx
import { listTickers, loadTicker } from "../../../lib/loaders";
import { assetPath } from "../../../lib/paths";

export const dynamicParams = true;

export async function generateStaticParams() {
  const tickers = listTickers();
  return tickers.map((t) => ({ symbol: t }));
}

export default function EarningsPage({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const data = loadTicker(symbol); // reuse ticker json for now; shows earnings-doc news if present

  const earningsNews = data.news.filter((n) =>
    /earnings|transcript|remarks|prepared/i.test(n.title || "")
  );

  return (
    <main className="max-w-5xl mx-auto p-6">
      <h1 className="text-xl font-semibold mb-4">{symbol} — Earnings</h1>

      {earningsNews.length === 0 ? (
        <p className="text-sm text-gray-500">No earnings documents captured for this window.</p>
      ) : (
        <ul className="space-y-2">
          {earningsNews.map((n, idx) => (
            <li key={idx} className="border rounded p-3">
              <div className="text-sm text-gray-500">{new Date(n.ts).toLocaleString()}</div>
              <a href={n.url} target="_blank" rel="noreferrer" className="font-medium underline">
                {n.title}
              </a>
              <div className="text-sm text-gray-600">Sentiment: {n.s.toFixed(3)} {n.source ? ` • ${n.source}` : ""}</div>
            </li>
          ))}
        </ul>
      )}

      <div className="mt-8">
        <a className="underline text-blue-600" href={assetPath("")}>
          ← Back
        </a>
      </div>
    </main>
  );
}
