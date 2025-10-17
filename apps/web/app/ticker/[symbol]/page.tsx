// apps/web/app/ticker/[symbol]/page.tsx
import { loadTicker } from "../../../lib/data";
import LineChart from "../../../components/LineChart";

export async function generateStaticParams() {
  // Build-time list; if _tickers.json missing during preview, fall back to empty
  try {
    const tickers: string[] = (await import("../../../lib/data")).loadTickers
      ? await (await import("../../../lib/data")).loadTickers()
      : [];
    return tickers.slice(0, 1200).map((t) => ({ symbol: t }));
  } catch {
    return [];
  }
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  const d = await loadTicker(symbol);

  if (!d || d.dates.length === 0) {
    return (
      <main className="mx-auto max-w-5xl px-4 py-8">
        <h1 className="text-xl font-semibold mb-4">{symbol}</h1>
        <p className="text-sm text-gray-500">No data found for this ticker.</p>
      </main>
    );
  }

  const series = {
    dates: d.dates,
    left: d.sentiment,
    overlay: d.sentiment_ma7,
    right: d.price,
    label: "Sentiment (S)",
  };

  return (
    <main className="mx-auto max-w-5xl px-4 py-8">
      <h1 className="text-xl font-semibold mb-4">{symbol}</h1>

      <div className="rounded-xl border p-4 mb-8">
        <h3 className="font-medium mb-3">Sentiment &amp; Price</h3>
        <LineChart series={series} height={320} />
      </div>

      <div className="rounded-xl border p-4">
        <h3 className="font-medium mb-3">Recent headlines</h3>
        {d.news.length === 0 ? (
          <div className="text-sm text-gray-500">No recent headlines.</div>
        ) : (
          <ul className="space-y-2">
            {d.news.slice(-15).reverse().map((n, i) => (
              <li key={i} className="text-sm">
                <span className="font-mono text-gray-500">{n.ts.slice(0, 10)}</span>{" "}
                <a href={n.url} target="_blank" rel="noreferrer" className="underline">
                  {n.title}
                </a>{" "}
                <span className="ml-2 text-gray-600">S={n.S.toFixed(2)}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </main>
  );
}
