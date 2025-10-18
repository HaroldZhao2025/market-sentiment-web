// apps/web/app/ticker/[symbol]/page.tsx
import Link from "next/link";
import { loadTickers, loadTickerSeries, loadTickerNews } from "../../../lib/data";
import TickerClient from "./TickerClient";

export async function generateStaticParams() {
  const tickers: string[] = await loadTickers();
  return tickers.slice(0, 1200).map((t) => ({ symbol: t }));
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  const [series, news] = await Promise.all([
    loadTickerSeries(symbol),
    loadTickerNews(symbol),
  ]);

  return (
    <main className="max-w-6xl mx-auto p-6">
      <div className="mb-4">
        <Link href="/" className="text-sm text-blue-600 hover:underline">← Back</Link>
      </div>
      <h1 className="text-2xl font-bold mb-4">{symbol}</h1>

      {series ? (
        <TickerClient left={series.left} right={series.right} overlay={series.overlay} news={news} />
      ) : (
        <div className="text-neutral-500">No data for {symbol}.</div>
      )}
    </main>
  );
}
