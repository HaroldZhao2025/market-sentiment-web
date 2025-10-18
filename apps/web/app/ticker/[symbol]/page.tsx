// apps/web/app/ticker/[symbol]/page.tsx
import Link from "next/link";
import { loadTickers, loadTickerSeries, loadTickerNews } from "../../../lib/data";
import TickerClient from "./TickerClient";

export async function generateStaticParams() {
  try {
    const tickers = await loadTickers();
    return tickers.slice(0, 1200).map((t) => ({ symbol: t }));
  } catch {
    return [];
  }
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  const series = await loadTickerSeries(symbol);
  const news = await loadTickerNews(symbol);

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Market Sentiment for {symbol}</h1>
        <Link href="/" className="text-sm text-blue-600 hover:underline">
          ← Back
        </Link>
      </div>

      {series ? (
        <TickerClient series={series} news={news} />
      ) : (
        <div className="text-neutral-500">No data for {symbol}.</div>
      )}
    </div>
  );
}
