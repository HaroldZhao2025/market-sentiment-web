// apps/web/app/ticker/[symbol]/page.tsx
import { notFound } from "next/navigation";
import { loadTickers, loadTicker } from "../../../lib/data";
import TickerClient from "./TickerClient";

type Params = { params: { symbol: string } };

export async function generateStaticParams() {
  try {
    const tickers = await loadTickers();
    // cap large lists so Pages build stays under limits
    return tickers.slice(0, 1200).map((symbol) => ({ symbol }));
  } catch {
    return [];
  }
}

export default async function Page({ params }: Params) {
  const symbol = (params.symbol || "").toUpperCase();
  const data = await loadTicker(symbol);

  if (!data) {
    notFound();
  }

  // Be defensive: ensure arrays so the chart never crashes at build/export
  const dates: string[] = Array.isArray(data.dates) ? data.dates : [];
  const price: number[] = Array.isArray(data.price) ? data.price : [];
  const sentiment: number[] = Array.isArray(data.sentiment) ? data.sentiment : [];
  const sentiment_ma7: number[] = Array.isArray(data.sentiment_ma7) ? data.sentiment_ma7 : [];
  const news = Array.isArray(data.news) ? data.news : [];

  return (
    <div className="max-w-5xl mx-auto p-4">
      <h1 className="text-2xl font-semibold mb-4">Market Sentiment — {symbol}</h1>

      <TickerClient
        series={{ date: dates, price, sentiment, sentiment_ma7, label: "Daily S" }}
        news={news}
      />
    </div>
  );
}
