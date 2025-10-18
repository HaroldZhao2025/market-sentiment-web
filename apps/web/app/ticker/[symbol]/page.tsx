// apps/web/app/ticker/[symbol]/page.tsx
import { loadTickers, loadTickerNews, loadTickerSeries } from "../../../lib/data";
import TickerClient from "./TickerClient";

export async function generateStaticParams() {
  const tickers = loadTickers();
  // generate pages for all tickers you exported (safe for static export)
  return tickers.map((symbol) => ({ symbol }));
}

export default function Page({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  const s = loadTickerSeries(symbol);
  const news = loadTickerNews(symbol);

  return (
    <div className="max-w-5xl mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-semibold">
        {symbol} — Price & Sentiment
      </h1>

      <TickerClient
        series={{
          date: s.date,
          price: s.price,
          sentiment: s.sentiment,
          sentiment_ma7: s.sentiment_ma7,
          label: "Daily S",
        }}
        news={news}
      />
    </div>
  );
}
