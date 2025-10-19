// server component
import { notFound } from "next/navigation";
import TickerClient from "./TickerClient";
import { loadTickers, loadTickerSeries, loadTickerNews } from "@/lib/data";

export async function generateStaticParams() {
  const tickers = await loadTickers();
  return tickers.map((t) => ({ symbol: t }));
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const symbol = (params.symbol || "").toUpperCase();
  const tickers = await loadTickers();
  if (!tickers.includes(symbol)) {
    notFound();
  }

  const series = await loadTickerSeries(symbol);
  const news = await loadTickerNews(symbol);

  return (
    <div className="min-h-screen">
      <TickerClient symbol={symbol} series={series} news={news} />
    </div>
  );
}
