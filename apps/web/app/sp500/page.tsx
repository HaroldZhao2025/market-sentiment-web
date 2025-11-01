// apps/web/app/sp500/page.tsx
import SeriesClient, { SeriesIn } from "../components/SeriesClient";
import { loadPortfolio, loadTicker } from "../../lib/data";

export const dynamic = "error";
export const revalidate = false;

export default async function SP500Page() {
  // Sentiment: use the portfolio average (dates + S)
  const portfolio = await loadPortfolio();

  // Price: use index-level (^GSPC) if present, else SPY
  const spx = await loadTicker("^GSPC");
  const spy = await loadTicker("SPY");
  const priceSrc = spx?.price?.length ? spx : spy?.price?.length ? spy : null;

  const dates: string[] = portfolio?.dates ?? portfolio?.date ?? [];
  const sentiment: number[] = portfolio?.S ?? portfolio?.sentiment ?? [];
  const price: number[] = priceSrc?.price ?? [];

  const n = Math.min(dates.length, price.length || Infinity, sentiment.length || Infinity);
  const series: SeriesIn = {
    date: dates.slice(0, n),
    price: price.slice(0, n),
    sentiment: sentiment.slice(0, n),
  };

  return (
    <div className="min-h-screen p-6">
      <SeriesClient title="S&P 500 Index (Price + Avg Sentiment)" series={series} />
    </div>
  );
}
