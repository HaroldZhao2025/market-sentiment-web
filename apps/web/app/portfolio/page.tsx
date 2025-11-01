// apps/web/app/portfolio/page.tsx
import SeriesClient, { SeriesIn } from "../components/SeriesClient";
import { loadPortfolio, loadTicker } from "../../lib/data";

export const dynamic = "error";
export const revalidate = false;

export default async function PortfolioPage() {
  // Average sentiment time series (already built by your pipeline)
  const portfolio = await loadPortfolio(); // expects dates + S (or sentiment)
  // Price: align with homepage logic (prefer SPY, else ^GSPC)
  const spy = await loadTicker("SPY");
  const spx = await loadTicker("^GSPC");
  const priceSrc = spy?.price?.length ? spy : spx?.price?.length ? spx : null;

  const dates: string[] = portfolio?.dates ?? portfolio?.date ?? [];
  const sentiment: number[] = portfolio?.S ?? portfolio?.sentiment ?? [];
  const price: number[] = priceSrc?.price ?? [];

  // Align lengths safely (SeriesClient will also slice, but we keep it neat here)
  const n = Math.min(dates.length, price.length || Infinity, sentiment.length || Infinity);
  const series: SeriesIn = {
    date: dates.slice(0, n),
    price: price.slice(0, n),
    sentiment: sentiment.slice(0, n),
  };

  return (
    <div className="min-h-screen p-6">
      <SeriesClient title="S&P 500 Portfolio (Avg Sentiment)" series={series} />
    </div>
  );
}
