// apps/web/app/ticker/[symbol]/page.tsx
import { notFound } from "next/navigation";
import TickerClient from "./TickerClient";
import { loadTickerSeries, loadTickerNews } from "../../../lib/data"; // <-- up 3 levels

type Params = { params: { symbol: string } };

export default async function Page({ params: { symbol } }: Params) {
  const upper = (symbol || "").toUpperCase();
  const [raw, news] = await Promise.all([
    loadTickerSeries(upper),
    loadTickerNews(upper),
  ]);

  if (!raw) return notFound();

  // Map your JSON (`S`) into the prop shape TickerClient expects (`sentiment`)
  const series = {
    date: raw.date ?? [],
    price: raw.price ?? [],
    sentiment: (raw.S ?? raw.sentiment ?? []).map((x: any) =>
      typeof x === "number" ? x : Number(x || 0)
    ),
  };

  return (
    <div className="min-h-screen">
      <TickerClient symbol={upper} series={series} news={news} />
    </div>
  );
}
