import fs from "fs";
import path from "path";
import TickerClient from "./TickerClient";

type Params = { symbol: string };

export async function generateStaticParams(): Promise<Params[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "index.json");
    const raw = fs.readFileSync(p, "utf-8");
    const arr = JSON.parse(raw) as { ticker: string }[];
    // generate pages for all tickers present in index.json
    return arr.map((r) => ({ symbol: r.ticker }));
  } catch {
    // fall back so export still succeeds on first run
    return [{ symbol: "MSFT" }];
  }
}

// no on-demand params at runtime for static export
export const dynamicParams = false;

export default function Page({ params }: { params: Params }) {
  const symbol = (params.symbol || "MSFT").toUpperCase();
  return <TickerClient symbol={symbol} />;
}
