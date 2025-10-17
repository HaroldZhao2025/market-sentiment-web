import { promises as fs } from "node:fs";
import path from "node:path";
import TickerClient from "./TickerClient";

export const dynamicParams = false;

export async function generateStaticParams() {
  const p = path.join(process.cwd(), "public", "data", "_tickers.json");
  try {
    const buf = await fs.readFile(p, "utf8");
    const arr = JSON.parse(buf) as string[];
    return (arr || []).map((t) => ({ symbol: t }));
  } catch {
    // fall back – at least build one page
    return [{ symbol: "AAPL" }];
  }
}

export default function Page({ params }: { params: { symbol: string } }) {
  return <TickerClient symbol={params.symbol} />;
}
