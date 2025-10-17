// apps/web/app/ticker/[symbol]/page.tsx
import { promises as fs } from "node:fs";
import path from "node:path";
import TickerClient from "./TickerClient";

export const dynamicParams = false;

export async function generateStaticParams() {
  // Build-time: read the tickers list from public/data
  const p = path.join(process.cwd(), "public", "data", "_tickers.json");
  try {
    const buf = await fs.readFile(p, "utf8");
    const arr = JSON.parse(buf) as string[];
    return (arr || []).slice(0, 2000).map((t) => ({ symbol: t }));
  } catch {
    // No data yet — generate a tiny set to avoid build failure
    return [{ symbol: "AAPL" }];
  }
}

export default function Page({ params }: { params: { symbol: string } }) {
  return <TickerClient symbol={params.symbol} />;
}
