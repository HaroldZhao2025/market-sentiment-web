import fs from "fs";
import path from "path";
import EarningsClient from "./EarningsClient";

type Params = { symbol: string };

export async function generateStaticParams(): Promise<Params[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "index.json");
    const raw = fs.readFileSync(p, "utf-8");
    const arr = JSON.parse(raw) as { ticker: string }[];
    return arr.map((r) => ({ symbol: r.ticker }));
  } catch {
    return [{ symbol: "MSFT" }];
  }
}

export const dynamicParams = false;

export default function Page({ params }: { params: Params }) {
  const symbol = (params.symbol || "MSFT").toUpperCase();
  return <EarningsClient symbol={symbol} />;
}
