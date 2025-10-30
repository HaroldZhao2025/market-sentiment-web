// apps/web/app/ticker/[symbol]/page.tsx
import fs from "fs";
import path from "path";
import TickerClient from "./TickerClient";

export async function generateStaticParams() {
  const dir = path.join(process.cwd(), "public", "data", "ticker");
  const files = fs.existsSync(dir) ? fs.readdirSync(dir) : [];
  return files
    .filter((f) => f.endsWith(".json"))
    .map((f) => ({ symbol: f.replace(/\.json$/i, "") }));
}

export default function Page({ params }: { params: { symbol: string } }) {
  const symbol = params.symbol.toUpperCase();
  return (
    <>
      <h1 className="text-2xl font-semibold">Market Sentiment for {symbol}</h1>
      <TickerClient symbol={symbol} />
    </>
  );
}
