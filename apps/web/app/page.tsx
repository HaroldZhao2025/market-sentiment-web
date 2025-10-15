import fs from "node:fs";
import path from "node:path";
import Link from "next/link";
import { dataPath } from "../lib/paths";

type Tickers = string[];

export const revalidate = false;

function loadTickers(): Tickers {
  try {
    const p = path.join(process.cwd(), "public", "data", "_tickers.json");
    const s = fs.readFileSync(p, "utf8");
    return JSON.parse(s);
  } catch {
    return [];
  }
}

export default function Home() {
  const tickers = loadTickers();
  return (
    <main className="p-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold mb-4">Market Sentiment — S&amp;P 500</h1>
      <div className="flex gap-4 mb-6">
        <Link className="underline" href="/portfolio">Portfolio</Link>
      </div>
      {tickers.length === 0 ? (
        <p>No data generated yet.</p>
      ) : (
        <>
          <div className="mb-2 text-sm text-gray-600">Tickers ({tickers.length})</div>
          <ul className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 gap-2">
            {tickers.map(sym => (
              <li key={sym}>
                <Link className="underline" href={`/ticker/${encodeURIComponent(sym)}`}>{sym}</Link>
              </li>
            ))}
          </ul>
        </>
      )}
    </main>
  );
}
