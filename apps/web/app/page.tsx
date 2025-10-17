// apps/web/app/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";
import { assetPath } from "../lib/paths";

export const dynamic = "error"; // fully static
export const revalidate = false;

async function getTickers(): Promise<string[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "_tickers.json");
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export default async function HomePage() {
  const tickers = await getTickers();

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Market Sentiment — S&amp;P 500</h1>

      <div className="flex items-center gap-4 mb-6">
        <Link
          href={`${assetPath("/portfolio/")}`}
          className="px-3 py-2 rounded-xl bg-gray-100 hover:bg-gray-200 text-sm font-medium"
        >
          Portfolio
        </Link>
      </div>

      <h2 className="text-lg font-semibold mb-3">Browse tickers</h2>
      {tickers.length === 0 ? (
        <p className="text-sm text-gray-500">No data generated yet.</p>
      ) : (
        <ul className="flex flex-wrap gap-2">
          {tickers.map((t) => (
            <li key={t}>
              <Link
                href={`${assetPath(`/ticker/${t}/`)}`}
                className="inline-block px-2 py-1 rounded-md bg-gray-100 hover:bg-gray-200 text-sm font-medium"
              >
                {t}
              </Link>
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}
