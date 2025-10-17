// apps/web/app/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";
import { assetPath } from "../lib/paths";

export const dynamic = "error"; // fully static
export const revalidate = false;

async function readJson(filePath: string): Promise<any | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function hasNonEmptySeries(j: any): boolean {
  if (!j || typeof j !== "object") return false;
  const dates = j.date || j.dates || j.DATE || j.DATES;
  return Array.isArray(dates) && dates.length > 0;
}

async function getRenderableTickers(): Promise<string[]> {
  const tickersPath = path.join(process.cwd(), "public", "data", "_tickers.json");
  const raw = await readJson(tickersPath);
  const all = Array.isArray(raw) ? (raw as string[]) : [];
  if (all.length === 0) return [];

  // Only show tickers that actually have a non-empty file
  const base = path.join(process.cwd(), "public", "data", "ticker");
  const ok: string[] = [];
  for (const t of all) {
    const p = path.join(base, `${t}.json`);
    const j = await readJson(p);
    if (hasNonEmptySeries(j)) ok.push(t);
  }
  return ok;
}

export default async function HomePage() {
  const tickers = await getRenderableTickers();

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Market Sentiment — S&amp;P 500</h1>

      <div className="flex items-center gap-4 mb-6">
        <Link
          href={assetPath("/portfolio/")}
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
                href={assetPath(`/ticker/${t}/`)}
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
