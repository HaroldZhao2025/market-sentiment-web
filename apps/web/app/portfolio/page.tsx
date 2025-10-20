// Server component: reads portfolio.json and (optionally) an index price JSON later.
import fs from "node:fs/promises";
import path from "node:path";
import PortfolioClient from "./PortfolioClient";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

const DATA_ROOT = path.join(process.cwd(), "public", "data");

type PortfolioJSON = {
  dates: string[];
  S: number[];
  price?: number[]; // optional if you later output index price
};

async function readJSON<T>(p: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(p, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export default async function Page() {
  const pf = (await readJSON<PortfolioJSON>(path.join(DATA_ROOT, "portfolio.json"))) || {
    dates: [],
    S: [],
  };

  // If you later write an index price series, attach it here; otherwise this stays blank.
  const dates = pf.dates ?? [];
  const sentiment = pf.S ?? [];
  const price = pf.price ?? [];

  if (dates.length === 0 || sentiment.length === 0) {
    return (
      <div className="min-h-screen p-6">
        <div className="max-w-5xl mx-auto">
          <h1 className="text-2xl font-semibold mb-4">S&amp;P 500 — Aggregate Sentiment</h1>
          <div className="text-neutral-500">No portfolio data yet.</div>
        </div>
      </div>
    );
  }

  return <PortfolioClient dates={dates} sentiment={sentiment} price={price} />;
}
