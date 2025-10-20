// SSG page for aggregate /portfolio with graceful fallback
import fs from "node:fs/promises";
import path from "node:path";
import PortfolioClient from "./PortfolioClient";

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T>(p: string): Promise<T | null> {
  try {
    return JSON.parse(await fs.readFile(p, "utf8")) as T;
  } catch {
    return null;
  }
}

export default async function Page() {
  const pf = await readJSON<{ dates: string[]; S: number[]; price?: number[] }>(
    path.join(DATA_ROOT, "portfolio.json")
  );

  const dates = pf?.dates ?? [];
  const sentiment = pf?.S ?? [];
  const price = pf?.price ?? undefined;

  if (!dates.length || !sentiment.length) {
    return (
      <div className="max-w-5xl mx-auto p-6 text-neutral-500">
        No portfolio data yet.
      </div>
    );
  }

  return <PortfolioClient dates={dates} sentiment={sentiment} price={price} />;
}
