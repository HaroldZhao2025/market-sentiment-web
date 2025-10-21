import fs from "node:fs/promises";
import path from "node:path";
import PortfolioClient from "./PortfolioClient";

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJSON<T>(p: string): Promise<T | null> {
  try { return JSON.parse(await fs.readFile(p, "utf8")) as T; } catch { return null; }
}

export default async function Page() {
  const pf = await readJSON<{ dates: string[]; S: number[]; price?: number[] }>(
    path.join(DATA_ROOT, "portfolio.json")
  );

  const dates = pf?.dates ?? [];
  const sentiment = pf?.S ?? [];
  const price = pf?.price ?? undefined;

  if (!dates.length || !sentiment.length) {
    return <div className="page"><h1 className="page-title">S&amp;P 500 Sentiment</h1><p className="muted">No portfolio data yet.</p></div>;
  }

  return (
    <div className="page">
      <PortfolioClient dates={dates} sentiment={sentiment} price={price} />
    </div>
  );
}
