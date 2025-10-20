// apps/web/app/portfolio/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import PortfolioClient from "./PortfolioClient";

type PortfolioJson = { dates?: string[]; S?: number[]; sentiment?: number[] };

const DATA_ROOT = path.join(process.cwd(), "public", "data");

async function readJson<T = any>(p: string): Promise<T | null> {
  try {
    return JSON.parse(await fs.readFile(p, "utf8")) as T;
  } catch {
    return null;
  }
}

export default async function Page() {
  // Aggregate sentiment (required)
  const pf = (await readJson<PortfolioJson>(path.join(DATA_ROOT, "portfolio.json"))) || {};
  const dates = pf.dates ?? [];
  const sentiment = (pf.S ?? pf.sentiment ?? []).map((x) => Number(x) || 0);

  // Optional index price (SPY or ^GSPC if available)
  let price: number[] | undefined;
  const tryFiles = [path.join(DATA_ROOT, "ticker", "SPY.json"), path.join(DATA_ROOT, "ticker", "^GSPC.json")];
  for (const f of tryFiles) {
    const obj = await readJson<any>(f);
    if (obj && (obj.price || obj.close)) {
      price = (obj.price ?? obj.close ?? []).map((x: any) => Number(x) || 0);
      break;
    }
  }

  return <PortfolioClient dates={dates} sentiment={sentiment} price={price} />;
}
