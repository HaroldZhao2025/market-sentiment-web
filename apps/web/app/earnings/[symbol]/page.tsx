// apps/web/app/earnings/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type EarnDoc = { ts: string; title: string; url: string; S: number };

async function loadTickers(): Promise<string[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "_tickers.json");
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

async function loadEarnings(sym: string): Promise<EarnDoc[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "earnings", `${sym}.json`);
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export async function generateStaticParams() {
  const list = await loadTickers();
  return list.map((symbol) => ({ symbol }));
}

export default async function EarningsPage({ params }: { params: { symbol: string } }) {
  const { symbol } = params;
  const docs = await loadEarnings(symbol);

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Earnings docs: {symbol}</h1>
      {docs.length === 0 ? (
        <p className="text-sm text-gray-500">No earnings/transcripts detected.</p>
      ) : (
        <ul className="space-y-2">
          {docs.slice(0, 15).map((d, i) => (
            <li key={i} className="rounded-xl border p-3 bg-white">
              <div className="text-sm text-gray-500">{d.ts}</div>
              <div className="font-medium">
                <a href={d.url} target="_blank" rel="noreferrer" className="underline">
                  {d.title}
                </a>
              </div>
              <div className="text-sm">S (FinBERT): {Number(d.S).toFixed(3)}</div>
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}
