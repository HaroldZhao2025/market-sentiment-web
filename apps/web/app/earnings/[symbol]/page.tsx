// apps/web/app/earnings/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type EarnDoc = { ts?: string; title?: string; url?: string; S?: number };

async function readJson(filePath: string): Promise<any | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function loadTickers(): Promise<string[]> {
  const p = path.join(process.cwd(), "public", "data", "_tickers.json");
  const raw = await readJson(p);
  return Array.isArray(raw) ? (raw as string[]) : [];
}

async function loadEarnings(sym: string): Promise<EarnDoc[]> {
  const p = path.join(process.cwd(), "public", "data", "earnings", `${sym}.json`);
  const raw = await readJson(p);
  if (Array.isArray(raw)) return raw as EarnDoc[];
  if (raw && Array.isArray(raw.docs)) return raw.docs as EarnDoc[];
  return [];
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
          {docs.slice(0, 20).map((d, i) => (
            <li key={i} className="rounded-xl border p-3 bg-white">
              <div className="text-sm text-gray-500">{d.ts ?? ""}</div>
              <div className="font-medium">
                {d.url ? (
                  <a href={d.url} target="_blank" rel="noreferrer" className="underline">
                    {d.title ?? d.url}
                  </a>
                ) : (
                  <span>{d.title ?? "Untitled doc"}</span>
                )}
              </div>
              {"S" in d && typeof d.S === "number" && (
                <div className="text-sm">S (FinBERT): {Number(d.S).toFixed(3)}</div>
              )}
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}
