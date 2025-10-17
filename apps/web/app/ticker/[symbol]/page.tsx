// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type TickerSeries = {
  date: string[];        // yyyy-mm-dd
  price: number[];
  S: number[];           // combined daily sentiment
  S_news?: number[];
  S_earn?: number[];
};

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

async function loadTickerJson(sym: string): Promise<TickerSeries | null> {
  try {
    const p = path.join(process.cwd(), "public", "data", "ticker", `${sym}.json`);
    const raw = await fs.readFile(p, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function generateStaticParams() {
  const list = await loadTickers();
  return list.map((symbol) => ({ symbol }));
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const { symbol } = params;
  const data = await loadTickerJson(symbol);

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Ticker: {symbol}</h1>
      {!data ? (
        <p className="text-sm text-gray-500">No data file found for {symbol}.</p>
      ) : data.date.length === 0 ? (
        <p className="text-sm text-gray-500">No time series for {symbol}.</p>
      ) : (
        <div className="space-y-3">
          <p className="text-sm">Points: {data.date.length}</p>
          <div className="rounded-xl border p-4 bg-white">
            <h3 className="font-semibold mb-2">Last 5 rows</h3>
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-gray-500">
                  <th className="py-1 pr-4">Date</th>
                  <th className="py-1 pr-4">Price</th>
                  <th className="py-1 pr-4">S</th>
                </tr>
              </thead>
              <tbody>
                {data.date.slice(-5).map((d, i) => {
                  const idx = data.date.length - 5 + i;
                  return (
                    <tr key={d}>
                      <td className="py-1 pr-4">{d}</td>
                      <td className="py-1 pr-4">{data.price[idx]?.toFixed(2)}</td>
                      <td className="py-1 pr-4">{data.S[idx]?.toFixed(3)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </main>
  );
}
