// apps/web/app/ticker/[symbol]/page.tsx
import fs from "node:fs/promises";
import path from "node:path";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

type TickerSeries = {
  date: string[];
  price: number[];
  S: number[];
  S_news?: number[];
  S_earn?: number[];
};

async function readJson(filePath: string): Promise<any | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function toNumArray(x: any, n: number): number[] {
  if (Array.isArray(x)) {
    return x.map((v) => (v == null || Number.isNaN(Number(v)) ? 0 : Number(v)));
  }
  return Array.from({ length: n }, () => 0);
}

function normalizeTickerData(j: any): TickerSeries | null {
  if (!j || typeof j !== "object") return null;

  // Accept multiple historical shapes
  const dates =
    j.date || j.dates || j.DATE || j.DATES || (Array.isArray(j.Date) ? j.Date : undefined);
  if (!Array.isArray(dates) || dates.length === 0) return null;

  const n = dates.length;
  const priceRaw = j.price ?? j.close ?? j.PRICE ?? j.CLOSE ?? j.prices;
  const sRaw = j.S ?? j.s ?? j.SENTIMENT;
  const sNewsRaw = j.S_news ?? j.S_NEWS ?? j.newsS;
  const sEarnRaw = j.S_earn ?? j.S_EARN ?? j.earnS;

  let S = toNumArray(sRaw, n);
  if (S.every((v) => v === 0) && (Array.isArray(sNewsRaw) || Array.isArray(sEarnRaw))) {
    const ns = toNumArray(sNewsRaw, n);
    const es = toNumArray(sEarnRaw, n);
    S = Array.from({ length: n }, (_, i) => {
      const a = ns[i], b = es[i];
      if (typeof a === "number" && typeof b === "number") return (a + b) / 2;
      if (typeof a === "number") return a;
      if (typeof b === "number") return b;
      return 0;
    });
  }

  return {
    date: dates.map((d: any) => String(d)),
    price: toNumArray(priceRaw, n),
    S,
    S_news: Array.isArray(sNewsRaw) ? toNumArray(sNewsRaw, n) : undefined,
    S_earn: Array.isArray(sEarnRaw) ? toNumArray(sEarnRaw, n) : undefined,
  };
}

async function loadTickers(): Promise<string[]> {
  const p = path.join(process.cwd(), "public", "data", "_tickers.json");
  const raw = await readJson(p);
  return Array.isArray(raw) ? (raw as string[]) : [];
}

async function loadTickerJson(sym: string): Promise<TickerSeries | null> {
  const p = path.join(process.cwd(), "public", "data", "ticker", `${sym}.json`);
  const raw = await readJson(p);
  return normalizeTickerData(raw);
}

export async function generateStaticParams() {
  const list = await loadTickers();
  const base = path.join(process.cwd(), "public", "data", "ticker");

  const params: { symbol: string }[] = [];
  for (const symbol of list) {
    const p = path.join(base, `${symbol}.json`);
    const raw = await readJson(p);
    const norm = normalizeTickerData(raw);
    if (norm && norm.date.length > 0) params.push({ symbol });
  }
  return params;
}

export default async function TickerPage({ params }: { params: { symbol: string } }) {
  const { symbol } = params;
  const data = await loadTickerJson(symbol);

  if (!data) {
    return (
      <main className="mx-auto max-w-5xl p-6">
        <h1 className="text-2xl font-bold mb-4">Ticker: {symbol}</h1>
        <p className="text-sm text-gray-500">No data file found or empty series.</p>
      </main>
    );
  }

  const n = data.date.length;

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Ticker: {symbol}</h1>
      <div className="space-y-3">
        <p className="text-sm">Points: {n}</p>
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
                const idx = n - 5 + i;
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
    </main>
  );
}
