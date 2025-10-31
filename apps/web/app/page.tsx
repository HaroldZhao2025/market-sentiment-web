// apps/web/app/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

async function getTickers(): Promise<string[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "_tickers.json");
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map((x: any) => String(x).toUpperCase()) : ["AAPL"];
  } catch {
    return ["AAPL"];
  }
}

export default async function Home() {
  const tickers = await getTickers();

  return (
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-6xl space-y-6">
        <header className="flex items-center justify-between">
          <h1 className="text-3xl font-bold tracking-tight">Market Sentiment</h1>
          {/* Route-only home link; Next will prepend basePath at build time */}
          <Link
            href="/"
            className="rounded-xl border px-3 py-1.5 text-sm hover:bg-neutral-50"
          >
            Home
          </Link>
        </header>

        <section>
          <h2 className="sr-only">Tickers</h2>
          <ul className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-3">
            {tickers.map((t) => (
              <li key={t}>
                {/* IMPORTANT: route-only href, no manual NEXT_PUBLIC_BASE_PATH */}
                <Link
                  href={`/ticker/${t}`}
                  className="block rounded-xl border shadow-sm px-4 py-3 hover:shadow transition"
                >
                  {t}
                </Link>
              </li>
            ))}
          </ul>
        </section>
      </div>
    </main>
  );
}
