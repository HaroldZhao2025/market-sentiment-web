// apps/web/app/page.tsx
import Link from "next/link";
import { listTickers } from "../lib/loaders";

export default function HomePage() {
  const syms = listTickers();
  return (
    <main className="p-6 space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Market Sentiment — S&P 500</h1>
        <Link href="/portfolio" className="underline">
          Portfolio
        </Link>
      </header>
      {syms.length === 0 ? (
        <div className="text-sm text-gray-500">No data generated yet.</div>
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
          {syms.map((s) => (
            <Link
              key={s}
              className="border rounded px-2 py-1 text-sm hover:bg-gray-50"
              href={`/ticker/${encodeURIComponent(s)}`}
            >
              {s}
            </Link>
          ))}
        </div>
      )}
    </main>
  );
}
