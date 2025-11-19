import type { Metadata } from "next";
import fs from "node:fs/promises";
import path from "node:path";

type Sp500HistoryRow = {
  date: string;
  close: number;
  sentiment_cap_weighted: number | null;
};

type Sp500IndexPayload = {
  symbol: string;
  name: string;
  as_of: string;
  history: Sp500HistoryRow[];
};

export const metadata: Metadata = {
  title: "S&P 500 Sentiment | Market Sentiment",
  description: "S&P 500 price and cap-weighted news sentiment index.",
};

async function readSp500Index(): Promise<Sp500IndexPayload | null> {
  try {
    // Next.js app is built from apps/web, so public/ is relative to process.cwd()
    const filePath = path.join(process.cwd(), "public", "data", "sp500_index.json");
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as Sp500IndexPayload;
  } catch (err) {
    console.error("[portfolio] Failed to read sp500_index.json:", err);
    return null;
  }
}

function formatPct(x: number | null | undefined): string {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return (x * 100).toFixed(2) + " %";
}

function formatNumber(x: number | null | undefined): string {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return x.toFixed(2);
}

export default async function PortfolioPage() {
  const spx = await readSp500Index();

  if (!spx || !spx.history || spx.history.length === 0) {
    return (
      <main className="mx-auto max-w-6xl px-4 py-8 space-y-6">
        <header className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">
            S&amp;P 500 Sentiment
          </h1>
          <p className="text-sm text-muted-foreground">
            No S&amp;P 500 index data yet. Make sure <code>sp500_index.json</code> exists
            under <code>apps/web/public/data/</code> and the build step ran successfully.
          </p>
        </header>
      </main>
    );
  }

  const historySorted = [...spx.history].sort((a, b) =>
    a.date.localeCompare(b.date),
  );
  const latest = historySorted[historySorted.length - 1];

  const lastClose = latest?.close ?? null;
  const lastSentiment = latest?.sentiment_cap_weighted ?? null;

  // Take last ~30 days for the mini table
  const recent = historySorted.slice(-30).reverse();

  return (
    <main className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">
          S&amp;P 500 Sentiment
        </h1>
        <p className="text-sm text-muted-foreground">
          {spx.name} — cap-weighted news sentiment aggregated across the
          current S&amp;P 500 universe. Updated through {spx.as_of}.
        </p>
      </header>

      {/* Summary cards, similar to a single-ticker header */}
      <section className="grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border bg-card p-4 shadow-sm">
          <div className="text-xs font-medium text-muted-foreground">
            Last Close
          </div>
          <div className="mt-1 text-2xl font-semibold">
            {formatNumber(lastClose)}
          </div>
          <div className="mt-2 text-xs text-muted-foreground">
            Index level of S&amp;P 500 price index (^GSPC).
          </div>
        </div>

        <div className="rounded-xl border bg-card p-4 shadow-sm">
          <div className="text-xs font-medium text-muted-foreground">
            Cap-weighted Sentiment
          </div>
          <div className="mt-1 text-2xl font-semibold">
            {formatPct(lastSentiment)}
          </div>
          <div className="mt-2 text-xs text-muted-foreground">
            News sentiment averaged across constituents, weighted by market cap.
          </div>
        </div>

        <div className="rounded-xl border bg-card p-4 shadow-sm">
          <div className="text-xs font-medium text-muted-foreground">
            History Length
          </div>
          <div className="mt-1 text-2xl font-semibold">
            {historySorted.length}{" "}
            <span className="text-base font-normal text-muted-foreground">
              days
            </span>
          </div>
          <div className="mt-2 text-xs text-muted-foreground">
            Daily close and sentiment observations in the current sample.
          </div>
        </div>
      </section>

      {/* Mini “price & sentiment” history table */}
      <section className="space-y-3">
        <h2 className="text-lg font-semibold tracking-tight">
          Recent Price &amp; Sentiment
        </h2>
        <p className="text-xs text-muted-foreground">
          Last 30 trading days. Sentiment values are the capitalisation-weighted
          mean of per-ticker FinBERT scores. (No news section is shown for the
          index itself.)
        </p>

        <div className="overflow-x-auto rounded-xl border bg-card">
          <table className="min-w-full text-sm">
            <thead className="border-b bg-muted/50">
              <tr>
                <th className="px-3 py-2 text-left font-medium">Date</th>
                <th className="px-3 py-2 text-right font-medium">Close</th>
                <th className="px-3 py-2 text-right font-medium">
                  Sentiment (cap-wtd)
                </th>
              </tr>
            </thead>
            <tbody>
              {recent.map((row) => (
                <tr key={row.date} className="border-b last:border-0">
                  <td className="px-3 py-1.5">{row.date}</td>
                  <td className="px-3 py-1.5 text-right">
                    {formatNumber(row.close)}
                  </td>
                  <td className="px-3 py-1.5 text-right">
                    {formatPct(row.sentiment_cap_weighted)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* NOTE: intentionally no News section here */}
    </main>
  );
}
