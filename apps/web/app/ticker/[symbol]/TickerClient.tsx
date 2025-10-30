"use client";

import React from "react";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;
  source?: string;
  provider?: string;
};

type Props = {
  symbol: string;
  series?: SeriesIn | null;
  news?: NewsItem[];
  newsTotal?: number;
};

function basePrefix() {
  // For GH Pages with `basePath`, we must prefix manual fetches.
  // This value is inlined at build time by Next.
  const p = process.env.NEXT_PUBLIC_BASE_PATH || "";
  // ensure no trailing slash
  return p.endsWith("/") ? p.slice(0, -1) : p;
}

async function fetchTicker(symbol: string): Promise<{
  date: string[];
  price?: number[];
  S?: number[];
  sentiment?: number[];
  news?: NewsItem[];
  news_count?: { finnhub?: number; yfinance?: number; total?: number };
}> {
  const url = `${basePrefix()}/data/ticker/${encodeURIComponent(symbol)}.json`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return (await res.json()) as any;
}

export default function TickerClient({ symbol, series, news, newsTotal }: Props) {
  const [ser, setSer] = React.useState<SeriesIn | null>(series ?? null);
  const [items, setItems] = React.useState<NewsItem[]>(news ?? []);
  const [total, setTotal] = React.useState<number | undefined>(newsTotal);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;

    // If the server already provided everything, do nothing.
    if (ser && (items?.length ?? 0) > 0 && typeof total !== "undefined") return;

    fetchTicker(symbol)
      .then((obj) => {
        if (cancelled) return;
        const date = Array.isArray(obj?.date) ? obj.date.map(String) : [];
        const priceArr =
          Array.isArray(obj?.price) ? obj.price.map((x: any) => Number(x) || 0) : [];
        const S =
          Array.isArray(obj?.S) ? obj.S.map((x: any) => Number(x) || 0) : Array.isArray(obj?.sentiment)
            ? obj.sentiment.map((x: any) => Number(x) || 0)
            : [];
        const n = Math.min(date.length, priceArr.length || Infinity, S.length || Infinity);
        const seriesFetched: SeriesIn | null =
          n > 0 ? { date: date.slice(0, n), price: priceArr.slice(0, n), sentiment: S.slice(0, n) } : null;

        const newsFetched: NewsItem[] = Array.isArray(obj?.news)
          ? obj.news
              .map((r: any) => ({
                ts: String(r?.ts ?? r?.date ?? ""),
                title: String(r?.title ?? ""),
                url: String(r?.url ?? ""),
                text: r?.text ? String(r.text) : undefined,
                source: r?.source ? String(r.source) : undefined,
                provider: r?.provider ? String(r.provider) : undefined,
              }))
              .filter((r: NewsItem) => r.ts && r.title)
          : [];

        const tot =
          obj?.news_count?.total ??
          (typeof obj?.news_total === "number" ? obj.news_total : newsFetched.length);

        setSer((prev) => prev ?? seriesFetched);
        setItems((prev) => (prev && prev.length ? prev : newsFetched));
        setTotal((prev) => (typeof prev === "number" ? prev : tot));
      })
      .catch((e) => {
        if (cancelled) return;
        setErr(String(e));
      });

    return () => {
      cancelled = true;
    };
  }, [symbol, ser, items, total]);

  if (!ser) {
    const prefix = basePrefix();
    return (
      <div className="space-y-2">
        <div className="text-red-600 font-medium">No data yet for {symbol}.</div>
        <div className="text-sm text-neutral-500">
          If this is the first deploy, the JSON may not be ready. The client will try fetching from{" "}
          <code>{`${prefix}/data/ticker/${symbol}.json`}</code>.
        </div>
        {err && <div className="text-xs text-red-500">Fetch error: {err}</div>}
      </div>
    );
  }

  // --- very lightweight UI (you can keep your existing chart component here) ---
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold">{symbol}</h2>
        <div className="text-sm text-neutral-500">
          Points: {ser.date.length} &middot; First: {ser.date[0]} &middot; Last:{" "}
          {ser.date[ser.date.length - 1]}
        </div>
      </div>

      <div className="rounded-2xl border p-4">
        <div className="text-sm font-medium mb-2">Sentiment (first 10)</div>
        <pre className="text-xs overflow-x-auto">
{JSON.stringify(ser.sentiment.slice(0, 10), null, 2)}
        </pre>
      </div>

      <div className="rounded-2xl border p-4">
        <div className="text-sm font-medium mb-2">
          News items (showing up to 10) • total{" "}
          {typeof total === "number" ? total : items.length}
        </div>
        {items.length === 0 ? (
          <div className="text-sm text-neutral-500">No news found in window.</div>
        ) : (
          <ul className="space-y-2">
            {items.slice(0, 10).map((n, i) => (
              <li key={`${n.ts}-${i}`} className="text-sm">
                <span className="text-neutral-500">{n.ts.slice(0, 16).replace("T", " ")}</span>{" "}
                —{" "}
                <a className="text-blue-600 hover:underline" href={n.url} target="_blank">
                  {n.title}
                </a>
                {n.source ? <span className="text-neutral-400"> · {n.source}</span> : null}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
