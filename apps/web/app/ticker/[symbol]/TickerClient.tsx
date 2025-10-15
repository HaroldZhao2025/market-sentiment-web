"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import LineChart from "../../../components/LineChart";
import { assetPath } from "../../../lib/paths";

function fmtPct(x?: number) { return typeof x === "number" ? `${(x*100).toFixed(2)}%` : "—"; }
function fmtNum(x?: number, d=2) { return typeof x === "number" ? x.toFixed(d) : "—"; }

type ViewKey = "total" | "news" | "earnings";

export default function TickerClient({ symbol }: { symbol: string }) {
  const s = symbol.toUpperCase();
  const [data, setData] = useState<any | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [view, setView] = useState<ViewKey>("total");

  useEffect(() => {
    fetch(assetPath(`data/${s}.json`))
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("No data"))))
      .then(setData)
      .catch((e) => setErr(e.message));
  }, [s]);

  const kpis = useMemo(() => {
    if (!data?.series) return { S7: 0, pred: 0, news7: 0 };
    const S7 = data?.meta?.S_total_7d ?? 0;
    const pred = data?.insights?.predicted_return ?? 0;
    const news7 = data?.meta?.news_7d ?? 0;
    return { S7, pred, news7 };
  }, [data]);

  if (err) return <div className="card">Error: {err}</div>;
  if (!data) return <div className="card">Loading…</div>;

  const series = data.series;
  const sentimentMap: Record<ViewKey, { raw: number[]; ma7: number[]; label: string }> = {
    total: { raw: series.sentiment_total,    ma7: series.sentiment_total_ma7,    label: "Sentiment (Total)" },
    news:  { raw: series.sentiment_news,     ma7: series.sentiment_news_ma7,     label: "Sentiment (News)" },
    earnings: { raw: series.sentiment_earnings, ma7: series.sentiment_earnings_ma7, label: "Sentiment (Earnings)" },
  };

  const sel = sentimentMap[view];

  return (
    <div className="space-y-6">
      <div className="card flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">Market Sentiment — {s}</h2>
          <div className="text-xs text-gray-500">Updated: {new Date(data?.meta?.last_updated ?? Date.now()).toLocaleString()}</div>
        </div>
        <div className="flex gap-2">
          <button className={`btn ${view==='total'?'bg-gray-100':''}`} onClick={() => setView("total")}>Total</button>
          <button className={`btn ${view==='news'?'bg-gray-100':''}`} onClick={() => setView("news")}>News</button>
          <button className={`btn ${view==='earnings'?'bg-gray-100':''}`} onClick={() => setView("earnings")}>Earnings</button>
          <Link href={`/earnings/${s}`} className="btn">Earnings Page</Link>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Price & {sentimentMap[view].label}</h3>
        <LineChart
          dates={series.date}
          price={series.price}
          sentiment={sel.raw}
          sentiment_ma7={sel.ma7}
          label={sentimentMap[view].label}
        />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <div className="kpi">{fmtNum(kpis.S7, 2)}</div>
          <div className="kpi-sub">7-day Avg Sentiment (Total)</div>
        </div>
        <div className="card">
          <div className="kpi">{fmtPct(kpis.pred)}</div>
          <div className="kpi-sub">Predicted Next-Day Return</div>
        </div>
        <div className="card">
          <div className="kpi">{kpis.news7}</div>
          <div className="kpi-sub">News Count (7d)</div>
        </div>
        <div className="card">
          <div className="kpi">{data.insights?.advisory ?? "—"}</div>
          <div className="kpi-sub">Advisory</div>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Recent Headlines</h3>
        <div className="divide-y">
          {data.recent_headlines?.map((h: any, i: number) => (
            <div key={i} className="py-2 flex items-start gap-3">
              <div className="w-48 text-xs text-gray-500">{new Date(h.ts).toLocaleString()}</div>
              <div className="flex-1">
                <a className="text-blue-600 underline" href={h.url} target="_blank" rel="noreferrer">{h.title}</a>
                <div className="text-xs text-gray-500">{h.source}</div>
              </div>
            </div>
          ))}
          {(!data.recent_headlines || data.recent_headlines.length === 0) && (
            <div className="text-sm text-gray-500">No recent headlines found in range.</div>
          )}
        </div>
      </div>
    </div>
  );
}
