"use client";

import { useState } from "react";
import EarningsIntelligenceClient, { type EarningsArtifact } from "../../earnings/[symbol]/EarningsIntelligenceClient";
import TickerClient, { type NewsItem, type SeriesIn } from "./TickerClient";

type Props = {
  symbol: string;
  series: SeriesIn | null;
  news: NewsItem[];
  newsTotal: number;
  earnings: EarningsArtifact;
  initialTab?: "news" | "earnings";
};

export default function CompanyDetailTabs({ symbol, series, news, newsTotal, earnings, initialTab = "news" }: Props) {
  const [tab, setTab] = useState<"news" | "earnings">(initialTab);
  const calls = Array.isArray(earnings.calls) ? earnings.calls.length : 0;
  const links = Array.isArray((earnings as EarningsArtifact & { call_links?: unknown[] }).call_links)
    ? ((earnings as EarningsArtifact & { call_links?: unknown[] }).call_links?.length ?? 0)
    : 0;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 pb-3">
        <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
          <button
            type="button"
            onClick={() => setTab("news")}
            className={`rounded-lg px-4 py-2 text-sm transition ${tab === "news" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}
          >
            Daily News
          </button>
          <button
            type="button"
            onClick={() => setTab("earnings")}
            className={`rounded-lg px-4 py-2 text-sm transition ${tab === "earnings" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}
          >
            Earnings Call
            {(calls > 0 || links > 0) ? <span className="ml-2 rounded-full bg-emerald-400/10 px-2 py-0.5 text-[10px] text-emerald-300">{calls || links}</span> : null}
          </button>
        </div>
        <div className="text-xs text-neutral-600">One company workspace · switch without leaving the page</div>
      </div>

      {tab === "news" ? (
        series ? <TickerClient symbol={symbol} series={series} news={news} newsTotal={newsTotal} /> : (
          <div className="card p-5 text-sm text-neutral-500">Price history is still building. Recent company news remains available from the Companies surface.</div>
        )
      ) : (
        <EarningsIntelligenceClient symbol={symbol} data={earnings} />
      )}
    </div>
  );
}
