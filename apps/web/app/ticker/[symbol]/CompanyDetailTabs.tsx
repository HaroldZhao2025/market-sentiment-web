"use client";

import { useEffect, useState } from "react";
import EarningsIntelligenceClientV2, { type EarningsArtifact } from "../../earnings/[symbol]/EarningsIntelligenceClientV2";
import CompanyNewsArchive from "./CompanyNewsArchive";
import TickerClientV2, { type NewsItem, type SeriesIn } from "./TickerClientV2";

type Tab = "news" | "earnings";
type Props = {
  symbol: string;
  series: SeriesIn | null;
  news: NewsItem[];
  newsTotal: number;
  earnings: EarningsArtifact;
  initialTab?: Tab;
};

export default function CompanyDetailTabs({ symbol, series, news, newsTotal, earnings, initialTab = "news" }: Props) {
  const [tab, setTab] = useState<Tab>(initialTab);
  const calls = Array.isArray(earnings.calls) ? earnings.calls.length : 0;
  const links = Array.isArray(earnings.call_links) ? earnings.call_links.length : 0;

  useEffect(() => {
    const syncFromHash = () => setTab(window.location.hash === "#earnings" ? "earnings" : "news");
    syncFromHash();
    window.addEventListener("hashchange", syncFromHash);
    return () => window.removeEventListener("hashchange", syncFromHash);
  }, []);

  const choose = (next: Tab) => {
    setTab(next);
    const url = `${window.location.pathname}${window.location.search}#${next}`;
    window.history.replaceState(null, "", url);
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 pb-3">
        <div className="flex rounded-xl border border-white/10 bg-black/30 p-1" role="tablist" aria-label={`${symbol} company workspace`}>
          <button type="button" role="tab" aria-selected={tab === "news"} onClick={() => choose("news")} className={`rounded-lg px-4 py-2 text-sm transition ${tab === "news" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>
            Daily News
            {newsTotal > 0 ? <span className="ml-2 rounded-full bg-white/[0.06] px-2 py-0.5 text-[10px] text-neutral-400">{newsTotal}</span> : null}
          </button>
          <button type="button" role="tab" aria-selected={tab === "earnings"} onClick={() => choose("earnings")} className={`rounded-lg px-4 py-2 text-sm transition ${tab === "earnings" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>
            Earnings Call
            {(calls > 0 || links > 0) ? <span className="ml-2 rounded-full bg-emerald-400/10 px-2 py-0.5 text-[10px] text-emerald-300">{calls || links}</span> : null}
          </button>
        </div>
        <div className="text-xs text-neutral-600">News, history and calls in one workspace</div>
      </div>

      {tab === "news" ? (
        <div className="space-y-8">
          {series ? <TickerClientV2 symbol={symbol} series={series} news={news} newsTotal={newsTotal} /> : <div className="card p-5 text-sm text-neutral-500">Price history is still being fulfilled for this company.</div>}
          <CompanyNewsArchive news={news} />
        </div>
      ) : <EarningsIntelligenceClientV2 symbol={symbol} data={earnings} />}
    </div>
  );
}
