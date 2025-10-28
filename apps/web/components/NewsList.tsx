"use client";

import * as React from "react";

type NewsItem = {
  ts?: string;         // ISO
  title?: string;
  url?: string;
  source?: string;
};

export default function NewsList({
  items,
  totalCount,
  periodLabel,
}: {
  items: NewsItem[];
  totalCount: number;
  periodLabel: string;
}) {
  const safeItems = Array.isArray(items) ? items : [];

  return (
    <div className="w-full rounded-2xl shadow-sm border p-4 bg-white">
      <div className="flex items-center justify-between mb-2">
        <div className="text-lg font-semibold">News Items</div>
        <div className="text-sm text-gray-500">
          <span className="font-medium">{totalCount}</span> (period: {periodLabel})
        </div>
      </div>

      {safeItems.length === 0 ? (
        <div className="text-sm text-gray-500">No news items to display.</div>
      ) : (
        <ul className="space-y-3">
          {safeItems.slice(0, 20).map((n, idx) => (
            <li key={idx} className="p-3 rounded-xl border hover:bg-gray-50 transition">
              <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
                {n.source ? (
                  <span className="inline-flex items-center px-2 py-0.5 rounded-full border">
                    {n.source}
                  </span>
                ) : null}
                {n.ts ? <span>{new Date(n.ts).toLocaleString()}</span> : null}
              </div>
              <div className="text-sm">
                {n.url ? (
                  <a href={n.url} target="_blank" rel="noreferrer" className="font-medium underline decoration-dotted">
                    {n.title || "(no title)"}
                  </a>
                ) : (
                  <span className="font-medium">{n.title || "(no title)"}</span>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
      {safeItems.length > 20 && (
        <div className="text-xs text-gray-500 mt-2">Showing 20 of {safeItems.length} recent items.</div>
      )}
    </div>
  );
}
