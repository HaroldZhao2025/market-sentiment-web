"use client";
import React from "react";

type NewsItem = {
  ts: string;
  title: string;
  url: string;
  source?: string;
  provider?: string;
};

function timeAgo(iso?: string) {
  try {
    if (!iso) return "";
    const now = new Date();
    const then = new Date(iso);
    const mins = Math.round((now.getTime() - then.getTime()) / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.round(hrs / 24);
    return `${days}d ago`;
  } catch {
    return "";
  }
}

export default function NewsList({ items }: { items: NewsItem[] }) {
  return (
    <div className="w-full rounded-2xl shadow p-4 bg-white">
      <h3 className="text-lg font-semibold mb-3">Latest News</h3>
      <ul className="space-y-3">
        {items.map((n, idx) => (
          <li key={idx} className="flex items-start gap-3">
            <div className="h-2 w-2 mt-2 rounded-full bg-gray-400" />
            <div className="flex-1">
              <a
                className="text-sm font-medium text-indigo-600 hover:underline"
                href={n.url || "#"}
                target="_blank"
                rel="noreferrer"
              >
                {n.title || "(untitled)"}
              </a>
              <div className="mt-1 text-xs text-gray-500 flex items-center gap-2">
                <span className="px-1.5 py-0.5 rounded bg-gray-100 text-gray-700">
                  {n.source || n.provider || "news"}
                </span>
                <span>{timeAgo(n.ts)}</span>
              </div>
            </div>
          </li>
        ))}
      </ul>
      <p className="mt-3 text-xs text-gray-500">Showing up to 20 most recent items across Finnhub + Yahoo Finance.</p>
    </div>
  );
}
