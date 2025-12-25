// apps/web/app/research/ResearchIndexClient.tsx
"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import type { ResearchIndexItem } from "../../lib/research";

type Section = {
  id: string;
  title: string;
  description?: string;
  conclusions?: string[];
  slugs: string[];
};

type Meta = {
  updated_at?: string;
  n_studies?: number;
  n_tickers?: number;
  n_obs_panel?: number;
  date_range?: [string, string];
};

const Badge = ({ children }: { children: React.ReactNode }) => (
  <span className="text-xs px-2 py-1 rounded-full border border-zinc-200 bg-white">{children}</span>
);

function normalize(s: string) {
  return s.toLowerCase().trim();
}

export default function ResearchIndexClient({
  items,
  sections,
  meta,
}: {
  items: ResearchIndexItem[];
  sections: Section[];
  meta?: Meta | null;
}) {
  const [q, setQ] = useState("");
  const [sort, setSort] = useState<"updated" | "title" | "category">("updated");
  const [tag, setTag] = useState<string>("");

  const allTags = useMemo(() => {
    const s = new Set<string>();
    for (const it of items) for (const t of it.tags ?? []) s.add(t);
    return Array.from(s).sort((a, b) => a.localeCompare(b));
  }, [items]);

  const bySlug = useMemo(() => new Map(items.map((x) => [x.slug, x])), [items]);

  const filtered = useMemo(() => {
    const qq = normalize(q);
    return items.filter((it) => {
      if (tag && !(it.tags ?? []).includes(tag)) return false;
      if (!qq) return true;
      const hay = normalize(
        `${it.title} ${it.summary} ${(it.tags ?? []).join(" ")} ${it.category ?? ""} ${it.highlight ?? ""}`
      );
      return hay.includes(qq);
    });
  }, [items, q, tag]);

  const sorted = useMemo(() => {
    const arr = [...filtered];
    if (sort === "updated") arr.sort((a, b) => (b.updated_at ?? "").localeCompare(a.updated_at ?? ""));
    if (sort === "title") arr.sort((a, b) => (a.title ?? "").localeCompare(b.title ?? ""));
    if (sort === "category") arr.sort((a, b) => (a.category ?? "").localeCompare(b.category ?? ""));
    return arr;
  }, [filtered, sort]);

  // If user is searching/filtering, show a flat list (better UX)
  const searching = q.trim().length > 0 || !!tag;

  const Card = ({ it }: { it: ResearchIndexItem }) => (
    <Link
      href={`/research/${it.slug}`}
      className="rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm transition"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1">
          <div className="text-lg font-semibold leading-snug">{it.title}</div>
          <div className="text-sm text-zinc-600">{it.summary}</div>
          {it.highlight ? (
            <div className="text-xs text-zinc-500 mt-2 line-clamp-2">
              <span className="font-semibold text-zinc-600">Key finding:</span> {it.highlight}
            </div>
          ) : null}
        </div>
        <div className="text-right space-y-2 shrink-0">
          <Badge>{(it.status ?? "draft").toUpperCase()}</Badge>
          <div className="text-xs text-zinc-500">{it.updated_at}</div>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        {(it.tags ?? []).slice(0, 7).map((t) => (
          <span key={t} className="text-xs text-zinc-600 px-2 py-1 rounded-full bg-zinc-100">
            {t}
          </span>
        ))}
      </div>

      {it.key_stats?.length ? (
        <div className="mt-4 grid grid-cols-2 gap-2">
          {it.key_stats.slice(0, 4).map((s) => (
            <div key={s.label} className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">{s.label}</div>
              <div className="text-sm font-semibold">{s.value}</div>
            </div>
          ))}
        </div>
      ) : null}
    </Link>
  );

  return (
    <div className="space-y-6">
      {/* meta */}
      {meta ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">Studies</div>
              <div className="text-sm font-semibold">{meta.n_studies ?? items.length}</div>
            </div>
            <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">Tickers</div>
              <div className="text-sm font-semibold">{meta.n_tickers ?? "—"}</div>
            </div>
            <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">Panel obs</div>
              <div className="text-sm font-semibold">{meta.n_obs_panel ?? "—"}</div>
            </div>
            <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">Date range</div>
              <div className="text-sm font-semibold">
                {meta.date_range ? `${meta.date_range[0]} → ${meta.date_range[1]}` : "—"}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      {/* controls */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search titles, tags, key findings…"
            className="w-full rounded-xl border border-zinc-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-zinc-200"
          />

          <select
            value={sort}
            onChange={(e) => setSort(e.target.value as any)}
            className="w-full rounded-xl border border-zinc-200 px-3 py-2 text-sm bg-white"
          >
            <option value="updated">Sort: Updated</option>
            <option value="title">Sort: Title</option>
            <option value="category">Sort: Category</option>
          </select>

          <select
            value={tag}
            onChange={(e) => setTag(e.target.value)}
            className="w-full rounded-xl border border-zinc-200 px-3 py-2 text-sm bg-white"
          >
            <option value="">Filter tag: (all)</option>
            {allTags.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>

        <div className="mt-3 text-xs text-zinc-500">
          Showing <span className="font-semibold text-zinc-700">{sorted.length}</span> of{" "}
          <span className="font-semibold text-zinc-700">{items.length}</span> studies.
        </div>
      </section>

      {/* content */}
      {searching ? (
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {sorted.map((it) => (
            <Card key={it.slug} it={it} />
          ))}
        </section>
      ) : (
        <div className="space-y-10">
          {sections.map((sec) => {
            const secItems = sec.slugs.map((s) => bySlug.get(s)).filter(Boolean) as ResearchIndexItem[];
            if (!secItems.length) return null;

            return (
              <section key={sec.id} className="space-y-4">
                <div className="space-y-2">
                  <div className="flex items-baseline justify-between gap-3">
                    <h2 className="text-xl font-semibold">{sec.title}</h2>
                    <div className="text-xs text-zinc-500">{secItems.length} studies</div>
                  </div>
                  {sec.description ? <div className="text-sm text-zinc-600">{sec.description}</div> : null}
                  {sec.conclusions?.length ? (
                    <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                      {sec.conclusions.slice(0, 3).map((c, i) => (
                        <li key={i}>{c}</li>
                      ))}
                    </ul>
                  ) : null}
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {secItems.map((it) => (
                    <Card key={it.slug} it={it} />
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </div>
  );
}
