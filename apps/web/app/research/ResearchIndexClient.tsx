// apps/web/app/research/ResearchIndexClient.tsx
"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

type IndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string;
  status?: string;
  tags?: string[];
  key_stats?: { label: string; value: string }[];
  highlight?: string;
  category?: string;
};

type Section = {
  id: string;
  title: string;
  description?: string;
  conclusions?: string[];
  slugs: string[];
};

type Props = {
  items: IndexItem[];
  sections: Section[];
};

const Badge = ({ children }: { children: React.ReactNode }) => (
  <span className="text-[11px] px-2 py-1 rounded-full border border-zinc-200 bg-white">
    {children}
  </span>
);

const Chip = ({
  active,
  children,
  onClick,
}: {
  active?: boolean;
  children: React.ReactNode;
  onClick?: () => void;
}) => (
  <button
    type="button"
    onClick={onClick}
    className={[
      "text-[11px] px-2.5 py-1 rounded-full border transition",
      active
        ? "border-zinc-900 bg-zinc-900 text-white"
        : "border-zinc-200 bg-white text-zinc-700 hover:border-zinc-300",
    ].join(" ")}
  >
    {children}
  </button>
);

function norm(s: string) {
  return (s || "").toLowerCase().trim();
}

function uniq<T>(arr: T[]) {
  return Array.from(new Set(arr));
}

function getTopTags(items: IndexItem[], k = 16) {
  const cnt = new Map<string, number>();
  for (const it of items) {
    for (const t of it.tags ?? []) {
      const tt = t.trim();
      if (!tt) continue;
      cnt.set(tt, (cnt.get(tt) ?? 0) + 1);
    }
  }
  const sorted = Array.from(cnt.entries()).sort((a, b) => b[1] - a[1]);
  return sorted.slice(0, k).map(([t]) => t);
}

function parseDate(s: string) {
  const d = new Date(s);
  return Number.isFinite(d.getTime()) ? d.getTime() : 0;
}

export default function ResearchIndexClient({ items, sections }: Props) {
  const [q, setQ] = useState("");
  const [activeCat, setActiveCat] = useState<string>("All");
  const [activeTag, setActiveTag] = useState<string>("All");
  const [sort, setSort] = useState<"recent" | "alpha">("recent");

  const categories = useMemo(() => {
    const cats = uniq(items.map((x) => x.category?.trim() || "Other")).sort((a, b) => a.localeCompare(b));
    return ["All", ...cats];
  }, [items]);

  const topTags = useMemo(() => ["All", ...getTopTags(items, 18)], [items]);

  const bySlug = useMemo(() => new Map(items.map((x) => [x.slug, x])), [items]);

  const filtered = useMemo(() => {
    const qq = norm(q);
    const catOk = (it: IndexItem) => activeCat === "All" || (it.category?.trim() || "Other") === activeCat;
    const tagOk = (it: IndexItem) =>
      activeTag === "All" || (it.tags ?? []).some((t) => t.trim() === activeTag);

    const qOk = (it: IndexItem) => {
      if (!qq) return true;
      const hay = [
        it.slug,
        it.title,
        it.summary,
        it.category ?? "",
        ...(it.tags ?? []),
        it.highlight ?? "",
      ]
        .map(norm)
        .join(" | ");
      return hay.includes(qq);
    };

    const out = items.filter((it) => catOk(it) && tagOk(it) && qOk(it));

    out.sort((a, b) => {
      if (sort === "alpha") return a.title.localeCompare(b.title);
      return parseDate(b.updated_at) - parseDate(a.updated_at);
    });

    return out;
  }, [items, q, activeCat, activeTag, sort]);

  const filteredSet = useMemo(() => new Set(filtered.map((x) => x.slug)), [filtered]);

  const visibleSections = useMemo(() => {
    // preserve your overview sections ordering, but hide empty sections after filtering
    const out: Section[] = [];
    for (const sec of sections) {
      const slugs = sec.slugs.filter((s) => filteredSet.has(s));
      if (slugs.length) out.push({ ...sec, slugs });
    }
    // if overview.json is missing or empty, fallback to one “All results” section
    if (!out.length && filtered.length) {
      out.push({
        id: "all-results",
        title: "Results",
        description: "Filtered results",
        conclusions: [],
        slugs: filtered.map((x) => x.slug),
      });
    }
    return out;
  }, [sections, filteredSet, filtered]);

  return (
    <div className="space-y-8">
      {/* Controls */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-4 md:p-5 space-y-4">
        <div className="flex flex-col md:flex-row md:items-end gap-3 md:gap-4">
          <div className="flex-1">
            <div className="text-sm font-semibold text-zinc-900">Explore studies</div>
            <div className="text-xs text-zinc-500 mt-1">
              Search titles / specs / tags. Filter categories like an actual research index.
            </div>
          </div>

          <div className="flex items-center gap-2">
            <label className="text-xs text-zinc-500">Sort</label>
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value as any)}
              className="text-sm border border-zinc-200 rounded-xl px-3 py-2 bg-white"
            >
              <option value="recent">Most recent</option>
              <option value="alpha">Alphabetical</option>
            </select>
          </div>
        </div>

        <div className="flex flex-col md:flex-row gap-3">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search (e.g., Fama–MacBeth, event study, placebo, distributed lag)…"
            className="w-full md:flex-1 border border-zinc-200 rounded-xl px-4 py-2 text-sm"
          />
          <div className="text-xs text-zinc-500 self-center">
            {filtered.length} / {items.length} studies
          </div>
        </div>

        <div className="space-y-2">
          <div className="text-xs text-zinc-500">Category</div>
          <div className="flex flex-wrap gap-2">
            {categories.map((c) => (
              <Chip key={c} active={c === activeCat} onClick={() => setActiveCat(c)}>
                {c}
              </Chip>
            ))}
          </div>
        </div>

        <div className="space-y-2">
          <div className="text-xs text-zinc-500">Top tags</div>
          <div className="flex flex-wrap gap-2">
            {topTags.map((t) => (
              <Chip key={t} active={t === activeTag} onClick={() => setActiveTag(t)}>
                {t}
              </Chip>
            ))}
          </div>
        </div>
      </section>

      {/* Sections */}
      <div className="space-y-10">
        {visibleSections.map((sec) => {
          const secItems = sec.slugs.map((s) => bySlug.get(s)).filter(Boolean) as IndexItem[];
          if (!secItems.length) return null;

          return (
            <section key={sec.id} className="space-y-4">
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <h2 className="text-xl font-semibold">{sec.title}</h2>
                  <div className="text-xs text-zinc-500">{secItems.length} studies</div>
                </div>

                {sec.description ? <div className="text-sm text-zinc-600">{sec.description}</div> : null}

                {sec.conclusions?.length ? (
                  <div className="rounded-2xl border border-zinc-200 bg-white p-4">
                    <div className="text-sm font-semibold">Section takeaways</div>
                    <ul className="mt-2 list-disc pl-5 text-sm text-zinc-700 space-y-1">
                      {sec.conclusions.slice(0, 3).map((c, i) => (
                        <li key={i}>{c}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {secItems.map((it) => (
                  <Link
                    key={it.slug}
                    href={`/research/${it.slug}`}
                    className="group rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm transition"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-1">
                        <div className="text-lg font-semibold leading-snug group-hover:underline">
                          {it.title}
                        </div>
                        <div className="text-sm text-zinc-600">{it.summary}</div>

                        {it.highlight ? (
                          <div className="text-xs text-zinc-600 mt-2 line-clamp-2">
                            <span className="font-semibold text-zinc-700">Key finding:</span>{" "}
                            {it.highlight}
                          </div>
                        ) : null}
                      </div>

                      <div className="text-right space-y-2 shrink-0">
                        <Badge>{(it.status ?? "draft").toUpperCase()}</Badge>
                        <div className="text-xs text-zinc-500">{it.updated_at}</div>
                      </div>
                    </div>

                    {(it.tags ?? []).length ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {(it.tags ?? []).slice(0, 10).map((t) => (
                          <span
                            key={t}
                            className="text-[11px] text-zinc-700 px-2 py-1 rounded-full bg-zinc-100"
                          >
                            {t}
                          </span>
                        ))}
                      </div>
                    ) : null}

                    {it.key_stats?.length ? (
                      <div className="mt-4 grid grid-cols-2 gap-2">
                        {it.key_stats.slice(0, 6).map((s) => (
                          <div
                            key={s.label}
                            className="rounded-xl bg-zinc-50 p-3 border border-zinc-100"
                          >
                            <div className="text-[11px] text-zinc-500">{s.label}</div>
                            <div className="text-sm font-semibold text-zinc-900">{s.value}</div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </Link>
                ))}
              </div>
            </section>
          );
        })}
      </div>
    </div>
  );
}
