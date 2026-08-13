import Link from "next/link";

const primaryNav = [
  { href: "/", label: "Market" },
  { href: "/companies", label: "Companies" },
  { href: "/sp500", label: "S&P 500" },
  { href: "/lab", label: "Research Lab" },
];

const intelligenceNav = [
  { href: "/screener", label: "Screener", description: "Cross-sectional signal explorer" },
  { href: "/news", label: "News", description: "Expanded company news evidence" },
  { href: "/earnings", label: "Earnings", description: "Calls, surprises, and filings" },
  { href: "/events", label: "Events", description: "Persistent event memory" },
  { href: "/attribution", label: "Attribution", description: "Company-to-sector contribution" },
];

const researchNav = [
  { href: "/portfolio", label: "Portfolio", description: "Sentiment strategy research" },
  { href: "/research", label: "Research", description: "Empirical study library" },
  { href: "/agent", label: "Agent", description: "Machine-readable interface" },
  { href: "/data", label: "Data", description: "Public data contracts" },
  { href: "/methodology", label: "Methodology", description: "Definitions and caveats" },
];

const mobileNav = [...primaryNav, ...intelligenceNav, ...researchNav];

function Brand() {
  return (
    <Link href="/" className="group flex shrink-0 items-center gap-3">
      <span className="grid h-9 w-9 place-items-center rounded-xl border border-emerald-400/30 bg-emerald-400/10 text-sm font-black text-emerald-300">SI</span>
      <span>
        <span className="block text-sm font-semibold tracking-tight text-white">Sentiment Intelligence</span>
        <span className="hidden text-[11px] text-neutral-500 sm:block">Market evidence, not market noise</span>
      </span>
    </Link>
  );
}

function MenuGroup({ title, items }: { title: string; items: typeof intelligenceNav }) {
  return (
    <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-2">
      <div className="px-2 pb-1.5 pt-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-neutral-600">{title}</div>
      {items.map((item) => (
        <Link key={item.href} href={item.href} className="block rounded-lg px-2.5 py-2.5 transition hover:bg-white/[0.06]">
          <div className="text-sm font-medium text-neutral-200">{item.label}</div>
          <div className="mt-0.5 text-[11px] leading-4 text-neutral-600">{item.description}</div>
        </Link>
      ))}
    </div>
  );
}

export default function GlobalNavV2() {
  return (
    <header className="sticky top-0 z-[100] isolate border-b border-white/10 bg-neutral-950">
      <nav className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-3 md:px-6">
        <Brand />

        <div className="hidden items-center gap-1 lg:flex">
          <div className="flex items-center gap-1 rounded-xl border border-white/10 bg-white/[0.03] p-1 text-sm">
            {primaryNav.map((item) => (
              <Link key={item.href} href={item.href} className="whitespace-nowrap rounded-lg px-3 py-2 text-neutral-400 transition hover:bg-white/[0.06] hover:text-white">{item.label}</Link>
            ))}
            <details className="group relative">
              <summary className="cursor-pointer list-none rounded-lg px-3 py-2 text-neutral-400 transition hover:bg-white/[0.06] hover:text-white [&::-webkit-details-marker]:hidden">
                <span className="inline-flex items-center gap-1.5">More <span className="text-[10px] transition group-open:rotate-180">▾</span></span>
              </summary>
              <div className="absolute right-0 top-[calc(100%+12px)] z-[120] w-[600px] rounded-2xl border border-white/10 bg-neutral-950 p-3 shadow-[0_24px_80px_rgba(0,0,0,0.78)] ring-1 ring-black">
                <div className="grid grid-cols-2 gap-2">
                  <MenuGroup title="Intelligence" items={intelligenceNav} />
                  <MenuGroup title="Research & platform" items={researchNav} />
                </div>
              </div>
            </details>
          </div>
          <Link href="/ask" className="ml-2 rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-4 py-2.5 text-sm font-semibold text-emerald-300 transition hover:border-emerald-400/50 hover:bg-emerald-400/15">Ask</Link>
        </div>

        <div className="flex items-center gap-2 lg:hidden">
          <Link href="/ask" className="rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-3 py-2 text-xs font-semibold text-emerald-300">Ask</Link>
          <details className="group relative">
            <summary className="cursor-pointer list-none rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-xs text-neutral-300 [&::-webkit-details-marker]:hidden">
              <span className="inline-flex items-center gap-1.5">Explore <span className="text-[10px] transition group-open:rotate-180">▾</span></span>
            </summary>
            <div className="absolute right-0 top-[calc(100%+12px)] z-[120] max-h-[72vh] w-72 overflow-y-auto rounded-2xl border border-white/10 bg-neutral-950 p-2 shadow-[0_24px_80px_rgba(0,0,0,0.78)] ring-1 ring-black">
              {mobileNav.map((item) => (
                <Link key={item.href} href={item.href} className="block rounded-xl px-3 py-2.5 text-sm text-neutral-300 transition hover:bg-white/[0.06] hover:text-white">{item.label}</Link>
              ))}
            </div>
          </details>
        </div>
      </nav>
    </header>
  );
}
