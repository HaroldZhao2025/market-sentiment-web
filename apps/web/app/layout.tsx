import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Sentiment Intelligence",
    template: "%s | Sentiment Intelligence",
  },
  description:
    "A transparent market-intelligence layer for S&P 500 news sentiment, price reactions, portfolio signals, and empirical research.",
};

const navItems = [
  { href: "/", label: "Market" },
  { href: "/ask", label: "Ask" },
  { href: "/screener", label: "Screener" },
  { href: "/sp500", label: "S&P 500" },
  { href: "/attribution", label: "Attribution" },
  { href: "/events", label: "Events" },
  { href: "/lab", label: "Lab" },
  { href: "/agent", label: "Agent" },
  { href: "/portfolio", label: "Portfolio" },
  { href: "/research", label: "Research" },
  { href: "/data", label: "Data" },
  { href: "/methodology", label: "Methodology" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-neutral-950 text-neutral-100 antialiased">
        <header className="sticky top-0 z-50 border-b border-white/10 bg-neutral-950/90 backdrop-blur-xl">
          <nav className="mx-auto flex max-w-7xl items-center justify-between gap-6 px-4 py-3 md:px-6">
            <Link href="/" className="group flex shrink-0 items-center gap-3">
              <span className="grid h-9 w-9 place-items-center rounded-xl border border-emerald-400/30 bg-emerald-400/10 text-sm font-black text-emerald-300">SI</span>
              <span>
                <span className="block text-sm font-semibold tracking-tight text-white">Sentiment Intelligence</span>
                <span className="hidden text-[11px] text-neutral-500 sm:block">Market evidence, not market noise</span>
              </span>
            </Link>

            <div className="flex max-w-[72vw] items-center gap-1 overflow-x-auto rounded-xl border border-white/10 bg-white/[0.03] p-1 text-sm [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
              {navItems.map((item) => (
                <Link key={item.href} href={item.href} className="whitespace-nowrap rounded-lg px-3 py-2 text-neutral-400 transition hover:bg-white/[0.06] hover:text-white">
                  {item.label}
                </Link>
              ))}
            </div>
          </nav>
        </header>

        <div className="mx-auto min-h-[calc(100vh-144px)] max-w-7xl px-4 py-6 md:px-6 md:py-8">{children}</div>

        <footer className="border-t border-white/10 bg-black/20">
          <div className="mx-auto flex max-w-7xl flex-col gap-3 px-4 py-6 text-xs text-neutral-500 md:flex-row md:items-center md:justify-between md:px-6">
            <div>Sentiment Intelligence · S&P 500 market-intelligence research project</div>
            <div className="flex flex-wrap gap-4">
              <Link href="/ask" className="hover:text-neutral-300">Ask the Market</Link>
              <Link href="/agent" className="hover:text-neutral-300">Agent Interface</Link>
              <Link href="/methodology" className="hover:text-neutral-300">Methodology</Link>
              <Link href="/data" className="hover:text-neutral-300">Machine-readable data</Link>
              <span>Not investment advice</span>
            </div>
          </div>
        </footer>
      </body>
    </html>
  );
}
