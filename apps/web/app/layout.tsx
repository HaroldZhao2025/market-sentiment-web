import type { Metadata } from "next";
import Link from "next/link";
import GlobalNavV2 from "../components/GlobalNavV2";
import { LanguageProvider } from "../components/LanguageProvider";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Sentiment Intelligence",
    template: "%s | Sentiment Intelligence",
  },
  description:
    "A transparent market-intelligence layer for U.S. company news sentiment, earnings evidence, S&P 500 attribution, portfolio signals, and empirical research.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="min-h-screen bg-neutral-950 text-neutral-100 antialiased">
        <LanguageProvider>
          <GlobalNavV2 />

          <div className="mx-auto min-h-[calc(100vh-144px)] max-w-7xl px-4 py-6 md:px-6 md:py-8">{children}</div>

          <footer className="border-t border-white/10 bg-black/20">
            <div className="mx-auto flex max-w-7xl flex-col gap-3 px-4 py-6 text-xs text-neutral-500 md:flex-row md:items-center md:justify-between md:px-6">
              <div>Sentiment Intelligence · U.S. company evidence and S&amp;P 500 research platform</div>
              <div className="flex flex-wrap gap-4">
                <Link href="/ask" className="hover:text-neutral-300">Ask the Market</Link>
                <Link href="/agent" className="hover:text-neutral-300">Agent Interface</Link>
                <Link href="/methodology" className="hover:text-neutral-300">Methodology</Link>
                <Link href="/data" className="hover:text-neutral-300">Machine-readable data</Link>
                <span>Not investment advice</span>
              </div>
            </div>
          </footer>
        </LanguageProvider>
      </body>
    </html>
  );
}
