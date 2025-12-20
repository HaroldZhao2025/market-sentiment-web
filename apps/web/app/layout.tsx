// apps/web/app/layout.tsx
import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Market Sentiment — S&P 500",
  description: "News & sentiment dashboard for S&P 500 tickers.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-white text-gray-900">
        <header className="w-full border-b">
          <nav className="mx-auto max-w-6xl flex items-center gap-6 p-4">
            {/* Next's <Link> automatically respects basePath */}
            <Link href="/">Home</Link>
            <Link href="/sp500">S&amp;P 500</Link>
            <Link href="/portfolio">Portfolio</Link>
          </nav>
        </header>
        <main className="mx-auto max-w-6xl p-4">{children}</main>
        <footer className="mx-auto max-w-6xl p-4 text-sm text-gray-500 border-t">
          © 2025 Market Sentiment
        </footer>
      </body>
    </html>
  );
}
