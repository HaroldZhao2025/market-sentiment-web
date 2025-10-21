import "./globals.css";
import type { Metadata } from "next";
import { Inter } from "next/font/google";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Market Sentiment",
  description: "News-driven sentiment with prices and headlines",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={inter.className}>
      <body className="min-h-screen bg-gradient-to-b from-white to-neutral-50 text-neutral-900 antialiased">
        <header className="border-b bg-white/80 backdrop-blur">
          <div className="mx-auto max-w-6xl px-4 py-3 flex items-center justify-between">
            <div className="font-semibold tracking-tight">Market Sentiment</div>
            <nav className="text-sm text-neutral-600">
              <a href="." className="hover:underline mr-4">Home</a>
              <a href="/market-sentiment-web/portfolio/" className="hover:underline">S&amp;P 500</a>
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-4 py-8">{children}</main>
        <footer className="py-10 text-center text-sm text-neutral-500">© {new Date().getFullYear()} Market Sentiment</footer>
      </body>
    </html>
  );
}
