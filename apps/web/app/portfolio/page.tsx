// apps/web/pages/portfolio.tsx
import Head from "next/head";
import Link from "next/link";

export default function PortfolioPage() {
  return (
    <>
      <Head>
        <title>Portfolio | Market Sentiment</title>
      </Head>

      <main style={{ maxWidth: 980, margin: "0 auto", padding: 16 }}>
        <h1>Portfolio</h1>
        <p style={{ opacity: 0.85 }}>
          This page is reserved for future portfolio features (watchlists, custom baskets, etc.).
        </p>

        <p style={{ marginTop: 12 }}>
          For the S&amp;P 500 index view, go to{" "}
          <Link href="/sp500">/sp500</Link>.
        </p>

        <div style={{ marginTop: 22, opacity: 0.8 }}>
          <Link href="/">← Back to Home</Link>
        </div>
      </main>
    </>
  );
}
