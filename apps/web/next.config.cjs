// apps/web/next.config.mjs
/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === "production";
const repo = "market-sentiment-web";

const nextConfig = {
  // Export for GitHub Pages
  output: "export",
  trailingSlash: true,

  // Make all URLs/assets work under /market-sentiment-web on Pages
  basePath: isProd ? `/${repo}` : "",
  assetPrefix: isProd ? `/${repo}/` : "",

  // App Router + static export needs this so <Image> doesn’t try to optimize
  images: { unoptimized: true },

  // Helpful if you fetch local JSON during build
  experimental: {
    // Keep this off unless you know you want per-route server bundles
    // staticGenerationSearchParams: true,
  },
};

export default nextConfig;
