// apps/web/next.config.mjs
const base = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Produce static assets suitable for GitHub Pages
  output: "export",
  // Tell Next that the site lives under /{repo}
  basePath: base,
  // Prefix all chunk/script URLs so hydration works on Pages
  assetPrefix: `${base}/`,
  // Keep links stable on Pages
  trailingSlash: true,
  // Disable remote loader on Pages
  images: { unoptimized: true },
  reactStrictMode: true,
};

export default nextConfig;
