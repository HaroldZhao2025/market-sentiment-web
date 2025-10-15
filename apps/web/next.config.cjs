/** @type {import('next').NextConfig} */
const base = process.env.NEXT_PUBLIC_BASE_PATH || ""; // e.g. "/market-sentiment-web"

module.exports = {
  // This is the key: it tells Next to produce a static export in apps/web/out
  output: "export",
  basePath: base,
  assetPrefix: base ? `${base}/` : undefined,
  trailingSlash: true,
  images: { unoptimized: true },
  reactStrictMode: true,
};
