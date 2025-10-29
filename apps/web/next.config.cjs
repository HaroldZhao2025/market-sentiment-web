/** @type {import('next').NextConfig} */
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

module.exports = {
  // Required for GitHub Pages static hosting
  output: "export",
  basePath,
  assetPrefix: basePath,
  trailingSlash: true,
  images: { unoptimized: true },

  experimental: {
    typedRoutes: false,
  },
};
