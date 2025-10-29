// apps/web/next.config.cjs
/** @type {import('next').NextConfig} */
const base = process.env.NEXT_PUBLIC_BASE_PATH || "";

module.exports = {
  // Static HTML export
  output: "export",
  // Required for GitHub Pages under a subpath
  basePath: base,
  assetPrefix: base ? `${base}/` : "",
  // Ensure all routes end with / (helps GH Pages + static hosting)
  trailingSlash: true,
  // Disable image optimizer for static export
  images: {
    unoptimized: true,
  },
  // Optional: quieten build a bit
  reactStrictMode: true,
};
