// apps/web/next.config.cjs
/** @type {import('next').NextConfig} */
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

module.exports = {
  output: "export",          // <-- this makes `next build` write to `out/`
  basePath,                  // for GitHub Pages subpath
  images: { unoptimized: true },
  trailingSlash: true,
  reactStrictMode: false,
};
