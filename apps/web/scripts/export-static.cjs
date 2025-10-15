// apps/web/scripts/export-static.cjs
// Minimal exporter: copies prerendered HTML from .next to out/, plus static assets and public/

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const ROOT = process.cwd();              // apps/web
const NEXT_DIR = path.join(ROOT, ".next");
const OUT_DIR = path.join(ROOT, "out");
const PUBLIC_DIR = path.join(ROOT, "public");

// Where Next writes app-router prerendered HTML:
const APP_DIR = path.join(NEXT_DIR, "server", "app");
// Next static assets:
const NEXT_STATIC_DIR = path.join(NEXT_DIR, "static");

async function rmrf(p) {
  if (fs.existsSync(p)) await fsp.rm(p, { recursive: true, force: true });
}

async function mkdirp(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function copyFile(src, dst) {
  await mkdirp(path.dirname(dst));
  await fsp.copyFile(src, dst);
}

async function copyDir(src, dst) {
  if (!fs.existsSync(src)) return;
  const entries = await fsp.readdir(src, { withFileTypes: true });
  for (const e of entries) {
    const s = path.join(src, e.name);
    const d = path.join(dst, e.name);
    if (e.isDirectory()) {
      await copyDir(s, d);
    } else if (e.isFile()) {
      await mkdirp(path.dirname(d));
      await fsp.copyFile(s, d);
    }
  }
}

function toPrettyRoute(outRoot, relFile) {
  // relFile: path relative to APP_DIR, e.g. 'ticker/AAPL.html'
  // We want '/ticker/AAPL/index.html' for pretty URLs.
  const ext = path.extname(relFile);
  if (ext !== ".html") {
    // keep non-HTML files as-is (js/css/map/…)
    return path.join(outRoot, relFile);
  }
  const base = relFile.slice(0, -ext.length); // drop .html
  if (path.basename(relFile) === "index.html") {
    // already index.html under some folder
    return path.join(outRoot, relFile);
  }
  return path.join(outRoot, base, "index.html");
}

async function exportAppHtml() {
  if (!fs.existsSync(APP_DIR)) {
    console.warn("No app HTML found at", APP_DIR);
    return;
  }
  const walk = async (dir) => {
    const ents = await fsp.readdir(dir, { withFileTypes: true });
    for (const ent of ents) {
      const abs = path.join(dir, ent.name);
      const rel = path.relative(APP_DIR, abs);
      if (ent.isDirectory()) {
        await walk(abs);
      } else if (ent.isFile()) {
        // Copy all files; map *.html to pretty routes
        const outFile = rel.endsWith(".html")
          ? toPrettyRoute(OUT_DIR, rel)
          : path.join(OUT_DIR, rel);
        await mkdirp(path.dirname(outFile));
        await fsp.copyFile(abs, outFile);
      }
    }
  };
  await walk(APP_DIR);
}

async function main() {
  await rmrf(OUT_DIR);
  await mkdirp(OUT_DIR);

  // 1) Copy prerendered app HTML/js/css to out/
  await exportAppHtml();

  // 2) Copy Next static assets to out/_next/static
  if (fs.existsSync(NEXT_STATIC_DIR)) {
    await copyDir(NEXT_STATIC_DIR, path.join(OUT_DIR, "_next", "static"));
  }

  // 3) Copy public/ into out/ (images, robots.txt, data/, etc.)
  if (fs.existsSync(PUBLIC_DIR)) {
    await copyDir(PUBLIC_DIR, OUT_DIR);
  }

  console.log("Static export complete →", OUT_DIR);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
