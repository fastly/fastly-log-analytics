import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const packageDir = join(root, "node_modules", "@duckdb", "duckdb-wasm");
const srcDir = join(packageDir, "dist");
const { version } = JSON.parse(
  readFileSync(join(packageDir, "package.json"), "utf8"),
);
const publicDir = join(root, "public", "duckdb");
// Versioned paths keep the year-long immutable cache safe across upgrades; drop stale copies.
const destDir = join(publicDir, version);

// DuckDB autoloads parquet for read_parquet; serving the vendored signed build from our own
// origin keeps the browser off extensions.duckdb.org at runtime.
const vendorDir = join(root, "vendor", "duckdb-extensions");
const manifestRaw = readFileSync(join(vendorDir, "manifest.json"), "utf8");
const manifest = JSON.parse(manifestRaw);
const coreFiles = ["duckdb-eh.wasm", "duckdb-browser-eh.worker.js"];

const isUpToDate = () => {
  if (!existsSync(destDir)) return false;
  const entries = readdirSync(publicDir);
  if (entries.length !== 1 || entries[0] !== version) return false;
  for (const file of coreFiles) {
    const src = join(srcDir, file);
    const dest = join(destDir, file);
    if (!existsSync(src) || !existsSync(dest)) return false;
    if (statSync(src).size !== statSync(dest).size) return false;
  }
  for (const [file, sha256] of Object.entries(manifest.files)) {
    const src = join(vendorDir, file);
    const dest = join(destDir, "extensions", file);
    if (!existsSync(src) || !existsSync(dest)) return false;
    if (statSync(src).size !== statSync(dest).size) return false;
    const digest = createHash("sha256").update(readFileSync(dest)).digest("hex");
    if (digest !== sha256) return false;
  }
  return true;
};

if (!isUpToDate()) {
  rmSync(publicDir, { recursive: true, force: true });
  mkdirSync(destDir, { recursive: true });

  for (const file of coreFiles) {
    const src = join(srcDir, file);
    if (!existsSync(src)) {
      throw new Error(`Missing DuckDB asset: ${src}`);
    }
    copyFileSync(src, join(destDir, file));
  }

  for (const [file, sha256] of Object.entries(manifest.files)) {
    const src = join(vendorDir, file);
    const digest = createHash("sha256").update(readFileSync(src)).digest("hex");
    if (digest !== sha256) {
      throw new Error(`DuckDB extension ${file} sha256 ${digest} != ${sha256}`);
    }
    const dest = join(destDir, "extensions", file);
    mkdirSync(dirname(dest), { recursive: true });
    copyFileSync(src, dest);
  }
}
