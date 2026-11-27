import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const DUCKDB_HARDENED_WORKER_FILE = "duckdb-browser-eh.v2.worker.js";
export const DUCKDB_LEGACY_WORKER_SHIM = 'importScripts("./duckdb-browser-eh.v2.worker.js");\n';

/**
 * Installs fetch retry and unhandled-rejection propagation inside the DuckDB
 * browser worker. Keep completely self-contained so `.toString()` can be serialized
 * verbatim as the worker preamble without closing over module imports.
 *
 * @param {{
 *   __flpDuckdbWorkerHardened?: boolean;
 *   location?: { origin?: string; href?: string };
 *   fetch?: typeof fetch;
 *   addEventListener?: (
 *     type: string,
 *     listener: (event: { reason?: unknown; preventDefault?: () => void }) => void,
 *   ) => void;
 * }} [scope]
 * @returns {void}
 */
export function installDuckdbWorkerHardening(scope = globalThis) {
  if (!scope || scope.__flpDuckdbWorkerHardened) return;
  scope.__flpDuckdbWorkerHardened = true;

  if (typeof scope.addEventListener === "function") {
    scope.addEventListener("unhandledrejection", (event) => {
      if (event && typeof event.preventDefault === "function") {
        event.preventDefault();
      }
      const reason = event && "reason" in event ? event.reason : undefined;
      const err =
        reason instanceof Error
          ? reason
          : new Error(String(reason ?? "Unhandled promise rejection in DuckDB worker"));
      setTimeout(() => {
        throw err;
      }, 0);
    });
  }

  const origFetch = typeof scope.fetch === "function" ? scope.fetch.bind(scope) : null;
  if (!origFetch) return;
  const baseHref = (scope.location && scope.location.href) || "http://localhost/";
  const pageOrigin = scope.location && scope.location.origin;

  const resolveWasmUrl = (input, init) => {
    const method = (
      (init && init.method) ||
      (input && typeof input === "object" && "method" in input && input.method) ||
      "GET"
    ).toUpperCase();
    if (method !== "GET") return null;
    const rawUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input && typeof input === "object" && "url" in input
            ? String(input.url)
            : "";
    if (!rawUrl) return null;
    let parsed;
    try {
      parsed = new URL(rawUrl, baseHref);
    } catch {
      return null;
    }
    if (pageOrigin && parsed.origin !== pageOrigin) return null;
    if (!parsed.pathname.endsWith(".wasm")) return null;
    return parsed.href;
  };

  const bufferWasmResponse = async (response) => {
    if (!response.ok) return response;
    const headers = new Headers(response.headers);
    const contentType = headers.get("content-type");
    if (!contentType || !contentType.includes("wasm")) {
      headers.set("content-type", "application/wasm");
    }
    const bytes = await response.arrayBuffer();
    headers.set("content-length", String(bytes.byteLength));
    return new Response(bytes, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  };

  scope.fetch = async (input, init) => {
    const wasmUrl = resolveWasmUrl(input, init);
    if (!wasmUrl) {
      return origFetch(input, init);
    }
    try {
      const first = await origFetch(input, init);
      if (first.status >= 500) {
        throw new Error(`HTTP ${first.status} fetching ${wasmUrl}`);
      }
      return await bufferWasmResponse(first);
    } catch {
      const retry = await origFetch(wasmUrl, {
        method: "GET",
        cache: "no-store",
        credentials: "same-origin",
      });
      return await bufferWasmResponse(retry);
    }
  };
}

/**
 * @param {string} workerSource
 * @returns {string}
 */
export function renderHardenedDuckdbWorker(workerSource) {
  return `(${installDuckdbWorkerHardening.toString()})(globalThis);\n${workerSource}`;
}

function sha256(bytesOrText) {
  return createHash("sha256").update(bytesOrText).digest("hex");
}

function defaultSyncPaths() {
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const packageDir = join(root, "node_modules", "@duckdb", "duckdb-wasm");
  const srcDir = join(packageDir, "dist");
  const { version } = JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8"));
  const publicDir = join(root, "public", "duckdb");
  // DuckDB autoloads parquet for read_parquet; serving the vendored signed build from our own
  // origin keeps the browser off extensions.duckdb.org at runtime.
  const vendorDir = join(root, "vendor", "duckdb-extensions");
  return { srcDir, vendorDir, publicDir, version };
}

/**
 * @param {{ srcDir?: string; vendorDir?: string; publicDir?: string; version?: string }} [options]
 * @returns {{ copied: boolean; destDir: string; version: string }}
 */
export function syncDuckdbAssets(options = {}) {
  const defaults = options.srcDir && options.vendorDir && options.publicDir && options.version
    ? options
    : { ...defaultSyncPaths(), ...options };
  const { srcDir, vendorDir, publicDir, version } = defaults;
  // Versioned paths keep the year-long immutable cache safe across upgrades; drop stale copies.
  const destDir = join(publicDir, version);
  const manifest = JSON.parse(readFileSync(join(vendorDir, "manifest.json"), "utf8"));
  const wasmSrc = join(srcDir, "duckdb-eh.wasm");
  const workerSrc = join(srcDir, "duckdb-browser-eh.worker.js");
  if (!existsSync(wasmSrc)) {
    throw new Error(`Missing DuckDB asset: ${wasmSrc}`);
  }
  if (!existsSync(workerSrc)) {
    throw new Error(`Missing DuckDB asset: ${workerSrc}`);
  }
  const hardenedWorker = renderHardenedDuckdbWorker(readFileSync(workerSrc, "utf8"));
  const hardenedWorkerSha = sha256(hardenedWorker);
  const shimSha = sha256(DUCKDB_LEGACY_WORKER_SHIM);

  const isUpToDate = () => {
    if (!existsSync(destDir)) return false;
    const entries = readdirSync(publicDir);
    if (entries.length !== 1 || entries[0] !== version) return false;

    const wasmDest = join(destDir, "duckdb-eh.wasm");
    if (!existsSync(wasmDest) || statSync(wasmSrc).size !== statSync(wasmDest).size) return false;

    const v2Dest = join(destDir, DUCKDB_HARDENED_WORKER_FILE);
    if (!existsSync(v2Dest) || sha256(readFileSync(v2Dest)) !== hardenedWorkerSha) return false;

    const shimDest = join(destDir, "duckdb-browser-eh.worker.js");
    if (!existsSync(shimDest) || sha256(readFileSync(shimDest)) !== shimSha) return false;

    for (const [file, expectedSha] of Object.entries(manifest.files)) {
      const extSrc = join(vendorDir, file);
      const extDest = join(destDir, "extensions", file);
      if (!existsSync(extSrc) || !existsSync(extDest)) return false;
      if (statSync(extSrc).size !== statSync(extDest).size) return false;
      if (sha256(readFileSync(extDest)) !== expectedSha) return false;
    }
    return true;
  };

  if (isUpToDate()) {
    return { copied: false, destDir, version };
  }

  rmSync(publicDir, { recursive: true, force: true });
  mkdirSync(destDir, { recursive: true });

  copyFileSync(wasmSrc, join(destDir, "duckdb-eh.wasm"));
  writeFileSync(join(destDir, DUCKDB_HARDENED_WORKER_FILE), hardenedWorker, "utf8");
  writeFileSync(join(destDir, "duckdb-browser-eh.worker.js"), DUCKDB_LEGACY_WORKER_SHIM, "utf8");

  for (const [file, expectedSha] of Object.entries(manifest.files)) {
    const extSrc = join(vendorDir, file);
    const digest = sha256(readFileSync(extSrc));
    if (digest !== expectedSha) {
      throw new Error(`DuckDB extension ${file} sha256 ${digest} != ${expectedSha}`);
    }
    const extDest = join(destDir, "extensions", file);
    mkdirSync(dirname(extDest), { recursive: true });
    copyFileSync(extSrc, extDest);
  }

  return { copied: true, destDir, version };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  syncDuckdbAssets();
}
