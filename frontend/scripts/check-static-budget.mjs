import {
  readFileSync,
  realpathSync,
  statSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants as zlibConstants } from "node:zlib";

export const BUDGETS = {
  dashboardJs: 550 * 1024,
  dashboardCss: 35 * 1024,
  largestJs: 75 * 1024,
};

const BROTLI_OPTIONS = {
  params: {
    [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
  },
};

function isInside(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
}

function htmlPathForRoute(outDir, route) {
  const cleanRoute = `/${route.split(/[?#]/, 1)[0].replace(/^\/+|\/+$/g, "")}`;
  if (cleanRoute === "/") return path.join(outDir, "index.html");
  const flat = path.join(outDir, `${cleanRoute.slice(1)}.html`);
  const nested = path.join(outDir, cleanRoute.slice(1), "index.html");
  try {
    if (statSync(flat).isFile()) return flat;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  return nested;
}

function assetReferences(html) {
  const references = [];
  const tagPattern = /<(script|link)\b[^>]*>/gi;
  for (const match of html.matchAll(tagPattern)) {
    const attribute = /\b(?:src|href)\s*=\s*(["'])(.*?)\1/i.exec(match[0]);
    if (!attribute) continue;
    references.push(attribute[2]);
  }
  return references;
}

function localAsset(outDir, route, reference) {
  const routeUrl = `https://static-export.invalid${route}`;
  const url = new URL(reference, routeUrl);
  if (url.protocol !== "https:" || url.hostname !== "static-export.invalid") {
    throw new Error(`Cannot measure external dashboard asset: ${reference}`);
  }

  let pathname;
  try {
    pathname = decodeURIComponent(url.pathname);
  } catch {
    throw new Error(`Dashboard asset has an invalid URL encoding: ${reference}`);
  }
  const candidate = path.resolve(outDir, `.${pathname}`);
  if (!isInside(outDir, candidate)) {
    throw new Error(`Dashboard asset escapes the static export root: ${reference}`);
  }

  let resolved;
  try {
    resolved = realpathSync(candidate);
  } catch {
    throw new Error(`Dashboard asset is missing from the static export: ${reference}`);
  }
  if (!isInside(realpathSync(outDir), resolved)) {
    throw new Error(`Dashboard asset escapes the static export root: ${reference}`);
  }
  return resolved;
}

function assetKind(reference) {
  const pathname = new URL(reference, "https://static-export.invalid").pathname.toLowerCase();
  if (pathname.endsWith(".js") || pathname.endsWith(".mjs")) return "javascript";
  if (pathname.endsWith(".css")) return "css";
  return null;
}

function compressedSize(contents) {
  return brotliCompressSync(Buffer.concat(contents), BROTLI_OPTIONS).byteLength;
}

export function measureRoute(outDir, route = "/dashboard") {
  const root = path.resolve(outDir);
  const entry = htmlPathForRoute(root, route);
  const html = readFileSync(entry, "utf8");
  const assets = {
    javascript: new Map(),
    css: new Map(),
  };

  for (const reference of assetReferences(html)) {
    const kind = assetKind(reference);
    if (!kind) continue;
    const assetPath = localAsset(root, route, reference);
    if (!assets[kind].has(assetPath)) {
      assets[kind].set(assetPath, {
        path: assetPath,
        content: readFileSync(assetPath),
      });
    }
  }

  const javascript = [...assets.javascript.values()];
  const css = [...assets.css.values()];
  const javascriptChunks = javascript.map((asset) => ({
    ...asset,
    brotliBytes: brotliCompressSync(asset.content, BROTLI_OPTIONS).byteLength,
  }));
  const largest = javascriptChunks.reduce(
    (current, chunk) =>
      !current || chunk.brotliBytes > current.brotliBytes ? chunk : current,
    null,
  );

  return {
    route,
    javascript_files: javascript.length,
    css_files: css.length,
    javascript_raw_bytes: javascript.reduce((total, asset) => total + asset.content.byteLength, 0),
    javascript_brotli_bytes: compressedSize(javascript.map((asset) => asset.content)),
    css_raw_bytes: css.reduce((total, asset) => total + asset.content.byteLength, 0),
    css_brotli_bytes: compressedSize(css.map((asset) => asset.content)),
    largest_js_brotli_bytes: largest?.brotliBytes ?? 0,
    largest_js_chunk: largest ? path.relative(root, largest.path).split(path.sep).join("/") : null,
  };
}

export function assertBudgets(measurement, budgets = BUDGETS) {
  const failures = [];
  const checks = [
    ["dashboard JavaScript", measurement.javascript_brotli_bytes, budgets.dashboardJs],
    ["dashboard CSS", measurement.css_brotli_bytes, budgets.dashboardCss],
    ["largest JavaScript chunk", measurement.largest_js_brotli_bytes, budgets.largestJs],
  ];
  for (const [name, actual, limit] of checks) {
    if (actual > limit) {
      failures.push(`${name} is ${actual} bytes (${actual - limit} bytes over the ${limit}-byte budget)`);
    }
  }
  if (failures.length > 0) {
    throw new Error(`Static export budget exceeded: ${failures.join("; ")}`);
  }
  return measurement;
}

function runCli() {
  const input = path.resolve(process.argv[2] ?? path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "out"));
  let outDir = input;
  let route = "/dashboard";
  if (statSync(input).isFile()) {
    outDir = path.dirname(input);
    route = `/${path.basename(input, path.extname(input))}`;
  }
  const measurement = measureRoute(outDir, route);
  console.log(JSON.stringify(measurement));
  assertBudgets(measurement);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    runCli();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
