#!/usr/bin/env node
/**
 * Live Playwright browser verification & telemetry harness for fastly-logs-parquet (flp).
 *
 * Modes:
 *   --mode ensure-auth
 *     Checks if browser-state.json has a valid session against /api/auth/session.
 *     If expired or missing, uses CDP WebAuthn Virtual Authenticator + passkeys.json
 *     to sign in at /sign-in and saves the refreshed browser-state.json.
 *
 *   --mode verify
 *     1. Ensures authentication is valid and --service-id matches the sole configured service.
 *     2. Loads /dashboard (cold and warm), optionally polling until
 *        [data-testid="total-requests"] >= --expect-min-requests, and reports:
 *          - cold_load_ms, warm_load_ms
 *          - parquet_files_fetched, parquet_download_ms, worker_post_download_ms
 *          - range_read_206_count, range_probe_count, full_download_200_count
 *          - last_fetch telemetry (listed, fetched, cached, CDN hits, rows, bytes, vCPU)
 *          - query telemetry beacons (/api/telemetry/query) & SSE ingest events
 *          - UI Web Vitals & layout health (CLS score + shifting elements, FCP, LCP,
 *            Long Tasks >50ms, horizontal overflow, stuck skeletons, JS heap, OPFS/CacheStorage)
 *          - Full /api/* request waterfall + per-endpoint p50/p95 latencies + slowest calls
 *          - total_requests, visible_at_epoch_s, extents freshness lag
 *     3. Benchmarks clean and filtered loads across fastly-log-analytics presets
 *        (1h, 3h, 6h, 12h, 24h; never 10m) and filter expressions (+field:value, -field:value).
 *     4. Sweeps all hosted pages and tabs (/dashboard, /logs + tabs, /admin,
 *        /admin/tick, /usage, /how-it-works, /cache, plus any active analytics routes
 *        such as /origin, /security, /performance, etc. clean and filtered),
 *        recording load times, CLS/Web Vitals, API waterfalls, and asserting zero errors.
 *
 *   --mode cold-only --service-id <id> --out <path>
 *     After the same auth and monitored-service preflight, performs exactly one cold
 *     /dashboard load in a fresh browser context (no warm reloads, sweep, ranges, or
 *     traffic) and persists cold load, last-fetch/Analysis, catalog/API timings, file
 *     counts, fresh state, visible timestamp, and errors. Exits 1 if the load failed.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PLAYWRIGHT_PATH = path.resolve(
  __dirname,
  "../web/node_modules/playwright/index.mjs",
);

/**
 * Built-in presets matching app/web/lib/dashboard/range.ts (PRESETS)
 * and app/web/components/FilterBar/FilterBar.tsx (EXTENDED_PRESETS).
 * Note: "10m" is NOT a preset in fastly-log-analytics and is explicitly rejected.
 */
export const DASHBOARD_PRESETS = Object.freeze([
  "1h",
  "3h",
  "6h",
  "12h",
  "24h",
]);
export const EXTENDED_PRESETS = Object.freeze([
  "1h",
  "3h",
  "6h",
  "12h",
  "24h",
  "3d",
  "7d",
  "30d",
]);

/**
 * Analytics routes that render the shared FilterBar in fastly-log-analytics
 * (matching app/web/components/FilterBar/FilterBar.tsx FILTER_BAR_ROUTE_PREFIXES).
 */
export const FILTER_BAR_ROUTE_PREFIXES = Object.freeze([
  "/dashboard",
  "/usage",
  "/fastly-value",
  "/performance",
  "/origin",
  "/security",
  "/network",
  "/streaming",
  "/rum",
  "/sessions",
  "/query",
]);

export const DEFAULT_BENCHMARK_RANGES = Object.freeze(["24h", "1h", "6h"]);
export const DEFAULT_BENCHMARK_FILTERS = Object.freeze([
  "",
  "+status:200",
  "+method:GET",
  "-status:503",
  "+status:200&+method:GET",
]);

export function optionalRoutesToProbe(hrefs) {
  return Array.from(
    new Set([
      "/cloud-files",
      "/origin",
      "/security",
      ...FILTER_BAR_ROUTE_PREFIXES.filter(
        (prefix) =>
          prefix !== "/dashboard" &&
          prefix !== "/usage" &&
          hrefs.some(
            (href) => href === prefix || href.startsWith(`${prefix}?`),
          ),
      ),
    ]),
  );
}

export function validateDashboardPresets(ranges, allowExtended = false) {
  const allowed = new Set(allowExtended ? EXTENDED_PRESETS : DASHBOARD_PRESETS);
  for (const r of ranges) {
    const trimmed = String(r || "").trim();
    if (!trimmed) continue;
    if (trimmed === "10m") {
      throw new Error(
        `Invalid preset "10m": 10m is not a preset in fastly-log-analytics. Allowed presets: ${Array.from(allowed).join(", ")}`,
      );
    }
    if (!allowed.has(trimmed)) {
      throw new Error(
        `Unsupported range preset "${trimmed}". Allowed presets: ${Array.from(allowed).join(", ")}`,
      );
    }
  }
  return ranges.map((r) => String(r).trim()).filter(Boolean);
}

/**
 * Build a URL query string matching fastly-log-analytics FilterBar / filterStore conventions:
 *   ?range=24h&f=%2Bstatus%3A200&f=%2Bmethod%3AGET
 * Accepts a filterSpec that is either empty, a single "+field:value" / "-field:value" token,
 * or multiple tokens joined by "&" or ";" (e.g. "+status:200&+method:GET").
 */
export function buildFilterQueryString(range = "24h", filterSpec = "") {
  const params = new URLSearchParams();
  if (range) {
    params.set("range", range);
  }
  const rawTokens = Array.isArray(filterSpec)
    ? filterSpec
    : String(filterSpec || "")
        .split(/[&;]/)
        .map((t) => t.trim())
        .filter(Boolean);
  for (const token of rawTokens) {
    const cleaned = token.startsWith("f=") ? token.slice(2) : token;
    if (!cleaned) continue;
    if (!/^[+-][a-zA-Z0-9_.]+:.+$/.test(cleaned)) {
      throw new Error(
        `Invalid filter token "${cleaned}": must match fastly-log-analytics (+|-)field:value syntax`,
      );
    }
    params.append("f", cleaned);
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}

export function parseArgs(argv) {
  const args = {
    mode: "verify",
    baseUrl: "https://flp-g2-live-20261029.global.ssl.fastly.net",
    stateDir: path.join(
      process.env.HOME || "",
      ".flp-state",
      "flp-g2-live-20261029",
    ),
    serviceId: "",
    outPath: "",
    expectMinRequests: 0,
    waitTimeoutMs: 180_000,
    pollIntervalMs: 15_000,
    incrementalUrl: "",
    incrementalRequests: 300,
    incrementalTimeoutMs: 480_000,
    incrementalReloadMs: 30_000,
    caseTimeoutMs: 120_000,
    ranges: [],
    filters: [],
    coldFiltered: false,
    iterations: 1,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === "--mode" && next) {
      args.mode = next;
      i += 1;
    } else if ((key === "--base-url" || key === "--url") && next) {
      args.baseUrl = next.replace(/\/+$/, "");
      i += 1;
    } else if (key === "--state-dir" && next) {
      args.stateDir = next;
      i += 1;
    } else if (key === "--service-id" && next) {
      args.serviceId = next;
      i += 1;
    } else if (key === "--out" && next) {
      args.outPath = next;
      i += 1;
    } else if (key === "--expect-min-requests" && next) {
      args.expectMinRequests = Number.parseInt(next, 10) || 0;
      i += 1;
    } else if (key === "--wait-timeout-ms" && next) {
      args.waitTimeoutMs = Number.parseInt(next, 10) || 180_000;
      i += 1;
    } else if (key === "--incremental-url" && next) {
      args.incrementalUrl = next.replace(/\/+$/, "");
      i += 1;
    } else if (key === "--incremental-requests" && next) {
      args.incrementalRequests = Number.parseInt(next, 10) || 0;
      i += 1;
    } else if (key === "--incremental-timeout-ms" && next) {
      args.incrementalTimeoutMs = Number.parseInt(next, 10) || 480_000;
      i += 1;
    } else if (key === "--case-timeout-ms" && next) {
      args.caseTimeoutMs = Number.parseInt(next, 10) || 120_000;
      i += 1;
    } else if (key === "--poll-interval-ms" && next) {
      args.pollIntervalMs = Number.parseInt(next, 10) || 15_000;
      i += 1;
    } else if (key === "--ranges" && next) {
      args.ranges = validateDashboardPresets(
        next
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
        false,
      );
      i += 1;
    } else if (key === "--filters" && next) {
      args.filters = next
        .split(",")
        .map((value) => value.trim())
        .filter((value) => value !== undefined);
      i += 1;
    } else if (key === "--cold-filtered") {
      args.coldFiltered = true;
    } else if (key === "--iterations" && next) {
      args.iterations = Math.max(1, Number.parseInt(next, 10) || 1);
      i += 1;
    }
  }
  return args;
}

const FRESH_TOTAL = '[data-testid="total-requests"][data-fresh="true"]';
const LAST_FETCH_SELECTOR = '[data-testid="last-fetch"]';

export function parseFormattedCount(raw) {
  if (!raw) return 0;
  const cleaned = String(raw).replace(/,/g, "").trim();
  const parsed = Number.parseInt(cleaned, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseHumanBytes(numStr, unitStr) {
  const val = Number.parseFloat(String(numStr).replace(/,/g, ""));
  if (!Number.isFinite(val)) return 0;
  const unit = String(unitStr || "B").toUpperCase();
  const multipliers = {
    B: 1,
    KB: 1024,
    KIB: 1024,
    MB: 1024 ** 2,
    MIB: 1024 ** 2,
    GB: 1024 ** 3,
    GIB: 1024 ** 3,
  };
  return Math.round(val * (multipliers[unit] || 1));
}

/**
 * Parse the dashboard `[data-testid="last-fetch"]` telemetry string.
 * Example formats:
 *   "Last fetch: 14 listed, 4 fetched (10 cached, 4 CDN hit), 12,340 rows, 1.4 MB in 412 ms · Compute 18.2 ms vCPU"
 *   "Last fetch: 8 listed, 0 fetched (8 cached, 0 CDN hit), 6,600 rows, 512 KB in 85 ms"
 */
export function parseLastFetchText(rawText) {
  const raw = String(rawText || "").trim();
  if (!raw) {
    return null;
  }
  const listedMatch =
    raw.match(/([\d,]+)\s+listed/i) || raw.match(/\bof\s+([\d,]+)\s+files?\b/i);
  const fetchedMatch = raw.match(/([\d,]+)\s+fetched/i);
  const cachedMatch = raw.match(/([\d,]+)\s+cached/i);
  const cdnHitMatch = raw.match(/([\d,]+)\s+CDN\s+hits?/i);
  const rowsMatch = raw.match(/([\d,]+)\s+rows/i);
  const bytesMatch = raw.match(
    /([\d,.]+)\s*(B|KB|KiB|MB|MiB|GB|GiB)(?:\s+in\b|\s*·|$)/i,
  );
  const elapsedMatch =
    raw.match(/\bin\s+([\d,]+(?:\.\d+)?)\s*ms\b/i) ||
    raw.match(/·\s*([\d,]+(?:\.\d+)?)\s*ms(?:\s*·|$)/i);
  const vcpuMatch = raw.match(/([\d,]+(?:\.\d+)?)\s*ms\s*vCPU/i);

  return {
    raw,
    filesListed: listedMatch ? parseFormattedCount(listedMatch[1]) : null,
    filesFetched: fetchedMatch ? parseFormattedCount(fetchedMatch[1]) : null,
    filesCached: cachedMatch ? parseFormattedCount(cachedMatch[1]) : null,
    cdnHits: cdnHitMatch ? parseFormattedCount(cdnHitMatch[1]) : null,
    rowsScanned: rowsMatch ? parseFormattedCount(rowsMatch[1]) : null,
    bytesScanned: bytesMatch
      ? parseHumanBytes(bytesMatch[1], bytesMatch[2])
      : null,
    clientElapsedMs: elapsedMatch
      ? Number.parseFloat(elapsedMatch[1].replace(/,/g, ""))
      : null,
    computeVcpuMs: vcpuMatch
      ? Number.parseFloat(vcpuMatch[1].replace(/,/g, ""))
      : null,
  };
}

/**
 * Compute count, min, p50, p95, max, mean, sum for any numeric series.
 */
export function summarizeNumericSeries(values) {
  const nums = (values || [])
    .filter((v) => typeof v === "number" && Number.isFinite(v))
    .sort((a, b) => a - b);
  if (nums.length === 0) {
    return {
      count: 0,
      min: null,
      p50: null,
      p95: null,
      max: null,
      mean: null,
      sum: 0,
    };
  }
  const percentile = (p) => {
    if (nums.length === 1) return nums[0];
    const rank = (p / 100) * (nums.length - 1);
    const lo = Math.floor(rank);
    const hi = Math.ceil(rank);
    const weight = rank - lo;
    return Number((nums[lo] * (1 - weight) + nums[hi] * weight).toFixed(2));
  };
  const sum = nums.reduce((acc, n) => acc + n, 0);
  return {
    count: nums.length,
    min: Number(nums[0].toFixed(2)),
    p50: percentile(50),
    p95: percentile(95),
    max: Number(nums[nums.length - 1].toFixed(2)),
    mean: Number((sum / nums.length).toFixed(2)),
    sum: Number(sum.toFixed(2)),
  };
}

function classifyApiEndpoint(call) {
  try {
    const parsed = new URL(call.url);
    const pathname = parsed.pathname;
    if (
      pathname === "/api/file" ||
      (pathname.startsWith("/fos/") && pathname.endsWith(".parquet"))
    ) {
      if (call.status === 206 || call.rangeHeader) {
        if (call.rangeHeader === "bytes=0-0") {
          return "/api/file (206 range probe)";
        }
        return "/api/file (206 range read)";
      }
      return "/api/file (200 full)";
    }
    if (pathname === "/api/files") {
      const ds = parsed.searchParams.get("dataset") || "request";
      return `/api/files?dataset=${ds}`;
    }
    return pathname;
  } catch {
    return call.endpoint || call.url || "unknown";
  }
}

/**
 * Summarize all captured /api/* calls for a page or benchmark probe.
 */
export function summarizeApiCalls(calls) {
  const list = Array.isArray(calls) ? calls : [];
  const byEndpointMap = new Map();
  let totalBytes = 0;
  let rangeProbeCount = 0;
  let rangeReadCount = 0;
  let fullFileDownloadCount = 0;
  let cdnHitCount = 0;
  const queryTelemetryBeacons = [];
  const ingestApplySpans = [];

  for (const call of list) {
    const groupKey = classifyApiEndpoint(call);
    if (!byEndpointMap.has(groupKey)) {
      byEndpointMap.set(groupKey, []);
    }
    byEndpointMap.get(groupKey).push(call);
    if (Number.isFinite(call.bytes) && call.bytes > 0) {
      totalBytes += call.bytes;
    }
    if (call.isRangeProbe) rangeProbeCount += 1;
    else if (call.isRangeRead) rangeReadCount += 1;
    else if (call.endpoint === "/api/file") fullFileDownloadCount += 1;
    if (call.cdnHit) cdnHitCount += 1;
    if (call.queryTelemetryBeacon) {
      queryTelemetryBeacons.push(call.queryTelemetryBeacon);
    }
    if (
      Array.isArray(call.ingestApplySpans) &&
      call.ingestApplySpans.length > 0
    ) {
      ingestApplySpans.push(...call.ingestApplySpans);
    }
  }

  const byEndpoint = {};
  for (const [endpoint, items] of Array.from(byEndpointMap.entries()).sort(
    (a, b) => a[0].localeCompare(b[0]),
  )) {
    byEndpoint[endpoint] = {
      count: items.length,
      errorCount: items.filter((i) => (i.status ?? 0) >= 400 || i.failed)
        .length,
      totalBytes: items.reduce((acc, i) => acc + (Number(i.bytes) || 0), 0),
      cdnHitCount: items.filter((i) => i.cdnHit).length,
      durationMs: summarizeNumericSeries(
        items
          .map((i) => i.durationMs)
          .filter((v) => v !== null && v !== undefined),
      ),
      ttfbMs: summarizeNumericSeries(
        items.map((i) => i.ttfbMs).filter((v) => v !== null && v !== undefined),
      ),
      vcpuMs: summarizeNumericSeries(
        items.map((i) => i.vcpuMs).filter((v) => v !== null && v !== undefined),
      ),
    };
  }

  const slowestCalls = [...list]
    .filter((c) => Number.isFinite(c.durationMs))
    .sort((a, b) => (b.durationMs || 0) - (a.durationMs || 0))
    .slice(0, 10)
    .map((c) => ({
      endpoint: classifyApiEndpoint(c),
      url: c.url,
      method: c.method,
      status: c.status,
      rangeHeader: c.rangeHeader ?? null,
      contentRange: c.contentRange ?? null,
      durationMs: c.durationMs,
      ttfbMs: c.ttfbMs,
      downloadMs: c.downloadMs,
      bytes: c.bytes,
      cdnHit: c.cdnHit,
      vcpuMs: c.vcpuMs,
      traceId: c.traceId,
    }));

  return {
    totalCalls: list.length,
    totalBytes,
    rangeProbeCount,
    rangeReadCount,
    fullFileDownloadCount,
    cdnHitCount,
    durationMs: summarizeNumericSeries(
      list
        .map((c) => c.durationMs)
        .filter((v) => v !== null && v !== undefined),
    ),
    ttfbMs: summarizeNumericSeries(
      list.map((c) => c.ttfbMs).filter((v) => v !== null && v !== undefined),
    ),
    vcpuMs: summarizeNumericSeries(
      list.map((c) => c.vcpuMs).filter((v) => v !== null && v !== undefined),
    ),
    byEndpoint,
    slowestCalls,
    queryTelemetryBeacons,
    ingestApplySpans,
  };
}

/**
 * Init script injected into every page to observe CLS (Cumulative Layout Shift),
 * FCP, LCP, and Main-Thread Long Tasks (>50ms) from navigation start.
 */
export const WEB_VITALS_INIT_SCRIPT = `
(() => {
  if (window.__flpWebVitals) return;
  const state = {
    clsScore: 0,
    clsSupported: false,
    clsSessionValue: 0,
    clsSessionEntries: [],
    clsMaxSessionValue: 0,
    clsEntries: [],
    fcpMs: null,
    lcpMs: null,
    lcpElement: null,
    longTaskCount: 0,
    longTaskTotalMs: 0,
    longTaskMaxMs: 0,
    longTasks: [],
  };
  window.__flpWebVitals = state;

  function describeNode(node) {
    if (!node || node.nodeType !== 1) return null;
    const tag = (node.tagName || "").toLowerCase();
    const id = node.id ? "#" + node.id : "";
    const testId = node.getAttribute && node.getAttribute("data-testid")
      ? '[data-testid="' + node.getAttribute("data-testid") + '"]'
      : "";
    const cls = typeof node.className === "string" && node.className.trim()
      ? "." + node.className.trim().split(/\\s+/).slice(0, 2).join(".")
      : "";
    return tag + id + testId + cls;
  }

  try {
    let firstSessionTs = 0;
    let prevSessionTs = 0;
    const clsObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.hadRecentInput) continue;
        const val = Number(entry.value || 0);
        if (
          state.clsSessionValue > 0 &&
          entry.startTime - prevSessionTs < 1000 &&
          entry.startTime - firstSessionTs < 5000
        ) {
          state.clsSessionValue += val;
        } else {
          state.clsSessionValue = val;
          firstSessionTs = entry.startTime;
        }
        prevSessionTs = entry.startTime;
        if (state.clsSessionValue > state.clsMaxSessionValue) {
          state.clsMaxSessionValue = state.clsSessionValue;
        }
        state.clsScore = Number(state.clsMaxSessionValue.toFixed(4));
        if (state.clsEntries.length < 15 && val > 0.001) {
          const sources = Array.isArray(entry.sources)
            ? entry.sources.slice(0, 3).map((s) => ({
                selector: describeNode(s.node),
                previousRect: s.previousRect
                  ? { x: Math.round(s.previousRect.x), y: Math.round(s.previousRect.y), w: Math.round(s.previousRect.width), h: Math.round(s.previousRect.height) }
                  : null,
                currentRect: s.currentRect
                  ? { x: Math.round(s.currentRect.x), y: Math.round(s.currentRect.y), w: Math.round(s.currentRect.width), h: Math.round(s.currentRect.height) }
                  : null,
              }))
            : [];
          state.clsEntries.push({
            startTimeMs: Math.round(entry.startTime),
            value: Number(val.toFixed(4)),
            sources,
          });
        }
      }
    });
    clsObserver.observe({ type: "layout-shift", buffered: true });
    state.clsSupported = true;
  } catch {}

  try {
    const paintObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.name === "first-contentful-paint" && state.fcpMs === null) {
          state.fcpMs = Math.round(entry.startTime);
        }
      }
    });
    paintObserver.observe({ type: "paint", buffered: true });
  } catch {}

  try {
    const lcpObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        state.lcpMs = Math.round(entry.startTime);
        state.lcpElement = describeNode(entry.element);
      }
    });
    lcpObserver.observe({ type: "largest-contentful-paint", buffered: true });
  } catch {}

  try {
    const longTaskObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const dur = Number(entry.duration || 0);
        state.longTaskCount += 1;
        state.longTaskTotalMs = Math.round(state.longTaskTotalMs + dur);
        if (dur > state.longTaskMaxMs) {
          state.longTaskMaxMs = Math.round(dur);
        }
        if (state.longTasks.length < 15) {
          state.longTasks.push({
            startTimeMs: Math.round(entry.startTime),
            durationMs: Math.round(dur),
          });
        }
      }
    });
    longTaskObserver.observe({ type: "longtask", buffered: true });
  } catch {}
})();
`;

export async function collectUiHealth(page) {
  try {
    return await page.evaluate(async () => {
      const vitals = window.__flpWebVitals || null;
      const clsScore = vitals?.clsSupported
        ? Number(vitals.clsScore.toFixed(4))
        : null;
      const navEntry = performance.getEntriesByType("navigation")[0];
      const navigationTiming =
        navEntry && "responseStart" in navEntry
          ? {
              ttfbMs: Math.round(
                Math.max(0, navEntry.responseStart - navEntry.requestStart),
              ),
              ttlbMs: Math.round(
                Math.max(0, navEntry.responseEnd - navEntry.requestStart),
              ),
              domInteractiveMs: Math.round(
                Math.max(0, navEntry.domInteractive),
              ),
              domCompleteMs: Math.round(Math.max(0, navEntry.domComplete)),
            }
          : null;

      const docEl = document.documentElement;
      const horizontalOverflowPx = docEl
        ? Math.max(0, Math.round(docEl.scrollWidth - window.innerWidth))
        : 0;
      const stuckSkeletonCount = document.querySelectorAll(
        '[aria-busy="true"], [data-testid*="skeleton"]',
      ).length;
      const alerts = Array.from(document.querySelectorAll('[role="alert"]'))
        .map((el) => (el.textContent || "").trim())
        .filter(Boolean);

      const mem = performance.memory
        ? {
            jsHeapUsedBytes: performance.memory.usedJSHeapSize,
            jsHeapTotalBytes: performance.memory.totalJSHeapSize,
            jsHeapLimitBytes: performance.memory.jsHeapSizeLimit,
          }
        : null;

      let storage = null;
      if (
        navigator.storage &&
        typeof navigator.storage.estimate === "function"
      ) {
        try {
          const est = await navigator.storage.estimate();
          storage = {
            usageBytes: est.usage ?? null,
            quotaBytes: est.quota ?? null,
          };
        } catch {}
      }

      return {
        measurementScope: "document-navigation",
        timeOriginEpochMs: performance.timeOrigin,
        measuredAtEpochMs: Date.now(),
        documentAgeMs: Math.round(performance.now()),
        webVitalsAvailable: Boolean(vitals),
        clsSupported: vitals?.clsSupported === true,
        clsScore,
        clsWarning: clsScore === null ? null : clsScore > 0.1,
        clsPoor: clsScore === null ? null : clsScore > 0.25,
        clsEntries: vitals?.clsEntries || [],
        fcpMs: vitals?.fcpMs ?? null,
        lcpMs: vitals?.lcpMs ?? null,
        lcpElement: vitals?.lcpElement ?? null,
        longTaskCount: vitals?.longTaskCount ?? null,
        longTaskTotalMs: vitals?.longTaskTotalMs ?? null,
        longTaskMaxMs: vitals?.longTaskMaxMs ?? null,
        longTasks: vitals?.longTasks || [],
        horizontalOverflowPx,
        hasHorizontalOverflow: horizontalOverflowPx > 4,
        stuckSkeletonCount,
        errorBannerCount: alerts.length,
        errorBannerTexts: alerts.slice(0, 5),
        navigationTiming,
        memory: mem,
        storage,
      };
    });
  } catch {
    return null;
  }
}

/**
 * Attach request/response waterfall listeners to a Playwright Page to record
 * every /api/* call, HTTP status, Range headers (206 vs 200), vCPU header,
 * CDN cache status, query telemetry beacons, and SSE ingest events.
 */
export function attachApiWaterfallRecorder(page, t0 = Date.now()) {
  const pendingByReq = new Map();
  const completedCalls = [];
  let extentsFreshnessLagS = null;
  let extentsMaxTsIso = null;

  const parseReqMeta = (req) => {
    const url = req.url();
    if (!url.includes("/api/") && !url.includes("/fos/")) return null;
    let endpoint = url;
    let dataset = null;
    let key = null;
    try {
      const parsed = new URL(url);
      if (
        parsed.pathname.startsWith("/fos/") &&
        parsed.pathname.endsWith(".parquet")
      ) {
        endpoint = "/api/file";
        key = decodeURIComponent(parsed.pathname.slice(5));
      } else {
        endpoint = parsed.pathname;
        dataset = parsed.searchParams.get("dataset");
        key = parsed.searchParams.get("key");
      }
    } catch {}
    const headers = req.headers();
    const rangeHeader = headers["range"] || null;
    let queryTelemetryBeacon = null;
    let ingestApplySpans = null;
    if (req.method() === "POST" && endpoint === "/api/telemetry/query") {
      try {
        queryTelemetryBeacon = JSON.parse(req.postData() || "null");
      } catch {}
    } else if (req.method() === "POST" && endpoint === "/api/traces") {
      try {
        const body = JSON.parse(req.postData() || "null");
        const spans = Array.isArray(body?.spans) ? body.spans : [];
        const applySpans = spans.filter(
          (s) => s && s.name === "dashboard.ingest.apply",
        );
        if (applySpans.length > 0) {
          ingestApplySpans = applySpans;
        }
      } catch {}
    }
    return {
      url,
      endpoint,
      dataset,
      key,
      method: req.method(),
      rangeHeader,
      isRangeProbe: rangeHeader === "bytes=0-0",
      startMs: Date.now() - t0,
      startEpochMs: Date.now(),
      queryTelemetryBeacon,
      ingestApplySpans,
    };
  };

  const onRequest = (req) => {
    const meta = parseReqMeta(req);
    if (meta) {
      pendingByReq.set(req, meta);
    }
  };

  const onResponse = async (res) => {
    const req = res.request();
    const meta = pendingByReq.get(req) || parseReqMeta(req);
    if (!meta) return;
    const responseHeadersAtMs = Date.now() - t0;
    const status = res.status();
    const headers = res.headers();
    const contentRange = headers["content-range"] || null;
    const xCache = headers["x-flp-cdn-cache"] || headers["x-cache"] || null;
    const cdnHit = Boolean(
      xCache && String(xCache).toUpperCase().includes("HIT"),
    );
    const vcpuRaw = headers["x-flp-vcpu-ms"];
    const wallRaw = headers["x-flp-wall-ms"];
    const vcpuMs = vcpuRaw !== undefined ? Number.parseFloat(vcpuRaw) : null;
    const wallMs = wallRaw !== undefined ? Number.parseFloat(wallRaw) : null;
    const traceId = headers["x-flp-trace-id"] || null;
    const clHeader = headers["content-length"];
    let bytes = clHeader !== undefined ? Number.parseInt(clHeader, 10) : null;

    if (meta.endpoint !== "/api/ingest/events") {
      try {
        if (await res.finished()) return;
      } catch {
        return;
      }
    }

    if (meta.endpoint === "/api/logs/extents" && status === 200) {
      try {
        const body = await res.json();
        const maxTs = body?.max_ts || body?.maxTs || null;
        if (maxTs) {
          extentsMaxTsIso = String(maxTs);
          const parsedMs =
            typeof maxTs === "number" ? maxTs : Date.parse(String(maxTs));
          if (Number.isFinite(parsedMs) && parsedMs > 0) {
            extentsFreshnessLagS = Number(
              Math.max(0, (Date.now() - parsedMs) / 1000).toFixed(2),
            );
          }
        }
      } catch {}
    }

    let finishedAtMs = responseHeadersAtMs;
    try {
      const timing = req.timing();
      if (timing && timing.responseEnd > 0) {
        finishedAtMs = meta.startMs + Math.round(timing.responseEnd);
      }
    } catch {}

    const ttfbMs = Math.max(0, responseHeadersAtMs - meta.startMs);
    const durationMs = Math.max(ttfbMs, finishedAtMs - meta.startMs);
    const downloadMs = Math.max(0, durationMs - ttfbMs);

    completedCalls.push({
      ...meta,
      status,
      contentRange,
      isRangeRead: status === 206 || Boolean(meta.rangeHeader),
      ttfbMs,
      downloadMs,
      durationMs,
      bytes: Number.isFinite(bytes) ? bytes : null,
      xCache,
      cdnHit,
      vcpuMs: Number.isFinite(vcpuMs) ? vcpuMs : null,
      wallMs: Number.isFinite(wallMs) ? wallMs : null,
      traceId,
      failed: status >= 400,
    });
    pendingByReq.delete(req);
  };

  const onRequestFailed = (req) => {
    const meta = pendingByReq.get(req) || parseReqMeta(req);
    if (!meta) return;
    // Ignore aborted SSE stream on navigation close
    if (meta.endpoint === "/api/ingest/events") return;
    const endMs = Date.now() - t0;
    completedCalls.push({
      ...meta,
      status: 0,
      contentRange: null,
      isRangeRead: Boolean(meta.rangeHeader),
      ttfbMs: null,
      downloadMs: null,
      durationMs: Math.max(0, endMs - meta.startMs),
      bytes: null,
      xCache: null,
      cdnHit: false,
      vcpuMs: null,
      wallMs: null,
      traceId: null,
      failed: true,
      failureText: req.failure()?.errorText || "request_failed",
    });
    pendingByReq.delete(req);
  };

  page.on("request", onRequest);
  page.on("response", onResponse);
  page.on("requestfailed", onRequestFailed);

  return {
    getWaterfall: () => completedCalls,
    getPending: () =>
      Array.from(pendingByReq.values(), (request) => ({
        ...request,
        pendingMs: Math.max(0, Date.now() - t0 - request.startMs),
      })),
    getSummary: () => summarizeApiCalls(completedCalls),
    getExtentsFreshness: () => ({
      extentsMaxTsIso,
      extentsFreshnessLagS,
    }),
    detach: () => {
      page.off("request", onRequest);
      page.off("response", onResponse);
      page.off("requestfailed", onRequestFailed);
    },
  };
}

async function checkSessionCookie(baseUrl, storageStatePath) {
  if (!fs.existsSync(storageStatePath)) {
    return { valid: false, reason: "browser_state_missing" };
  }
  try {
    const state = JSON.parse(fs.readFileSync(storageStatePath, "utf8"));
    const cookies = Array.isArray(state.cookies) ? state.cookies : [];
    if (cookies.length === 0) {
      return { valid: false, reason: "no_cookies_in_state" };
    }
    const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join("; ");
    const res = await fetch(`${baseUrl}/api/auth/session`, {
      headers: { Cookie: cookieHeader },
    });
    if (!res.ok) {
      return { valid: false, reason: `http_${res.status}` };
    }
    const body = await res.json();
    if (body && body.user && body.user.id) {
      return {
        valid: true,
        user: {
          id: body.user.id,
          name: body.user.name,
          role: body.user.role,
        },
        computeVersion: body.compute_service_version ?? null,
      };
    }
    return { valid: false, reason: "session_user_null" };
  } catch (err) {
    return { valid: false, reason: String(err.message || err) };
  }
}

async function signInWithPasskey(
  browser,
  baseUrl,
  storageStatePath,
  passkeysPath,
) {
  if (!fs.existsSync(passkeysPath)) {
    throw new Error(`Passkeys file not found at ${passkeysPath}`);
  }
  const passkeys = JSON.parse(fs.readFileSync(passkeysPath, "utf8"));
  const credentials = Array.isArray(passkeys)
    ? passkeys
    : Array.isArray(passkeys?.credentials)
      ? passkeys.credentials
      : [];
  if (credentials.length === 0) {
    throw new Error(`No credentials in ${passkeysPath}`);
  }

  const context = await browser.newContext();
  const page = await context.newPage();
  const cdp = await context.newCDPSession(page);
  await cdp.send("WebAuthn.enable", { enableUI: false });
  const { authenticatorId } = await cdp.send(
    "WebAuthn.addVirtualAuthenticator",
    {
      options: {
        protocol: "ctap2",
        transport: "internal",
        hasResidentKey: true,
        hasUserVerification: true,
        isUserVerified: true,
        automaticPresenceSimulation: true,
      },
    },
  );
  for (const cred of credentials) {
    await cdp.send("WebAuthn.addCredential", {
      authenticatorId,
      credential: cred,
    });
  }

  await page.goto(`${baseUrl}/sign-in`, {
    waitUntil: "domcontentloaded",
    timeout: 30_000,
  });
  const signInBtn = page.getByRole("button", {
    name: /sign in with a passkey/i,
  });
  await signInBtn.waitFor({ state: "visible", timeout: 15_000 });
  await signInBtn.click();
  await page.waitForURL((url) => !url.pathname.startsWith("/sign-in"), {
    timeout: 30_000,
  });
  const updated = await cdp.send("WebAuthn.getCredentials", {
    authenticatorId,
  });
  if (Array.isArray(updated?.credentials) && updated.credentials.length > 0) {
    const outPayload = Array.isArray(passkeys)
      ? updated.credentials
      : { ...passkeys, credentials: updated.credentials };
    fs.writeFileSync(passkeysPath, JSON.stringify(outPayload, null, 2) + "\n");
  }
  await context.storageState({ path: storageStatePath });
  await context.close();
}

async function ensureAuth(browser, args) {
  const storageStatePath = path.join(args.stateDir, "browser-state.json");
  const passkeysPath = path.join(args.stateDir, "passkeys.json");

  let check = await checkSessionCookie(args.baseUrl, storageStatePath);
  let renewed = false;
  if (!check.valid) {
    await signInWithPasskey(
      browser,
      args.baseUrl,
      storageStatePath,
      passkeysPath,
    );
    renewed = true;
    check = await checkSessionCookie(args.baseUrl, storageStatePath);
    if (!check.valid) {
      throw new Error(
        `Passkey sign-in completed but session still invalid: ${check.reason}`,
      );
    }
  }
  return {
    ok: true,
    renewed,
    user: check.user,
    computeVersion: check.computeVersion,
    storageStatePath,
  };
}

async function createInstrumentedContext(browser, storageStatePath) {
  const context = await browser.newContext({
    storageState: storageStatePath,
    viewport: { width: 1600, height: 1000 },
  });
  await context.addInitScript(WEB_VITALS_INIT_SCRIPT);
  return context;
}

export async function assertMonitoredService(context, baseUrl, serviceId) {
  if (!serviceId) return;
  const response = await context.request.get(`${baseUrl}/api/services`);
  if (!response.ok()) {
    throw new Error(`Monitored service preflight HTTP ${response.status()}`);
  }
  const { services } = await response.json();
  if (
    !Array.isArray(services) ||
    services.length !== 1 ||
    services[0]?.service_id !== serviceId
  ) {
    throw new Error(
      `Expected sole configured monitored service ${serviceId}; refusing to measure another identity`,
    );
  }
}

async function readLastFetchLocator(page) {
  try {
    const loc = page.locator(LAST_FETCH_SELECTOR).first();
    if ((await loc.count()) === 0) return null;
    const text = await loc.textContent({ timeout: 2_000 });
    return parseLastFetchText(text);
  } catch {
    return null;
  }
}

export async function runDashboardCase(
  page,
  caseLabel,
  measure,
  timeoutMs = 120_000,
) {
  let result;
  let timer;
  try {
    result = await Promise.race([
      measure(),
      new Promise((_, reject) => {
        timer = setTimeout(
          () =>
            reject(
              new Error(
                `${caseLabel}: dashboard not ready within ${timeoutMs}ms`,
              ),
            ),
          timeoutMs,
        );
      }),
    ]);
    if (result.freshReady === false) {
      throw new Error(`${caseLabel}: fresh dashboard result not ready`);
    }
    return result;
  } catch (error) {
    const failure = { caseLabel, error: String(error), result };
    try {
      failure.document = await page.evaluate(() => ({
        url: location.href,
        text: document.body?.innerText ?? "",
        totals: Array.from(
          document.querySelectorAll('[data-testid="total-requests"]'),
          (element) => ({
            text: element.textContent,
            fresh: element.getAttribute("data-fresh"),
          }),
        ),
      }));
      failure.uiHealth = await collectUiHealth(page);
    } catch (captureError) {
      failure.captureError = String(captureError);
    }
    error.dashboardFailure = failure;
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function measureDashboardOnce(
  context,
  baseUrl,
  query = "",
  timeoutMs = 120_000,
  allowTimeout = false,
  existingPage = null,
) {
  const page = existingPage ?? (await context.newPage());
  const shouldClosePage = !existingPage;
  const consoleErrors = [];
  const pageErrors = [];
  const httpErrors = [];
  let parquetFilesFetched = 0;
  let rangeReadCount = 0;
  let rangeProbeCount = 0;
  let fullFileDownloadCount = 0;
  const fetchedKeys = new Set();
  let manifestRequests = 0;
  let firstParquetStartMs = null;
  let lastParquetEndMs = 0;

  const t0 = Date.now();
  const apiRecorder = attachApiWaterfallRecorder(page, t0);

  const onConsole = (m) => {
    if (m.type() === "error") {
      consoleErrors.push({ url: page.url(), message: m.text() });
    }
  };
  const onPageError = (e) =>
    pageErrors.push({ url: page.url(), message: String(e) });
  const onRequest = (req) => {
    if (parquetKey(req.url()) && firstParquetStartMs === null) {
      firstParquetStartMs = Date.now() - t0;
    }
  };
  const onResponse = (res) => {
    const url = res.url();
    const status = res.status();
    if (status >= 400) {
      httpErrors.push({ url, status });
    }
    if (url.includes("/api/files?")) {
      manifestRequests += 1;
    }
    const pKey = parquetKey(url);
    if (pKey) {
      const rangeHeader = res.request().headers()["range"] || null;
      if (rangeHeader === "bytes=0-0") {
        rangeProbeCount += 1;
      } else if (status === 206 || rangeHeader) {
        rangeReadCount += 1;
      } else {
        fullFileDownloadCount += 1;
      }
      parquetFilesFetched += 1;
      fetchedKeys.add(pKey);
      lastParquetEndMs = Date.now() - t0;
    }
  };

  page.on("console", onConsole);
  page.on("pageerror", onPageError);
  page.on("request", onRequest);
  page.on("response", onResponse);

  try {
    const targetUrl = `${baseUrl}/dashboard${query}`;
    if (existingPage && page.url() === targetUrl) {
      await page.reload({
        waitUntil: "domcontentloaded",
        timeout: 60_000,
      });
    } else {
      await page.goto(targetUrl, {
        waitUntil: "domcontentloaded",
        timeout: 60_000,
      });
    }
    // A reload paints the persisted result first; wait for the fresh run
    // or an error state (Retry button) so failed queries do not hang until timeout.
    let freshReady = true;
    const retryBtn = page.getByRole("button", { name: "Retry", exact: true });
    try {
      const outcome = await Promise.race([
        page
          .locator(FRESH_TOTAL)
          .waitFor({ state: "visible", timeout: timeoutMs })
          .then(() => "fresh"),
        retryBtn
          .waitFor({ state: "visible", timeout: timeoutMs })
          .then(() => "error"),
      ]);
      if (outcome === "error") {
        freshReady = false;
      }
    } catch (error) {
      if (!allowTimeout) throw error;
      freshReady = false;
      pageErrors.push({ url: page.url(), message: String(error) });
    }
    await page
      .getByText("Loading Progress")
      .waitFor({ state: "hidden", timeout: 30_000 })
      .catch(() => {});

    const totalLoadMs = Date.now() - t0;
    const visibleAtEpochS = Date.now() / 1000;
    const totalRequestsText = freshReady
      ? await page.locator(FRESH_TOTAL).textContent()
      : "";
    const totalRequests = parseFormattedCount(totalRequestsText);
    const lastFetch = await readLastFetchLocator(page);
    const uiHealth = await collectUiHealth(page);
    if (!freshReady && uiHealth?.errorBannerTexts?.length) {
      for (const bannerText of uiHealth.errorBannerTexts) {
        pageErrors.push({ url: page.url(), message: bannerText });
      }
      if (!allowTimeout) {
        throw new Error(uiHealth.errorBannerTexts.join("\n"));
      }
    }
    const navigationTiming = uiHealth?.navigationTiming ?? null;

    const parquetDownloadMs =
      firstParquetStartMs !== null && lastParquetEndMs >= firstParquetStartMs
        ? lastParquetEndMs - firstParquetStartMs
        : 0;
    const workerPostDownloadMs =
      lastParquetEndMs > 0 ? Math.max(0, totalLoadMs - lastParquetEndMs) : 0;

    // Give trailing query telemetry beacon and background CacheStorage/OPFS storeQueue writes a moment to flush
    await page
      .waitForTimeout(parquetFilesFetched > 0 ? 250 : 100)
      .catch(() => {});
    const apiWaterfall = apiRecorder.getWaterfall();
    const apiSummary = apiRecorder.getSummary();
    const extentsFreshness = apiRecorder.getExtentsFreshness();

    return {
      totalLoadMs,
      navigationTtfbMs: navigationTiming?.ttfbMs ?? null,
      navigationTtlbMs: navigationTiming?.ttlbMs ?? null,
      parquetFilesFetched: fetchedKeys.size || parquetFilesFetched,
      parquetHttpRequests: parquetFilesFetched,
      rangeProbeCount,
      rangeReadCount,
      fullFileDownloadCount,
      fetchedKeys,
      manifestRequests,
      preParquetBootstrapMs: firstParquetStartMs ?? 0,
      parquetDownloadMs,
      workerPostDownloadMs,
      totalRequests,
      totalRequestsText: (totalRequestsText || "").trim(),
      visibleAtEpochS,
      lastFetch,
      uiHealth,
      extentsFreshness,
      apiSummary,
      apiWaterfall,
      consoleErrors,
      pageErrors,
      httpErrors,
      freshReady,
    };
  } finally {
    page.off("console", onConsole);
    page.off("pageerror", onPageError);
    page.off("request", onRequest);
    page.off("response", onResponse);
    apiRecorder.detach();
    if (shouldClosePage) {
      await page.close();
    }
  }
}

const LOADING_REGION_SELECTOR =
  'section[aria-label="Dashboard loading progress"]';
const ANALYSIS_STAGE_SOURCE =
  'DOM: LoadingProgress <ol aria-label="Loading stages"> li[aria-current="step"] = "Analysis", MutationObserver performance.now() from becoming current until it stops being current';

// LoadingProgress renders no durations, so the Analysis stage is timed from DOM transitions as they commit.
export const LOADING_STAGE_INIT_SCRIPT = `
(() => {
  const state = { timeline: [], current: undefined };
  window.__flpLoadingStages = state;
  const read = () => {
    const steps = Array.from(
      document.querySelectorAll(
        'ol[aria-label="Loading stages"] li[aria-current="step"]',
      ),
      (step) => step.textContent.trim(),
    );
    return steps.includes("Analysis") ? "Analysis" : (steps[0] ?? null);
  };
  const check = () => {
    const stage = read();
    if (stage === state.current) return;
    state.current = stage;
    state.timeline.push({
      stage,
      atMs: performance.now(),
      regionPresent: !!document.querySelector(
        ${JSON.stringify(LOADING_REGION_SELECTOR)},
      ),
    });
  };
  new MutationObserver(check).observe(document, {
    subtree: true,
    childList: true,
    attributes: true,
    attributeFilter: ["aria-current"],
    characterData: true,
  });
})();
`;

export function summarizeAnalysisStage(timeline) {
  const entries = Array.isArray(timeline)
    ? timeline.filter((entry, index) => index > 0 || entry.stage !== null)
    : null;
  const base = { source: ANALYSIS_STAGE_SOURCE, timeline: entries ?? [] };
  if (!entries) {
    return {
      ...base,
      durationMs: null,
      status: "unavailable",
      endReason: null,
      episodes: [],
    };
  }
  const episodes = [];
  let open = null;
  for (const entry of entries) {
    if (entry.stage === "Analysis") {
      open = { startMs: entry.atMs, endMs: null, endReason: null };
      episodes.push(open);
    } else if (open) {
      open.endMs = entry.atMs;
      open.endReason =
        entry.stage === null && !entry.regionPresent
          ? "region-removed"
          : `stage-changed:${entry.stage}`;
      open = null;
    }
  }
  const first = episodes[0] ?? null;
  if (!first) {
    return {
      ...base,
      durationMs: null,
      status: "not-observed",
      endReason: null,
      episodes,
    };
  }
  return {
    ...base,
    durationMs: first.endMs === null ? null : first.endMs - first.startMs,
    status: first.endMs === null ? "incomplete" : "complete",
    endReason: first.endReason,
    episodes,
  };
}

export function buildColdOnlyReport(
  serviceId,
  startedAtEpochS,
  cold,
  error,
  stageTimeline = null,
) {
  const lastFetch = cold?.lastFetch ?? null;
  const apiSummary = cold?.apiSummary ?? summarizeApiCalls([]);
  const errors = {
    measurementError: error ? String(error) : null,
    consoleErrors: cold?.consoleErrors ?? [],
    pageErrors: cold?.pageErrors ?? [],
    httpErrors: cold?.httpErrors ?? [],
  };
  const ok =
    !error &&
    cold?.freshReady === true &&
    errors.consoleErrors.length === 0 &&
    errors.pageErrors.length === 0 &&
    errors.httpErrors.length === 0;
  const visibleAtEpochS = cold?.visibleAtEpochS ?? null;
  return {
    ok,
    status: ok ? "complete" : "failed",
    mode: "cold-only",
    serviceId,
    trafficSent: false,
    startedAtEpochS,
    finishedAtEpochS: Date.now() / 1000,
    dashboard: {
      coldLoadMs: cold?.totalLoadMs ?? null,
      freshReady: cold?.freshReady ?? false,
      totalRequests: cold?.freshReady ? cold.totalRequests : null,
      totalRequestsText: cold?.totalRequestsText ?? "",
      visibleAtEpochS,
      visibleAtIso:
        visibleAtEpochS === null
          ? null
          : new Date(visibleAtEpochS * 1000).toISOString(),
      navigationTtfbMs: cold?.navigationTtfbMs ?? null,
      navigationTtlbMs: cold?.navigationTtlbMs ?? null,
      files: {
        filesListed: lastFetch?.filesListed ?? null,
        filesFetched: lastFetch?.filesFetched ?? null,
        filesCached: lastFetch?.filesCached ?? null,
        cdnHits: lastFetch?.cdnHits ?? null,
        parquetFilesFetched: cold?.parquetFilesFetched ?? 0,
        parquetHttpRequests: cold?.parquetHttpRequests ?? 0,
        rangeProbeCount: cold?.rangeProbeCount ?? 0,
        rangeReadCount: cold?.rangeReadCount ?? 0,
        fullFileDownloadCount: cold?.fullFileDownloadCount ?? 0,
        manifestRequests: cold?.manifestRequests ?? 0,
        fetchedKeys: cold?.fetchedKeys ? Array.from(cold.fetchedKeys) : [],
      },
      analysisStage: summarizeAnalysisStage(stageTimeline),
      lastFetchProxies: {
        note: "Not the Analysis stage: Last fetch elapsed and parquet-download-relative proxies",
        lastFetch,
        preParquetBootstrapMs: cold?.preParquetBootstrapMs ?? null,
        parquetDownloadMs: cold?.parquetDownloadMs ?? null,
        workerPostDownloadMs: cold?.workerPostDownloadMs ?? null,
      },
      catalog: Object.fromEntries(
        Object.entries(apiSummary.byEndpoint).filter(([endpoint]) =>
          endpoint.startsWith("/api/files"),
        ),
      ),
      extentsFreshness: cold?.extentsFreshness ?? null,
      uiHealth: cold?.uiHealth ?? null,
    },
    apiTelemetry: apiSummary,
    apiWaterfall: cold?.apiWaterfall ?? [],
    errors,
  };
}

/** One cold /dashboard load in a fresh context; never sends traffic or reloads. */
export async function measureColdDashboardOnly(context, args) {
  await assertMonitoredService(context, args.baseUrl, args.serviceId);
  await context.addInitScript(LOADING_STAGE_INIT_SCRIPT);
  const page = await context.newPage();
  const startedAtEpochS = Date.now() / 1000;
  let cold = null;
  let error = null;
  let stageTimeline = null;
  try {
    cold = await measureDashboardOnce(
      context,
      args.baseUrl,
      "",
      args.caseTimeoutMs,
      true,
      page,
    );
    // Bounded: an Analysis stage still open after this is reported incomplete, not timed.
    await page
      .locator(LOADING_REGION_SELECTOR)
      .waitFor({ state: "hidden", timeout: 30_000 })
      .catch(() => {});
  } catch (caught) {
    error = caught;
  }
  try {
    stageTimeline = await page.evaluate(
      () => window.__flpLoadingStages?.timeline ?? null,
    );
  } catch {
    stageTimeline = null;
  }
  await page.close().catch(() => {});
  return buildColdOnlyReport(
    args.serviceId,
    startedAtEpochS,
    cold,
    error,
    stageTimeline,
  );
}

function parquetKey(url) {
  try {
    const parsed = new URL(url);
    if (parsed.pathname === "/api/file") {
      return parsed.searchParams.get("key");
    }
    if (
      parsed.pathname.startsWith("/fos/") &&
      parsed.pathname.endsWith(".parquet")
    ) {
      return decodeURIComponent(parsed.pathname.slice(5));
    }
  } catch {}
  return null;
}

async function sendBurst(trafficUrl, count) {
  const run = `flp-incremental-${Date.now()}`;
  let ok = 0;
  let next = 0;
  async function lane() {
    while (next < count) {
      const i = next++;
      try {
        const res = await fetch(`${trafficUrl}/${run}/${i}`, {
          headers: { "user-agent": "flp-browser-verify-incremental" },
        });
        await res.arrayBuffer();
        ok += 1;
      } catch {
        // Counted as unsent below.
      }
    }
  }
  await Promise.all(Array.from({ length: 10 }, lane));
  return { run, sent: ok, unsent: count - ok };
}

// Proves a primed tab picks up newly ingested rows across several ingest
// cycles while downloading only Parquet keys it has never fetched before.
async function measureIncrementalReloads(
  context,
  baseUrl,
  args,
  seenKeys,
  existingPage = null,
) {
  // A rolling preset drops rows older than its start as time advances, so pin
  // one absolute range that covers the whole check.
  const minute = 60_000;
  const now = Date.now();
  const pinned = new URLSearchParams({
    from: new Date(
      Math.floor((now - 3_600_000) / minute) * minute,
    ).toISOString(),
    to: new Date(
      Math.ceil((now + args.incrementalTimeoutMs + 300_000) / minute) * minute,
    ).toISOString(),
  });
  const query = `?${pinned}`;
  const baselineReload = await reloadCounting(
    context,
    baseUrl,
    seenKeys,
    query,
    existingPage,
  );
  const baseline = baselineReload.totalRequests;
  const burst = await sendBurst(args.incrementalUrl, args.incrementalRequests);
  const target = baseline + burst.sent;
  const t0 = Date.now();
  const reloads = [baselineReload];
  let last = baselineReload;
  while (
    last.totalRequests < target &&
    Date.now() - t0 < args.incrementalTimeoutMs
  ) {
    await new Promise((r) => setTimeout(r, args.incrementalReloadMs));
    last = await reloadCounting(
      context,
      baseUrl,
      seenKeys,
      query,
      existingPage,
    );
    reloads.push({ ...last, atS: Math.round((Date.now() - t0) / 1000) });
  }
  // One more reload with nothing new must download nothing.
  const settled = await reloadCounting(
    context,
    baseUrl,
    seenKeys,
    query,
    existingPage,
  );
  reloads.push({ ...settled, atS: Math.round((Date.now() - t0) / 1000) });
  const refetched = reloads.flatMap((r) => r.refetchedKeys);
  const newDataVisible = settled.totalRequests >= target;
  const settledFetchOnlyNew =
    settled.parquetFilesFetched === settled.newKeys.length;
  return {
    ok:
      burst.unsent === 0 &&
      newDataVisible &&
      refetched.length === 0 &&
      baselineReload.refetchedKeys.length === 0 &&
      settledFetchOnlyNew,
    burst,
    range: { from: pinned.get("from"), to: pinned.get("to") },
    baselineTotal: baseline,
    targetTotal: target,
    finalTotal: settled.totalRequests,
    newDataVisible,
    waitedS: Math.round((Date.now() - t0) / 1000),
    refetchedKeys: refetched,
    reloads: reloads.map(
      ({
        refetchedKeys,
        newKeys,
        consoleErrors,
        pageErrors,
        httpErrors,
        ...r
      }) => ({
        ...r,
        newFiles: newKeys.length,
        refetched: refetchedKeys.length,
      }),
    ),
    consoleErrors: reloads.flatMap((r) => r.consoleErrors),
    pageErrors: reloads.flatMap((r) => r.pageErrors),
    httpErrors: reloads.flatMap((r) => r.httpErrors),
  };
}

async function reloadCounting(
  context,
  baseUrl,
  seenKeys,
  query = "",
  existingPage = null,
) {
  const newKeys = [];
  const refetchedKeys = [];
  const uniqueKeysInReload = new Set();
  const onResponse = (res) => {
    const key = parquetKey(res.url());
    if (key === null) return;
    // Ignore 0-byte range probes or subsequent range slices of the same key within the same reload
    if (uniqueKeysInReload.has(key)) return;
    uniqueKeysInReload.add(key);
    if (seenKeys.has(key)) refetchedKeys.push(key);
    else {
      seenKeys.add(key);
      newKeys.push(key);
    }
  };
  context.on("response", onResponse);
  try {
    const run = await measureWarmDashboardReload(
      context,
      baseUrl,
      query,
      existingPage,
    );
    return {
      loadMs: run.warmLoadMs,
      totalRequests: run.totalRequests,
      visibleAtEpochS: run.visibleAtEpochS,
      parquetFilesFetched: uniqueKeysInReload.size,
      newKeys,
      refetchedKeys,
      lastFetch: run.lastFetch,
      uiHealth: run.uiHealth,
      apiSummary: run.apiSummary,
      apiWaterfall: run.apiWaterfall,
      consoleErrors: run.consoleErrors,
      pageErrors: run.pageErrors,
      httpErrors: run.httpErrors,
    };
  } finally {
    context.off("response", onResponse);
  }
}

async function measureWarmDashboardReload(
  context,
  baseUrl,
  query = "",
  existingPage = null,
) {
  const page = existingPage ?? (await context.newPage());
  const shouldClosePage = !existingPage;
  const consoleErrors = [];
  const pageErrors = [];
  const httpErrors = [];

  const t0 = Date.now();
  const apiRecorder = attachApiWaterfallRecorder(page, t0);

  const onConsole = (m) => {
    if (m.type() === "error") {
      consoleErrors.push({ url: page.url(), message: m.text() });
    }
  };
  const onPageError = (e) =>
    pageErrors.push({ url: page.url(), message: String(e) });
  const onResponse = (res) => {
    if (res.status() >= 400) {
      httpErrors.push({
        url: res.url(),
        method: res.request().method(),
        status: res.status(),
        rsc: res.request().headers()["rsc"] ?? null,
      });
    }
  };

  page.on("console", onConsole);
  page.on("pageerror", onPageError);
  page.on("response", onResponse);

  try {
    // Measure warm load in the already-primed tab, preserving sessionStorage,
    // CacheStorage, OPFS, and localStorage like a normal user reloading.
    const targetUrl = `${baseUrl}/dashboard${query}`;
    const currentUrl = page.url();
    const isSameDashboardRoute =
      existingPage &&
      (currentUrl === targetUrl ||
        (query === "" && currentUrl === `${baseUrl}/dashboard?range=24h`));
    if (isSameDashboardRoute) {
      await page.reload({
        waitUntil: "domcontentloaded",
        timeout: 60_000,
      });
    } else {
      await page.goto(targetUrl, {
        waitUntil: "domcontentloaded",
        timeout: 60_000,
      });
    }
    // A reload paints the persisted result first; wait for the fresh run.
    await page.locator(FRESH_TOTAL).waitFor({
      state: "visible",
      timeout: 120_000,
    });
    await page
      .getByText("Loading Progress")
      .waitFor({ state: "hidden", timeout: 30_000 })
      .catch(() => {});
    const warmLoadMs = Date.now() - t0;
    const totalRequestsText = await page.locator(FRESH_TOTAL).textContent();
    const visibleAtEpochS = Date.now() / 1000;
    const lastFetch = await readLastFetchLocator(page);
    const uiHealth = await collectUiHealth(page);
    const navigationTiming = uiHealth?.navigationTiming ?? null;

    // Verify Cards popover and FieldSearchDialog controls on /dashboard
    const cardsBtn = page.getByRole("button", { name: /^Cards/i });
    await cardsBtn.waitFor({ state: "visible", timeout: 15_000 });
    const fieldSearchBtn = page.getByRole("button", {
      name: "Search Client IP",
    });
    await fieldSearchBtn.waitFor({ state: "visible", timeout: 15_000 });

    await page.waitForTimeout(100).catch(() => {});
    const apiWaterfall = apiRecorder.getWaterfall();
    const apiSummary = apiRecorder.getSummary();

    return {
      warmLoadMs,
      navigationTtfbMs: navigationTiming?.ttfbMs ?? null,
      navigationTtlbMs: navigationTiming?.ttlbMs ?? null,
      totalRequests: parseFormattedCount(totalRequestsText),
      visibleAtEpochS,
      lastFetch,
      uiHealth,
      apiSummary,
      apiWaterfall,
      consoleErrors,
      pageErrors,
      httpErrors,
    };
  } finally {
    page.off("console", onConsole);
    page.off("pageerror", onPageError);
    page.off("response", onResponse);
    apiRecorder.detach();
    if (shouldClosePage) {
      await page.close();
    }
  }
}

/**
 * Measure a single non-dashboard route (clean or filtered) with its own API waterfall & UI health.
 */
async function measureRouteWithTelemetry(page, url, waitFn) {
  const t0 = Date.now();
  const apiRecorder = attachApiWaterfallRecorder(page, t0);
  try {
    const res = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 45_000,
    });
    const status = res ? res.status() : 0;
    if (status === 404) {
      return { deployed: false, status: 404 };
    }
    if (waitFn) {
      await waitFn(page);
    }
    const loadMs = Date.now() - t0;
    const lastFetch = await readLastFetchLocator(page);
    const uiHealth = await collectUiHealth(page);
    await page.waitForTimeout(50).catch(() => {});
    const apiSummary = apiRecorder.getSummary();
    return {
      deployed: true,
      status,
      loadMs,
      lastFetch,
      uiHealth,
      apiSummary,
    };
  } finally {
    apiRecorder.detach();
  }
}

async function sweepPages(context, baseUrl, existingPage = null) {
  const page = existingPage ?? (await context.newPage());
  const shouldClosePage = !existingPage;
  const sweepRecorder = attachApiWaterfallRecorder(page);
  const consoleErrors = [];
  const pageErrors = [];
  const httpErrors = [];

  const onConsole = (m) => {
    if (m.type() === "error") {
      consoleErrors.push({ url: page.url(), message: m.text() });
    }
  };
  const onPageError = (e) =>
    pageErrors.push({ url: page.url(), message: String(e) });
  const onResponse = (res) => {
    // Allow 404 probes on optional future analytics pages without failing the run
    if (res.status() >= 400 && !res.__allow404) {
      httpErrors.push({
        url: res.url(),
        method: res.request().method(),
        status: res.status(),
        rsc: res.request().headers()["rsc"] ?? null,
      });
    }
  };

  page.on("console", onConsole);
  page.on("pageerror", onPageError);
  page.on("response", onResponse);

  const pagesReport = {};
  const routeCoverage = {};
  try {
    // 1. /logs + tabs
    {
      const t0 = Date.now();
      const logsRecorder = attachApiWaterfallRecorder(page, t0);
      await page.goto(`${baseUrl}/logs`, {
        waitUntil: "domcontentloaded",
        timeout: 45_000,
      });
      await page
        .getByRole("heading", { name: "Data Management" })
        .waitFor({ state: "visible", timeout: 30_000 });
      await page
        .getByRole("tab", { name: "Cron Runs" })
        .waitFor({ state: "visible", timeout: 15_000 });
      const overviewLoadMs = Date.now() - t0;
      const overviewUiHealth = await collectUiHealth(page);
      const tabs = {};
      for (const tabName of [
        "Cron Runs",
        "Service History",
        "FOS Usage",
        "Audit",
        "API & Query Telemetry",
        "Platform Errors",
        "Storage",
        "Quarantine",
        "Retention & Cleanup",
        "Schema",
      ]) {
        const tabStart = Date.now();
        const tabRecorder = attachApiWaterfallRecorder(page, tabStart);
        const tab = page.getByRole("tab", { name: tabName, exact: true });
        if (
          (tabName === "FOS Usage" || tabName === "API & Query Telemetry") &&
          (await tab.count()) === 0
        ) {
          tabRecorder.detach();
          continue;
        }
        await tab.waitFor({ state: "visible", timeout: 15_000 });
        await tab.click();
        if (tabName === "Schema") {
          await page
            .getByRole("heading", { name: /DuckDB Table Schema/i })
            .waitFor({ state: "visible", timeout: 15_000 });
        } else if (tabName === "Quarantine") {
          await page
            .getByRole("region", { name: "Quarantine" })
            .waitFor({ state: "visible", timeout: 15_000 });
        } else {
          await page
            .locator('[role="tabpanel"]:not([hidden])')
            .first()
            .waitFor({ state: "visible", timeout: 15_000 });
        }
        const tabLoadMs = Date.now() - tabStart;
        const tabApiSummary = tabRecorder.getSummary();
        tabRecorder.detach();
        tabs[tabName] = { loadMs: tabLoadMs, apiSummary: tabApiSummary };
      }
      const logsApiSummary = logsRecorder.getSummary();
      logsRecorder.detach();
      pagesReport["/logs"] = {
        loadMs: overviewLoadMs,
        uiHealth: overviewUiHealth,
        apiSummary: logsApiSummary,
        tabs,
      };
    }

    // 2. /admin
    {
      const res = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/admin`,
        async (p) => {
          await p
            .getByRole("heading", { name: "Admin", exact: true })
            .waitFor({ state: "visible", timeout: 30_000 });
        },
      );
      pagesReport["/admin"] = {
        loadMs: res.loadMs,
        uiHealth: res.uiHealth,
        apiSummary: res.apiSummary,
      };
    }

    // 3. /admin/tick
    {
      let healthyText = "";
      const res = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/admin/tick`,
        async (p) => {
          await p
            .getByRole("heading", { name: "Tick & Budget Health" })
            .waitFor({ state: "visible", timeout: 30_000 });
          await p
            .getByRole("heading", {
              name: /Stage Cadence, Budget Ceiling/i,
            })
            .waitFor({ state: "visible", timeout: 30_000 });
          // Match only the summary line, not the "Error" column header or a badge.
          const summaryLine = p
            .getByText(
              /^(\d+ Starved|\d+ Error|\d+ Catching up|All stages healthy)$/,
            )
            .first();
          healthyText = await summaryLine
            .waitFor({ state: "visible", timeout: 60_000 })
            .then(() => summaryLine.textContent())
            .catch(() => "");
        },
      );
      pagesReport["/admin/tick"] = {
        loadMs: res.loadMs,
        uiHealth: res.uiHealth,
        apiSummary: res.apiSummary,
        summary: {
          statusText: (healthyText || "").trim(),
          starved: (healthyText || "").includes("Starved") ? "1" : "0",
        },
      };
    }

    // 4. /usage (clean and filtered with EXTENDED_PRESETS / FilterBar)
    {
      const cleanUsage = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/usage`,
        async (p) => {
          await p
            .getByRole("heading", { name: "Usage & Cost" })
            .waitFor({ state: "visible", timeout: 30_000 });
        },
      );
      pagesReport["/usage"] = {
        loadMs: cleanUsage.loadMs,
        uiHealth: cleanUsage.uiHealth,
        apiSummary: cleanUsage.apiSummary,
      };

      const filteredUsage = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/usage${buildFilterQueryString("24h", "+status:200")}`,
        async (p) => {
          await p
            .getByRole("heading", { name: "Usage & Cost" })
            .waitFor({ state: "visible", timeout: 30_000 });
        },
      );
      pagesReport["/usage (filtered)"] = {
        loadMs: filteredUsage.loadMs,
        uiHealth: filteredUsage.uiHealth,
        apiSummary: filteredUsage.apiSummary,
      };
    }

    // 5. /how-it-works
    {
      const res = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/how-it-works`,
        async (p) => {
          await p
            .getByRole("heading", { level: 1 })
            .first()
            .waitFor({ state: "visible", timeout: 30_000 });
        },
      );
      pagesReport["/how-it-works"] = {
        loadMs: res.loadMs,
        uiHealth: res.uiHealth,
        apiSummary: res.apiSummary,
      };
    }

    // 6. /cache
    {
      const res = await measureRouteWithTelemetry(
        page,
        `${baseUrl}/cache`,
        async (p) => {
          await p
            .getByRole("heading", { name: "Browser Cache", exact: true })
            .waitFor({ state: "visible", timeout: 30_000 });
          await p
            .getByText("flp-parquet-v1", { exact: true })
            .waitFor({ state: "visible", timeout: 15_000 });
          await p
            .getByText(/^flp-parquet-rollup-[0-9a-z]+$/)
            .first()
            .waitFor({ state: "visible", timeout: 15_000 });
        },
      );
      pagesReport["/cache"] = {
        loadMs: res.loadMs,
        uiHealth: res.uiHealth,
        apiSummary: res.apiSummary,
      };
    }

    // 7. Discover any additional deployed analytics pages from FILTER_BAR_ROUTE_PREFIXES
    // (/origin, /security, /performance, /fastly-value, /network, /streaming, /rum, /sessions, /query)
    // via navigation links in the shell, and measure them both clean and filtered.
    const navHrefs = await page
      .evaluate(() =>
        Array.from(document.querySelectorAll("a[href]"))
          .map((a) => a.getAttribute("href"))
          .filter(Boolean),
      )
      .catch(() => []);
    const discoveredAnalyticsRoutes = optionalRoutesToProbe(navHrefs);
    for (const route of discoveredAnalyticsRoutes) {
      const probe = await context.request.get(`${baseUrl}${route}`);
      const probeStatus = probe.status();
      await probe.dispose();
      routeCoverage[route] = {
        probed: true,
        deployed: probeStatus !== 404,
        status: probeStatus,
      };
      if (probeStatus === 404) continue;
      if (probeStatus >= 400)
        throw new Error(`Optional route ${route} returned HTTP ${probeStatus}`);
      const cleanRes = await measureRouteWithTelemetry(
        page,
        `${baseUrl}${route}`,
        async (p) => {
          await p
            .getByRole("heading", { level: 1 })
            .first()
            .waitFor({ state: "visible", timeout: 30_000 });
        },
      );
      if (cleanRes.deployed) {
        pagesReport[route] = {
          loadMs: cleanRes.loadMs,
          lastFetch: cleanRes.lastFetch,
          uiHealth: cleanRes.uiHealth,
          apiSummary: cleanRes.apiSummary,
        };
        if (route === "/cloud-files") continue;
        const filtRes = await measureRouteWithTelemetry(
          page,
          `${baseUrl}${route}${buildFilterQueryString("24h", "+status:200")}`,
          async (p) => {
            await p
              .getByRole("heading", { level: 1 })
              .first()
              .waitFor({ state: "visible", timeout: 30_000 });
          },
        );
        pagesReport[`${route} (filtered)`] = {
          loadMs: filtRes.loadMs,
          lastFetch: filtRes.lastFetch,
          uiHealth: filtRes.uiHealth,
          apiSummary: filtRes.apiSummary,
        };
      }
    }

    return {
      pages: pagesReport,
      routeCoverage,
      apiWaterfall: sweepRecorder.getWaterfall(),
      consoleErrors,
      pageErrors,
      httpErrors,
    };
  } finally {
    sweepRecorder.detach();
    page.off("console", onConsole);
    page.off("pageerror", onPageError);
    page.off("response", onResponse);
    if (shouldClosePage) {
      await page.close();
    }
  }
}

/**
 * Compute p50 / p95 distributions across all dashboard loads, filtered vs clean cases,
 * page sweeps, CLS scores, and API calls.
 */
export function computeBenchmarkDistributions(
  coldDashboard,
  warmDashboard,
  benchmarkCases,
  pagesReport,
) {
  // Prefer warm browser-cache runs when both a cache-populate run and a warm run exist
  // for the same (iteration, range, filter) so normal user warm-cache latency is accurately reported.
  const effectiveCases = benchmarkCases.filter((c) => {
    if (c.cacheMode === "cold") return false;
    if (c.cacheMode !== "cache-populate") return true;
    return !benchmarkCases.some(
      (other) =>
        other.cacheMode === "warm" &&
        other.iteration === c.iteration &&
        other.range === c.range &&
        other.filter === c.filter,
    );
  });

  const cleanCases = effectiveCases.filter((c) => !c.filter);
  const filteredCases = effectiveCases.filter((c) => Boolean(c.filter));
  const coldCases = benchmarkCases.filter((c) => c.cacheMode === "cold");
  const cachePopulateCases = benchmarkCases.filter(
    (c) => c.cacheMode === "cache-populate",
  );
  const warmCases = benchmarkCases.filter((c) => c.cacheMode === "warm");
  const verifiedWarmLoadMs =
    warmDashboard.cacheMode === "warm" ? warmDashboard.warmLoadMs : null;

  const byRange = {};
  for (const r of DASHBOARD_PRESETS) {
    const matchingAll = benchmarkCases.filter((c) => c.range === r);
    const matchingEffective = effectiveCases.filter((c) => c.range === r);
    if (matchingAll.length > 0) {
      byRange[r] = {
        allLoadMs: summarizeNumericSeries(matchingAll.map((c) => c.loadMs)),
        cleanLoadMs: summarizeNumericSeries(
          matchingEffective.filter((c) => !c.filter).map((c) => c.loadMs),
        ),
        filteredLoadMs: summarizeNumericSeries(
          matchingEffective
            .filter((c) => Boolean(c.filter))
            .map((c) => c.loadMs),
        ),
        cachePopulateLoadMs: summarizeNumericSeries(
          matchingAll
            .filter((c) => c.cacheMode === "cache-populate")
            .map((c) => c.loadMs),
        ),
        warmLoadMs: summarizeNumericSeries(
          matchingAll
            .filter((c) => c.cacheMode === "warm")
            .map((c) => c.loadMs),
        ),
        parquetDownloadMs: summarizeNumericSeries(
          matchingEffective.map((c) => c.parquetDownloadMs),
        ),
        workerPostDownloadMs: summarizeNumericSeries(
          matchingEffective.map((c) => c.workerPostDownloadMs),
        ),
      };
    }
  }

  const byFilter = {};
  for (const c of effectiveCases) {
    const key = c.filter || "(unfiltered)";
    if (!byFilter[key]) byFilter[key] = [];
    byFilter[key].push(c.loadMs);
  }
  const byFilterStats = {};
  for (const [k, vals] of Object.entries(byFilter)) {
    byFilterStats[k] = summarizeNumericSeries(vals);
  }

  const allPageLoads = Object.values(pagesReport || {})
    .map((p) => p?.loadMs)
    .filter((v) => Number.isFinite(v));

  const allClsScores = [
    coldDashboard?.uiHealth?.clsScore,
    warmDashboard?.uiHealth?.clsScore,
    ...benchmarkCases.map((c) => c.uiHealth?.clsScore),
    ...Object.values(pagesReport || {}).map((p) => p?.uiHealth?.clsScore),
  ].filter((v) => v !== null && v !== undefined && Number.isFinite(v));

  const cleanStats = summarizeNumericSeries([
    coldDashboard.totalLoadMs,
    warmDashboard.warmLoadMs,
    ...cleanCases.map((c) => c.loadMs),
  ]);
  const warmCleanStats = summarizeNumericSeries([
    verifiedWarmLoadMs,
    ...warmCases.filter((c) => !c.filter).map((c) => c.loadMs),
  ]);
  const filteredStats = summarizeNumericSeries(
    filteredCases.map((c) => c.loadMs),
  );
  const warmFilteredStats = summarizeNumericSeries(
    warmCases.filter((c) => Boolean(c.filter)).map((c) => c.loadMs),
  );

  return {
    dashboardCleanLoadMs: cleanStats,
    cleanDashboardLoadMs: cleanStats,
    warmCleanDashboardLoadMs: warmCleanStats,
    dashboardFilteredLoadMs: filteredStats,
    filteredDashboardLoadMs: filteredStats,
    warmFilteredDashboardLoadMs: warmFilteredStats,
    dashboardCachePopulateLoadMs: summarizeNumericSeries(
      cachePopulateCases.map((c) => c.loadMs),
    ),
    dashboardColdCasesLoadMs: summarizeNumericSeries([
      coldDashboard.totalLoadMs,
      ...coldCases.map((c) => c.loadMs),
    ]),
    dashboardWarmCasesLoadMs: summarizeNumericSeries([
      verifiedWarmLoadMs,
      ...warmCases.map((c) => c.loadMs),
    ]),
    parquetDownloadMs: summarizeNumericSeries([
      coldDashboard.parquetDownloadMs,
      ...effectiveCases.map((c) => c.parquetDownloadMs),
    ]),
    workerPostDownloadMs: summarizeNumericSeries([
      coldDashboard.workerPostDownloadMs,
      ...effectiveCases.map((c) => c.workerPostDownloadMs),
    ]),
    pageSweepLoadMs: summarizeNumericSeries(allPageLoads),
    clsScore: summarizeNumericSeries(allClsScores),
    byRange,
    byFilter: byFilterStats,
  };
}

export function formatBenchmarkCaseEntry(
  iter,
  cacheMode,
  range,
  filter,
  query,
  result,
) {
  if (cacheMode !== "cold") cacheMode = classifyDashboardCache(result);
  return {
    iteration: iter + 1,
    cacheMode,
    range,
    filter,
    query,
    loadMs: result.totalLoadMs,
    navigationTtfbMs: result.navigationTtfbMs,
    navigationTtlbMs: result.navigationTtlbMs,
    parquetFilesFetched: result.parquetFilesFetched,
    parquetHttpRequests: result.parquetHttpRequests,
    rangeProbeCount: result.rangeProbeCount,
    rangeReadCount: result.rangeReadCount,
    fullFileDownloadCount: result.fullFileDownloadCount,
    parquetDownloadMs: result.parquetDownloadMs,
    workerPostDownloadMs: result.workerPostDownloadMs,
    totalRequests: result.totalRequests,
    lastFetch: result.lastFetch,
    uiHealth: result.uiHealth,
    extentsFreshness: result.extentsFreshness,
    apiSummary: result.apiSummary,
    apiWaterfall: result.apiWaterfall,
    errors: [
      ...(result.freshReady ? [] : [{ message: "fresh data timeout" }]),
      ...result.consoleErrors,
      ...result.pageErrors,
      ...result.httpErrors,
    ],
  };
}

function classifyDashboardCache(result) {
  if (result.freshReady === false) return "unverified";
  if (
    result.parquetFilesFetched > 0 ||
    result.lastFetch?.filesFetched > 0 ||
    result.apiWaterfall?.some((call) => call.endpoint === "/api/file")
  )
    return "cache-populate";
  return result.lastFetch?.filesFetched === 0 &&
    result.lastFetch?.filesCached > 0
    ? "warm"
    : "unverified";
}

export async function main(argv = process.argv) {
  const args = parseArgs(argv);
  const { chromium } = await import(PLAYWRIGHT_PATH);
  const browser = await chromium.launch({ headless: true });
  let dashboardPage;
  let sessionRecorder;
  const completedMeasurements = [];
  const writeProgress = (extra = {}) => {
    if (!args.outPath) return;
    fs.mkdirSync(path.dirname(path.resolve(args.outPath)), { recursive: true });
    fs.writeFileSync(
      args.outPath,
      `${JSON.stringify(
        {
          ok: false,
          status: "running",
          serviceId: args.serviceId,
          completedMeasurements,
          ...extra,
        },
        null,
        2,
      )}\n`,
      "utf8",
    );
  };
  const measureCase = async (label, measure) => {
    const result = await runDashboardCase(
      dashboardPage,
      label,
      measure,
      args.caseTimeoutMs,
    );
    completedMeasurements.push({
      caseLabel: label,
      ...result,
      fetchedKeys: result.fetchedKeys
        ? Array.from(result.fetchedKeys)
        : undefined,
    });
    writeProgress();
    return result;
  };
  try {
    if (args.mode === "cold-only" && !args.serviceId) {
      throw new Error("--service-id is required for --mode cold-only");
    }
    const auth = await ensureAuth(browser, args);
    if (args.mode === "ensure-auth") {
      console.log(JSON.stringify(auth));
      return auth;
    }
    if (args.mode === "cold-only") {
      const coldContext = await createInstrumentedContext(
        browser,
        auth.storageStatePath,
      );
      let report;
      try {
        report = await measureColdDashboardOnly(coldContext, args);
      } finally {
        await coldContext.close();
      }
      if (args.outPath) {
        fs.mkdirSync(path.dirname(path.resolve(args.outPath)), {
          recursive: true,
        });
        fs.writeFileSync(
          args.outPath,
          `${JSON.stringify(report, null, 2)}\n`,
          "utf8",
        );
      }
      console.log(JSON.stringify(report));
      if (!report.ok) {
        process.exitCode = 1;
      }
      return report;
    }
    if (args.mode === "sweep-only") {
      const verifyContext = await createInstrumentedContext(
        browser,
        auth.storageStatePath,
      );
      await assertMonitoredService(verifyContext, args.baseUrl, args.serviceId);
      const sweep = await sweepPages(verifyContext, args.baseUrl);
      await verifyContext.close();
      console.log(JSON.stringify(sweep, null, 2));
      return sweep;
    }

    // Mode: verify. Keep a single persistent tab (dashboardPage) open across the
    // user session so sessionStorage (flp-dashboard-cache-session), CacheStorage
    // (flp-parquet-v1 & flp-parquet-rollup-*), OPFS, localStorage, and HTTP cache
    // work together like a normal user's browser tab.
    const deadline = Date.now() + args.waitTimeoutMs;
    const activeContext = await createInstrumentedContext(
      browser,
      auth.storageStatePath,
    );
    await assertMonitoredService(activeContext, args.baseUrl, args.serviceId);
    dashboardPage = await activeContext.newPage();
    sessionRecorder = attachApiWaterfallRecorder(dashboardPage);
    const coldDashboard = await measureCase("cold default 24h", () =>
      measureDashboardOnce(
        activeContext,
        args.baseUrl,
        "",
        120_000,
        false,
        dashboardPage,
      ),
    );
    const seenKeys = new Set(coldDashboard.fetchedKeys);
    const waitReloads = [];
    let observedTotal = coldDashboard.totalRequests;
    let observedVisibleAtEpochS = coldDashboard.visibleAtEpochS;
    let pollAttempts = 1;
    while (
      args.expectMinRequests > 0 &&
      observedTotal < args.expectMinRequests &&
      Date.now() < deadline
    ) {
      await new Promise((r) => setTimeout(r, args.pollIntervalMs));
      pollAttempts += 1;
      const reload = await reloadCounting(
        activeContext,
        args.baseUrl,
        seenKeys,
        "",
        dashboardPage,
      );
      waitReloads.push(reload);
      observedTotal = reload.totalRequests;
      observedVisibleAtEpochS = reload.visibleAtEpochS;
    }

    // Measure warm dashboard load & sweep remaining pages in the same primed tab
    const warmDashboard = await measureCase("initial dashboard reload", () =>
      measureWarmDashboardReload(
        activeContext,
        args.baseUrl,
        "",
        dashboardPage,
      ),
    );
    warmDashboard.cacheMode = classifyDashboardCache(warmDashboard);

    const benchmarkCases = [];
    const filterList = Array.from(
      new Set([
        "",
        ...args.filters.map((f) => String(f || "").trim()).filter(Boolean),
      ]),
    );
    for (let iter = 0; iter < args.iterations; iter += 1) {
      for (const range of args.ranges) {
        for (const filter of filterList) {
          const query = buildFilterQueryString(range, filter);
          const result = await measureCase(
            `iteration ${iter + 1} ${range} ${filter || "clean"}`,
            () =>
              measureDashboardOnce(
                activeContext,
                args.baseUrl,
                query,
                args.caseTimeoutMs,
                true,
                dashboardPage,
              ),
          );
          for (const k of result.fetchedKeys) {
            seenKeys.add(k);
          }
          const fetchedNewFiles =
            result.parquetFilesFetched > 0 ||
            (result.lastFetch?.filesFetched ?? 0) > 0;
          if (fetchedNewFiles) {
            benchmarkCases.push(
              formatBenchmarkCaseEntry(
                iter,
                "cache-populate",
                range,
                filter,
                query,
                result,
              ),
            );
            // If the cache-populating pass succeeded, immediately re-run on the same tab
            // so we measure the warm browser-cache hit (0 fetched, N cached) like a normal user.
            if (result.freshReady) {
              const warmResult = await measureCase(
                `iteration ${iter + 1} ${range} ${filter || "clean"} warm attempt`,
                () =>
                  measureDashboardOnce(
                    activeContext,
                    args.baseUrl,
                    query,
                    args.caseTimeoutMs,
                    true,
                    dashboardPage,
                  ),
              );
              for (const k of warmResult.fetchedKeys) {
                seenKeys.add(k);
              }
              benchmarkCases.push(
                formatBenchmarkCaseEntry(
                  iter,
                  "warm",
                  range,
                  filter,
                  query,
                  warmResult,
                ),
              );
            }
          } else {
            benchmarkCases.push(
              formatBenchmarkCaseEntry(
                iter,
                "warm",
                range,
                filter,
                query,
                result,
              ),
            );
          }

          if (args.coldFiltered && filter) {
            const coldFilterCtx = await createInstrumentedContext(
              browser,
              auth.storageStatePath,
            );
            try {
              const coldRes = await measureDashboardOnce(
                coldFilterCtx,
                args.baseUrl,
                query,
                args.caseTimeoutMs,
                true,
              );
              benchmarkCases.push(
                formatBenchmarkCaseEntry(
                  iter,
                  "cold",
                  range,
                  filter,
                  query,
                  coldRes,
                ),
              );
            } finally {
              await coldFilterCtx.close();
            }
          }
        }
      }
    }

    const incremental =
      args.incrementalUrl && args.incrementalRequests > 0
        ? await measureIncrementalReloads(
            activeContext,
            args.baseUrl,
            args,
            seenKeys,
            dashboardPage,
          )
        : null;
    const sweep = await sweepPages(activeContext, args.baseUrl, dashboardPage);
    await dashboardPage.close();
    await activeContext.close();

    const allConsoleErrors = [
      ...coldDashboard.consoleErrors,
      ...waitReloads.flatMap((r) => r.consoleErrors),
      ...warmDashboard.consoleErrors,
      ...(incremental?.consoleErrors ?? []),
      ...benchmarkCases.flatMap((item) =>
        item.errors.filter((error) => error.message),
      ),
      ...sweep.consoleErrors,
    ];
    const allPageErrors = [
      ...coldDashboard.pageErrors,
      ...waitReloads.flatMap((r) => r.pageErrors),
      ...warmDashboard.pageErrors,
      ...(incremental?.pageErrors ?? []),
      ...benchmarkCases.flatMap((item) =>
        item.errors.filter((error) => error.message),
      ),
      ...sweep.pageErrors,
    ];
    const allHttpErrors = [
      ...coldDashboard.httpErrors,
      ...waitReloads.flatMap((r) => r.httpErrors),
      ...warmDashboard.httpErrors,
      ...(incremental?.httpErrors ?? []),
      ...benchmarkCases.flatMap((item) =>
        item.errors.filter((error) => error.status),
      ),
      ...sweep.httpErrors,
    ];

    const minRequestsMet =
      args.expectMinRequests <= 0 || observedTotal >= args.expectMinRequests;
    const waitRefetched = waitReloads.flatMap((r) => r.refetchedKeys);
    const tickStarvedCount =
      sweep.pages["/admin/tick"]?.summary?.starved ?? "0";
    const ok =
      minRequestsMet &&
      waitRefetched.length === 0 &&
      (incremental === null || incremental.ok) &&
      allConsoleErrors.length === 0 &&
      allPageErrors.length === 0 &&
      allHttpErrors.length === 0 &&
      tickStarvedCount === "0";

    const pagesCombined = {
      "/dashboard (cold)": {
        loadMs: coldDashboard.totalLoadMs,
        lastFetch: coldDashboard.lastFetch,
        uiHealth: coldDashboard.uiHealth,
        apiSummary: coldDashboard.apiSummary,
      },
      "/dashboard (warm)": {
        loadMs: warmDashboard.warmLoadMs,
        cacheMode: warmDashboard.cacheMode,
        lastFetch: warmDashboard.lastFetch,
        uiHealth: warmDashboard.uiHealth,
        apiSummary: warmDashboard.apiSummary,
      },
      ...sweep.pages,
    };

    const distributions = computeBenchmarkDistributions(
      coldDashboard,
      warmDashboard,
      benchmarkCases,
      pagesCombined,
    );

    const combinedApiWaterfall = [
      ...(coldDashboard.apiWaterfall || []),
      ...(warmDashboard.apiWaterfall || []),
      ...waitReloads.flatMap((item) => item.apiWaterfall || []),
      ...benchmarkCases.flatMap((item) => item.apiWaterfall || []),
      ...(incremental?.reloads || []).flatMap(
        (item) => item.apiWaterfall || [],
      ),
      ...(sweep.apiWaterfall || []),
    ].sort((a, b) => a.startEpochMs - b.startEpochMs);
    const combinedApiSummary = summarizeApiCalls(combinedApiWaterfall);

    const report = {
      ok,
      auth: {
        renewed: auth.renewed,
        user: auth.user,
        computeVersion: auth.computeVersion,
      },
      serviceId: args.serviceId || null,
      dashboard: {
        coldLoadMs: coldDashboard.totalLoadMs,
        warmLoadMs:
          warmDashboard.cacheMode === "warm" ? warmDashboard.warmLoadMs : null,
        warmNavigationTtfbMs: warmDashboard.navigationTtfbMs,
        warmNavigationTtlbMs: warmDashboard.navigationTtlbMs,
        parquetFilesFetched: coldDashboard.parquetFilesFetched,
        parquetHttpRequests: coldDashboard.parquetHttpRequests,
        rangeProbeCount: coldDashboard.rangeProbeCount,
        rangeReadCount: coldDashboard.rangeReadCount,
        fullFileDownloadCount: coldDashboard.fullFileDownloadCount,
        parquetDownloadMs: coldDashboard.parquetDownloadMs,
        workerPostDownloadMs: coldDashboard.workerPostDownloadMs,
        navigationTtfbMs: coldDashboard.navigationTtfbMs,
        navigationTtlbMs: coldDashboard.navigationTtlbMs,
        benchmarkCases,
        distributions,
        totalRequests: observedTotal,
        expectMinRequests: args.expectMinRequests,
        minRequestsMet,
        visibleAtEpochS: observedVisibleAtEpochS,
        extentsFreshness: coldDashboard.extentsFreshness,
        pollAttempts,
        waitReloads: waitReloads.map((r) => ({
          loadMs: r.loadMs,
          totalRequests: r.totalRequests,
          newFiles: r.newKeys.length,
          refetched: r.refetchedKeys.length,
          lastFetch: r.lastFetch,
          uiHealth: r.uiHealth,
        })),
        cold: {
          loadMs: coldDashboard.totalLoadMs,
          totalRequests: coldDashboard.totalRequests,
          parquetFilesFetched: coldDashboard.parquetFilesFetched,
          parquetHttpRequests: coldDashboard.parquetHttpRequests,
          rangeProbeCount: coldDashboard.rangeProbeCount,
          rangeReadCount: coldDashboard.rangeReadCount,
          fullFileDownloadCount: coldDashboard.fullFileDownloadCount,
          manifestRequests: coldDashboard.manifestRequests,
          preParquetBootstrapMs: coldDashboard.preParquetBootstrapMs,
          parquetDownloadMs: coldDashboard.parquetDownloadMs,
          workerPostDownloadMs: coldDashboard.workerPostDownloadMs,
          lastFetch: coldDashboard.lastFetch,
          uiHealth: coldDashboard.uiHealth,
          extentsFreshness: coldDashboard.extentsFreshness,
          apiSummary: coldDashboard.apiSummary,
          apiWaterfall: coldDashboard.apiWaterfall,
        },
        warm: {
          loadMs:
            warmDashboard.cacheMode === "warm"
              ? warmDashboard.warmLoadMs
              : null,
          measuredLoadMs: warmDashboard.warmLoadMs,
          cacheMode: warmDashboard.cacheMode,
          totalRequests: warmDashboard.totalRequests,
          lastFetch: warmDashboard.lastFetch,
          uiHealth: warmDashboard.uiHealth,
          apiSummary: warmDashboard.apiSummary,
          apiWaterfall: warmDashboard.apiWaterfall,
        },
        incremental: incremental && {
          ...incremental,
          consoleErrors: undefined,
          pageErrors: undefined,
          httpErrors: undefined,
        },
      },
      pages: pagesCombined,
      routeCoverage: sweep.routeCoverage,
      distributions,
      apiTelemetry: combinedApiSummary,
      apiWaterfall: combinedApiWaterfall,
      uiQuality: {
        coldDashboardCls: coldDashboard.uiHealth?.clsScore ?? null,
        warmDashboardCls: warmDashboard.uiHealth?.clsScore ?? null,
        maxClsScore: distributions.clsScore.max,
        clsP50: distributions.clsScore.p50,
        clsP95: distributions.clsScore.p95,
        anyClsWarning:
          distributions.clsScore.max === null
            ? null
            : distributions.clsScore.max > 0.1,
        anyHorizontalOverflow: Object.values(pagesCombined).some(
          (p) => p?.uiHealth?.hasHorizontalOverflow,
        ),
        totalLongTasks:
          (coldDashboard.uiHealth?.longTaskCount ?? 0) +
          (warmDashboard.uiHealth?.longTaskCount ?? 0),
      },
      errors: {
        consoleErrors: allConsoleErrors,
        pageErrors: allPageErrors,
        httpErrors: allHttpErrors,
      },
    };
    const serialized = JSON.stringify(report, null, 2);
    if (args.outPath) {
      fs.mkdirSync(path.dirname(path.resolve(args.outPath)), {
        recursive: true,
      });
      fs.writeFileSync(args.outPath, `${serialized}\n`, "utf8");
    }
    console.log(JSON.stringify(report));
    if (!ok) {
      process.exitCode = 1;
    }
    return report;
  } catch (error) {
    const failure = error.dashboardFailure ?? { error: String(error) };
    if (args.outPath && dashboardPage && !dashboardPage.isClosed()) {
      const screenshotPath = `${args.outPath}.failure.png`;
      if (new URL(dashboardPage.url()).pathname === "/dashboard") {
        try {
          await dashboardPage.screenshot({
            path: screenshotPath,
            fullPage: true,
            mask: [
              dashboardPage.locator(
                'input[type="password"],input[name*="token"],input[name*="secret"]',
              ),
            ],
          });
          failure.screenshotPath = screenshotPath;
        } catch (captureError) {
          failure.screenshotError = String(captureError);
        }
      }
    }
    writeProgress({
      status: "failed",
      failure,
      apiWaterfall: sessionRecorder?.getWaterfall() ?? [],
      pendingRequests: sessionRecorder?.getPending() ?? [],
    });
    throw error;
  } finally {
    sessionRecorder?.detach();
    await browser.close();
  }
}

const isDirectExecution =
  process.argv[1] &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;

if (isDirectExecution) {
  main().catch((err) => {
    console.error(
      JSON.stringify({
        ok: false,
        error: String(err?.stack || err?.message || err),
      }),
    );
    process.exit(1);
  });
}
