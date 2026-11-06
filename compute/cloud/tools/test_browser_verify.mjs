import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import { pathToFileURL } from "node:url";

import {
  attachApiWaterfallRecorder,
  assertMonitoredService,
  optionalRoutesToProbe,
  collectUiHealth,
  WEB_VITALS_INIT_SCRIPT,
  buildFilterQueryString,
  computeBenchmarkDistributions,
  DASHBOARD_PRESETS,
  DEFAULT_BENCHMARK_RANGES,
  formatBenchmarkCaseEntry,
  EXTENDED_PRESETS,
  FILTER_BAR_ROUTE_PREFIXES,
  parseArgs,
  parseLastFetchText,
  PLAYWRIGHT_PATH,
  runDashboardCase,
  summarizeApiCalls,
  summarizeNumericSeries,
  validateDashboardPresets,
} from "./browser_verify.mjs";

test("dashboard failure capture completes before another case can navigate", async () => {
  const { chromium } = await import(pathToFileURL(PLAYWRIGHT_PATH).href);
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.goto("data:text/html,<h1>Failed method query</h1>");
    const failedUrl = page.url();
    let advanced = false;
    await assert.rejects(
      async () => {
        await runDashboardCase(page, "1h +method:GET", async () => ({
          freshReady: false,
          lastFetch: null,
        }));
        advanced = true;
        await page.goto("data:text/html,<h1>Next preset</h1>");
      },
      (error) => {
        assert.match(error.message, /1h \+method:GET.*not ready/);
        assert.equal(error.dashboardFailure.document.url, failedUrl);
        assert.match(
          error.dashboardFailure.document.text,
          /Failed method query/,
        );
        assert.equal(error.dashboardFailure.result.freshReady, false);
        assert.equal(
          error.dashboardFailure.uiHealth.measurementScope,
          "document-navigation",
        );
        return true;
      },
    );
    assert.equal(advanced, false);
    assert.equal(page.url(), failedUrl);

    await assert.rejects(
      runDashboardCase(page, "cold 24h", async () => {
        await page.locator("#missing-fresh-result").waitFor({ timeout: 10 });
      }),
      (error) => {
        assert.equal(error.dashboardFailure.caseLabel, "cold 24h");
        assert.equal(error.dashboardFailure.document.url, failedUrl);
        return true;
      },
    );
    await assert.rejects(
      runDashboardCase(page, "blocked query", () => new Promise(() => {}), 10),
      (error) => {
        assert.match(error.message, /blocked query.*within 10ms/);
        assert.equal(error.dashboardFailure.document.url, failedUrl);
        return true;
      },
    );
    const fresh = { freshReady: true, totalRequests: 42 };
    assert.equal(
      await runDashboardCase(page, "warm 24h", async () => fresh),
      fresh,
    );
  } finally {
    await browser.close();
  }
});

test("optional route coverage probes cloud files and unlinked origin/security", () => {
  const routes = optionalRoutesToProbe([
    "/performance",
    "/performance?range=24h",
    "/unknown",
  ]);
  assert.deepEqual(routes, [
    "/cloud-files",
    "/origin",
    "/security",
    "/performance",
  ]);
  assert.ok(!routes.some((route) => route.includes("10m")));
});

test("CLS is unmeasured without instrumentation and document-scoped when initialized", async () => {
  const { chromium } = await import(pathToFileURL(PLAYWRIGHT_PATH).href);
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.goto("data:text/html,<h1>Local measurement</h1>");
    const missing = await collectUiHealth(page);
    assert.equal(missing.clsScore, null);
    assert.equal(missing.webVitalsAvailable, false);
    await context.addInitScript({ content: WEB_VITALS_INIT_SCRIPT });
    await page.goto("data:text/html,<h1>Fresh document</h1>");
    const first = await collectUiHealth(page);
    assert.equal(first.clsSupported, true);
    assert.equal(first.clsScore, 0);
    assert.equal(first.measurementScope, "document-navigation");
    await page.evaluate(() => {
      window.__flpWebVitals.clsScore = 0.6;
    });
    await page.goto("data:text/html,<h1>Another document</h1>");
    const second = await collectUiHealth(page);
    assert.equal(second.clsScore, 0);
    assert.ok(second.timeOriginEpochMs > first.timeOriginEpochMs);
    await context.close();
  } finally {
    await browser.close();
  }
});

test("browser service preflight rejects absent or mixed identities and HTTP errors", async () => {
  const context = (services, status = 200) => ({
    request: {
      get: async (url) => {
        assert.equal(url, "https://example.test/api/services");
        return {
          ok: () => status === 200,
          status: () => status,
          json: async () => ({ services }),
        };
      },
    },
  });
  await assertMonitoredService(
    context([{ service_id: "requested" }]),
    "https://example.test",
    "requested",
  );
  for (const services of [
    [],
    null,
    [{ service_id: "historical" }],
    [{ service_id: "requested" }, { service_id: "historical" }],
  ]) {
    await assert.rejects(
      assertMonitoredService(
        context(services),
        "https://example.test",
        "requested",
      ),
      /refusing to measure another identity/,
    );
  }
  await assert.rejects(
    assertMonitoredService(
      context([], 403),
      "https://example.test",
      "requested",
    ),
    /HTTP 403/,
  );
});

test("API waterfall waits for the response body before measuring download time", async (t) => {
  let now = 1000;
  t.mock.method(Date, "now", () => now);
  const page = new EventEmitter();
  const recorder = attachApiWaterfallRecorder(page);
  let responseEnd = -1;
  const request = {
    url: () => "https://example.test/api/file?key=one.parquet",
    method: () => "GET",
    headers: () => ({}),
    timing: () => ({ responseEnd }),
  };
  let finish;
  const finished = new Promise((resolve) => {
    finish = resolve;
  });
  const response = {
    request: () => request,
    status: () => 200,
    headers: () => ({ "content-length": "1000" }),
    finished: () => finished,
  };
  try {
    page.emit("request", request);
    now = 1010;
    page.emit("response", response);
    assert.equal(recorder.getWaterfall().length, 0);
    assert.equal(recorder.getPending()[0].pendingMs, 10);
    assert.equal(recorder.getPending()[0].endpoint, "/api/file");
    now = 1100;
    responseEnd = 100;
    finish(null);
    await new Promise(setImmediate);
    const [call] = recorder.getWaterfall();
    assert.equal(call.ttfbMs, 10);
    assert.equal(call.downloadMs, 90);
    assert.equal(call.durationMs, 100);
    assert.equal(recorder.getSummary().durationMs.p50, 100);
    assert.deepEqual(recorder.getPending(), []);
  } finally {
    finish(null);
    recorder.detach();
  }
});

test("API waterfall records an interrupted response body only as a failed request", async () => {
  const page = new EventEmitter();
  const recorder = attachApiWaterfallRecorder(page);
  const request = {
    url: () => "https://example.test/api/file?key=one.parquet",
    method: () => "GET",
    headers: () => ({}),
    timing: () => ({ responseEnd: -1 }),
    failure: () => ({ errorText: "net::ERR_CONNECTION_RESET" }),
  };
  let finish;
  const finished = new Promise((resolve) => {
    finish = resolve;
  });
  try {
    page.emit("request", request);
    page.emit("response", {
      request: () => request,
      status: () => 200,
      headers: () => ({}),
      finished: () => finished,
    });
    page.emit("requestfailed", request);
    finish(new Error("net::ERR_CONNECTION_RESET"));
    await new Promise(setImmediate);
    const calls = recorder.getWaterfall();
    assert.equal(calls.length, 1);
    assert.equal(calls[0].failed, true);
    assert.equal(calls[0].failureText, "net::ERR_CONNECTION_RESET");
  } finally {
    finish(null);
    recorder.detach();
  }
});

test("API waterfall does not wait for a long-lived ingest event stream to close", async () => {
  const page = new EventEmitter();
  const recorder = attachApiWaterfallRecorder(page);
  const request = {
    url: () => "https://example.test/api/ingest/events",
    method: () => "GET",
    headers: () => ({}),
    timing: () => ({ responseEnd: -1 }),
  };
  try {
    page.emit("request", request);
    page.emit("response", {
      request: () => request,
      status: () => 200,
      headers: () => ({}),
      finished: () => new Promise(() => {}),
    });
    await new Promise(setImmediate);
    assert.equal(recorder.getWaterfall().length, 1);
    assert.equal(recorder.getWaterfall()[0].endpoint, "/api/ingest/events");
  } finally {
    recorder.detach();
  }
});

test("DASHBOARD_PRESETS and EXTENDED_PRESETS match fastly-log-analytics and never include 10m", () => {
  assert.deepEqual(DASHBOARD_PRESETS, ["1h", "3h", "6h", "12h", "24h"]);
  assert.deepEqual(DEFAULT_BENCHMARK_RANGES, ["24h", "1h", "6h"]);
  assert.equal(DEFAULT_BENCHMARK_RANGES[0], "24h");
  assert.deepEqual(EXTENDED_PRESETS, [
    "1h",
    "3h",
    "6h",
    "12h",
    "24h",
    "3d",
    "7d",
    "30d",
  ]);
  assert.ok(!DASHBOARD_PRESETS.includes("10m"));
  assert.ok(!EXTENDED_PRESETS.includes("10m"));
  assert.ok(FILTER_BAR_ROUTE_PREFIXES.includes("/dashboard"));
  assert.ok(FILTER_BAR_ROUTE_PREFIXES.includes("/origin"));
  assert.ok(FILTER_BAR_ROUTE_PREFIXES.includes("/security"));
});

test("validateDashboardPresets rejects 10m and unknown presets", () => {
  assert.deepEqual(validateDashboardPresets(["1h", "6h", "24h"]), [
    "1h",
    "6h",
    "24h",
  ]);
  assert.throws(
    () => validateDashboardPresets(["10m", "1h"]),
    /Invalid preset "10m"/,
  );
  assert.throws(
    () => parseArgs(["node", "browser_verify.mjs", "--ranges", "10m,1h"]),
    /Invalid preset "10m"/,
  );
  assert.deepEqual(validateDashboardPresets(["7d", "30d"], true), [
    "7d",
    "30d",
  ]);
});

test("buildFilterQueryString formats single and multi-filter expressions", () => {
  assert.equal(buildFilterQueryString("1h", ""), "?range=1h");
  assert.equal(
    buildFilterQueryString("6h", "+status:200"),
    "?range=6h&f=%2Bstatus%3A200",
  );
  assert.equal(
    buildFilterQueryString("24h", "+status:200&+method:GET&-status:503"),
    "?range=24h&f=%2Bstatus%3A200&f=%2Bmethod%3AGET&f=-status%3A503",
  );
  assert.throws(
    () => buildFilterQueryString("1h", "status=200"),
    /Invalid filter token/,
  );
});

test("parseLastFetchText extracts listed, fetched, cached, CDN hits, rows, bytes, elapsed, and vCPU", () => {
  const sample =
    "Last fetch: 18 listed, 4 fetched (14 cached, 3 CDN hit), 66,600 rows, 1.5 MB in 420 ms · Compute 19.4 ms vCPU";
  const parsed = parseLastFetchText(sample);
  assert.equal(parsed.filesListed, 18);
  assert.equal(parsed.filesFetched, 4);
  assert.equal(parsed.filesCached, 14);
  assert.equal(parsed.cdnHits, 3);
  assert.equal(parsed.rowsScanned, 66600);
  assert.equal(parsed.bytesScanned, Math.round(1.5 * 1024 * 1024));
  assert.equal(parsed.clientElapsedMs, 420);
  assert.equal(parsed.computeVcpuMs, 19.4);

  const liveDashboardSample =
    "Last fetch: 178 fetched, 0 cached (0 CDN hits) of 178 files · 61,500 rows · 12.8 MB · 8,087 ms · server vCPU not measured (browser-local aggregation)";
  const parsedLive = parseLastFetchText(liveDashboardSample);
  assert.equal(parsedLive.filesListed, 178);
  assert.equal(parsedLive.filesFetched, 178);
  assert.equal(parsedLive.filesCached, 0);
  assert.equal(parsedLive.cdnHits, 0);
  assert.equal(parsedLive.rowsScanned, 61500);
  assert.equal(parsedLive.bytesScanned, Math.round(12.8 * 1024 * 1024));
  assert.equal(parsedLive.clientElapsedMs, 8087);
  assert.equal(parsedLive.computeVcpuMs, null);
});

test("summarizeNumericSeries computes min, p50, p95, max, mean, sum", () => {
  const empty = summarizeNumericSeries([]);
  assert.equal(empty.count, 0);
  assert.equal(empty.p50, null);
  assert.equal(empty.p95, null);

  const stats = summarizeNumericSeries([
    10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
  ]);
  assert.equal(stats.count, 10);
  assert.equal(stats.min, 10);
  assert.equal(stats.p50, 55);
  assert.equal(stats.p95, 95.5);
  assert.equal(stats.max, 100);
  assert.equal(stats.mean, 55);
  assert.equal(stats.sum, 550);
});

test("missing browser measurements never become zero-valued samples", () => {
  const stats = summarizeNumericSeries([
    null,
    undefined,
    "",
    false,
    NaN,
    Infinity,
    0,
    10,
  ]);
  assert.equal(stats.count, 2);
  assert.equal(stats.p50, 5);
});

test("benchmark cases require cache evidence before claiming true warm", () => {
  const result = {
    freshReady: true,
    parquetFilesFetched: 0,
    lastFetch: { filesFetched: 0, filesCached: 4 },
    apiWaterfall: [{ endpoint: "/api/files", status: 200 }],
    consoleErrors: [],
    pageErrors: [],
    httpErrors: [],
  };
  const entry = (overrides = {}) =>
    formatBenchmarkCaseEntry(0, "warm", "24h", "", "", {
      ...result,
      ...overrides,
    });
  assert.equal(entry().cacheMode, "warm");
  assert.deepEqual(entry().apiWaterfall, result.apiWaterfall);
  assert.equal(entry({ lastFetch: null }).cacheMode, "unverified");
  assert.equal(
    entry({ lastFetch: { filesFetched: 0, filesCached: 0 } }).cacheMode,
    "unverified",
  );
  assert.equal(entry({ parquetFilesFetched: 1 }).cacheMode, "cache-populate");
  const failed = entry({ freshReady: false });
  assert.equal(failed.cacheMode, "unverified");
  assert.equal(failed.errors[0].message, "fresh data timeout");
});

test("summarizeApiCalls groups 200 vs 206 range probes/reads and ranks slowest calls", () => {
  const calls = [
    {
      url: "https://example.test/api/files?dataset=request&from=2026-11-03T00:00:00Z",
      endpoint: "/api/files",
      method: "GET",
      status: 200,
      durationMs: 120,
      ttfbMs: 95,
      downloadMs: 25,
      bytes: 4096,
      cdnHit: false,
      vcpuMs: 8.5,
    },
    {
      url: "https://example.test/api/file?key=processed/request/a.parquet",
      endpoint: "/api/file",
      method: "GET",
      status: 206,
      rangeHeader: "bytes=0-0",
      contentRange: "bytes 0-0/1048576",
      isRangeProbe: true,
      isRangeRead: true,
      durationMs: 40,
      ttfbMs: 38,
      downloadMs: 2,
      bytes: 1,
      cdnHit: true,
      vcpuMs: 1.2,
    },
    {
      url: "https://example.test/api/file?key=processed/request/a.parquet",
      endpoint: "/api/file",
      method: "GET",
      status: 206,
      rangeHeader: "bytes=1024-65535",
      contentRange: "bytes 1024-65535/1048576",
      isRangeProbe: false,
      isRangeRead: true,
      durationMs: 85,
      ttfbMs: 50,
      downloadMs: 35,
      bytes: 64512,
      cdnHit: true,
      vcpuMs: 2.1,
    },
    {
      url: "https://example.test/api/telemetry/query",
      endpoint: "/api/telemetry/query",
      method: "POST",
      status: 202,
      durationMs: 25,
      ttfbMs: 25,
      downloadMs: 0,
      bytes: 0,
      cdnHit: false,
      vcpuMs: 0.9,
      queryTelemetryBeacon: {
        files_listed: 12,
        files_fetched: 2,
        rows_scanned: 5000,
        duration_ms: 310,
      },
    },
  ];
  const summary = summarizeApiCalls(calls);
  assert.equal(summary.totalCalls, 4);
  assert.equal(summary.rangeProbeCount, 1);
  assert.equal(summary.rangeReadCount, 1);
  assert.equal(summary.cdnHitCount, 2);
  assert.equal(summary.queryTelemetryBeacons.length, 1);
  assert.ok(summary.byEndpoint["/api/file (206 range probe)"]);
  assert.ok(summary.byEndpoint["/api/file (206 range read)"]);
  assert.equal(summary.slowestCalls[0].durationMs, 120);
});

test("computeBenchmarkDistributions summarizes clean vs filtered and per-range p50/p95", () => {
  const cold = {
    totalLoadMs: 1200,
    parquetDownloadMs: 600,
    workerPostDownloadMs: 250,
    uiHealth: { clsScore: 0.01 },
  };
  const warm = {
    warmLoadMs: 350,
    cacheMode: "warm",
    uiHealth: { clsScore: 0.0 },
  };
  const cases = [
    {
      iteration: 1,
      range: "1h",
      filter: "",
      cacheMode: "cache-populate",
      loadMs: 1450,
      parquetDownloadMs: 850,
      workerPostDownloadMs: 300,
      uiHealth: { clsScore: 0.01 },
    },
    {
      iteration: 1,
      range: "1h",
      filter: "",
      cacheMode: "warm",
      loadMs: 320,
      parquetDownloadMs: 0,
      workerPostDownloadMs: 180,
      uiHealth: { clsScore: 0.0 },
    },
    {
      iteration: 1,
      range: "1h",
      filter: "+status:200",
      cacheMode: "warm",
      loadMs: 410,
      parquetDownloadMs: 0,
      workerPostDownloadMs: 220,
      uiHealth: { clsScore: 0.02 },
    },
  ];
  const pages = {
    "/dashboard (cold)": { loadMs: 1200, uiHealth: { clsScore: 0.01 } },
    "/usage": { loadMs: 280, uiHealth: { clsScore: 0.0 } },
  };
  const dist = computeBenchmarkDistributions(cold, warm, cases, pages);
  assert.equal(dist.dashboardCleanLoadMs.count, 3);
  assert.equal(dist.warmCleanDashboardLoadMs.count, 2);
  assert.equal(dist.dashboardCachePopulateLoadMs.count, 1);
  assert.equal(dist.dashboardCachePopulateLoadMs.p50, 1450);
  assert.equal(dist.dashboardFilteredLoadMs.count, 1);
  assert.equal(dist.dashboardFilteredLoadMs.p50, 410);
  assert.equal(dist.byRange["1h"].allLoadMs.count, 3);
  assert.equal(dist.byRange["1h"].cleanLoadMs.count, 1);
  assert.equal(dist.byRange["1h"].cleanLoadMs.p50, 320);
  assert.equal(dist.byRange["1h"].cachePopulateLoadMs.p50, 1450);
  assert.equal(dist.byRange["1h"].warmLoadMs.count, 2);
  assert.equal(dist.clsScore.max, 0.02);
});

test("cache-populating reloads do not count as true warm latency samples", () => {
  const stats = computeBenchmarkDistributions(
    { totalLoadMs: 500 },
    { warmLoadMs: 450, cacheMode: "cache-populate" },
    [],
    {},
  );
  assert.equal(stats.warmCleanDashboardLoadMs.count, 0);
  assert.equal(stats.dashboardWarmCasesLoadMs.p50, null);
});

async function withColdOnlyServer(options, run) {
  const http = await import("node:http");
  const fsp = await import("node:fs/promises");
  const path = await import("node:path");
  const { fileURLToPath } = await import("node:url");
  const hits = { dashboard: 0, load: 0, file: 0, other: [] };
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    const json = (body, status = 200) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };
    if (url.pathname === "/api/auth/session") {
      return json({ user: { id: "u1", name: "Fixture", role: "admin" } });
    }
    if (url.pathname === "/api/services") {
      return json({ services: options.services });
    }
    if (url.pathname === "/dashboard") {
      hits.dashboard += 1;
      res.writeHead(200, { "content-type": "text/html" });
      // Mirrors LoadingProgress markup: the current stage is aria-current="step".
      const hold = options.analysisHoldMs;
      return res.end(`<!doctype html><meta charset="utf-8"><body><p>Loading</p><script>
const stagesOn = ${hold !== undefined};
const setStage = (current) => {
  if (!stagesOn) return;
  document.body.innerHTML =
    '<section role="region" aria-label="Dashboard loading progress"><ol aria-label="Loading stages">' +
    ["Account", "Log range", "File list", "Analysis"]
      .map((l) => "<li" + (l === current ? ' aria-current="step"' : "") + "><span>" + l + "</span></li>")
      .join("") +
    "</ol></section>";
};
(async () => {
  setStage("File list");
  const listing = await (await fetch("/api/files?dataset=request")).json();
  setStage("Analysis");
  const statuses = [];
  for (const file of listing.files) {
    statuses.push((await fetch("/api/file?key=" + encodeURIComponent(file.key))).status);
  }
  await new Promise((r) => setTimeout(r, ${hold ?? 0}));
  document.body.innerHTML = statuses.every((s) => s === 200)
    ? '<div data-testid="total-requests" data-fresh="true">2,500</div>' +
      '<p data-testid="last-fetch">Last fetch: 2 fetched, 0 cached (0 CDN hits) of 2 files · 2,500 rows · 1.5 KB · 45 ms · server vCPU not measured</p>'
    : '<p role="alert">Query failed</p><button>Retry</button>';
})();
</script></body>`);
    }
    if (url.pathname === "/api/files") {
      return json({
        files: [
          { key: "service_id=svc/dt=2026-11-04/hour=22/open-a.parquet" },
          { key: "service_id=svc/dt=2026-11-04/hour=22/open-b.parquet" },
        ],
      });
    }
    if (url.pathname === "/api/file") {
      hits.file += 1;
      res.writeHead(options.fileStatus ?? 200, {
        "content-type": "application/octet-stream",
        "x-flp-vcpu-ms": "3",
      });
      return res.end(Buffer.alloc(16));
    }
    if (url.pathname === "/load") hits.load += 1;
    else hits.other.push(url.pathname);
    res.writeHead(404);
    res.end();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const dir = await fsp.mkdtemp(
    path.join(path.dirname(fileURLToPath(import.meta.url)), ".cold-only-"),
  );
  await fsp.writeFile(
    path.join(dir, "browser-state.json"),
    JSON.stringify({
      cookies: [
        {
          name: "session",
          value: "fixture",
          domain: "127.0.0.1",
          path: "/",
          expires: -1,
          httpOnly: true,
          secure: false,
          sameSite: "Lax",
        },
      ],
      origins: [],
    }),
  );
  const previousExitCode = process.exitCode;
  try {
    return await run({
      baseUrl,
      dir,
      hits,
      outPath: path.join(dir, "out.json"),
    });
  } finally {
    process.exitCode = previousExitCode;
    server.close();
    await fsp.rm(dir, { recursive: true, force: true });
  }
}

const coldOnlyArgv = (baseUrl, dir, outPath, serviceId = "svc") => [
  "node",
  "browser_verify.mjs",
  "--mode",
  "cold-only",
  "--base-url",
  baseUrl,
  "--state-dir",
  dir,
  "--out",
  outPath,
  "--case-timeout-ms",
  "15000",
  ...(serviceId ? ["--service-id", serviceId] : []),
];

test("cold-only performs exactly one cold dashboard load and persists its metrics", async () => {
  const { main } = await import("./browser_verify.mjs");
  const fsp = await import("node:fs/promises");
  await withColdOnlyServer(
    { services: [{ service_id: "svc" }] },
    async ({ baseUrl, dir, hits, outPath }) => {
      const before = Date.now() / 1000;
      const report = await main(coldOnlyArgv(baseUrl, dir, outPath));
      assert.deepEqual(JSON.parse(await fsp.readFile(outPath, "utf8")), report);
      assert.equal(process.exitCode, undefined);
      assert.deepEqual(
        { dashboard: hits.dashboard, load: hits.load, file: hits.file },
        { dashboard: 1, load: 0, file: 2 },
      );
      assert.equal(report.ok, true);
      assert.equal(report.mode, "cold-only");
      assert.equal(report.trafficSent, false);
      const d = report.dashboard;
      assert.equal(d.freshReady, true);
      assert.equal(d.totalRequests, 2500);
      assert.ok(d.coldLoadMs > 0 && d.coldLoadMs < 15_000);
      assert.ok(d.visibleAtEpochS >= before);
      assert.equal(
        new Date(d.visibleAtIso).getTime() / 1000,
        d.visibleAtEpochS,
      );
      assert.equal(d.files.parquetFilesFetched, 2);
      assert.equal(d.files.fullFileDownloadCount, 2);
      assert.equal(d.files.manifestRequests, 1);
      assert.equal(d.files.filesListed, 2);
      assert.equal(d.lastFetchProxies.lastFetch.filesFetched, 2);
      assert.equal(d.lastFetchProxies.lastFetch.rowsScanned, 2500);
      assert.equal("analysis" in d, false);
      assert.deepEqual(
        {
          durationMs: d.analysisStage.durationMs,
          status: d.analysisStage.status,
        },
        { durationMs: null, status: "not-observed" },
      );
      assert.equal(d.catalog["/api/files?dataset=request"].count, 1);
      assert.equal(
        report.apiTelemetry.byEndpoint["/api/file (200 full)"].count,
        2,
      );
      assert.equal(report.apiTelemetry.vcpuMs.sum, 6);
      assert.equal(report.apiWaterfall.length, 3);
      assert.deepEqual(report.errors.httpErrors, []);
      assert.equal(
        "warm" in d || "benchmarkCases" in d || "pages" in report,
        false,
      );
    },
  );
});

test("cold-only records a failed query and sets a failing exit status", async () => {
  const { main } = await import("./browser_verify.mjs");
  const fsp = await import("node:fs/promises");
  await withColdOnlyServer(
    { services: [{ service_id: "svc" }], fileStatus: 503 },
    async ({ baseUrl, dir, hits, outPath }) => {
      const report = await main(coldOnlyArgv(baseUrl, dir, outPath));
      assert.equal(process.exitCode, 1);
      assert.equal(hits.dashboard, 1);
      const persisted = JSON.parse(await fsp.readFile(outPath, "utf8"));
      assert.equal(persisted.ok, false);
      assert.equal(persisted.status, "failed");
      assert.equal(report.dashboard.freshReady, false);
      assert.equal(report.dashboard.totalRequests, null);
      assert.deepEqual(
        report.errors.httpErrors.map((e) => e.status),
        [503, 503],
      );
    },
  );
});

for (const [label, services, serviceId, pattern] of [
  [
    "another monitored identity",
    [{ service_id: "other" }],
    "svc",
    /another identity/,
  ],
  ["no --service-id", [{ service_id: "svc" }], "", /--service-id is required/],
]) {
  test(`cold-only refuses to load the dashboard with ${label}`, async () => {
    const { main } = await import("./browser_verify.mjs");
    const fsp = await import("node:fs/promises");
    await withColdOnlyServer(
      { services },
      async ({ baseUrl, dir, hits, outPath }) => {
        await assert.rejects(
          main(coldOnlyArgv(baseUrl, dir, outPath, serviceId)),
          pattern,
        );
        assert.equal(hits.dashboard, 0);
        const persisted = JSON.parse(await fsp.readFile(outPath, "utf8"));
        assert.equal(persisted.ok, false);
        assert.equal(persisted.status, "failed");
      },
    );
  });
}

for (const [label, holdMs] of [
  ["a held", 1500],
  ["a brief", 0],
]) {
  test(`cold-only records ${label} Analysis stage that ends before the harness reads it`, async () => {
    const { main } = await import("./browser_verify.mjs");
    await withColdOnlyServer(
      { services: [{ service_id: "svc" }], analysisHoldMs: holdMs },
      async ({ baseUrl, dir, hits, outPath }) => {
        const report = await main(coldOnlyArgv(baseUrl, dir, outPath));
        assert.equal(report.ok, true);
        assert.equal(hits.dashboard, 1);
        const stage = report.dashboard.analysisStage;
        assert.equal(stage.status, "complete");
        assert.equal(stage.endReason, "region-removed");
        assert.match(stage.source, /aria-current="step"/);
        assert.equal(stage.episodes.length, 1);
        assert.ok(
          stage.durationMs >= holdMs && stage.durationMs < holdMs + 1_000,
          `durationMs ${stage.durationMs} for hold ${holdMs}`,
        );
        assert.equal(
          stage.durationMs,
          stage.episodes[0].endMs - stage.episodes[0].startMs,
        );
        assert.deepEqual(
          stage.timeline.map((t) => t.stage),
          ["File list", "Analysis", null],
        );
        assert.ok(stage.durationMs < report.dashboard.coldLoadMs);
      },
    );
  });
}
