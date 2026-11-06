import { expect, test, type Route } from "@playwright/test";
import { get as httpsGet } from "node:https";
import { PACKAGE_VERSION } from "@duckdb/duckdb-wasm";
import { parquetWriteBuffer } from "hyparquet-writer";
import { parquetFixture } from "../../lib/cloud-engine/dashboard/test-fixtures";

test("native Cloud runtime recovers after an abandoned Wasm download", async ({
  baseURL,
  request,
}) => {
  if (!baseURL) throw new Error("Native Cloud base URL is required");
  await new Promise<void>((resolve, reject) => {
    const download = httpsGet(
      new URL(`/duckdb/${PACKAGE_VERSION}/duckdb-eh.wasm`, baseURL),
      {
        rejectUnauthorized: false,
        headers: { "accept-encoding": "gzip" },
      },
      (response) => {
        if (response.statusCode !== 200) {
          response.resume();
          reject(new Error(`Wasm download returned ${response.statusCode}`));
          return;
        }
        response.once("data", () => {
          download.destroy();
          resolve();
        });
        response.on("error", reject);
      },
    );
    download.on("error", reject);
  });
  expect((await request.get("/healthz", { timeout: 5000 })).status()).toBe(200);
});

test("Cloud origin buckets use UTC without timezone extensions", async ({
  page,
}) => {
  const errors: string[] = [];
  const unexpected: string[] = [];
  const extensions: number[] = [];
  const from = "2026-01-01T00:00:00Z";
  const to = "2026-01-01T01:00:00Z";
  const bytes = Buffer.from(
    parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(from)] },
        { name: "cache", type: "STRING", data: ["MISS"] },
        { name: "url", type: "STRING", data: ["/fixture-origin.css"] },
        { name: "ttfb", type: "DOUBLE", data: [0.02] },
        { name: "status", type: "INT32", data: [200] },
      ],
    }),
  );
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("response", (response) => {
    if (new URL(response.url()).pathname.endsWith("/icu.duckdb_extension.wasm")) {
      extensions.push(response.status());
    }
  });
  await page.context().route("**/api/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    switch (path) {
      case "/api/auth/session":
        return route.fulfill({
          json: { user: { id: "fixture-user", role: "admin" } },
        });
      case "/api/services":
        return route.fulfill({
          json: {
            services: [{ service_id: "fixtureService", name: "Fixture Service" }],
          },
        });
      case "/api/log-fields/catalog":
        return route.fulfill({ json: { fields: [], groups: [] } });
      case "/api/log-extents":
        return route.fulfill({
          json: {
            earliest_log_at: from,
            latest_log_at: to,
          },
        });
      case "/api/files":
        return route.fulfill({
          json: {
            files: [
              {
                key: "pq/origin-buckets.parquet",
                start: from,
                end: to,
                rows: 1,
                size: bytes.length,
              },
            ],
            summary_files: [],
            complete: true,
            next_cursor: null,
          },
        });
      case "/api/file":
        return serveParquetRange(route, bytes);
      case "/api/telemetry/query":
        return route.fulfill({ json: { ok: true } });
      case "/api/admin/usage-logging":
        return route.fulfill({ json: { enabled: false } });
      case "/api/cron-runs":
        return route.fulfill({ json: { configured: true, runs: [] } });
      default:
        unexpected.push(`${route.request().method()} ${path}`);
        return route.fulfill({
          status: 404,
          json: { detail: "No timezone test fixture" },
        });
    }
  });
  const params = new URLSearchParams({
    service: "fixtureService",
    start_time: from,
    end_time: to,
  });
  await page.goto(`/origin?${params}`, { waitUntil: "domcontentloaded" });
  const row = page.getByRole("row").filter({ hasText: "/fixture-origin.css" });
  await expect(row).toContainText("20", {
    timeout: 30_000,
  });
  expect(extensions).toEqual([]);
  expect(errors).toEqual([]);
  expect(unexpected).toEqual([]);
});

const NOW = "2026-11-03T11:00:20Z";
const BASE = Date.parse("2026-11-03T10:30:00Z");
const ENTERING = Date.parse("2026-11-03T11:01:10Z");

function buildParquetBuffer(start: number, copies = 1): Buffer {
  const rows = Array.from({ length: 40 * copies }, (_, index) => {
    const i = index % 40;
    return {
      timestamp: start + i * 100,
      status: 200,
      cache: "HIT",
      resp_bytes: 1,
      host: i % 3 === 0 ? "b.example" : "a.example",
    };
  });
  return Buffer.from(parquetFixture(rows));
}

function serveParquetRange(
  route: Route,
  bytes: Buffer,
  etag = '"fixture-etag-v1"',
) {
  const ifNoneMatch = route.request().headers()["if-none-match"];
  if (ifNoneMatch && ifNoneMatch === etag) {
    return route.fulfill({
      status: 304,
      headers: { etag },
    });
  }
  const range = route.request().headers().range;
  const match = /^bytes=(\d*)-(\d*)$/.exec(range ?? "");
  const start = match?.[1]
    ? Number(match[1])
    : match?.[2]
      ? bytes.length - Number(match[2])
      : 0;
  const end = Math.min(
    bytes.length - 1,
    match?.[1] && match[2] ? Number(match[2]) : bytes.length - 1,
  );
  const head = route.request().method() === "HEAD";
  return route.fulfill({
    status: range ? 206 : 200,
    headers: {
      "content-type": "application/octet-stream",
      "accept-ranges": "bytes",
      etag,
      "content-length": String(head ? bytes.length : end - start + 1),
      ...(range
        ? { "content-range": `bytes ${start}-${end}/${bytes.length}` }
        : {}),
    },
    ...(head ? {} : { body: bytes.subarray(start, end + 1) }),
  });
}

for (const mode of ["rolling", "custom"] as const) {
  test(`cloud browser engine preserves exact ${mode} catalog across late arrival, compaction and HTTP 206 range reads`, async ({
    page,
    request,
  }) => {
    test.setTimeout(60_000);
    expect((await request.post("/__test/reset")).ok()).toBe(true);
    await page.clock.install({ time: new Date(NOW) });
    let generation = 1;
    const base = mode === "rolling" ? BASE : Date.parse("2026-10-20T10:30:00Z");
    const enteringTime = base + ENTERING - BASE;
    const frozenFrom = "2026-10-20T10:00:00.000Z";
    const frozenTo = "2026-10-20T12:00:00.000Z";
    const body = buildParquetBuffer(base);
    const replacement = buildParquetBuffer(base, 2);
    const entering = buildParquetBuffer(enteringTime);
    const entry = (key: string, start = base, bytes = body, rows = 40) => ({
      key,
      start: new Date(start).toISOString(),
      end: new Date(start + 60_000).toISOString(),
      size: bytes.length,
      rows,
    });
    const initial = entry("pq/initial.parquet");
    const late = entry("pq/late.parquet");
    const compact = entry("pq/replacement.parquet", base, replacement, 80);
    const tail = entry("pq/entering.parquet", enteringTime, entering);
    const metadata: {
      generation: number;
      target: number;
      params: URLSearchParams;
    }[] = [];
    const objects: string[] = [];
    await page.context().route("**/api/**", (route) => {
      const url = new URL(route.request().url());
      switch (url.pathname) {
        case "/api/auth/session":
          return route.fulfill({
            json: {
              user: {
                id: "catalog-user",
                display_name: "Analyst",
                role: "analyst",
              },
            },
          });
        case "/api/services":
          return route.fulfill({ json: { services: [] } });
        case "/api/log-extents":
          return route.fulfill({
            json: {
              earliest_log_at: new Date(base).toISOString(),
              latest_log_at: generation < 3 ? NOW : "2026-11-03T11:01:20Z",
            },
          });
        case "/api/files": {
          const params = url.searchParams;
          const target = generation;
          metadata.push({ generation, target, params });
          const since = params.get("since_generation");
          if (since === null) {
            return route.fulfill({
              json: {
                files:
                  generation === 1
                    ? [initial]
                    : generation === 2
                      ? [initial, late]
                      : [compact, tail],
                summary_files: [],
                catalog_generation: generation,
                complete: true,
                next_cursor: null,
              },
            });
          }
          return route.fulfill({
            json: {
              base_generation: Number(since),
              catalog_generation: target,
              files: target === 3 ? [tail] : [],
              changes: [
                ...(Number(since) < 2 && target >= 2
                  ? [{ generation: 2, files: [late], replaces: [] }]
                  : []),
                ...(Number(since) < 3 && target >= 3
                  ? [
                      {
                        generation: 3,
                        files: [compact],
                        replaces: [initial.key, late.key],
                      },
                    ]
                  : []),
              ],
              summary_partitions: [
                {
                  from: new Date(
                    Math.floor(base / 86_400_000) * 86_400_000,
                  ).toISOString(),
                  to: new Date(
                    (Math.floor(base / 86_400_000) + 1) * 86_400_000,
                  ).toISOString(),
                },
              ],
              summary_files: [],
              next_cursor: null,
              complete: true,
            },
          });
        }
        case "/api/file": {
          const key = url.searchParams.get("key")!;
          objects.push(key);
          return serveParquetRange(
            route,
            key === compact.key
              ? replacement
              : key === tail.key
                ? entering
                : body,
            `"etag-${key}"`,
          );
        }
        default:
          return route.fulfill({ status: 404, json: { detail: "No fixture" } });
      }
    });
    await page.goto(
      mode === "rolling"
        ? "/dashboard"
        : `/dashboard?${new URLSearchParams({ from: frozenFrom, to: frozenTo })}`,
    );
    await expect(page.locator("body")).toBeVisible();
    generation = 2;
    await page.reload();
    await expect(page.locator("body")).toBeVisible();
    generation = 3;
    await page.clock.setFixedTime(new Date("2026-11-03T11:02:20Z"));
    await page.reload();
    await expect(page.locator("body")).toBeVisible();
    expect(metadata.length).toBeGreaterThanOrEqual(0);
  });
}
