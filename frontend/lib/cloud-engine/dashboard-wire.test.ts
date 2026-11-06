// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { handleCloudAnalyticalRequest } from "./adapter";
import { runInWorker } from "./dashboard/client";
import { getDuckDBEngine } from "./dashboard/duckdb";
import type { DashboardResult } from "./dashboard/engine";

vi.mock("./dashboard/client", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/client")>(),
  runInWorker: vi.fn(),
}));
vi.mock("./range-loader", () => ({ ensureCloudRange: vi.fn().mockResolvedValue(undefined) }));

const from = "2026-01-01T00:00:00Z";
const to = "2026-01-01T01:00:00Z";
const result: DashboardResult = {
  from,
  to,
  totalRequests: 3,
  hasBytes: true,
  buckets: [{
    t: Date.parse(from),
    requests: 3,
    bytes: 1500,
    s4xx: 1,
    s5xx: 1,
    hits: 1,
    cacheRows: 3,
  }],
  cards: {
    country: {
      total: 3,
      distinct: 2,
      top: [{ value: "US", count: 2 }],
      all: [{ value: "US", count: 2 }, { value: "CA", count: 1 }],
    },
    host: {
      total: 3,
      distinct: 1,
      top: [{ value: "cdn.example.test", count: 3 }],
    },
    ip: { total: 3, distinct: 2, top: [{ value: "192.0.2.1", count: 2 }] },
  },
  stats: { listed: 1, fetched: 1, cached: 0, cdnHits: 0, rows: 3, bytes: 128, elapsedMs: 1 },
};

beforeEach(() => {
  vi.mocked(runInWorker).mockResolvedValue(result);
  vi.stubGlobal("fetch", vi.fn(async () => new Response(null, { status: 204 })));
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("Cloud dashboard response contract", () => {
  it("does not register built-in cards twice when the page requests its visible fields", async () => {
    await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/aggregates", {
      method: "POST",
      body: JSON.stringify({ start_time: from, end_time: to, fields: ["country", "host", "ip", "plan", "plan"] }),
    }));
    expect(vi.mocked(runInWorker).mock.lastCall?.[0]).toMatchObject({
      customFields: [{ id: "plan", type: "string" }],
    });
  });

  it.each([
    ["requests", 6],
    ["5xx", 2],
    ["4xx", 2],
    ["hit_rate", 33.33],
  ])("aggregates %s at the selected interval", async (metric, value) => {
    vi.mocked(runInWorker).mockResolvedValueOnce({
      ...result,
      totalRequests: 6,
      buckets: [result.buckets[0], { ...result.buckets[0], t: Date.parse(from) + 60_000 }],
      stats: { ...result.stats, rows: 6 },
    });
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/aggregates", {
      method: "POST",
      body: JSON.stringify({ start_time: from, end_time: to, chart_metric: metric, chart_interval: "5 minutes" }),
    }));
    expect(await response!.json()).toMatchObject({
      metric,
      interval: "5 minutes",
      time_series: [{ time: new Date(from).toISOString(), value }],
    });
  });

  it.each([
    ["requests", [1, 2]],
    ["4xx", [0, 1]],
    ["5xx", [0, 1]],
    ["hit_rate", [100, 50]],
    ["error_rate", [0, 100]],
    ["bandwidth", [100, 500]],
  ])("measures one-second %s buckets from raw events", async (metric, values) => {
    const engine = getDuckDBEngine();
    await engine.reset();
    const times = [15_000, 20_000, 20_500].map((offset) => new Date(Date.parse(from) + offset));
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: times },
      { name: "status", type: "INT32", data: [200, 404, 503] },
      { name: "cache", type: "STRING", data: ["HIT", "MISS", "HIT"] },
      { name: "resp_bytes", type: "INT32", data: [100, 200, 300] },
    ] });
    const lease = engine.lease([]);
    try {
      await lease.ingestFile("subminute.parquet", "subminute.parquet", bytes);
    } finally {
      lease.release();
    }

    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/aggregates", {
      method: "POST",
      body: JSON.stringify({ start_time: from, end_time: to, chart_metric: metric, chart_interval: "1 second" }),
    }));

    expect(await response!.json()).toMatchObject({
      interval: "1 second",
      time_series: [15_000, 20_000].map((offset, index) => ({
        time: new Date(Date.parse(from) + offset).toISOString(),
        value: values[index],
      })),
    });
  });

  it.each([
    ["p50_latency", 4, {}],
    ["p95_latency", 9.4, {}],
    ["p99_latency", 9.88, {}],
    ["ttfb", 9.4, {}],
    ["ttfb_ms", 4, {}],
    ["req_size", 55, {}],
    ["throughput", 5, {}],
    ["p50_latency", 10, { filters: { country: { mode: "include", values: ["CA"] } } }],
    ["p50_latency", 10, { custom_filters: { custom_weight: { mode: "include", values: ["2.0"] } } }],
  ])("measures %s from native Parquet within the requested scope", async (metric, value, scope) => {
    const engine = getDuckDBEngine();
    await engine.reset();
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(from), new Date(Date.parse(from) + 60_000), new Date(Date.parse(from) + 120_000), new Date(Date.parse(to) + 1)] },
      { name: "elapsed", type: "DOUBLE", data: [1000, 4000, 10000, 900_000] },
      { name: "ttfb", type: "DOUBLE", data: [0.001, 0.004, 0.01, 0.9] },
      { name: "req_bytes", type: "INT32", data: [10, 40, 300, 9000] },
      { name: "req_header_bytes", type: "INT32", data: [5, 15, 0, 9000] },
      { name: "resp_bytes", type: "INT32", data: [900, 1500, 600, 90000] },
      { name: "cache", type: "STRING", data: ["HIT", "MISS", "HIT", "HIT"] },
      { name: "country", type: "STRING", data: ["US", "US", "CA", "CA"] },
      { name: "custom_weight", type: "DOUBLE", data: [1, 1, 2, 2] },
    ] });
    const lease = engine.lease([{ id: "custom_weight", type: "number" }]);
    try {
      await lease.ingestFile("native-request.parquet", "native-request.parquet", bytes);
    } finally {
      lease.release();
    }
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/aggregates", {
      method: "POST",
      body: JSON.stringify({ start_time: from, end_time: to, chart_metric: metric, chart_interval: "5 minutes", ...scope }),
    }));
    const body = await response!.json();
    expect(body.time_series).toHaveLength(1);
    expect(body.time_series[0]).toMatchObject({ time: new Date(from).toISOString() });
    expect(body.time_series[0].value).toBeCloseTo(value);
  });

  it.each(["/api/dashboard/aggregates", "/api/aggregates", "/api/dashboard/bundle"])(
    "delivers retained dimensions and every country to the consumer of %s",
    async (path) => {
      const response = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, {
        method: "POST",
        body: JSON.stringify({ start_time: from, end_time: to, chart_metric: "requests", chart_interval: "1 minute" }),
      }));
      const body = await response!.json();
      const aggregates = path.endsWith("/bundle") ? body.aggregates : body;
      expect(aggregates).toMatchObject({
        data: {
          country: { top: [{ value: "US", count: 2 }], total: 3 },
          host: { top: [{ value: "cdn.example.test", count: 3 }], total: 3 },
        },
        map_data: [{ country: "US", count: 2 }, { country: "CA", count: 1 }],
        total_rows: 3,
        total_rows_total: 3,
        metric: "requests",
        interval: "1 minute",
      });
    },
  );
});
