import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest } from "./adapter";
import { ORIGIN_SCAN_COLUMNS, setOriginSqlExecutor } from "./origin";
import { PERFORMANCE_SCAN_COLUMNS, setPerformanceEngineClient } from "./performance";
import { securityScanColumns, setSecurityEngineClient } from "./security";
import { getDuckDBEngine } from "./dashboard/duckdb";
import * as usage from "./usage/summary";
import { fixtureUsageInput } from "./usage/fixtures";
import * as ops from "./telemetry/client";
import { CloudBudgetError, ensureCloudRange } from "./range-loader";
import { insightScanColumns } from "./insight-scans";
import { NETWORK_SCAN_COLUMNS } from "./network";
import { VALUE_SCAN_COLUMNS } from "./value";
import { ASSETS_SCAN_COLUMNS } from "./assets";
import { SESSIONS_SCAN_COLUMNS } from "./sessions";
import { setCloudModeRuntime } from "./mode";
import { networkFromRollup } from "./network-rollup";
import { originFromRollup } from "./origin-rollup";
import { performanceFromRollup } from "./performance-rollup";
import { insightsBaselineFromRollup } from "./insights-rollup";
import { mergeRollups } from "./dashboard/rollup-query";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

vi.mock("./network-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./network-rollup")>(),
  networkFromRollup: vi.fn(async () => null),
}));

vi.mock("./origin-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./origin-rollup")>(),
  originFromRollup: vi.fn(async () => null),
}));

vi.mock("./performance-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./performance-rollup")>(),
  performanceFromRollup: vi.fn(async () => null),
}));

vi.mock("./insights-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./insights-rollup")>(),
  insightsBaselineFromRollup: vi.fn(async () => null),
}));

describe("adapter.ts Milestone 3 domain route dispatching", () => {
  it.each(["analytics", "live-events"])("returns an explicit client error for unsupported RUM %s filters", async (route) => {
    const filters = JSON.stringify({ unknown_field: { mode: "include", values: ["offline-value"] } });
    const response = await handleCloudAnalyticalRequest(new Request(
      `http://localhost/api/services/svc-1/rum/${route}?start_time=2026-11-01T00:00:00Z&end_time=2026-11-02T00:00:00Z&filters=${encodeURIComponent(filters)}`,
    ));
    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({
      detail: { error: "rum_filters_unsupported", message: "Unsupported RUM filter" },
    });
  });

  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn().mockImplementation(async (input: RequestInfo | URL) =>
      Response.json(String(input).includes("/api/services")
        ? { services: [{ service_id: "svc-1", rum: { enabled: true }, scoring: { enabled: false } }] }
        : { latest_log_at: "2026-11-06T00:00:00Z" })));
    setOriginSqlExecutor({
      tableName: "requests",
      getColumns: async () => new Set(["ottfb", "ost", "edge", "pop", "oip", "url", "host"]),
      query: async () => [],
    });
    setPerformanceEngineClient({
      getAvailableColumns: async () => new Set(["url", "asn", "ttl", "elapsed", "ottfb", "ottlb", "cache"]),
      query: async () => [],
    });
    setSecurityEngineClient({
      getAvailableColumns: async () => new Set(["ja3", "ja4", "ua", "ip", "rtt_min", "tcp_rtt", "lat", "lon", "pop", "country"]),
      query: async () => [],
    });
    const engine = getDuckDBEngine();
    vi.spyOn(engine, "executeSql").mockResolvedValue([]);
    vi.spyOn(engine, "getPresentColumns").mockResolvedValue(
      new Set(["status", "url", "ip", "ja4", "tcp_rtt", "edge", "edge_sid", "cmcd_sid"]),
    );
  });

  afterEach(() => {
    setOriginSqlExecutor(null);
    setPerformanceEngineClient(null);
    setSecurityEngineClient(null);
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("preserves historical usage bounds and requested hourly buckets", async () => {
    const from = Date.parse("2026-10-30T12:00:00Z");
    const to = from + 2 * 3_600_000;
    const input = {
      ...fixtureUsageInput,
      now: to,
      rows: {
        fos_usage: [0, 1].map((hour) => ({
          ...fixtureUsageInput.rows.fos_usage[0],
          timestamp: from + hour * 3_600_000 + 1000,
          op_count: 1,
        })),
        api_telemetry: [],
        cron_runs: [],
        cron_ticks: [],
      },
    };
    const summary = usage.summarizeUsage(input);
    const fetchSummary = vi.spyOn(usage, "fetchUsageSummary").mockResolvedValue({
      ...summary, input, summary, lines: [], totalUsd: 0,
    });
    const params = new URLSearchParams({
      start: new Date(from).toISOString(),
      end: new Date(to).toISOString(),
      by: "hour",
    });

    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/usage/operations?${params}`),
    );

    expect(fetchSummary).toHaveBeenCalledWith(
      expect.objectContaining({ from, to }),
    );
    expect(await response!.json()).toMatchObject({
      total_class_a: 2,
      data: expect.arrayContaining([
        expect.objectContaining({ date: "2026-10-30T12:00:00.000Z", class_a: 1 }),
        expect.objectContaining({ date: "2026-10-30T13:00:00.000Z", class_a: 1 }),
      ]),
    });
  });

  it("returns filtered Cloud share audit rows with parseable timestamps and requested limits", async () => {
    const since = Date.parse("2026-01-01T00:00:00Z");
    const until = since + 1500;
    const rows = [
      { timestamp: since + 2000, event_id: "later", actor: "admin@example.test", action: "logout", target: "later" },
      { timestamp: since + 1200, event_id: "selected", actor: "admin@example.test", action: "login", target: "selected" },
      { timestamp: since + 1000, event_id: "older", actor: "admin@example.test", action: "login", target: "older" },
      { timestamp: since + 500, event_id: "other", actor: "other@example.test", action: "login", target: "other" },
      ...Array.from({ length: 250 }, (_, index) => ({
        timestamp: since + 250 - index,
        event_id: `extra-${index}`, actor: "other@example.test",
        action: "logout", target: `extra-${index}`,
      })),
    ];
    const queryLogs = vi.spyOn(ops, "queryOpsLogs").mockResolvedValue({
      kind: "admin_audit", from: "", to: "", rows, oldestLiveMs: null, status: null,
      stats: {
        listed: 0, read: 0, cached: 0, cdnHits: 0, skippedCovered: 0,
        ignoredCompact: 0, evicted: 0, liveRows: rows.length,
      },
    });
    const params = new URLSearchParams({
      since: new Date(since).toISOString(),
      until: new Date(until).toISOString(),
      event_type: "login", email: "admin", limit: "1",
    });

    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/admin/share/audit-logs?${params}`),
    );

    expect(await response!.json()).toMatchObject({
      audit_logs: [{
        timestamp: "2026-01-01T00:00:01.200Z",
        event_type: "login", email: "admin@example.test", details: "selected",
      }],
    });
    expect(queryLogs).toHaveBeenCalledWith(
      "admin_audit",
      { from: new Date(since).toISOString(), to: new Date(until + 1).toISOString() },
      expect.objectContaining({ signal: expect.anything() }),
    );
    const defaultResponse = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/admin/share/audit-logs?since=${new Date(since).toISOString()}&until=${new Date(until).toISOString()}`),
    );
    expect((await defaultResponse!.json()).audit_logs).toHaveLength(200);
  });

  it.each([
    ["limit=0", 422],
    ["limit=2001", 422],
    ["since=not-a-date", 400],
  ])("rejects invalid Audit query %s before reading logs", async (query, status) => {
    const queryLogs = vi.spyOn(ops, "queryOpsLogs");
    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/admin/share/audit-logs?${query}`),
    );
    expect(response!.status).toBe(status);
    expect(queryLogs).not.toHaveBeenCalled();
  });

  it("dispatches /api/origin/* routes through handleOriginRequest", async () => {
    const aggRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/origin/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sections: ["summary"] }),
      }),
    );
    expect(aggRes).not.toBeNull();
    expect(aggRes!.status).toBe(200);
    const aggJson = await aggRes!.json();
    expect(aggJson).toHaveProperty("summary");

    const shieldRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/origin/shielding-analysis", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(shieldRes).not.toBeNull();
    expect(shieldRes!.status).toBe(200);

    const drillRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/origin/drilldown", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ dimension: "host" }),
      }),
    );
    expect(drillRes).not.toBeNull();
    expect(drillRes!.status).toBe(200);
  });

  it("dispatches /api/performance/aggregates through handlePerformanceRequest", async () => {
    const res = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/performance/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sections: ["top_urls", "waterfall"] }),
      }),
    );
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    const json = await res!.json();
    expect(json).toHaveProperty("top_urls");
    expect(json).toHaveProperty("waterfall");
  });

  it("answers an over-budget range with a terminal 413 instead of a retryable failure", async () => {
    const message = "Cloud query range exceeds the browser scan budget; choose a smaller range";
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(new CloudBudgetError(message));
    const res = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/performance/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sections: ["waterfall"] }),
      }),
    );
    expect(res?.status).toBe(413);
    expect(await res?.json()).toEqual({ detail: message });
  });

  it("keeps an over-budget speculative prefetch out of the page's query results but reports the requested window", async () => {
    setCloudModeRuntime(true);
    const states: string[] = [];
    const listener = (event: Event) => states.push((event as CustomEvent<{ state: string }>).detail.state);
    window.addEventListener("fla:cloud-query", listener);
    const request = (headers: Record<string, string>) => new Request("http://localhost/api/insights", {
      method: "POST", headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 720 }),
    });
    try {
      vi.mocked(ensureCloudRange).mockRejectedValueOnce(new CloudBudgetError("over budget"));
      expect((await handleCloudAnalyticalRequest(request({ "x-flp-cloud-prefetch": "1" })))?.status).toBe(413);
      expect(states).toEqual([]);
      vi.mocked(ensureCloudRange).mockRejectedValue(new CloudBudgetError("over budget"));
      expect((await handleCloudAnalyticalRequest(request({})))?.status).toBe(413);
      expect(states).toEqual(["pending", "error"]);
    } finally {
      vi.mocked(ensureCloudRange).mockReset();
      vi.mocked(ensureCloudRange).mockImplementation(async () => getDuckDBEngine().readContext());
      window.removeEventListener("fla:cloud-query", listener);
      setCloudModeRuntime(false);
    }
  });

  it("shortens an over-budget Insights baseline step by step and reports the baseline it applied", async () => {
    const budget = new CloudBudgetError("over budget");
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(budget).mockRejectedValueOnce(budget);
    const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/insights", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 720, filters: {} }),
    }));
    expect(res?.status).toBe(200);
    const body = await res!.json() as { baseline_hours: number; window_hours: number };
    expect(body.baseline_hours).toBe(24);
    expect(body.window_hours).toBe(24);
    const starts = vi.mocked(ensureCloudRange).mock.calls.map(([start]) => Date.parse(start));
    expect(starts).toHaveLength(3);
    expect(starts[0]).toBeLessThan(starts[1]);
    expect(starts[1]).toBeLessThan(starts[2]);
  });

  it("does not shorten other over-budget scans, which have no baseline to give up", async () => {
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(new CloudBudgetError("over budget"));
    const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/insights/cache-collapse-detail", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ url: "/a", window_size_hrs: 24, baseline_hours: 168 }),
    }));
    expect(res?.status).toBe(413);
    expect(vi.mocked(ensureCloudRange)).toHaveBeenCalledTimes(1);
  });

  it("loads Insights scans as column projections, with a smaller own budget for prefetches", async () => {
    const options = async (path: string, init: RequestInit = {}) => {
      vi.mocked(ensureCloudRange).mockClear();
      const res = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, init));
      expect(res).not.toBeNull();
      return vi.mocked(ensureCloudRange).mock.calls[0]?.[4];
    };
    const post = (body: Record<string, unknown>, headers: Record<string, string> = {}) => ({
      method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body),
    });
    const insights = await options("/api/insights", post({
      window_size_hrs: 24, baseline_hours: 168, filters: { host_1: { mode: "include", values: ["example.com"] } },
    }));
    expect(insights?.projection?.columns).toEqual(expect.arrayContaining(["url", "status", "transport", "host"]));
    expect(insights?.projection?.maxBytes).toBeUndefined();
    expect((await options("/api/insight-availability"))?.projection?.columns)
      .toEqual(insightScanColumns([]));
    expect((await options("/api/insights/cache-collapse-detail", post({ url: "/a", window_size_hrs: 24, baseline_hours: 168 })))
      ?.projection?.columns).toEqual(insightScanColumns([]));
    expect((await options("/api/insights", post({ window_size_hrs: 24, baseline_hours: 720 }, { "x-flp-cloud-prefetch": "1" })))
      ?.projection?.maxBytes).toBe(32 * 1024 * 1024);
    expect((await options("/api/security/aggregates", post({ sections: ["summary"] })))?.projection)
      .toEqual({ columns: securityScanColumns(undefined) });
  });

  it("loads every /network scan as the same column projection, plus filtered fields", async () => {
    const options = async (path: string, init: RequestInit = {}) => {
      vi.mocked(ensureCloudRange).mockClear();
      const res = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, init));
      expect(res).not.toBeNull();
      return vi.mocked(ensureCloudRange).mock.calls[0]?.[4];
    };
    const post = (body: Record<string, unknown>) => ({
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
    });
    const columns = [...NETWORK_SCAN_COLUMNS];
    expect((await options("/api/network-health", post({ sections: ["summary"] })))?.projection)
      .toEqual({ columns });
    expect((await options("/api/network-quality", post({})))?.projection).toEqual({ columns });
    expect((await options("/api/network/bundle", post({ sections: ["transport"] })))?.projection).toEqual({ columns });
    expect((await options("/api/network/pop-health"))?.projection).toEqual({ columns });
    expect((await options("/api/network-health", post({
      sections: ["summary"], filters: { host: { mode: "include", values: ["example.com"] } },
    })))?.projection?.columns).toEqual([...columns, "host"]);
  });

  it("answers unfiltered /network reads from summaries before any raw scan, over the scan's range and service", async () => {
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce({ served: "rollup" } as never);
    const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network-health", {
      method: "POST",
      headers: { "content-type": "application/json", "x-service-id": "svc-1" },
      body: JSON.stringify({ sections: ["summary", "heatmap"], bucket_seconds: 900 }),
    }));
    expect(await res!.json()).toEqual({ served: "rollup" });
    expect(ensureCloudRange).not.toHaveBeenCalled();
    const [path, params, source] = vi.mocked(networkFromRollup).mock.calls[0];
    expect(path).toBe("/api/network-health");
    expect(params).toMatchObject({
      sections: ["summary", "heatmap"], bucket_seconds: 900, service_id: "svc-1",
      start_time: "2026-11-05T00:00:00.000Z", end_time: "2026-11-06T00:00:00Z",
    });
    expect(source.cacheScope).toContain("svc-1");

    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce({ served: "pops" } as never);
    const pops = await handleCloudAnalyticalRequest(new Request(
      "http://localhost/api/network/pop-health?start_time=2026-11-05T12:00:00Z&end_time=2026-11-05T18:00:00Z",
    ));
    expect(await pops!.json()).toEqual({ served: "pops" });
    expect(vi.mocked(networkFromRollup).mock.calls[0][1])
      .toMatchObject({ start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z" });
    expect(ensureCloudRange).not.toHaveBeenCalled();
  });

  it("falls back to the raw /network scan when summaries can't answer or fail, but not when aborted", async () => {
    const health = (init: RequestInit = {}) => handleCloudAnalyticalRequest(new Request("http://localhost/api/network-health", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sections: ["summary"] }), ...init,
    }));
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce(null);
    expect((await health())!.status).toBe(200);
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(networkFromRollup).mockClear().mockRejectedValueOnce(new Error("truncated bundle"));
    expect((await health())!.status).toBe(200);
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("reading raw Parquet"), expect.any(Error));

    const controller = new AbortController();
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(networkFromRollup).mockClear().mockImplementationOnce(async () => {
      controller.abort();
      throw controller.signal.reason;
    });
    await expect(health({ signal: controller.signal })).rejects.toBeDefined();
    expect(ensureCloudRange).not.toHaveBeenCalled();

    vi.mocked(networkFromRollup).mockClear();
    await health({ body: JSON.stringify({ sections: ["summary"], filter_pop: "IAD" }) });
    expect(networkFromRollup).not.toHaveBeenCalled();
  });

  it("answers unfiltered /origin and /performance sections from summaries before any raw scan, over the scan's range and service", async () => {
    const post = (path: string, body: Record<string, unknown>) => handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-service-id": "svc-1" },
      body: JSON.stringify(body),
    }));
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(originFromRollup).mockClear().mockResolvedValueOnce({ served: "origin" } as never);
    const origin = await post("/api/origin/aggregates", { sections: ["ip_health"], filters: {} });
    expect(await origin!.json()).toEqual({ served: "origin" });
    const [path, params, source] = vi.mocked(originFromRollup).mock.calls[0];
    expect(path).toBe("/api/origin/aggregates");
    expect(params).toMatchObject({
      sections: ["ip_health"], service_id: "svc-1",
      start_time: "2026-11-05T00:00:00.000Z", end_time: "2026-11-06T00:00:00Z",
    });
    expect(source.cacheScope).toContain("svc-1");

    vi.mocked(performanceFromRollup).mockClear().mockResolvedValueOnce({ served: "performance" } as never);
    const perf = await post("/api/performance/aggregates", {
      sections: ["waterfall"], start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z",
    });
    expect(await perf!.json()).toEqual({ served: "performance" });
    expect(vi.mocked(performanceFromRollup).mock.calls[0][1])
      .toMatchObject({ start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z" });
    expect(ensureCloudRange).not.toHaveBeenCalled();
  });

  it("falls back to the raw /origin and /performance scans when summaries can't answer or fail, but not when aborted", async () => {
    const origin = (init: RequestInit = {}) => handleCloudAnalyticalRequest(new Request("http://localhost/api/origin/aggregates", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sections: ["ip_health"] }), ...init,
    }));
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(originFromRollup).mockClear().mockResolvedValueOnce(null);
    expect((await origin())!.status).toBe(200);
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(performanceFromRollup).mockClear().mockRejectedValueOnce(new Error("truncated bundle"));
    const perf = await handleCloudAnalyticalRequest(new Request("http://localhost/api/performance/aggregates", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ sections: ["ttl_dist"] }),
    }));
    expect(perf!.status).toBe(200);
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("reading raw Parquet"), expect.any(Error));

    const controller = new AbortController();
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(originFromRollup).mockClear().mockImplementationOnce(async () => {
      controller.abort();
      throw controller.signal.reason;
    });
    await expect(origin({ signal: controller.signal })).rejects.toBeDefined();
    expect(ensureCloudRange).not.toHaveBeenCalled();

    vi.mocked(originFromRollup).mockClear();
    await origin({ body: JSON.stringify({ sections: ["ip_health"], filters: { pop: { mode: "include", values: ["IAD"] } } }) });
    expect(originFromRollup).not.toHaveBeenCalled();
  });

  it("reads only the Insights window raw when summaries answer its unfiltered baseline", async () => {
    const hour = 3_600_000;
    const end = Date.parse("2026-11-06T00:00:00Z");
    const iso = (ms: number) => new Date(ms).toISOString();
    const insights = async (headers: Record<string, string> = {}) => {
      vi.mocked(ensureCloudRange).mockClear();
      vi.mocked(insightsBaselineFromRollup).mockClear().mockResolvedValueOnce({
        rollup: mergeRollups([]), dataFromMs: end - 192 * hour, dataThroughMs: end - 25 * hour,
      });
      const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/insights", {
        method: "POST", headers: { "content-type": "application/json", "x-service-id": "svc-1", ...headers },
        body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 168, filters: {} }),
      }));
      expect(res?.status).toBe(200);
      return { json: await res!.json() as { rollup?: unknown }, range: vi.mocked(ensureCloudRange).mock.calls };
    };

    const served = await insights();
    expect(served.json.rollup).toMatchObject({ data_from: iso(end - 192 * hour), data_through: iso(end - 25 * hour) });
    expect(served.range).toHaveLength(1);
    expect(served.range[0].slice(0, 2)).toEqual([iso(end - 24 * hour), "2026-11-06T00:00:00Z"]);
    const [body, filters, source] = vi.mocked(insightsBaselineFromRollup).mock.calls[0];
    expect(body).toMatchObject({ window_size_hrs: 24, baseline_hours: 168, clamp_end: "2026-11-06T00:00:00Z" });
    expect(filters).toEqual([]);
    expect(source.cacheScope).toContain("svc-1");

    const prefetch = await insights({ "x-flp-cloud-prefetch": "1" });
    expect(prefetch.range[0][0]).toBe(iso(end - 24 * hour));
    expect(prefetch.range[0][4]?.projection?.maxBytes).toBeGreaterThan(0);
  });

  it("falls back to the raw Insights baseline when summaries can't answer or fail, but not when aborted", async () => {
    const insights = (init: RequestInit = {}) => handleCloudAnalyticalRequest(new Request("http://localhost/api/insights", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 168, filters: {} }), ...init,
    }));
    const rawStart = new Date(Date.parse("2026-11-06T00:00:00Z") - 192 * 3_600_000).toISOString();
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(insightsBaselineFromRollup).mockClear().mockResolvedValueOnce(null);
    const unserved = await insights();
    expect(await unserved!.json()).not.toHaveProperty("rollup");
    expect(vi.mocked(ensureCloudRange).mock.calls.map(([start]) => start)).toEqual([rawStart]);

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(insightsBaselineFromRollup).mockClear().mockRejectedValueOnce(new Error("truncated bundle"));
    expect((await insights())!.status).toBe(200);
    expect(vi.mocked(ensureCloudRange).mock.calls.map(([start]) => start)).toEqual([rawStart]);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("reading raw Parquet"), expect.any(Error));

    const controller = new AbortController();
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(insightsBaselineFromRollup).mockClear().mockImplementationOnce(async () => {
      controller.abort();
      throw controller.signal.reason;
    });
    await expect(insights({ signal: controller.signal })).rejects.toBeDefined();
    expect(ensureCloudRange).not.toHaveBeenCalled();

    vi.mocked(insightsBaselineFromRollup).mockClear();
    await insights({ body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 168,
      filters: { country: { mode: "include", values: ["US"] } } }) });
    expect(vi.mocked(insightsBaselineFromRollup).mock.calls[0]?.[1]).not.toEqual([]);
  });

  it("reads a deferred-detector follow-up raw under its own projection, shortening its baseline when over budget", async () => {
    const detectors = ["error_spikes", "pop_latency_regression"];
    const followUp = () => handleCloudAnalyticalRequest(new Request("http://localhost/api/insights", {
      method: "POST", headers: { "content-type": "application/json", "x-service-id": "svc-1" },
      body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 168, filters: {},
        end_time: "2026-11-06T00:00:00Z", detectors }),
    }));
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(insightsBaselineFromRollup).mockClear();
    expect((await followUp())?.status).toBe(200);
    expect(insightsBaselineFromRollup).not.toHaveBeenCalled();
    const [[start, end, , , options]] = vi.mocked(ensureCloudRange).mock.calls;
    expect([start, end]).toEqual([new Date(Date.parse("2026-11-06T00:00:00Z") - 192 * 3_600_000).toISOString(),
      "2026-11-06T00:00:00Z"]);
    expect(options?.projection?.columns).toEqual(insightScanColumns([], detectors));
    expect(options?.projection?.columns).not.toContain("ua");

    const budget = new CloudBudgetError("over budget");
    vi.mocked(ensureCloudRange).mockClear().mockRejectedValueOnce(budget);
    const shortened = await followUp();
    expect((await shortened!.json() as { baseline_hours: number }).baseline_hours).toBe(24);
    expect(vi.mocked(ensureCloudRange).mock.calls).toHaveLength(2);
  });

  it("loads /origin, /fastly-value, /assets-shield and /performance scans as their routes' column projections", async () => {
    const options = async (path: string, body?: Record<string, unknown>) => {
      vi.mocked(ensureCloudRange).mockClear();
      const res = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, body ? {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
      } : {}));
      expect(res).not.toBeNull();
      return vi.mocked(ensureCloudRange).mock.calls[0]?.[4]?.projection;
    };
    const filters = { host_1: { mode: "include", values: ["example.com"] } };
    expect(await options("/api/origin/aggregates", { filters, sections: ["summary"] }))
      .toEqual({ columns: [...ORIGIN_SCAN_COLUMNS.aggregates, "host"] });
    expect(await options("/api/origin/slow-urls")).toEqual({ columns: [...ORIGIN_SCAN_COLUMNS.aggregates] });
    expect(await options("/api/origin/shielding-analysis", {}))
      .toEqual({ columns: [...ORIGIN_SCAN_COLUMNS["shielding-analysis"]] });
    expect(await options("/api/origin/drilldown", { dimension: "pop" })).toEqual({ columns: [...ORIGIN_SCAN_COLUMNS.drilldown] });
    expect(await options("/api/value/summary", { sections: ["overview"], custom_filters: { team: { mode: "include", values: ["a"] } } }))
      .toEqual({ columns: [...VALUE_SCAN_COLUMNS, "team"] });
    expect(await options("/api/assets/aggregates", { filters: [{ column: "host", operator: "=", value: "a" }] }))
      .toEqual({ columns: [...ASSETS_SCAN_COLUMNS, "host"] });
    expect(await options("/api/origin/aggregates", { filters: [filters] })).toBeUndefined();
    expect(await options("/api/value/summary", { filters: "host" })).toBeUndefined();
    expect(await options("/api/assets/aggregates", { filters: 5 })).toBeUndefined();
    expect(await options("/api/performance/aggregates", { filters, sort_by: "p99" }))
      .toEqual({ columns: [...PERFORMANCE_SCAN_COLUMNS, "host"] });
    expect(await options("/api/performance", {})).toEqual({ columns: [...PERFORMANCE_SCAN_COLUMNS] });
    expect(await options("/api/performance/aggregates", { filters: [filters] })).toBeUndefined();
    const range = { start_time: "2026-11-05T00:00:00Z", end_time: "2026-11-06T00:00:00Z" };
    expect(await options("/api/sessions", range)).toEqual({ columns: [...SESSIONS_SCAN_COLUMNS] });
    expect(await options("/api/sessions", { ...range, filters: { host: ["example.com"] } }))
      .toEqual({ columns: [...SESSIONS_SCAN_COLUMNS, "host"] });
    expect(await options("/api/sessions", { ...range, filters: [filters] })).toBeUndefined();
  });

  it("dispatches /api/security/* routes including /proxies/export through handleSecurityRequest", async () => {
    const aggRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/security/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sections: ["tls_fingerprints"] }),
      }),
    );
    expect(aggRes).not.toBeNull();
    expect(aggRes!.status).toBe(200);

    const tiRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/security/threat-intel", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(tiRes).not.toBeNull();
    expect(tiRes!.status).toBe(200);

    const expRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/security/proxies/export?format=fastly-acl", {
        method: "GET",
      }),
    );
    expect(expRes).not.toBeNull();
    expect(expRes!.status).toBe(200);
    const csv = await expRes!.text();
    expect(csv).toContain("ip,comment");
  });

  it("dispatches /api/network/*, /api/value/summary, and /api/assets/aggregates", async () => {
    const netRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/network/bundle", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(netRes).not.toBeNull();
    expect(netRes!.status).toBe(200);
    const netJson = await netRes!.json();
    expect(netJson).toHaveProperty("network_health");
    expect(netJson).toHaveProperty("network_quality");

    const valRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/value/summary", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(valRes).not.toBeNull();
    expect(valRes!.status).toBe(200);
    const valJson = await valRes!.json();
    expect(valJson).toHaveProperty("overview");
    expect(valJson).toHaveProperty("caching");

    const assetsRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/assets/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(assetsRes).not.toBeNull();
    expect(assetsRes!.status).toBe(200);
    const assetsJson = await assetsRes!.json();
    expect(assetsJson).toHaveProperty("asset_type_breakdown");
  });

  it("dispatches /api/services/{id}/rum/* and /api/cmcd/aggregates", async () => {
    const rumRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/services/svc-1/rum/analytics", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(rumRes).not.toBeNull();
    expect(rumRes!.status).toBe(200);
    const rumJson = await rumRes!.json();
    expect(rumJson).toHaveProperty("metrics");

    const cmcdRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/cmcd/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(cmcdRes).not.toBeNull();
    expect(cmcdRes!.status).toBe(200);
    const cmcdJson = await cmcdRes!.json();
    expect(cmcdJson).toHaveProperty("sessions_ts");
    expect(cmcdJson).toHaveProperty("session_duration_dist");
  });

  it("dispatches /api/sessions, /api/services/{id}/scoring/*, /api/presets, /api/query, and /api/insights", async () => {
    const sessRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/sessions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          start_time: "2026-11-05T00:00:00Z",
          end_time: "2026-11-05T01:00:00Z",
        }),
      }),
    );
    expect(sessRes).not.toBeNull();
    expect(sessRes!.status).toBe(200);
    const sessJson = await sessRes!.json();
    expect(sessJson).toHaveProperty("sessions");

    const scoreRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/services/svc-1/scoring/status", {
        method: "GET",
      }),
    );
    expect(scoreRes).not.toBeNull();
    expect(scoreRes!.status).toBe(200);
    expect(await scoreRes!.json()).toMatchObject({ enabled: false, supported: false });

    const presetsRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/presets", { method: "GET" }),
    );
    expect(presetsRes).not.toBeNull();
    const presetsJson = await presetsRes!.json();
    expect(Array.isArray(presetsJson)).toBe(true);
    expect(presetsJson.length).toBe(26);

    const blockedQueryRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: "DROP TABLE logs" }),
      }),
    );
    expect(blockedQueryRes).not.toBeNull();
    expect(blockedQueryRes!.status).toBe(403);

    const availRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/insight-availability", { method: "GET" }),
    );
    expect(availRes).not.toBeNull();
    const availJson = await availRes!.json();
    expect(availJson.insights).toHaveLength(51);

    const insightsRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/insights", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ window_size_hrs: 24, baseline_hours: 168 }),
      }),
    );
    expect(insightsRes).not.toBeNull();
    expect(insightsRes!.status).toBe(200);
    const insightsJson = await insightsRes!.json();
    expect(Array.isArray(insightsJson.insights)).toBe(true);
  });
});
