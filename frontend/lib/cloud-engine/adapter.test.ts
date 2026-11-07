import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest } from "./adapter";
import { setOriginSqlExecutor } from "./origin";
import { setPerformanceEngineClient } from "./performance";
import { setSecurityEngineClient } from "./security";
import { getDuckDBEngine } from "./dashboard/duckdb";
import * as usage from "./usage/summary";
import { fixtureUsageInput } from "./usage/fixtures";
import * as ops from "./telemetry/client";
import { CloudBudgetError, ensureCloudRange } from "./range-loader";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
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
