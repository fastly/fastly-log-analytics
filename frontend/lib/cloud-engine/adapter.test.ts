import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest } from "./adapter";
import { setOriginSqlExecutor } from "./origin";
import { setPerformanceEngineClient } from "./performance";
import { setSecurityEngineClient } from "./security";
import { getDuckDBEngine } from "./dashboard/duckdb";

vi.mock("./range-loader", () => ({ ensureCloudRange: vi.fn().mockResolvedValue(undefined) }));

describe("adapter.ts Milestone 3 domain route dispatching", () => {
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
