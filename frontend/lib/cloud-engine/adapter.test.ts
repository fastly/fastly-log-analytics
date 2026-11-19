import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest } from "./adapter";
import { ORIGIN_SCAN_COLUMNS, setOriginSqlExecutor } from "./origin";
import { PERFORMANCE_SCAN_COLUMNS, setPerformanceEngineClient } from "./performance";
import { securityScanColumns, setSecurityEngineClient } from "./security";
import { getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import * as usage from "./usage/summary";
import { fixtureUsageInput } from "./usage/fixtures";
import { resetComputeBillingCache } from "./usage/computeUsage";
import * as ops from "./telemetry/client";
import { CloudBudgetError, ensureCloudRange } from "./range-loader";
import { insightScanColumns } from "./insight-scans";
import { NETWORK_SCAN_COLUMNS, networkScanColumns } from "./network";
import { VALUE_SCAN_COLUMNS } from "./value";
import { ASSETS_SCAN_COLUMNS } from "./assets";
import { SESSIONS_SCAN_COLUMNS } from "./sessions";
import { setCloudModeRuntime } from "./mode";
import { networkFromRollup } from "./network-rollup";
import { originFromRollup } from "./origin-rollup";
import { performanceFromRollup } from "./performance-rollup";
import { insightsBaselineFromRollup } from "./insights-rollup";
import { securityFromRollup } from "./security-rollup";
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

vi.mock("./security-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./security-rollup")>(),
  securityFromRollup: vi.fn(async () => null),
}));

vi.mock("./insights-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./insights-rollup")>(),
  insightsBaselineFromRollup: vi.fn(async () => null),
}));

describe("adapter.ts Milestone 3 domain route dispatching", () => {
  it("passes geography bounds and grouping to the Cloud admin route unchanged", async () => {
    const payload = {
      source: "fastly",
      group_by: "datacenter",
      data: [],
      requests: 0,
      bandwidth_bytes: 0,
    };
    const fetchMock = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetchMock);

    const response = await handleCloudAnalyticalRequest(new Request(
      "http://localhost/api/usage/geography?start=2026-11-01T00%3A00%3A00.000Z&end=2026-11-02T00%3A00%3A00.000Z&group_by=datacenter",
    ));

    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual(payload);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/admin/usage/geography?start=2026-11-01T00%3A00%3A00.000Z&end=2026-11-02T00%3A00%3A00.000Z&group_by=datacenter",
      expect.objectContaining({ signal: expect.anything() }),
    );
  });

  it("answers /api/usage/platform from the Cloud admin route unchanged", async () => {
    const payload = {
      source: "partial",
      storage: { status: "ok", scope: "account", bytes: 2048 },
      compute: { status: "ok", scope: "app_and_worker", requests: 7 },
      ngwaf: { status: "not_configured" },
    };
    const fetchMock = vi.fn().mockResolvedValue(Response.json(payload));
    vi.stubGlobal("fetch", fetchMock);

    const response = await handleCloudAnalyticalRequest(new Request(
      "http://localhost/api/usage/platform?from=1763424000000&to=1763510400000",
    ));

    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual(payload);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/admin/usage/platform?from=1763424000000&to=1763510400000"),
      expect.objectContaining({ signal: expect.anything() }),
    );
  });

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
    resetComputeBillingCache();
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

  it("answers /api/usage/compute from Platform Metrics without reading the usage logs", async () => {
    const from = Date.parse("2026-11-10T00:00:00Z");
    const to = from + 2 * 3_600_000;
    const fetchSummary = vi.spyOn(usage, "fetchUsageSummary");
    const fetchBilling = vi.spyOn(usage, "fetchComputeBilling").mockResolvedValue({
      requests: 5, executionTimeMs: 50, billedTimeMs: 500, coveredFrom: from, coveredTo: from + 3_600_000,
      computeBandwidthBytes: 0, cdnRequests: null, kvClassA: 0, kvClassB: 0, secrets: null,
      byRole: { app: { requests: 3, vcpuMs: 30 }, worker: { requests: 2, vcpuMs: 20 } },
      computeByHour: [{ t: from, appRequests: 3, workerRequests: 2, appVcpuMs: 30, workerVcpuMs: 20, billedMs: 500 }],
    });
    const params = new URLSearchParams({
      start_time: new Date(from).toISOString(),
      end_time: new Date(to).toISOString(),
    });

    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/usage/compute?${params}`),
    );

    expect(fetchBilling).toHaveBeenCalledWith({ from, to }, expect.anything());
    expect(fetchSummary).not.toHaveBeenCalled();
    expect(await response!.json()).toMatchObject({
      source: "fastly",
      requests: 5,
      vcpu_ms: 50,
      data: [{ time: "2026-11-10T00:00:00.000Z", app_requests: 3, worker_requests: 2 }],
    });
  });

  describe("Incurred Cost lines on /api/usage/current-storage", () => {
    const from = Date.parse("2026-11-10T00:00:00Z");
    const stats = {
      requests: 5, executionTimeMs: 50, billedTimeMs: 500, coveredFrom: from, coveredTo: from + 3_600_000,
      computeBandwidthBytes: 0, cdnRequests: null, kvClassA: 7, kvClassB: 0, secrets: 3,
      byRole: { app: { requests: 3, vcpuMs: 30 }, worker: { requests: 2, vcpuMs: 20 } },
      computeByHour: [],
    };
    const load = async (path: string, rangeMs: number) => {
      const params = new URLSearchParams({
        start_time: new Date(from).toISOString(),
        end_time: new Date(from + rangeMs).toISOString(),
      });
      const response = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}?${params}`));
      return response!.json();
    };
    const line = (body: { seven_line_table: Array<{ id: string }> }, id: string) =>
      body.seven_line_table.find((l) => l.id === id);
    beforeEach(() => {
      const summary = usage.summarizeUsage(fixtureUsageInput);
      vi.spyOn(usage, "fetchUsageSummary").mockResolvedValue({
        ...summary, input: fixtureUsageInput, summary, lines: [], totalUsd: 0,
      });
      vi.spyOn(usage, "fetchUsageKind").mockResolvedValue([]);
    });

    it("prices Compute, KV and secrets from Platform Metrics, shared with the Compute charts' read", async () => {
      const fetchBilling = vi.spyOn(usage, "fetchComputeBilling").mockResolvedValue(stats);

      await load("/api/usage/compute", 2 * 3_600_000);
      const body = await load("/api/usage/current-storage", 2 * 3_600_000);

      expect(fetchBilling).toHaveBeenCalledTimes(1);
      expect(line(body, "compute")).toMatchObject({ source: "fastly", detail: "5 reqs · 50 vCPU ms" });
      expect(line(body, "kv")).toMatchObject({ source: "fastly" });
      // 3 secrets over the 2-hour range, not a default 30 days.
      expect(line(body, "secrets")).toMatchObject({ source: "fastly", detail: "3 secrets × 0.1 days" });
    });

    it("falls back to the logged lines when Platform Metrics fails", async () => {
      vi.spyOn(usage, "fetchComputeBilling").mockRejectedValue(new Error("stats down"));

      const body = await load("/api/usage/current-storage", 2 * 3_600_000);

      expect(line(body, "compute")).toMatchObject({ source: "logged" });
      expect(line(body, "kv")).toMatchObject({ source: "none" });
    });

    it("falls back to the logged lines without a Stats read for ranges over 31 days", async () => {
      const fetchBilling = vi.spyOn(usage, "fetchComputeBilling").mockResolvedValue(stats);

      const body = await load("/api/usage/current-storage", 32 * 86_400_000);

      expect(fetchBilling).not.toHaveBeenCalled();
      expect(line(body, "compute")).toMatchObject({ source: "logged" });
    });
  });

  it.each([
    ["covers the billing window", 31, 0],
    ["is shorter than the billing window", 1 / 24, 1],
  ])("reports bytes deleted in the last 30 days as still billed when the range %s", async (_, rangeDays, kindReads) => {
    const now = fixtureUsageInput.now;
    vi.spyOn(Date, "now").mockReturnValue(now);
    const deletion = (ageDays: number, bytes: number) => ({
      ...fixtureUsageInput.rows.fos_usage[0], op: "DELETE_OBJECTS", timestamp: now - ageDays * 86_400_000,
      bytes_in: 0, bytes_deleted: bytes,
    });
    const fosUsage = [deletion(40, 9_000), deletion(1, 2_000), deletion(1 / 48, 500)];
    const input = { ...fixtureUsageInput, rows: { ...fixtureUsageInput.rows, fos_usage: rangeDays >= 30 ? fosUsage : [] } };
    const summary = usage.summarizeUsage(input);
    vi.spyOn(usage, "fetchUsageSummary").mockResolvedValue({ ...summary, input, summary, lines: [], totalUsd: 0 });
    const fetchKind = vi.spyOn(usage, "fetchUsageKind").mockResolvedValue(fosUsage);
    const params = new URLSearchParams({
      start: new Date(now - rangeDays * 86_400_000).toISOString(),
      end: new Date(now).toISOString(),
    });

    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/usage/current-storage?${params}`),
    );

    expect(await response!.json()).toMatchObject({ deleted_bytes: 2_500 });
    expect(fetchKind).toHaveBeenCalledTimes(kindReads);
    if (kindReads) {
      expect(fetchKind.mock.calls[0][1]).toEqual({ from: now - 30 * 86_400_000, to: now });
    }
  });

  const mockAuditRows = (rows: Array<Record<string, unknown>>) =>
    vi.spyOn(ops, "queryOpsLogs").mockResolvedValue({
      kind: "admin_audit", from: "", to: "", rows: rows as never, oldestLiveMs: null, status: null,
      stats: {
        listed: 0, read: 0, cached: 0, cdnHits: 0, skippedCovered: 0,
        ignoredCompact: 0, evicted: 0, liveRows: rows.length,
      },
    });
  const auth = { domain: "auth", source: "auth_archive" };
  const controlPlane = { domain: "control_plane", source: "svc_outbox" };

  it("returns filtered Cloud share audit rows with parseable timestamps and requested limits", async () => {
    const since = Date.parse("2026-01-01T00:00:00Z");
    const until = since + 1500;
    const rows = [
      { ...auth, timestamp: since + 2000, actor: "admin-user", action: "login", target: "admin-user", ip: "198.51.100.1" },
      { ...auth, timestamp: since + 1200, actor: "admin-user", action: "login", target: "admin-user", ip: "198.51.100.7" },
      { ...auth, timestamp: since + 1000, actor: "admin-user", action: "login", target: "admin-user", ip: "198.51.100.8" },
      { ...auth, timestamp: since + 900, actor: "other-user", action: "login", target: "other-user", ip: "198.51.100.9" },
      ...Array.from({ length: 250 }, (_, index) => index % 2
        ? { ...controlPlane, timestamp: since + 800, actor: "clock", action: "job.completed", job_id: `job-${index}` }
        : { ...auth, timestamp: since + 800, actor: "anonymous", action: "login_challenge", target: `c-${index}`, ip: "198.51.100.2" }),
      ...Array.from({ length: 250 }, (_, index) => ({
        ...auth, timestamp: since + 500 - index, actor: "other-user", action: "login", target: "other-user",
      })),
    ];
    const queryLogs = mockAuditRows(rows);
    const params = new URLSearchParams({
      since: new Date(since).toISOString(),
      until: new Date(until).toISOString(),
      event_type: "LOGIN_SUCCESS", email: "admin", limit: "1",
    });

    const response = await handleCloudAnalyticalRequest(
      new Request(`http://localhost/api/admin/share/audit-logs?${params}`),
    );

    expect(await response!.json()).toEqual({
      audit_logs: [{
        id: 1, timestamp: "2026-01-01T00:00:01.200Z", event_type: "LOGIN_SUCCESS",
        email: "admin-user", ip_address: "198.51.100.7", details: "",
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
    const defaultRows = (await defaultResponse!.json()).audit_logs as Array<{ event_type: string; ip_address: string }>;
    expect(defaultRows).toHaveLength(200);
    expect(new Set(defaultRows.map((r) => r.event_type))).toEqual(new Set(["LOGIN_SUCCESS"]));
    expect(defaultRows.at(-1)!.ip_address).toBe("-");
  });

  it("keeps only invite, accept, login and access events in the Cloud share audit under share event names", async () => {
    const at = Date.parse("2026-01-01T00:00:00Z");
    const ip = "203.0.113.4";
    const rows = [
      { ...auth, actor: "owner-id", action: "invite_created", target: "inv-1", ip },
      { ...auth, actor: "owner-id", action: "invite_revoked", target: "inv-2", ip },
      { ...auth, actor: "analyst-id", action: "user_registered", target: "analyst-id", ip },
      { ...auth, actor: "analyst-id", action: "login", target: "analyst-id", ip },
      { ...auth, actor: "anonymous", action: "authentication_failure", target: "login_verify", ip },
      { ...auth, actor: "owner-id", action: "role_changed", target: "analyst-id", ip },
      { ...auth, actor: "owner-id", action: "credential_revoked", target: "analyst-id", ip },
      { ...auth, actor: "owner-id", action: "user_revoked", target: "analyst-id", ip },
      { ...auth, actor: "setup", action: "setup_grant", target: "setup", ip },
      { actor: "owner-id", action: "invite_created", target: "inv-legacy" },
      ...[
        "login_challenge", "login_attempt", "registration_challenge", "registration_attempt",
        "challenge_rate", "setup_generation", "setup_closed", "setup_failure",
        "usage_logging_updated", "quarantine_purged",
      ].map((action) => ({ ...auth, actor: "anonymous", action, target: "x", ip })),
      ...["job.started", "job.completed", "endpoint.claimed", "bucket.checked", "config.updated", "data_reset.requested"]
        .map((action) => ({ ...controlPlane, actor: "clock", action, job_id: "job-1" })),
      { domain: "control_plane", actor: "clock", action: "invite_created", target: "inv-3" },
    ].map((row, index) => ({ ...row, timestamp: at - index }));
    mockAuditRows(rows);
    const query = (extra: Record<string, string> = {}) => handleCloudAnalyticalRequest(new Request(
      `http://localhost/api/admin/share/audit-logs?${new URLSearchParams({
        since: new Date(at - 1000).toISOString(), until: new Date(at).toISOString(), ...extra,
      })}`,
    ));

    const body = await (await query())!.json() as {
      audit_logs: Array<{ event_type: string; email: string; ip_address: string; details: string }>;
    };

    expect(body.audit_logs.map(({ event_type, email, ip_address, details }) => [event_type, email, ip_address, details]))
      .toEqual([
        ["INVITE_CREATE", "owner-id", ip, "inv-1"],
        ["INVITE_REVOKE", "owner-id", ip, "inv-2"],
        ["INVITE_CLAIMED", "analyst-id", ip, ""],
        ["LOGIN_SUCCESS", "analyst-id", ip, ""],
        ["LOGIN_FAIL", "anonymous", ip, "login_verify"],
        ["ROLE_CHANGED", "owner-id", ip, "analyst-id"],
        ["CREDENTIAL_REVOKED", "owner-id", ip, "analyst-id"],
        ["USER_REVOKED", "owner-id", ip, "analyst-id"],
        ["OWNER_SETUP", "setup", ip, ""],
        ["INVITE_CREATE", "owner-id", "-", "inv-legacy"],
      ]);
    const invites = await (await query({ event_type: "INVITE_CREATE" }))!.json() as typeof body;
    expect(invites.audit_logs.map((r) => r.details)).toEqual(["inv-1", "inv-legacy"]);
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

  describe("/api/query range loading", () => {
    const MiB = 1024 * 1024;
    const windows: string[][] = [];
    const reads: { release: ReturnType<typeof vi.fn> }[] = [];
    const fakeRead = (rows: number) => {
      const read = {
        assertCurrent: () => {},
        release: vi.fn(),
        executeSql: async () => Array.from({ length: rows }, (_, i) => ({ status: 200, i })),
      };
      reads.push(read);
      return read as unknown as DuckDBReadContext;
    };
    // The newest hour holds `recent` rows and four hours hold 100; anything over 6h is over budget.
    const rangeOf = (recent: number) => vi.mocked(ensureCloudRange).mockImplementation(async (from, to) => {
      windows.push([String(from), String(to)]);
      const hours = (Date.parse(String(to)) - Date.parse(String(from))) / 3_600_000;
      if (hours > 6) throw new CloudBudgetError("over", { unit: "bytes", needed: 129 * MiB, limit: 64 * MiB });
      return fakeRead(hours <= 1.01 ? recent : 100);
    });
    const query = (sql: string) => handleCloudAnalyticalRequest(new Request("http://localhost/api/query", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ sql, max_rows: 1000 }),
    }));
    beforeEach(() => { windows.length = 0; reads.length = 0; });

    it("answers the default SELECT * LIMIT 100 over 24h from the newest hour within the budget", async () => {
      rangeOf(100);
      const res = await query("SELECT * FROM logs LIMIT 100");
      expect(res?.status).toBe(200);
      expect((await res!.json()).row_count).toBe(100);
      expect(windows).toEqual([["2026-11-05T23:00:00.000Z", "2026-11-06T00:00:00.001Z"]]);
    });

    it("widens the newest window until the LIMIT is met and releases every read", async () => {
      rangeOf(10);
      const res = await query("SELECT *\nFROM logs\nWHERE status >= 500\nORDER BY timestamp DESC\nLIMIT 100");
      expect(res?.status).toBe(200);
      expect((await res!.json()).row_count).toBe(100);
      expect(windows).toEqual([
        ["2026-11-05T23:00:00.000Z", "2026-11-06T00:00:00.001Z"],
        ["2026-11-05T20:00:00.000Z", "2026-11-06T00:00:00.001Z"],
      ]);
      expect(reads.map((read) => read.release.mock.calls.length)).toEqual([1, 1]);
    });

    it("projects an aggregate over the full range to the columns it reads", async () => {
      vi.mocked(ensureCloudRange).mockImplementationOnce(async (from, to, _signal, _kinds, options) => {
        windows.push([String(from), String(to), ...options?.projection?.columns ?? []]);
        return fakeRead(2);
      });
      const res = await query("SELECT status, count(*) AS requests FROM logs GROUP BY status ORDER BY requests DESC");
      expect(res?.status).toBe(200);
      expect(windows).toEqual([["2026-11-05T00:00:00.000Z", "2026-11-06T00:00:00.001Z", "timestamp", "status"]]);
    });

    it("says what an over-budget query needs and how to narrow it", async () => {
      vi.mocked(ensureCloudRange).mockRejectedValue(
        new CloudBudgetError("over", { unit: "bytes", needed: 200 * MiB, limit: 64 * MiB }));
      const res = await query("SELECT status, count(*) AS requests FROM logs GROUP BY status");
      expect(res?.status).toBe(413);
      expect((await res!.json()).detail).toBe("Cloud query over this time range needs 200.0 MiB of log data " +
        "(columns: timestamp, status) but the browser scan budget is 64.0 MiB; choose a shorter time range or select fewer columns");
    });
  });

  describe("/api/sessions range loading", () => {
    const end = Date.parse("2026-11-06T00:00:00Z");
    const loads: { from: string; to: string; columns: readonly string[] | undefined; release: ReturnType<typeof vi.fn> }[] = [];
    // The window from `from` holds `sessionsIn(hours)` sessions, newest first, one a minute apart.
    const rangeOf = (sessionsIn: (hours: number) => number, columns: readonly string[] = SESSIONS_SCAN_COLUMNS) =>
      vi.mocked(ensureCloudRange).mockImplementation(async (from, to, _signal, _kinds, options) => {
        const release = vi.fn();
        loads.push({ from: String(from), to: String(to), columns: options?.projection?.columns, release });
        const count = Math.min(100, sessionsIn((end - Date.parse(String(from))) / 3_600_000));
        return {
          assertCurrent: () => {},
          release,
          getPresentColumns: async () => new Set(count ? ["timestamp", "ts_ms", ...columns] : []),
          getCustomColumnTypes: () => new Map(),
          executeSql: async (sql: string) => /page_keys/.test(sql)
            ? [{ i: 0, ua: "curl/8", unique_urls: 3 }]
            : Array.from({ length: count }, (_, i) => ({
              ip: `192.0.2.${i}`, ja4: "t13d", session_start: end - (i + 1) * 60_000, session_end: end - i * 60_000,
              req_count: 2, reqs_4xx: 0, reqs_5xx: 0, ua: null, unique_urls: 0,
            })),
        } as unknown as DuckDBReadContext;
      });
    const list = (body: Record<string, unknown> = {}) => handleCloudAnalyticalRequest(new Request("http://localhost/api/sessions", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ start_time: "2026-11-05T00:00:00Z", end_time: "2026-11-06T00:00:00Z", page: 1, limit: 100,
        sort_by: "session_start", sort_dir: "desc", ...body }),
    }));
    beforeEach(() => { loads.length = 0; });
    afterEach(() => { vi.mocked(ensureCloudRange).mockReset().mockImplementation(async () => getDuckDBEngine().readContext()); });

    it("loads a 24h newest-first page 1 from the last 45 minutes without ua or url, then only the page's span", async () => {
      rangeOf(() => 100);
      const res = await list();
      expect(res?.status).toBe(200);
      const body = await res!.json() as { sessions: { ua: string | null; unique_urls: number }[] };
      expect(body.sessions).toHaveLength(100);
      expect(body.sessions[0]).toMatchObject({ ua: "curl/8", unique_urls: 3 });
      expect(loads.map(({ from, to }) => [from, to])).toEqual([
        ["2026-11-05T23:15:00.000Z", "2026-11-06T00:00:00Z"],
        ["2026-11-05T22:20:00.000Z", "2026-11-06T00:00:00.001Z"],
      ]);
      expect(loads[0].columns).toEqual(SESSIONS_SCAN_COLUMNS.filter((column) => column !== "ua" && column !== "url"));
      expect(loads[1].columns).toEqual(expect.arrayContaining(["ip", "ja4", "ua", "url"]));
      expect(loads[1].columns).not.toContain("resp_bytes");
      expect(loads.map((load) => load.release.mock.calls.length)).toEqual([1, 1]);
    });

    it("widens to older files until the page fills and releases every read", async () => {
      rangeOf((hours) => hours <= 1 ? 0 : 100);
      expect((await list())?.status).toBe(200);
      expect(loads.map(({ from }) => from)).toEqual([
        "2026-11-05T23:15:00.000Z", "2026-11-05T22:30:00.000Z", "2026-11-05T22:20:00.000Z",
      ]);
      expect(loads.map((load) => load.release.mock.calls.length)).toEqual([1, 1, 1]);
    });

    it("takes the response's column flags from the files a widened read found when the newest window had none", async () => {
      rangeOf((hours) => hours <= 1 ? 0 : 100, SESSIONS_SCAN_COLUMNS.filter((column) => column !== "tcp_rtt"));
      const body = await (await list())!.json() as { has_rtt: boolean; has_ja4: boolean };
      expect(body).toMatchObject({ has_rtt: false, has_ja4: true });
    });

    it("answers a selective filter that widens past the scan budget with the budget's 413", async () => {
      const message = "Cloud query range needs 80.0 MiB but the browser scan budget is 64.0 MiB; choose a shorter time range";
      rangeOf((hours) => {
        if (hours > 1) throw new CloudBudgetError(message, { unit: "bytes", needed: 80 * 1024 * 1024, limit: 64 * 1024 * 1024 });
        return 0;
      });
      const res = await list({ flagged_only: true });
      expect(res?.status).toBe(413);
      expect(await res!.json()).toEqual({ detail: message });
      expect(loads.map(({ from }) => from)).toEqual(["2026-11-05T23:15:00.000Z", "2026-11-05T22:30:00.000Z"]);
      expect(loads[0].release).toHaveBeenCalledTimes(1);
    });
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
    // Availability needs only the files' schemas, which a projection keeps whole.
    expect((await options("/api/insight-availability"))?.projection?.columns).toEqual(["timestamp", "ts_ms"]);
    expect((await options("/api/insights/cache-collapse-detail", post({ url: "/a", window_size_hrs: 24, baseline_hours: 168 })))
      ?.projection?.columns).toEqual(insightScanColumns([]));
    expect((await options("/api/insights", post({ window_size_hrs: 24, baseline_hours: 720 }, { "x-flp-cloud-prefetch": "1" })))
      ?.projection?.maxBytes).toBe(32 * 1024 * 1024);
    expect((await options("/api/security/aggregates", post({ sections: ["summary"] })))?.projection)
      .toEqual({ columns: securityScanColumns(undefined) });
  });

  it("loads each /network scan as its own sections' column projection, plus filtered fields", async () => {
    const options = async (path: string, init: RequestInit = {}) => {
      vi.mocked(ensureCloudRange).mockClear();
      const res = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, init));
      expect(res).not.toBeNull();
      return vi.mocked(ensureCloudRange).mock.calls[0]?.[4];
    };
    const post = (body: Record<string, unknown>) => ({
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
    });
    const columns = (path: string, body: Record<string, unknown> = {}) => ({ columns: networkScanColumns(path, body) });
    expect((await options("/api/network-health", post({ sections: ["summary"] })))?.projection)
      .toEqual(columns("/api/network-health", { sections: ["summary"] }));
    expect((await options("/api/network-quality", post({})))?.projection).toEqual(columns("/api/network-quality"));
    const transport = (await options("/api/network/bundle", post({ sections: ["transport"] })))?.projection;
    expect(transport).toEqual(columns("/api/network/bundle", { sections: ["transport"] }));
    expect(transport?.columns).toEqual(expect.arrayContaining(["tcp_rtt", "delivery_rate", "proto"]));
    expect(transport?.columns).not.toContain("rid");
    expect((await options("/api/network/pop-health"))?.projection).toEqual(columns("/api/network/pop-health"));
    expect((await options("/api/network/bundle", post({})))?.projection?.columns?.toSorted())
      .toEqual([...NETWORK_SCAN_COLUMNS].toSorted());
    expect((await options("/api/network-health", post({
      sections: ["summary"], filters: { host: { mode: "include", values: ["example.com"] } },
    })))?.projection?.columns).toEqual([...columns("/api/network-health", { sections: ["summary"] }).columns!, "host"]);
  });

  it("says which /network scan is over budget, the columns it reads and how to narrow it", async () => {
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(
      new CloudBudgetError("over", { unit: "bytes", needed: 80 * 1024 * 1024, limit: 64 * 1024 * 1024 }));
    const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network/bundle", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sections: ["transport"], start_time: "2026-11-05T00:00:00Z", end_time: "2026-11-06T00:00:00Z" }),
    }));
    expect(res?.status).toBe(413);
    expect((await res!.json()).detail).toBe("Cloud /network transport over this time range needs 80.0 MiB of log data " +
      `(columns: ${networkScanColumns("/api/network/bundle", { sections: ["transport"] })!.join(", ")}) ` +
      "but the browser scan budget is 64.0 MiB; choose a shorter time range");
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(new CloudBudgetError("over", { unit: "files", needed: 300, limit: 256 }));
    const files = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network-quality", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({}),
    }));
    expect((await files!.json()).detail).toBe("Cloud /network quality over this time range needs 300 files of log data " +
      "but the browser scan budget is 256 files; choose a shorter time range");
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

    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce({ served: "bundle" } as never);
    const bundle = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network/bundle", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sections: ["summary", "pop_health"] }),
    }));
    expect(await bundle!.json()).toEqual({ served: "bundle" });
    expect(vi.mocked(networkFromRollup).mock.calls[0][0]).toBe("/api/network/bundle");
    expect(ensureCloudRange).not.toHaveBeenCalled();

    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce({ served: "transport" } as never);
    const transport = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network/bundle", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ sections: ["transport"] }),
    }));
    expect(await transport!.json()).toEqual({ served: "transport" });
    expect(vi.mocked(networkFromRollup).mock.calls[0][1]).toMatchObject({ sections: ["transport"] });
    expect(ensureCloudRange).not.toHaveBeenCalled();
  });

  it("reads raw only for the quality parts a summary answer lacked, over the summaries' range, and merges them in", async () => {
    const residual = {
      quality_parts: ["throughput", "asn_client"], quality_asns: [13335],
      start_time: "2026-11-05T01:00:00.000Z", end_time: "2026-11-05T23:00:00.000Z",
    };
    const answer = {
      summary: { total_requests: 5, avg_throughput_mbps: 0 }, by_region: [{ value: "VA" }],
      by_asn: [{ asn: 13335, avg_rttvar_ms: 9, avg_ploss_pct: 9 }],
    };
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(networkFromRollup).mockClear().mockResolvedValueOnce({ network_partial: true, answer, residual } as never);
    const res = await handleCloudAnalyticalRequest(new Request("http://localhost/api/network-quality", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ region_country: "US", filters: {} }),
    }));
    expect(res!.status).toBe(200);
    expect(networkFromRollup).toHaveBeenCalledTimes(1);
    const [from, to, , , options] = vi.mocked(ensureCloudRange).mock.calls[0];
    expect([from, to]).toEqual([residual.start_time, residual.end_time]);
    expect(options?.projection?.columns).toEqual(networkScanColumns("/api/network-quality", residual));
    expect(options?.projection?.columns).not.toContain("region");
    // The empty test engine has no rows, so the raw parts come back empty and zero the client-only figures.
    expect(await res!.json()).toEqual({
      ...answer, by_asn: [{ asn: 13335, avg_rttvar_ms: 0, avg_ploss_pct: 0 }],
    });
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
    const pageSections = ["summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"];
    const origin = await post("/api/origin/aggregates", {
      sections: pageSections, filters: {}, timeseries_metric: "ttfb", bucket_minutes: 60, split_by_leg: false,
    });
    expect(await origin!.json()).toEqual({ served: "origin" });
    const [path, params, source] = vi.mocked(originFromRollup).mock.calls[0];
    expect(path).toBe("/api/origin/aggregates");
    expect(params).toMatchObject({
      sections: pageSections, service_id: "svc-1",
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

    vi.mocked(performanceFromRollup).mockClear().mockResolvedValueOnce({ served: "rankings" } as never);
    const perfPage = { sections: ["ttl_dist", "waterfall", "top_urls", "top_asns"], sort_by: "p95", filters: {} };
    expect(await (await post("/api/performance/aggregates", perfPage))!.json()).toEqual({ served: "rankings" });
    expect(vi.mocked(performanceFromRollup).mock.calls[0][1]).toMatchObject(perfPage);
    expect(ensureCloudRange).not.toHaveBeenCalled();
  });

  it("answers unfiltered /security sections and threat intel from summaries before any raw scan, masked as the raw scan masks", async () => {
    const post = (path: string, body: Record<string, unknown>) => handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-service-id": "svc-1" },
      body: JSON.stringify(body),
    }));
    const range = { start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z" };
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(securityFromRollup).mockClear().mockResolvedValue({ served: "security" } as never);
    for (const path of ["/api/security/aggregates", "/api/security/bundle"]) {
      const res = await post(path, { sections: ["tls_fingerprints", "ipv6_adoption"], filters: {}, ...range });
      expect(await res!.json()).toEqual({ served: "security" });
    }
    const [path, params, source] = vi.mocked(securityFromRollup).mock.calls[0];
    expect(path).toBe("/api/security/aggregates");
    expect(params).toMatchObject({ sections: ["tls_fingerprints", "ipv6_adoption"], service_id: "svc-1", ...range });
    expect(source.cacheScope).toContain("svc-1");
    expect(ensureCloudRange).not.toHaveBeenCalled();

    vi.mocked(securityFromRollup).mockClear();
    const intel = await handleCloudAnalyticalRequest(new Request(
      `http://localhost/api/security/threat-intel?start_time=${range.start_time}&end_time=${range.end_time}`,
      { headers: { "x-service-id": "svc-1" } },
    ));
    expect(await intel!.json()).toEqual({ served: "security" });
    expect(vi.mocked(securityFromRollup).mock.calls[0].slice(0, 2))
      .toEqual(["/api/security/threat-intel", { ...range, service_id: "svc-1", mask_ips: false }]);

    // The raw scan masks IPs by header too, so the summaries must.
    vi.mocked(securityFromRollup).mockClear().mockResolvedValue(null);
    await handleCloudAnalyticalRequest(new Request("http://localhost/api/security/aggregates", {
      method: "POST",
      headers: { "content-type": "application/json", "x-session-role": "analyst-no-pii" },
      body: JSON.stringify({ sections: ["wellknown_bots", "proxies"], ...range }),
    }));
    expect(vi.mocked(securityFromRollup).mock.calls[0][1]).toMatchObject({ mask_ips: true });
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);
    vi.mocked(securityFromRollup).mockClear();
    await post("/api/security/aggregates", { sections: ["proxy_dist"], filters: { pop: { mode: "include", values: ["IAD"] } }, ...range });
    expect(securityFromRollup).not.toHaveBeenCalled();
    expect(ensureCloudRange).toHaveBeenCalledTimes(2);
    vi.mocked(securityFromRollup).mockReset().mockResolvedValue(null);
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
    const team = { team: { mode: "include", values: ["a"] } };
    const overview = await options("/api/value/summary", { sections: ["overview"], custom_filters: team });
    expect(overview?.columns).toEqual(expect.arrayContaining(["cache", "elapsed", "resp_bytes", "team"]));
    expect(overview?.columns).not.toContain("url");
    expect(await options("/api/value/summary", { custom_filters: team })).toEqual({ columns: [...VALUE_SCAN_COLUMNS, "team"] });
    expect(await options("/api/assets/aggregates", { filters: [{ column: "host", operator: "=", value: "a" }] }))
      .toEqual({ columns: [...ASSETS_SCAN_COLUMNS, "host"] });
    expect(await options("/api/origin/aggregates", { filters: [filters] })).toBeUndefined();
    expect(await options("/api/value/summary", { filters: "host" })).toBeUndefined();
    expect(await options("/api/assets/aggregates", { filters: 5 })).toBeUndefined();
    const tables = await options("/api/assets/aggregates", { filters: {}, sections: ["large_uncompressed_assets", "low_ttl_assets"] });
    expect(tables?.columns).toEqual(expect.arrayContaining(["url", "resp_bytes", "ttl", "cache"]));
    expect(tables?.columns).not.toContain("ttfb");
    expect(await options("/api/performance/aggregates", { filters, sort_by: "p99" }))
      .toEqual({ columns: [...PERFORMANCE_SCAN_COLUMNS, "host"] });
    expect(await options("/api/performance", {})).toEqual({ columns: [...PERFORMANCE_SCAN_COLUMNS] });
    expect(await options("/api/performance/aggregates", { filters: [filters] })).toBeUndefined();
    const range = { start_time: "2026-11-05T00:00:00Z", end_time: "2026-11-06T00:00:00Z" };
    const listed = SESSIONS_SCAN_COLUMNS.filter((column) => column !== "ua" && column !== "url");
    expect(await options("/api/sessions", range)).toEqual({ columns: listed });
    expect(await options("/api/sessions", { ...range, filters: { host: ["example.com"] } }))
      .toEqual({ columns: [...listed, "host"] });
    expect(await options("/api/sessions", { ...range, sort_by: "unique_urls" }))
      .toEqual({ columns: SESSIONS_SCAN_COLUMNS.filter((column) => column !== "ua") });
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
    expect(scoreRes).toBeNull();

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

  it("retries 409 credentials_pending during provision job stepping until secret propagates", async () => {
    setCloudModeRuntime(true);
    vi.useFakeTimers();
    let stepCalls = 0;
    const origFetch = globalThis.fetch;
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      if (url.endsWith("/api/provision/execute")) {
        return Response.json({ job: { id: "job-cred-1" } });
      }
      if (url.endsWith("/api/admin/jobs/job-cred-1/step")) {
        stepCalls += 1;
        if (stepCalls === 1) {
          return Response.json({ error: "credentials_pending", detail: "credentials_pending" }, { status: 409 });
        }
        return Response.json({ job: { id: "job-cred-1", status: "complete", step: "finalize", completed_steps: ["bucket", "finalize"] } });
      }
      return new Response("not found", { status: 404 });
    }) as typeof fetch;

    try {
      const res = await handleCloudAnalyticalRequest(
        new Request("http://localhost/api/provision/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ token: "tok", service_id: "svc1", capture: "attached" }),
        }),
      );
      expect(res).not.toBeNull();
      expect(res!.status).toBe(200);
      const textPromise = res!.text();
      await vi.advanceTimersByTimeAsync(3500);
      const body = await textPromise;
      expect(stepCalls).toBe(2);
      expect(body).toContain("Waiting for Fastly secret store propagation");
      expect(body).toContain('"status":"complete"');
      expect(body).not.toContain('"type":"error"');
    } finally {
      globalThis.fetch = origFetch;
      vi.useRealTimers();
    }
  });
});
