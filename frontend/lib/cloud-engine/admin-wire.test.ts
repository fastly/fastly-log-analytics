import { beforeEach, describe, expect, it, vi } from "vitest";
import { cloudAdminWire } from "./admin-wire";
import { adminRequest, deleteLogHistory, purgeQuarantine } from "./telemetry/admin";
import { queryOpsLogs } from "./telemetry/client";

vi.mock("./telemetry/admin", () => ({
  adminRequest: vi.fn(), deleteLogHistory: vi.fn(), purgeQuarantine: vi.fn(),
}));
vi.mock("./telemetry/client", () => ({ queryOpsLogs: vi.fn() }));
const request = (path: string, method = "GET") => new Request(`http://localhost${path}`, {
  method, headers: { "x-service-id": "fixture-service" },
});
beforeEach(() => vi.resetAllMocks());

describe("Cloud administrative wire parity", () => {
  it("collects native tree pages, canonicalizes prefixes and preserves file metadata", async () => {
    vi.mocked(adminRequest).mockResolvedValueOnce({
      root: "processed/", nodes: [{ name: "one", type: "directory", prefix: "request/one/" }],
      complete: false, next_cursor: "fixture-next",
    }).mockResolvedValueOnce({
      nodes: [{ name: "one", type: "directory", prefix: "request/one/" },
        { name: "file.parquet", type: "file", key: "processed/request/file.parquet", size: 12 }],
      complete: true,
    });
    const response = await cloudAdminWire(request("/api/admin/storage-tree?prefix=request"));
    expect(await response!.json()).toMatchObject({ complete: true, nodes: [
      { prefix: "request/one/" }, { key: "processed/request/file.parquet", size: 12 },
    ] });
    const calls = vi.mocked(adminRequest).mock.calls;
    expect(calls[0][0]).toContain("prefix=request%2F");
    expect(calls[1][0]).toContain("cursor=fixture-next");
    expect(calls[0][0]).toContain("service_id=fixture-service");
  });

  it("refuses incomplete listings instead of returning false empty directories", async () => {
    vi.mocked(adminRequest).mockResolvedValue({ nodes: [], complete: false, next_cursor: "repeated" });
    await expect(cloudAdminWire(request("/api/admin/raw-tree"))).rejects.toThrow("incomplete");
    expect(vi.mocked(adminRequest)).toHaveBeenCalledTimes(2);
  });

  it("maps real native ingested files", async () => {
    vi.mocked(adminRequest).mockResolvedValueOnce({
      files: [{ key: "processed/request/one.parquet", rows: 4, size: 128, last_modified: "2026-01-01T00:00:00Z" }],
      complete: true,
    });
    expect(await (await cloudAdminWire(request("/api/admin/ingested-files")))!.json()).toMatchObject({
      files: [{ file_name: "processed/request/one.parquet", row_count: 4, file_size_bytes: 128, ingested_at: "2026-01-01T00:00:00Z" }],
    });
  });

  const timestamp = Date.parse("2026-01-01T00:00:00Z");
  function cronEvidence() {
    vi.mocked(adminRequest).mockResolvedValue({
      configured: true, service_id: "fixture-service",
      schedules: [
        { task: "request_ingest", next_run_time: "2026-01-01T00:00:10Z", interval_s: 10 },
        { task: "rum_ingest", next_run_time: null, disabled_reason: "RUM is disabled" },
        { task: "raw_delete", next_run_time: null, schedule_basis: "pending_work" },
        { task: "retention_sweep", next_run_time: null, schedule_basis: "maintenance_turn" },
        { task: "log_flush", next_run_time: null, interval_s: 60 },
        { task: "cloud_ingest_clock", next_run_time: "2026-01-01T00:00:01Z", interval_s: 1 },
      ],
      runs: [{ id: "native-run", service_id: "fixture-service", started_at: "2026-01-01T00:00:00Z",
        finished_at: "2026-01-01T00:00:01Z", duration_ms: 1000, outcome: "idle" }],
    });
    vi.mocked(queryOpsLogs).mockResolvedValue({
      kind: "cron_runs", from: "2026-01-01T00:00:00Z", to: "2026-01-02T00:00:00Z",
      rows: [
        { timestamp, event_id: "ingest", run_id: "run-request_ingest", cron_kind: "request_ingest",
          service_id: "fixture-service", started_at: timestamp, duration_ms: 200, outcome: "done",
          converted_files: 2, rows_written: 4, bad_lines: 1 },
        { timestamp: timestamp + 1000, event_id: "delete", run_id: "run-raw_delete", cron_kind: "raw_delete",
          service_id: "fixture-service", started_at: timestamp + 1000, duration_ms: 100, outcome: "error", error: "delete failed" },
        { timestamp: timestamp + 2000, event_id: "flush", run_id: "run-log_flush", cron_kind: "log_flush",
          service_id: "fixture-service", started_at: timestamp + 2000, duration_ms: 50, outcome: "partial" },
        { timestamp, event_id: "other-service", cron_kind: "rum_ingest", service_id: "other-service", outcome: "done" },
      ],
      oldestLiveMs: null, status: null,
      stats: { listed: 1, read: 1, cached: 0, cdnHits: 0, skippedCovered: 0, ignoredCompact: 0, evicted: 0, liveRows: 0 },
    });
  }
  const cronRange = "start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z";

  it("rejects invalid cron ranges before reading native or telemetry data", async () => {
    for (const range of ["end=invalid", "start=invalid", "start=2026-01-02T00:00:00Z&end=2026-01-01T00:00:00Z"]) {
      expect((await cloudAdminWire(request(`/api/cron-runs?${range}`)))!.status).toBe(422);
    }
    expect(adminRequest).not.toHaveBeenCalled();
    expect(queryOpsLogs).not.toHaveBeenCalled();
  });

  it("shows actual per-stage execution evidence and keeps idle coordinators distinct", async () => {
    cronEvidence();
    const runs = await (await cloudAdminWire(request(`/api/cron-runs?${cronRange}`)))!.json();
    expect(runs).toMatchObject({ total: 4, entries: [
      { task: "log_flush", status: "partial_success", duration_s: 0.05 },
      { task: "raw_delete", status: "error", summary: "delete failed", error_message: "delete failed" },
      { task: "request_ingest", status: "success", duration_s: 0.2,
        files_downloaded: 2, rows_ingested: 4, corrupt_rows: 1 },
      { task: "cloud_ingest_clock", status: "skipped" },
    ] });
    expect(runs.entries.every((row: { id: unknown }) => typeof row.id === "number")).toBe(true);
    expect(queryOpsLogs).toHaveBeenCalledWith("cron_runs", {
      from: "2026-01-01T00:00:00Z", to: "2026-01-02T00:00:00Z",
    }, expect.objectContaining({ signal: expect.any(AbortSignal) }));
  });

  it("uses native schedules without inventing runs for disabled or unseen tasks", async () => {
    cronEvidence();
    const data = await (await cloudAdminWire(request(`/api/cron-schedule?${cronRange}`)))!.json();
    expect(data.schedules).toHaveLength(6);
    expect(data.schedules).toContainEqual(expect.objectContaining({
      task: "rum_ingest", next_run_time: null, disabled_reason: "RUM is disabled", last_run_time: null,
    }));
    expect(data.schedules).toContainEqual(expect.objectContaining({
      task: "log_flush", last_run_time: "2026-01-01T00:00:02.000Z",
    }));
    expect(data.schedules).toContainEqual(expect.objectContaining({
      task: "retention_sweep", last_run_time: null, next_run_time: null,
    }));
  });

  it("filters task, status and since_id before pagination", async () => {
    cronEvidence();
    const all = await (await cloudAdminWire(request(`/api/cron-runs?${cronRange}`)))!.json();
    const filtered = await (await cloudAdminWire(request(`/api/cron-runs?${cronRange}&task=raw_delete&status=error&per_page=1`)))!.json();
    expect(filtered).toMatchObject({ total: 1, entries: [{ task: "raw_delete", status: "error" }] });
    const newer = await (await cloudAdminWire(request(`/api/cron-runs?${cronRange}&since_id=${all.entries[0].id}`)))!.json();
    expect(newer.entries).toEqual([]);
    const second = await (await cloudAdminWire(request(`/api/cron-runs?${cronRange}&page=2&per_page=1`)))!.json();
    expect(second).toMatchObject({ total: 4, page: 2, entries: [{ task: "raw_delete" }] });
  });

  it("does not turn incomplete or failed telemetry reads into empty successful history", async () => {
    cronEvidence();
    vi.mocked(queryOpsLogs).mockRejectedValueOnce(new Error("telemetry unavailable"));
    await expect(cloudAdminWire(request(`/api/cron-runs?${cronRange}`))).rejects.toThrow("telemetry unavailable");
    const evidence = await queryOpsLogs("cron_runs", { from: "", to: "" });
    vi.mocked(queryOpsLogs).mockResolvedValueOnce({ ...evidence, liveTruncated: true });
    await expect(cloudAdminWire(request(`/api/cron-runs?${cronRange}`))).rejects.toThrow("incomplete");
  });

  it("honors cancellation and rejects service mismatches without querying another service's history", async () => {
    cronEvidence();
    const controller = new AbortController();
    controller.abort();
    await expect(cloudAdminWire(new Request(`http://localhost/api/cron-runs?${cronRange}`, {
      signal: controller.signal,
    }))).rejects.toThrow();
    expect(queryOpsLogs).not.toHaveBeenCalled();
    await expect(cloudAdminWire(request(`/api/cron-runs?${cronRange}&service_id=other-service`)))
      .rejects.toThrow("service");
    expect(queryOpsLogs).not.toHaveBeenCalled();
  });

  it("shares one cron history read across the page's concurrent default-range cron requests", async () => {
    cronEvidence();
    const abandoned = new AbortController();
    const reads = [
      cloudAdminWire(request("/api/cron-runs?page=1&per_page=500")),
      cloudAdminWire(request("/api/cron-runs?page=1&per_page=10")),
      cloudAdminWire(request("/api/cron-schedule")),
      cloudAdminWire(new Request("http://localhost/api/cron-runs?page=1&per_page=50", {
        headers: { "x-service-id": "fixture-service" }, signal: abandoned.signal,
      })),
    ];
    abandoned.abort();
    const results = await Promise.allSettled(reads);
    expect(results.slice(0, 3).map((result) => result.status)).toEqual(["fulfilled", "fulfilled", "fulfilled"]);
    expect(results[3].status).toBe("rejected");
    // The fixture rows are old, so every caller widens through the same three lookback tiers.
    expect(queryOpsLogs).toHaveBeenCalledTimes(3);
    const later = await cloudAdminWire(request("/api/cron-runs?page=1&per_page=10"));
    expect(later!.ok).toBe(true);
    expect(queryOpsLogs).toHaveBeenCalledTimes(6);
  });

  it("answers the header's last-sync probe from the last hour and maps log_discovery to the Cloud ingest stage", async () => {
    cronEvidence();
    const now = Date.now();
    vi.mocked(queryOpsLogs).mockResolvedValue({
      kind: "cron_runs", from: "", to: "",
      rows: [
        { timestamp: now - 20_000, event_id: "recent", run_id: "r1", cron_kind: "request_ingest",
          service_id: "fixture-service", started_at: now - 20_000, duration_ms: 300, outcome: "done" },
        { timestamp: now - 10_000, event_id: "delete", run_id: "r2", cron_kind: "raw_delete",
          service_id: "fixture-service", started_at: now - 10_000, duration_ms: 10, outcome: "done" },
      ],
      oldestLiveMs: null, status: null,
      stats: { listed: 1, read: 1, cached: 0, cdnHits: 0, skippedCovered: 0, ignoredCompact: 0, evicted: 0, liveRows: 0 },
    });
    const body = await (await cloudAdminWire(request(
      "/api/cron-runs?task=log_discovery&per_page=1&page=1&skip_total=true")))!.json();
    expect(body.entries).toHaveLength(1);
    expect(body.entries[0]).toMatchObject({ task: "request_ingest", status: "success", duration_s: 0.3 });
    expect(queryOpsLogs).toHaveBeenCalledTimes(1);
    const [, range] = vi.mocked(queryOpsLogs).mock.calls[0];
    expect(Date.parse(range.to) - Date.parse(range.from)).toBe(3600_000);
    expect(body.history_from).toBe(range.from);
  });

  it("widens the default lookback only until the requested page is full", async () => {
    cronEvidence();
    const now = Date.now();
    vi.mocked(queryOpsLogs).mockImplementation(async (_kind, range) => {
      const span = Date.parse(range.to) - Date.parse(range.from);
      const rows = span <= 3600_000 ? [] : [
        { timestamp: now - 7200_000, event_id: "old", run_id: "r0", cron_kind: "request_ingest",
          service_id: "fixture-service", started_at: now - 7200_000, duration_ms: 1, outcome: "done" },
      ];
      return { kind: "cron_runs", from: range.from, to: range.to, rows, oldestLiveMs: null, status: null,
        stats: { listed: 1, read: 1, cached: 0, cdnHits: 0, skippedCovered: 0, ignoredCompact: 0, evicted: 0, liveRows: 0 } };
    });
    const body = await (await cloudAdminWire(request("/api/cron-runs?task=request_ingest&per_page=1")))!.json();
    expect(body.entries).toHaveLength(1);
    expect(queryOpsLogs).toHaveBeenCalledTimes(2);
    expect(Date.parse(body.history_to) - Date.parse(body.history_from)).toBe(86400_000);
  });

  it("maps native audit details and honors filtering before pagination", async () => {
    vi.mocked(adminRequest).mockResolvedValue({ entries: [
      { id: "a", time: "2026-01-01T00:00:00Z", action: "service_updated", details: '{"changed":["groups"]}' },
      { id: "b", time: "2026-01-01T00:00:01Z", action: "login", details: "authenticated" },
    ] });
    expect(await (await cloudAdminWire(request("/api/audit-logs?event_type=service_updated&per_page=1")))!.json())
      .toMatchObject({ total: 1, entries: [{ event_type: "service_updated", details: { changed: ["groups"] } }] });
  });

  it("computes filtered usage totals before limiting rows and exports safe real CSV", async () => {
    const timestamp = Date.parse("2026-01-01T00:00:00Z");
    const rows = [
      { timestamp, op: "=formula", op_class: "A", op_count: 3, dataset: "request", caller: "ingest",
        bytes_in: 10, bytes_out: 20, est_cost_usd: 0.003 },
      { timestamp: timestamp + 1, op: "PUT", op_class: "A", op_count: 2, dataset: "request", caller: "ingest",
        bytes_in: 5, bytes_out: 5, est_cost_usd: 0.002 },
      { timestamp: timestamp + 2, op: "GET", op_class: "B", op_count: 9, dataset: "request", caller: "browser",
        bytes_in: 0, bytes_out: 100, est_cost_usd: 0.0001 },
    ];
    vi.mocked(queryOpsLogs).mockResolvedValue({
      kind: "fos_usage", from: "2026-01-01T00:00:00Z", to: "2026-01-02T00:00:00Z",
      rows: rows.map((row, index) => ({ ...row, event_id: `fixture-${index}` })),
      oldestLiveMs: null, status: null,
      stats: { listed: 1, read: 1, cached: 0, cdnHits: 0, skippedCovered: 0, ignoredCompact: 0, evicted: 0, liveRows: 0 },
    });
    const params = "?start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z&usage_type=FOS-A&page_size=1";
    const data = await (await cloudAdminWire(request(`/api/admin/usage-log${params}`)))!.json();
    expect(data).toMatchObject({ total: 2, aggregate: {
      total_class_a: 5, total_class_b: 0, total_fos_bytes: 40, estimated_cost_total: 0.005,
      class_a_breakdown: { "=formula": 3, PUT: 2 },
    } });
    expect(data.entries).toHaveLength(1);
    expect(data.entries[0]).toMatchObject({ count: 2, operation_class: "A", timestamp: "2026-01-01T00:00:00.001Z" });
    const csv = await cloudAdminWire(request(`/api/admin/usage-log/export${params}`));
    expect(csv!.headers.get("content-type")).toContain("text/csv");
    expect(await csv!.text()).toContain(`"'=formula"`);
    expect(queryOpsLogs).toHaveBeenCalledWith("fos_usage", {
      from: "2026-01-01T00:00:00Z", to: "2026-01-02T00:00:00Z",
    }, expect.anything());
  });

  it("uses native purge actions and never treats numeric evidence IDs as native record keys", async () => {
    vi.mocked(deleteLogHistory).mockResolvedValue({ job: {
      kind: "fos_usage", older_than_days: null, cutoff_ts: 1767225600, status: "deleting",
      actor: "fixture-user", started_at: 1767225600, updated_at: 1767225600, deleted: 0, error: null,
    } });
    vi.mocked(purgeQuarantine).mockResolvedValue({ purged: 2, failed: 0, skipped: 0 });
    expect((await cloudAdminWire(request("/api/admin/usage-log", "DELETE")))!.ok).toBe(true);
    expect(deleteLogHistory).toHaveBeenCalledWith("fos_usage");
    expect(await (await cloudAdminWire(request("/api/admin/quarantine/purge", "POST")))!.json()).toEqual({ purged: 2, failed: 0, skipped: 0 });
    expect(purgeQuarantine).toHaveBeenCalledWith({ target: "all" });
    expect((await cloudAdminWire(request("/api/admin/quarantine/purge?item_id=1", "POST")))!.status).toBe(501);
  });
});
