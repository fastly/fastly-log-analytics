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

  it("maps real native ingested files and clock state without fictional cron jobs", async () => {
    vi.mocked(adminRequest).mockResolvedValueOnce({
      files: [{ key: "processed/request/one.parquet", rows: 4, size: 128, last_modified: "2026-01-01T00:00:00Z" }],
      complete: true,
    });
    expect(await (await cloudAdminWire(request("/api/admin/ingested-files")))!.json()).toMatchObject({
      files: [{ file_name: "processed/request/one.parquet", row_count: 4, file_size_bytes: 128, ingested_at: "2026-01-01T00:00:00Z" }],
    });
    vi.mocked(adminRequest).mockResolvedValue({
      configured: true, last_tick_at: "2026-01-01T00:00:00Z", next_tick_at: "2026-01-01T00:02:00Z", clock_interval_s: 120,
      runs: [{ id: "native-run", started_at: "2026-01-01T00:00:00Z", finished_at: "2026-01-01T00:00:01Z",
        duration_ms: 1000, outcome: "done", converted_raw_files: 2, rows_written: 4 }],
    });
    const runs = await (await cloudAdminWire(request("/api/cron-runs?status=success")))!.json();
    expect(runs).toMatchObject({ total: 1, runs: [{ id: "native-run" }], entries: [
      { task: "cloud_ingest_clock", duration_s: 1, status: "success" },
    ] });
    expect(typeof runs.entries[0].id).toBe("number");
    expect(await (await cloudAdminWire(request("/api/cron-schedule")))!.json()).toEqual({
      schedules: [{ task: "cloud_ingest_clock", next_run_time: "2026-01-01T00:02:00Z", last_run_time: "2026-01-01T00:00:00Z", interval_s: 120 }],
    });
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
