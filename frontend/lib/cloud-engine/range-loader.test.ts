// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ensureCloudRange } from "./range-loader";
import { fetchManifest } from "./dashboard/manifest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { cloudLoadingStore } from "./loading-progress";
import { parquetFixture } from "./dashboard/test-fixtures";
import { parquetWriteBuffer } from "hyparquet-writer";
import { cloudRumAnalytics, cloudRumUsage } from "./rum-wire";

const source = vi.hoisted(() => ({ fetchJson: vi.fn(), fetchFile: vi.fn() }));
vi.mock("./dashboard/sources", () => ({
  browserSource: () => source,
  dashboardCacheScope: () => "fixture-session",
}));
vi.mock("./dashboard/manifest", () => ({ fetchManifest: vi.fn() }));

const from = "2026-11-01T00:00:00Z";
const to = "2026-11-02T00:00:00Z";
const signal = new AbortController().signal;
const file = { key: "processed/request/fixture.parquet", start: from, end: to, size: 100, rows: 1 };

beforeEach(() => {
  cloudLoadingStore.reset();
  vi.mocked(fetchManifest).mockResolvedValue({ files: [file], complete: true });
});
afterEach(() => { vi.restoreAllMocks(); cloudLoadingStore.reset(); });

describe("Cloud first-page analytical range loading", () => {
  it("installs optional native custom columns before ingesting the first scoring file", async () => {
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(from)] },
      { name: "edge_score_l1", type: "DOUBLE", data: [72] },
      { name: "edge_cookie_compliance", type: "STRING", data: ["pass"] },
    ] });
    source.fetchFile.mockResolvedValue({ bytes, cached: false });
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "fixture-service",
      customFields: [{ id: "edge_score_l1", type: "number" }, { id: "edge_cookie_compliance", type: "string" }] });
    expect(await getDuckDBEngine().executeSql("SELECT edge_score_l1, edge_cookie_compliance FROM requests"))
      .toEqual([{ edge_score_l1: 72, edge_cookie_compliance: "pass" }]);
    const fetchJson = vi.mocked(fetchManifest).mock.calls.at(-1)![0];
    await fetchJson("/api/files?dataset=request", signal);
    expect(source.fetchJson).toHaveBeenCalledWith("/api/files?dataset=request&service_id=fixture-service", signal);
  });

  it("maps native RUM parquet fields, identities and units into the frontend contract", async () => {
    const vitalsKey = "processed/rum/client_vitals/fixture.parquet";
    const errorsKey = "processed/rum/client_errors/fixture.parquet";
    const at = new Date(Date.parse(from) + 1000);
    const vitalBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [at, at, at] },
      { name: "metric_name", type: "STRING", data: ["LCP", "CLS", "pageLoadTime"] },
      { name: "metric_value", type: "DOUBLE", data: [2000, 0.05, 3500] },
      ...["pathname", "browser", "os", "device", "cid", "req_id"].map((name, index) => ({
        name, type: "STRING" as const,
        data: Array(3).fill(["/home", "Chrome", "Test OS", "desktop", "client-a", "beacon-a"][index]),
      })),
    ] });
    const errorBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [at] },
      { name: "error_message", type: "STRING", data: ["Failure"] },
      { name: "error_file", type: "STRING", data: ["app.js"] },
      { name: "error_line", type: "INT32", data: [12] },
      { name: "error_col", type: "INT32", data: [4] },
      { name: "pathname", type: "STRING", data: ["/home"] },
      { name: "req_id", type: "STRING", data: ["beacon-b"] },
    ] });
    vi.mocked(fetchManifest).mockResolvedValueOnce({ complete: true, files: [
      { ...file, key: vitalsKey }, { ...file, key: errorsKey },
    ] });
    source.fetchFile.mockImplementation(async (key: string) => ({
      bytes: key === vitalsKey ? vitalBytes : errorBytes, cached: false,
    }));
    await ensureCloudRange(from, to, signal, ["rum"]);
    const result = await cloudRumAnalytics({ start_time: from, end_time: to, signal });
    expect(result).toMatchObject({
      beacon_count: 2, pageview_count: 1, error_count: 1,
      environments: { browsers: { Chrome: 1 } },
      vitals: { lcp: { p75: 2 }, cls: { p75: 0.05 } },
      worst_pages: [{ path: "/home", avg_load_time: 3.5, lcp_p75: 2 }],
      errors: [{ message: "Failure", file: "app.js", line: 12, col: 4, count: 1 }],
    });
    expect(await cloudRumUsage({ start_time: from, end_time: to, signal }, 0.005)).toMatchObject({
      total_beacons: 2, total_estimated_class_a: 2, total_estimated_cost_usd: 0.00001,
      data: [{ date: "2026-11-01", beacon_count: 2 }],
    });
  });

  it("loads real Parquet into DuckDB without visiting dashboard and clears the previous range", async () => {
    const bytes = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 503, cache: "MISS", resp_bytes: 4096 },
    ]);
    source.fetchFile.mockResolvedValue({ bytes, cached: false });
    const snapshots: Array<ReturnType<typeof cloudLoadingStore.getSnapshot>> = [];
    const unsubscribe = cloudLoadingStore.subscribe(() => snapshots.push(cloudLoadingStore.getSnapshot()));
    await ensureCloudRange(from, to, signal);
    expect(await getDuckDBEngine().executeSql("SELECT status, resp_bytes FROM requests"))
      .toEqual([{ status: 503, resp_bytes: 4096 }]);
    expect(snapshots.some((snapshot) => snapshot?.stage === "listing")).toBe(true);
    expect(snapshots.some((snapshot) => snapshot?.progress?.done === 1 &&
      snapshot.progress.downloadedBytes === bytes.byteLength)).toBe(true);
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    vi.mocked(fetchManifest).mockResolvedValueOnce({ files: [], complete: true });
    await ensureCloudRange(from, to, signal);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 0 }]);
    unsubscribe();
  });

  it("records cached file bytes and clears progress when a scan fails", async () => {
    vi.spyOn(getDuckDBEngine(), "reset").mockResolvedValue(undefined);
    vi.spyOn(getDuckDBEngine(), "syncWindow").mockResolvedValue([]);
    source.fetchFile.mockResolvedValue({ bytes: new ArrayBuffer(16), cached: true });
    let cachedBytes = 0;
    const unsubscribe = cloudLoadingStore.subscribe(() => {
      cachedBytes = Math.max(cachedBytes, cloudLoadingStore.getSnapshot()?.progress?.cachedBytes ?? 0);
    });

    await ensureCloudRange(from, to, signal);
    expect(cachedBytes).toBe(16);
    source.fetchFile.mockRejectedValueOnce(new Error("file unavailable"));
    await expect(ensureCloudRange(from, to, signal)).rejects.toThrow("file unavailable");
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    unsubscribe();
  });

  it("keeps cached file buffers owned by the source across worker transfers and repeated scans", async () => {
    vi.spyOn(getDuckDBEngine(), "reset").mockResolvedValue(undefined);
    vi.spyOn(getDuckDBEngine(), "syncWindow").mockImplementation(async (files) => {
      const bytes = files[0].bytes;
      structuredClone(bytes, { transfer: [bytes] });
      return [];
    });
    const bytes = new ArrayBuffer(16);
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    await ensureCloudRange(from, to, signal);
    await ensureCloudRange(from, to, signal);
    expect(bytes.byteLength).toBe(16);
  });

  it.each([
    { complete: false, files: [file] },
    { complete: true, files: [{ ...file, size: 65 * 1024 * 1024 }] },
    { complete: true, files: Array.from({ length: 257 }, () => file) },
  ])("rejects incomplete or oversized ranges before any download: %j", async (manifest) => {
    source.fetchFile.mockClear();
    vi.mocked(fetchManifest).mockResolvedValueOnce(manifest);
    await expect(ensureCloudRange(from, to, signal)).rejects.toThrow(/incomplete|budget/);
    expect(source.fetchFile).not.toHaveBeenCalled();
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });
});
