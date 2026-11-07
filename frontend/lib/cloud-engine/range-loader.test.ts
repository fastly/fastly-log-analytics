// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CloudBudgetError, ensureCloudRange } from "./range-loader";
import { clearManifestCache, fetchManifest, filesOverlapping, type ManifestFile } from "./dashboard/manifest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { cloudLoadingStore } from "./loading-progress";
import { parquetFixture } from "./dashboard/test-fixtures";
import { parquetWriteBuffer } from "hyparquet-writer";
import { cloudRumAnalytics, cloudRumLiveEvents, cloudRumUsage } from "./rum-wire";
import { executePerformanceAggregates } from "./performance";
import { handleCloudAnalyticalRequest } from "./adapter";
import * as sqlBuilder from "../sqlBuilder";

const source = vi.hoisted(() => ({
  fetchJson: vi.fn(), fetchFile: vi.fn(), fetchRange: vi.fn(), readDerivedFile: vi.fn(), writeDerivedFile: vi.fn(),
}));
// Stands in for the persisted browser file cache, which outlives a page load.
const derived = new Map<string, { bytes: ArrayBuffer; meta: string }>();
vi.mock("./dashboard/sources", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/sources")>(),
  browserSource: () => source,
  dashboardCacheScope: () => "fixture-session",
}));
vi.mock("./dashboard/manifest", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/manifest")>(),
  fetchManifest: vi.fn(),
}));

const from = "2026-11-01T00:00:00Z";
const to = "2026-11-02T00:00:00Z";
const signal = new AbortController().signal;
const file = { key: "processed/request/fixture.parquet", start: from, end: to, size: 100, rows: 1 };

beforeEach(async () => {
  clearManifestCache();
  await getDuckDBEngine().reset();
  vi.mocked(fetchManifest).mockClear();
  source.fetchFile.mockClear();
  source.fetchRange.mockReset();
  derived.clear();
  source.readDerivedFile.mockReset().mockImplementation(async (key: string, identity: string) => {
    const hit = derived.get(JSON.stringify([key, identity]));
    return hit ? { byteLength: hit.bytes.byteLength, meta: hit.meta, bytes: async () => hit.bytes.slice(0) } : null;
  });
  source.writeDerivedFile.mockReset().mockImplementation(async (key: string, identity: string, bytes: ArrayBuffer, meta: string) => {
    derived.set(JSON.stringify([key, identity]), { bytes: bytes.slice(0), meta });
  });
  cloudLoadingStore.reset();
  vi.mocked(fetchManifest).mockResolvedValue({ files: [file], complete: true });
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); clearManifestCache(); cloudLoadingStore.reset(); });

async function analyticalFiles(entries: (ManifestFile & { bytes: ArrayBuffer })[]) {
  const actual = await vi.importActual<typeof import("./dashboard/manifest")>("./dashboard/manifest");
  vi.mocked(fetchManifest).mockImplementation(actual.fetchManifest);
  source.fetchJson.mockImplementation(async (path: string) => {
    const url = new URL(path, "http://localhost");
    expect(url.searchParams.get("service_id")).toBe("fixture-service");
    const nativeSeconds = (name: string) => Math.floor(Date.parse(url.searchParams.get(name)!) / 1000) * 1000;
    return {
      files: filesOverlapping(entries.map(({ bytes: _bytes, ...entry }) => entry),
        nativeSeconds("from"), nativeSeconds("to")),
      complete: true, next_cursor: null,
    };
  });
  source.fetchFile.mockImplementation(async (key: string) => ({
    bytes: entries.find((entry) => entry.key === key)!.bytes, cached: true,
  }));
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
    String(input) === "/api/log-extents" ? Response.json({ latest_log_at: "2026-11-06T00:00:00Z" })
      : String(input) === "/api/services" ? Response.json({ services: [{ service_id: "fixture-service" }] })
        : Response.json({ fields: [], groups: [] })));
}

async function analyticalPost(path: string, body: Record<string, unknown>) {
  const response = await handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, {
    method: "POST", headers: { "x-service-id": "fixture-service" }, body: JSON.stringify(body),
  }));
  expect(response?.status).toBe(200);
  return response!.json();
}

describe("Cloud first-page analytical range loading", () => {
  const wideFile = (index: number, rows = 1600) => {
    let seed = index + 1;
    const noise = () => Array.from({ length: 300 }, () => {
      seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
      return String.fromCharCode(33 + (seed >>> 0) % 90);
    }).join("");
    return parquetWriteBuffer({ codec: "SNAPPY", rowGroupSize: 400, columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(Date.parse(from) + index * 1000 + i)) },
      { name: "ua", type: "STRING", data: Array.from({ length: rows }, noise) },
      { name: "status", type: "INT32", data: Array.from({ length: rows }, (_, i) => i % 7 ? 200 : 503) },
      { name: "cache", type: "STRING", data: Array.from({ length: rows }, (_, i) => i % 3 ? "HIT" : "MISS") },
      { name: "referer", type: "STRING", data: Array.from({ length: rows }, noise) },
    ] });
  };
  const projectedFiles = (count: number) => {
    const entries = Array.from({ length: count }, (_, i) => {
      const bytes = wideFile(i);
      return { ...file, key: `processed/request/w${i}.parquet`, size: bytes.byteLength, bytes };
    });
    vi.mocked(fetchManifest).mockResolvedValue({ files: entries.map(({ bytes: _bytes, ...entry }) => entry), complete: true });
    let inFlight = 0;
    const ranges = { count: 0, bytes: 0, peak: 0 };
    source.fetchRange.mockImplementation(async (entry: ManifestFile, start: number, end: number) => {
      inFlight++;
      ranges.peak = Math.max(ranges.peak, inFlight);
      ranges.count++;
      ranges.bytes += end - start;
      await new Promise((resolve) => setTimeout(resolve, 1));
      inFlight--;
      return entries.find((candidate) => candidate.key === entry.key)!.bytes.slice(start, end);
    });
    return { entries, ranges, fullBytes: entries.reduce((total, entry) => total + entry.size, 0) };
  };
  const projection = (maxBytes: number) => ({ projection: { columns: ["timestamp", "status", "cache"], maxBytes } });

  it("loads only the needed column chunks, budgeted on their bytes, with exact results", async () => {
    const { ranges, fullBytes } = projectedFiles(4);
    const read = await ensureCloudRange(from, to, signal, ["request"], projection(fullBytes / 4));
    expect(source.fetchFile).not.toHaveBeenCalled();
    expect(ranges.peak).toBeGreaterThan(6);
    expect(ranges.peak).toBeLessThanOrEqual(16);
    expect(ranges.bytes).toBeLessThan(fullBytes / 2);
    expect(await read.executeSql(
      "SELECT count(*) AS n, count(*) FILTER (WHERE status = 503) AS errors, count(*) FILTER (WHERE cache = 'HIT') AS hits FROM requests",
    )).toEqual([{ n: 6400, errors: 4 * 229, hits: 4 * 1066 }]);
    expect([...await read.getPresentColumns()]).toEqual(expect.arrayContaining(["ua", "referer", "status"]));
    read.release?.();
  });

  it("rejects a projection over its byte budget after reading only footers, keeping resident files", async () => {
    source.fetchFile.mockResolvedValue({ cached: true, bytes: parquetFixture([
      { timestamp: Date.parse(from), status: 201, cache: "HIT", resp_bytes: 1 },
    ]) });
    (await ensureCloudRange(from, to, signal)).release?.();
    const engine = getDuckDBEngine();
    const resident = [...(await engine.getPresentColumns())];
    const { ranges, entries } = projectedFiles(3);
    await expect(ensureCloudRange(from, to, signal, ["request"], projection(4096)))
      .rejects.toBeInstanceOf(CloudBudgetError);
    expect(ranges.count).toBe(entries.length);
    expect([...await engine.getPresentColumns()]).toEqual(resident);
  });

  it("reuses persisted projections on the next page load without range reads, and footers for another projection", async () => {
    const { ranges, fullBytes, entries } = projectedFiles(4);
    (await ensureCloudRange(from, to, signal, ["request"], projection(fullBytes))).release?.();
    const cold = ranges.count;
    expect(cold).toBeGreaterThan(0);

    await getDuckDBEngine().reset();
    const read = await ensureCloudRange(from, to, signal, ["request"], projection(fullBytes));
    expect(ranges.count).toBe(cold);
    expect(await read.executeSql(
      "SELECT count(*) AS n, count(*) FILTER (WHERE status = 503) AS errors, count(*) FILTER (WHERE cache = 'HIT') AS hits FROM requests",
    )).toEqual([{ n: 6400, errors: 4 * 229, hits: 4 * 1066 }]);
    expect([...await read.getPresentColumns()]).toEqual(expect.arrayContaining(["ua", "referer", "status"]));
    read.release?.();

    await getDuckDBEngine().reset();
    source.fetchRange.mockClear();
    (await ensureCloudRange(from, to, signal, ["request"], { projection: { columns: ["timestamp", "ua"], maxBytes: fullBytes } }))
      .release?.();
    const footerReads = source.fetchRange.mock.calls.filter(([entry, , end]) =>
      end === entries.find((candidate) => candidate.key === (entry as ManifestFile).key)!.size);
    expect(source.fetchRange).toHaveBeenCalled();
    expect(footerReads).toEqual([]);
  });

  it("serves a persisted footer longer than the tail probe without reading it again", async () => {
    const rows = 4000;
    const bytes = parquetWriteBuffer({ codec: "SNAPPY", rowGroupSize: 2, columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(Date.parse(from) + i)) },
      { name: "status", type: "INT32", data: Array.from({ length: rows }, (_, i) => i % 7 ? 200 : 503) },
      { name: "cache", type: "STRING", data: Array.from({ length: rows }, (_, i) => i % 3 ? "HIT" : "MISS") },
    ] });
    const entry = { ...file, key: "processed/request/long-footer.parquet", size: bytes.byteLength };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entry], complete: true });
    source.fetchRange.mockImplementation(async (_entry: ManifestFile, start: number, end: number) => bytes.slice(start, end));
    (await ensureCloudRange(from, to, signal, ["request"], { projection: { columns: ["timestamp", "status"] } })).release?.();
    const footerStart = bytes.byteLength - 8 - new DataView(bytes).getUint32(bytes.byteLength - 8, true);
    expect(bytes.byteLength - footerStart).toBeGreaterThan(64 * 1024);

    await getDuckDBEngine().reset();
    source.fetchRange.mockClear();
    const read = await ensureCloudRange(from, to, signal, ["request"], { projection: { columns: ["timestamp", "cache"] } });
    expect(source.fetchRange.mock.calls.filter(([, , end]) => (end as number) > footerStart)).toEqual([]);
    expect(await read.executeSql("SELECT count(*) FILTER (WHERE cache = 'HIT') AS hits FROM requests"))
      .toEqual([{ hits: 2666 }]);
    read.release?.();
  });

  it("keeps the file-count limit for projected ranges", async () => {
    projectedFiles(1);
    vi.mocked(fetchManifest).mockResolvedValue({ complete: true,
      files: Array.from({ length: 257 }, (_, i) => ({ ...file, key: `processed/request/x${i}.parquet` })) });
    await expect(ensureCloudRange(from, to, signal, ["request"], projection(1 << 30)))
      .rejects.toBeInstanceOf(CloudBudgetError);
    expect(source.fetchRange).not.toHaveBeenCalled();
  });

  it("aborts in-flight range reads when the request is cancelled", async () => {
    projectedFiles(4);
    const controller = new AbortController();
    const seen: AbortSignal[] = [];
    source.fetchRange.mockImplementation((_entry: ManifestFile, _start: number, _end: number, rangeSignal: AbortSignal) => {
      seen.push(rangeSignal);
      if (seen.length === 3) controller.abort(new Error("page left"));
      return new Promise((_resolve, reject) => {
        rangeSignal.addEventListener("abort", () => reject(rangeSignal.reason), { once: true });
      });
    });
    await expect(ensureCloudRange(from, to, controller.signal, ["request"], projection(1 << 30)))
      .rejects.toThrow(/page left/);
    expect(seen.length).toBeLessThanOrEqual(6);
    expect(seen.every((rangeSignal) => rangeSignal.aborted)).toBe(true);
  });

  it("downloads a cold range with bounded concurrency and publishes its view once", async () => {
    const files = Array.from({ length: 24 }, (_, i) => ({ ...file, key: `processed/request/f${i}.parquet` }));
    vi.mocked(fetchManifest).mockResolvedValue({ files, complete: true });
    let inFlight = 0, peak = 0;
    source.fetchFile.mockImplementation(async (key: string) => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight--;
      const index = Number(key.match(/f(\d+)\.parquet$/)![1]);
      return { cached: false, bytes: parquetFixture([
        { timestamp: Date.parse(from) + index, status: 200 + index, cache: "HIT", resp_bytes: 1 },
      ]) };
    });
    const engine = getDuckDBEngine();
    const lease = engine.lease.bind(engine);
    const ingested: string[] = [];
    let publishes = 0;
    vi.spyOn(engine, "lease").mockImplementation((fields) => {
      const inner = lease(fields);
      return { ...inner,
        ingestFile: (...args: Parameters<typeof inner.ingestFile>) => {
          ingested.push(args[1]);
          expect(args[6]).toEqual({ deferView: true });
          return inner.ingestFile(...args);
        },
        publishView: (...args: Parameters<typeof inner.publishView>) => { publishes++; return inner.publishView(...args); },
      };
    });
    const read = await ensureCloudRange(from, to, signal);
    expect(peak).toBeGreaterThan(1);
    expect(peak).toBeLessThanOrEqual(6);
    expect(ingested).toEqual(files.map((entry) => entry.key));
    expect(publishes).toBe(1);
    expect(await read.executeSql("SELECT count(*) AS count, count(DISTINCT status) AS statuses FROM requests"))
      .toEqual([{ count: 24, statuses: 24 }]);
    read.release?.();
  });

  it("aborts the prefetched downloads when the request is cancelled", async () => {
    const files = Array.from({ length: 12 }, (_, i) => ({ ...file, key: `processed/request/f${i}.parquet` }));
    vi.mocked(fetchManifest).mockResolvedValue({ files, complete: true });
    const controller = new AbortController();
    const seen: AbortSignal[] = [];
    source.fetchFile.mockImplementation((_key: string, fileSignal: AbortSignal) => {
      seen.push(fileSignal);
      if (seen.length === 3) controller.abort(new Error("page left"));
      return new Promise((_resolve, reject) => {
        fileSignal.addEventListener("abort", () => reject(fileSignal.reason), { once: true });
      });
    });
    await expect(ensureCloudRange(from, to, controller.signal)).rejects.toThrow(/page left/);
    expect(seen.length).toBeGreaterThan(1);
    expect(seen.length).toBeLessThanOrEqual(6);
    expect(seen.every((fileSignal) => fileSignal.aborted)).toBe(true);
  });

  it("holds its exact selected bindings until the owning request releases them", async () => {
    source.fetchFile.mockResolvedValue({ cached: true, bytes: parquetFixture([
      { timestamp: Date.parse(from), status: 201, cache: "HIT", resp_bytes: 1 },
    ]) });
    const read = await ensureCloudRange(from, to, signal);
    await getDuckDBEngine().retainFiles({ request: [], vitals: [], errors: [] });
    expect(await read.executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 1 }]);
    read.release?.();
    await expect(read.executeSql("SELECT count(*) FROM requests")).rejects.toThrow(/expired/i);
  });

  it("rejects a request invalidated while awaiting healthy backend readiness", async () => {
    const pending = ensureCloudRange(from, to, signal);
    const rejected = expect(pending).rejects.toThrow(/generation|expired/i);
    await getDuckDBEngine().reset();
    await rejected;
    expect(fetchManifest).not.toHaveBeenCalled();
    expect(source.fetchFile).not.toHaveBeenCalled();
  });

  it("binds load-to-query metadata to the loaded generation rather than a replacement database", async () => {
    source.fetchFile.mockResolvedValue({ bytes: parquetFixture([
      { timestamp: Date.parse(from), status: 200, cache: "HIT", resp_bytes: 1 },
    ]), cached: true });
    const read = await ensureCloudRange(from, to, signal);
    await getDuckDBEngine().reset();
    await ensureCloudRange(from, to, signal);
    await expect(read.executeSql("SELECT count(*) FROM requests")).rejects.toThrow(/generation|expired/i);
    await expect(read.getPresentColumns()).rejects.toThrow(/generation|expired/i);
  });

  it("rejects a listing that completes after its generation was reset", async () => {
    let resume!: () => void, entered!: () => void;
    const waiting = new Promise<void>((resolve) => { resume = resolve; });
    const started = new Promise<void>((resolve) => { entered = resolve; });
    vi.mocked(fetchManifest).mockImplementationOnce(async () => {
      entered();
      await waiting;
      return { files: [file], complete: true };
    });
    source.fetchFile.mockResolvedValue({ bytes: parquetFixture([
      { timestamp: Date.parse(from), status: 200, cache: "HIT", resp_bytes: 1 },
    ]), cached: true });
    const loaded = ensureCloudRange(from, to, signal).then(
      () => "unexpected success", (error: Error) => error.message,
    );
    await started;
    await getDuckDBEngine().reset();
    resume();
    expect(await loaded).toMatch(/generation|expired/i);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM requests"))
      .toEqual([{ count: 0 }]);
  });

  it("carries the selected historical Query window independently of generated SQL", async () => {
    const recentFrom = "2026-11-05T00:00:00Z", recentTo = "2026-11-06T00:00:00Z";
    await analyticalFiles([
      { ...file, bytes: parquetFixture([{ timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10 }]) },
      { ...file, key: "recent.parquet", start: recentFrom, end: recentTo,
        bytes: parquetFixture([{ timestamp: Date.parse(recentFrom) + 1000, status: 200, cache: "HIT", resp_bytes: 10 }]) },
    ]);
    const body = sqlBuilder.buildQueryRequest({
      sql: sqlBuilder.buildStructuredSql({}, from, to, [], 10), max_rows: 10, explain: false,
    }, "fixture-service", from, to);
    expect((await analyticalPost("/api/query", body)).row_count).toBe(1);
    const rawSql = `SELECT count(*) AS count FROM logs WHERE timestamp >= '${recentFrom}' AND timestamp <= '${recentTo}'`;
    const explicit = sqlBuilder.buildQueryRequest({
      sql: rawSql, max_rows: 10, explain: false, service_id: "fixture-service",
      start_time: recentFrom, end_time: recentTo,
    }, "ignored-service", from, to);
    expect((await analyticalPost("/api/query", explicit)).data).toEqual([{ count: 1 }]);
  });

  it("keeps a Performance half-open window invariant under valid Parquet regrouping", async () => {
    const end = "2026-11-01T01:00:00Z", later = "2026-11-01T02:00:00Z";
    const rows = [Date.parse(from) + 1000, Date.parse(end)].map((timestamp) => ({
      timestamp, status: 200, cache: "HIT", resp_bytes: 10, ttl: 30,
    }));
    const split = [
      { ...file, end, bytes: parquetFixture([rows[0]]) },
      { ...file, key: "next.parquet", start: end, end: later, bytes: parquetFixture([rows[1]]) },
    ];
    const request = { start_time: from, end_time: end, sections: ["ttl_dist"] };
    const count = (body: { ttl_dist: { count: number }[] }) => body.ttl_dist.reduce((sum, bucket) => sum + bucket.count, 0);
    await analyticalFiles(split);
    expect(count(await analyticalPost("/api/performance/aggregates", request))).toBe(1);
    clearManifestCache();
    await analyticalFiles([{ ...file, key: "joined.parquet", end: later, bytes: parquetFixture(rows) }]);
    expect(count(await analyticalPost("/api/performance/aggregates", request))).toBe(1);
    const raw = sqlBuilder.buildQueryRequest({
      sql: `SELECT count(*) AS count FROM logs WHERE timestamp >= '${from}' AND timestamp <= '${end}'`,
      max_rows: 10, explain: false,
    }, "fixture-service", from, end);
    expect((await analyticalPost("/api/query", raw)).data).toEqual([{ count: 2 }]);
    clearManifestCache();
    await analyticalFiles(split);
    expect((await analyticalPost("/api/query", raw)).data).toEqual([{ count: 2 }]);
  });

  it("conservatively discovers sub-second windows despite native manifest second precision", async () => {
    const start = "2026-11-01T00:00:00.500Z", end = "2026-11-01T00:00:01.500Z";
    await analyticalFiles([{ ...file, start: "2026-11-01T00:00:01Z", end: "2026-11-01T00:00:02Z",
      bytes: parquetFixture([1100, 1600].map((offset) => ({
        timestamp: Date.parse(from) + offset, status: 200, cache: "HIT", resp_bytes: 10, ttl: 30,
      }))) }]);
    const data = await analyticalPost("/api/performance/aggregates", {
      start_time: start, end_time: end, sections: ["ttl_dist"],
    });
    expect(data.ttl_dist.reduce((sum: number, bucket: { count: number }) => sum + bucket.count, 0)).toBe(1);
  });

  it("isolates RUM filters from retired request custom columns without losing request filters", async () => {
    const requestBytes = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10, ttl: 30, tenant: "selected" },
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10, ttl: 30, tenant: "other" },
    ]);
    const rumKey = "processed/rum/client_vitals/transition.parquet";
    const rumBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(from)] },
      { name: "metric_name", type: "STRING", data: ["LCP"] },
      { name: "metric_value", type: "DOUBLE", data: [1000] },
      { name: "browser", type: "STRING", data: ["Chrome"] },
    ] });
    source.fetchFile.mockImplementation(async (key: string) => ({
      bytes: key === rumKey ? rumBytes : requestBytes, cached: true,
    }));
    const options = { serviceId: "fixture-service", customFields: [{ id: "tenant", type: "string" as const }] };
    const requestParams = {
      start_time: from, end_time: to, service_id: "fixture-service", sections: ["ttl_dist" as const],
      filters: { tenant: { mode: "include" as const, values: ["selected"] } },
    };
    const assertRequestFilter = async () => {
      const result = await executePerformanceAggregates(requestParams);
      expect(result.ttl_dist?.reduce((sum, bucket) => sum + bucket.count, 0)).toBe(1);
    };
    await ensureCloudRange(from, to, signal, ["request"], options);
    await assertRequestFilter();
    vi.mocked(fetchManifest).mockResolvedValueOnce({ complete: true, files: [{ ...file, key: rumKey }] });
    await ensureCloudRange(from, to, signal, ["rum"]);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 0 }]);
    const params = { start_time: from, end_time: to, filters: requestParams.filters };
    await expect(cloudRumAnalytics(params)).rejects.toThrow("Unsupported RUM filter");
    await expect(cloudRumLiveEvents(params)).rejects.toThrow("Unsupported RUM filter");
    expect((await cloudRumAnalytics({ filters: { browser_name: { mode: "include", values: ["Chrome"] } } })).beacon_count).toBe(1);
    expect(await cloudRumLiveEvents({ filters: { browser_name: { mode: "include", values: ["Chrome"] } } })).toHaveLength(1);
    await ensureCloudRange(from, to, signal, ["request"], options);
    await assertRequestFilter();
  });

  it("reuses unchanged resident files while discovering and ingesting fresh tails", async () => {
    const files = [0, 1].map((index) => ({ ...file, key: `resident-${index}.parquet`, etag: String(index) }));
    const buffers = files.map((_, index) => parquetFixture([
      { timestamp: Date.parse(from) + index + 1000, status: 200, cache: "HIT", resp_bytes: 10 },
    ]));
    vi.mocked(fetchManifest)
      .mockResolvedValueOnce({ complete: true, files: [files[0]] })
      .mockResolvedValueOnce({ complete: true, files: [files[0]] })
      .mockResolvedValueOnce({ complete: true, files });
    source.fetchFile.mockImplementation(async (key: string) => ({
      bytes: buffers[files.findIndex((entry) => entry.key === key)], cached: true,
    }));
    const engine = getDuckDBEngine();
    for (let index = 0; index < 3; index++) {
      await ensureCloudRange(from, to, signal, ["request"], { serviceId: "fixture-service" });
    }
    expect(await engine.executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 2 }]);
    expect(source.fetchFile).toHaveBeenCalledTimes(2);
    expect(fetchManifest).toHaveBeenCalledTimes(3);
  });

  it("removes replaced, retired and other-service rows before using a new manifest", async () => {
    const original = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10 },
    ]);
    const replacement = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 503, cache: "MISS", resp_bytes: 20 },
    ]);
    source.fetchFile.mockResolvedValueOnce({ bytes: original, cached: true })
      .mockResolvedValue({ bytes: replacement, cached: true });
    vi.mocked(fetchManifest).mockResolvedValueOnce({
      complete: true, files: [{ ...file, etag: "original" }],
    }).mockResolvedValue({ complete: true, files: [{ ...file, etag: "replacement" }] });
    const engine = getDuckDBEngine();
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "first" });
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "first" });
    expect(await engine.executeSql("SELECT status FROM requests")).toEqual([{ status: 503 }]);
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "second" });
    expect(source.fetchFile).toHaveBeenCalledTimes(3);
    expect(await engine.executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 1 }]);
    vi.mocked(fetchManifest).mockResolvedValueOnce({ complete: true, files: [] });
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "second" });
    expect(await engine.executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 0 }]);
  });

  it("retries an interrupted tail without duplicating already resident rows", async () => {
    const tail = { ...file, key: "tail.parquet" };
    const bytes = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10 },
    ]);
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    await ensureCloudRange(from, to, signal);
    vi.mocked(fetchManifest).mockResolvedValue({ complete: true, files: [file, tail] });
    source.fetchFile.mockRejectedValueOnce(new DOMException("Aborted", "AbortError"));
    await expect(ensureCloudRange(from, to, signal)).rejects.toThrow("Aborted");
    await ensureCloudRange(from, to, signal);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 2 }]);
    expect(source.fetchFile).toHaveBeenCalledTimes(3);
  });

  it("honors registered string, numeric and boolean custom filters in raw domain queries", async () => {
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(from), new Date(from)] },
      { name: "ttl", type: "DOUBLE", data: [30, 30] },
      { name: "tenant", type: "STRING", data: ["selected", "excluded"] },
      { name: "plan", type: "DOUBLE", data: [2, 3] },
      { name: "flag", type: "BOOLEAN", data: [true, false] },
    ] });
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    await ensureCloudRange(from, to, signal, ["request"], { serviceId: "fixture-service" });
    await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "fixture-service", customFields: [
        { id: "tenant", type: "string" }, { id: "plan", type: "number" }, { id: "flag", type: "boolean" },
      ],
    });
    for (const [field, value] of [["tenant", "selected"], ["plan", "2.0"], ["flag", "true"]]) {
      const result = await executePerformanceAggregates({
        start_time: from, end_time: to, service_id: "fixture-service", sections: ["ttl_dist"],
        filters: { [field]: { mode: "include", values: [value] } },
      });
      expect(result.ttl_dist?.reduce((total, bucket) => total + bucket.count, 0), field).toBe(1);
    }
    expect(source.fetchFile).toHaveBeenCalledTimes(1);
  });

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
    vi.mocked(fetchManifest).mockResolvedValue({
      complete: true, files: [{ ...file, key: vitalsKey }, { ...file, key: errorsKey }],
    });
    source.fetchFile.mockClear();
    await ensureCloudRange(from, to, signal, ["rum"]);
    expect(source.fetchFile).not.toHaveBeenCalled();
    vi.mocked(fetchManifest).mockResolvedValue({
      complete: true, files: [{ ...file, key: vitalsKey }],
    });
    await ensureCloudRange(from, to, signal, ["rum"]);
    expect(await cloudRumUsage({ start_time: from, end_time: to, signal }, 0.005))
      .toMatchObject({ total_beacons: 1 });
    vi.mocked(fetchManifest).mockResolvedValue({ complete: true, files: [] });
    await ensureCloudRange(from, to, signal, ["request"]);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM client_vitals")).toEqual([{ count: 0 }]);
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
    const bytes = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10 },
    ]);
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    let cachedBytes = 0;
    const unsubscribe = cloudLoadingStore.subscribe(() => {
      cachedBytes = Math.max(cachedBytes, cloudLoadingStore.getSnapshot()?.progress?.cachedBytes ?? 0);
    });

    await ensureCloudRange(from, to, signal);
    expect(cachedBytes).toBe(bytes.byteLength);
    vi.mocked(fetchManifest).mockResolvedValueOnce({
      files: [{ ...file, key: "unavailable.parquet" }], complete: true,
    });
    source.fetchFile.mockRejectedValueOnce(new Error("file unavailable"));
    await expect(ensureCloudRange(from, to, signal)).rejects.toThrow("file unavailable");
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    unsubscribe();
  });

  it("keeps cached file buffers owned by the source across worker transfers and repeated scans", async () => {
    const bytes = parquetFixture([{ timestamp: Date.parse(from), status: 200, cache: "HIT", resp_bytes: 1 }]);
    const original = new Uint8Array(bytes).slice();
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    await ensureCloudRange(from, to, signal);
    await ensureCloudRange(from, to, signal);
    expect(new Uint8Array(bytes)).toEqual(original);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 1 }]);
    expect(source.fetchFile).toHaveBeenCalledTimes(1);
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
