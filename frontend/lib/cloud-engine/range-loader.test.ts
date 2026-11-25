// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CloudBudgetError, ensureCloudRange } from "./range-loader";
import {
  clearManifestCache,
  fetchManifest,
  filesOverlapping,
  observeManifestCommit,
  type ManifestFile,
} from "./dashboard/manifest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { cloudLoadingStore } from "./loading-progress";
import { parquetFixture } from "./dashboard/test-fixtures";
import { parquetWriteBuffer } from "hyparquet-writer";
import { cloudRumAnalytics, cloudRumLiveEvents, cloudRumUsage } from "./rum-wire";
import { executePerformanceAggregates } from "./performance";
import { handleCloudAnalyticalRequest } from "./adapter";

const settings = vi.hoisted(() => ({ ids: new Set<string>(), asked: [] as (string | undefined)[] }));
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: vi.fn(async (serviceId: string | undefined) => {
    settings.asked.push(serviceId);
    return settings.ids;
  }),
}));
import { handleInsightAvailability, INSIGHT_DEFINITIONS } from "./insights";
import * as sqlBuilder from "../sqlBuilder";
import { insightsBaselineFromRollup } from "./insights-rollup";
import { mergeRollups } from "./dashboard/rollup-query";
import { dashboardManifestCacheScope } from "./dashboard/sources";
import { planParquetProjection, readWholeParquetFooter, wholeReadLimits } from "./dashboard/parquet-projection";
import { MAX_REGISTERED_BYTES } from "./dashboard/budget";

const DEFAULT_WHOLE_READ_LIMITS = { ...wholeReadLimits };

const { source, scopeState } = vi.hoisted(() => {
  const mock = {
    fetchJson: vi.fn(), fetchFile: vi.fn(), fetchRange: vi.fn(), readDerivedFile: vi.fn(), writeDerivedFile: vi.fn(),
    withOptions: vi.fn(),
  };
  mock.withOptions.mockReturnValue(mock);
  return { source: mock, scopeState: { value: "fixture-session" } };
});
// Stands in for the persisted browser file cache, which outlives a page load.
const derived = new Map<string, { bytes: ArrayBuffer; meta: string }>();
vi.mock("./dashboard/sources", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/sources")>(),
  browserSource: () => source,
  dashboardCacheScope: () => scopeState.value,
}));
vi.mock("./insights-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./insights-rollup")>(),
  insightsBaselineFromRollup: vi.fn(async () => null),
}));
// Lets a test stand in footer-planned sizes for large files without allocating them.
const planned = vi.hoisted(() => ({ projectedBytes: null as number | null }));
vi.mock("./dashboard/parquet-projection", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./dashboard/parquet-projection")>();
  return {
    ...actual,
    planParquetProjection: (...args: Parameters<typeof actual.planParquetProjection>) => {
      const plan = actual.planParquetProjection(...args);
      return planned.projectedBytes === null ? plan : { ...plan, projectedBytes: planned.projectedBytes };
    },
  };
});
vi.mock("./dashboard/manifest", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/manifest")>(),
  fetchManifest: vi.fn(),
}));

const from = "2026-11-01T00:00:00Z";
const to = "2026-11-02T00:00:00Z";
const signal = new AbortController().signal;
const file = { key: "processed/request/fixture.parquet", start: from, end: to, size: 100, rows: 1 };

beforeEach(async () => {
  settings.ids = new Set([
    ...INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields), "edge", "rid", "prid", "pop", "ttfb", "ottfb", "edge_score",
  ]);
  settings.asked = [];
  // Fixtures are small closed-hour files, so tests of footer-planned ranges opt out of whole reads.
  Object.assign(wholeReadLimits, { anyUpToBytes: 0, closedHourUpToBytes: 0 });
  scopeState.value = "fixture-session";
  clearManifestCache();
  await getDuckDBEngine().reset();
  vi.mocked(fetchManifest).mockClear();
  source.fetchFile.mockClear();
  source.fetchRange.mockReset();
  source.withOptions.mockReset().mockReturnValue(source);
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
afterEach(() => { Object.assign(wholeReadLimits, DEFAULT_WHOLE_READ_LIMITS); vi.restoreAllMocks(); vi.unstubAllGlobals(); clearManifestCache(); cloudLoadingStore.reset(); });

async function analyticalFiles(fixtures: (ManifestFile & { bytes: ArrayBuffer })[]) {
  const entries = fixtures.map((entry) => ({ ...entry, size: entry.bytes.byteLength }));
  source.fetchRange.mockImplementation(async (entry: ManifestFile, start: number, end: number) =>
    entries.find((candidate) => candidate.key === entry.key)!.bytes.slice(start, end));
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

  it("reads small closed-hour files whole in one request each, without footer or column ranges", async () => {
    Object.assign(wholeReadLimits, DEFAULT_WHOLE_READ_LIMITS);
    const { entries, ranges, fullBytes } = projectedFiles(3);
    expect(Math.max(...entries.map((entry) => entry.size))).toBeLessThan(DEFAULT_WHOLE_READ_LIMITS.closedHourUpToBytes);
    source.fetchFile.mockImplementation(async (key: string) => ({
      bytes: entries.find((entry) => entry.key === key)!.bytes.slice(0), cached: false,
    }));
    const read = await ensureCloudRange(from, to, signal, ["request"], projection(fullBytes));
    expect(ranges.count).toBe(0);
    expect(source.fetchFile.mock.calls.map(([key]) => key).sort()).toEqual(entries.map((entry) => entry.key).sort());
    expect(await read.executeSql(
      "SELECT count(*) AS n, count(*) FILTER (WHERE status = 503) AS errors, count(*) FILTER (WHERE cache = 'HIT') AS hits FROM requests",
    )).toEqual([{ n: 4800, errors: 3 * 229, hits: 3 * 1066 }]);
    read.release?.();
  });

  it("keeps footer-planned ranges for open-hour files above the small-file size, and reads tiny ones whole", async () => {
    Object.assign(wholeReadLimits, DEFAULT_WHOLE_READ_LIMITS);
    const { entries, ranges } = projectedFiles(2);
    const tiny = wideFile(9, 100);
    expect(tiny.byteLength).toBeLessThan(DEFAULT_WHOLE_READ_LIMITS.anyUpToBytes);
    expect(entries[0].size).toBeGreaterThan(DEFAULT_WHOLE_READ_LIMITS.anyUpToBytes);
    const open = new Date(Date.now() + 600_000).toISOString();
    const listed = [
      ...entries.map(({ bytes: _bytes, ...entry }) => ({ ...entry, end: open })),
      { ...file, key: "processed/request/tiny.parquet", end: open, size: tiny.byteLength },
    ];
    vi.mocked(fetchManifest).mockResolvedValue({ files: listed, complete: true });
    source.fetchFile.mockImplementation(async (key: string) => {
      expect(key).toBe("processed/request/tiny.parquet");
      return { bytes: tiny.slice(0), cached: false };
    });
    const read = await ensureCloudRange(from, to, signal, ["request"], projection(1 << 30));
    expect(source.fetchFile).toHaveBeenCalledTimes(1);
    expect(source.fetchRange.mock.calls.every(([entry]) => (entry as ManifestFile).key !== "processed/request/tiny.parquet"))
      .toBe(true);
    expect(ranges.count).toBeGreaterThan(2);
    expect(await read.executeSql("SELECT count(*) AS n FROM requests")).toEqual([{ n: 3300 }]);
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

  it("budgets a projection on what DuckDB can register, whatever larger budget the caller asks for", async () => {
    const { ranges, entries } = projectedFiles(3);
    planned.projectedBytes = Math.ceil(MAX_REGISTERED_BYTES * 0.4);
    try {
      const error = await ensureCloudRange(from, to, signal, ["request"], projection(96 * 1024 * 1024)).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(CloudBudgetError);
      expect((error as CloudBudgetError).detail).toEqual({ unit: "bytes", needed: 3 * planned.projectedBytes, limit: MAX_REGISTERED_BYTES });
      expect(ranges.count).toBe(entries.length);
    } finally {
      planned.projectedBytes = null;
    }
  });

  it("rejects a projection whose >=65% whole-file wire download exceeds the byte budget even when projectedBytes fits", async () => {
    // Reproduces /assets-shield and /fastly-value (filtered) downloading ~240 MB across 30+ compacted
    // files: projectedBytes fits under the 64 MiB DuckDB budget, but coalesced ranges cover >=65% of
    // each file so fetchProjected would fall back to fetchWhole and pull the entire files over HTTP.
    const { entries } = projectedFiles(3);
    const totalWire = entries.reduce((sum, entry) => sum + entry.size, 0);
    const budget = Math.floor(totalWire * 0.5);
    planned.projectedBytes = Math.floor(budget * 0.25);
    try {
      const error = await ensureCloudRange(from, to, signal, ["request"], {
        projection: { columns: ["timestamp", "ua", "status", "cache", "referer"], maxBytes: budget },
      }).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(CloudBudgetError);
      expect((error as CloudBudgetError).detail).toEqual({
        unit: "bytes",
        needed: expect.any(Number),
        limit: budget,
      });
      expect((error as CloudBudgetError).detail!.needed).toBeGreaterThan(budget);
      expect(source.fetchFile).not.toHaveBeenCalled();
    } finally {
      planned.projectedBytes = null;
    }
  });

  it("names the needed and allowed size when a projection is over budget", async () => {
    const { fullBytes } = projectedFiles(3);
    const error = await ensureCloudRange(from, to, signal, ["request"], projection(4096)).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(CloudBudgetError);
    expect((error as CloudBudgetError).message).toMatch(/needs \d+\.\d MiB .*budget is \d+\.\d MiB; choose a shorter time range/);
    expect((error as CloudBudgetError).detail).toEqual({ unit: "bytes", needed: expect.any(Number), limit: 4096 });
    expect((error as CloudBudgetError).detail!.needed).toBeGreaterThan(4096);
    expect((error as CloudBudgetError).detail!.needed).toBeLessThan(fullBytes);
  });

  it("loads only the columns an arbitrary SQL query reads", async () => {
    const entries = Array.from({ length: 3 }, (_, i) => {
      const bytes = wideFile(i);
      return { ...file, key: `processed/request/q${i}.parquet`, bytes };
    });
    const fullBytes = entries.reduce((total, entry) => total + entry.bytes.byteLength, 0);
    await analyticalFiles(entries);
    const result = await analyticalPost("/api/query", {
      sql: "SELECT status, count(*) AS requests FROM logs GROUP BY status ORDER BY status",
      start_time: from, end_time: to, max_rows: 10,
    });
    expect(result.data).toEqual([{ status: 200, requests: 3 * 1371 }, { status: 503, requests: 3 * 229 }]);
    expect(source.fetchFile).not.toHaveBeenCalled();
    const fetched = source.fetchRange.mock.calls.reduce((total, [, start, end]) => total + end - start, 0);
    expect(fetched).toBeLessThan(fullBytes / 2);
    // Raw ranges yield to summary reads in other realms too.
    expect(source.fetchRange.mock.calls.map((call) => call[4])).toEqual(source.fetchRange.mock.calls.map(() => "low"));
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

  it("starts DuckDB while the range is still listing", async () => {
    const engine = getDuckDBEngine();
    const prewarm = vi.spyOn(engine, "prewarm");
    let warmedBeforeListing = false;
    vi.mocked(fetchManifest).mockImplementation(async () => {
      warmedBeforeListing = prewarm.mock.calls.length > 0;
      return { files: [], complete: true };
    });
    const read = await ensureCloudRange(from, to, signal);
    read.release?.();
    expect(warmedBeforeListing).toBe(true);
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

  it("answers ORDER BY timestamp DESC LIMIT from the newest window, widening across overlapping files", async () => {
    const day = "2026-11-05T", at = (time: string) => Date.parse(`${day}${time}Z`);
    const row = (time: string, status: number) => ({ timestamp: at(time), status, cache: "HIT", resp_bytes: 1 });
    const hour = (key: string, startTime: string, rows: ReturnType<typeof row>[]) => ({
      ...file, key, start: `${day}${startTime}Z`, end: new Date(at(startTime) + 3_600_000).toISOString(),
      bytes: parquetFixture(rows),
    });
    await analyticalFiles([
      // A compacted day file overlaps every window; its 10:00 row must not leak into the newest one.
      { ...file, key: "processed/request/day.parquet", start: `${day}00:00:00Z`, end: "2026-11-06T00:00:00Z",
        bytes: parquetFixture([row("10:00:00", 1), row("23:30:00", 2)]) },
      hour("processed/request/h23.parquet", "23:00:00", [row("23:10:00", 3), row("23:50:00", 4)]),
      hour("processed/request/h20.parquet", "20:00:00", [row("20:30:00", 5)]),
      hour("processed/request/h03.parquet", "03:00:00", [row("03:15:00", 6)]),
    ]);
    const fetchedKeys = () => new Set([
      ...source.fetchFile.mock.calls.map(([key]) => key as string),
      ...source.fetchRange.mock.calls.map(([entry]) => (entry as ManifestFile).key),
    ]);
    const query = async (limit: number) => (await analyticalPost("/api/query", {
      sql: `SELECT status FROM logs ORDER BY timestamp DESC LIMIT ${limit}`,
      start_time: `${day}00:00:00Z`, end_time: "2026-11-06T00:00:00Z", max_rows: 100,
    })).data.map((r: { status: number }) => r.status);

    expect(await query(3)).toEqual([4, 2, 3]);
    expect(fetchedKeys()).not.toContain("processed/request/h20.parquet");
    expect(await query(4)).toEqual([4, 2, 3, 5]);
    expect(fetchedKeys()).not.toContain("processed/request/h03.parquet");
    expect(await query(10)).toEqual([4, 2, 3, 5, 1, 6]);
  });

  it("downloads the Insights window once across the summary pass and its raw follow-up", async () => {
    const windowStart = Date.parse("2026-11-05T00:00:00Z");
    // Distinct URLs put the url chunk ahead of the footer probe, so reading it takes its own range request.
    const hourFile = (key: string, start: string) => {
      const startMs = Date.parse(start);
      const rows = 2000;
      const columnData = [
        { name: "timestamp", type: "TIMESTAMP" as const, data: Array.from({ length: rows }, (_, i) => new Date(startMs + 1000 + i)) },
        { name: "url", type: "STRING" as const, data: Array.from({ length: rows }, () => `/${crypto.randomUUID()}${crypto.randomUUID()}`) },
        { name: "status", type: "INT32" as const, data: Array.from({ length: rows }, () => 503) },
        { name: "cache", type: "STRING" as const, data: Array.from({ length: rows }, () => "MISS") },
        { name: "resp_bytes", type: "INT64" as const, data: Array.from({ length: rows }, () => BigInt(10)) },
        { name: "country", type: "STRING" as const, data: Array.from({ length: rows }, () => "US") },
      ];
      return { ...file, key, start, end: new Date(startMs + 3_600_000).toISOString(), bytes: parquetWriteBuffer({ columnData }) };
    };
    const windowFile = hourFile("processed/request/window.parquet", "2026-11-05T12:00:00Z");
    const baselineFile = hourFile("processed/request/baseline.parquet", "2026-11-04T12:00:00Z");
    await analyticalFiles([windowFile, baselineFile]);
    const reads = (key: string) => source.fetchRange.mock.calls.filter(([entry]) => (entry as ManifestFile).key === key).length;
    vi.mocked(insightsBaselineFromRollup).mockResolvedValueOnce({
      rollup: mergeRollups([]), dataFromMs: windowStart - 24 * 3_600_000, dataThroughMs: windowStart,
    });
    const body = { window_size_hrs: 24, baseline_hours: 24, filters: {} };

    const first = await analyticalPost("/api/insights", body);
    expect(first.rollup).toBeDefined();
    const windowReads = reads(windowFile.key);
    expect(windowReads).toBeGreaterThan(0);
    expect(reads(baselineFile.key)).toBe(0);

    await analyticalPost("/api/insights", { ...body, end_time: first.window_end,
      detectors: ["error_spikes", "pop_latency_regression"] });
    expect(reads(windowFile.key)).toBe(windowReads);
    expect(reads(baselineFile.key)).toBeGreaterThan(0);
  });

  it("answers Insights availability from the log field settings, reading no files", async () => {
    const entries = Array.from({ length: 3 }, (_, i) => {
      const bytes = wideFile(i);
      return { ...file, key: `processed/request/a${i}.parquet`, start: "2026-11-05T12:00:00Z",
        end: "2026-11-05T13:00:00Z", size: bytes.byteLength, bytes };
    });
    await analyticalFiles(entries);
    const required = [...new Set(INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields))];
    settings.ids = new Set(required.filter((name) => name !== "referer"));
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/insight-availability", {
      headers: { "x-service-id": "fixture-service" },
    }));
    expect(response?.status).toBe(200);
    const expected = handleInsightAvailability("fixture-service", settings.ids);
    expect(await response!.json()).toEqual(JSON.parse(JSON.stringify(expected)));
    expect(expected.insights.some((d) => d.missing_fields?.includes("referer"))).toBe(true);
    expect(settings.asked).toEqual(["fixture-service"]);
    expect(source.fetchFile).not.toHaveBeenCalled();
    expect(source.fetchRange).not.toHaveBeenCalled();
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

  it("advances the native loader cache when the observed resync uses the same origin scope", async () => {
    const actual = await vi.importActual<typeof import("./dashboard/manifest")>(
      "./dashboard/manifest",
    );
    vi.mocked(fetchManifest).mockImplementation(actual.fetchManifest);
    const bytes = parquetFixture([
      { timestamp: Date.parse(from) + 1000, status: 200, cache: "HIT", resp_bytes: 10 },
    ]);
    source.fetchFile.mockResolvedValue({ bytes, cached: true });
    source.fetchJson.mockResolvedValue({
      files: [file],
      next_cursor: null,
      complete: true,
      catalog_generation: 8,
    });
    await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "fixture-service",
    });

    const observer = Object.assign(vi.fn(), {
      manifestCacheScope: () =>
        dashboardManifestCacheScope("http://localhost/", "fixture-service"),
    });
    const loader = vi.mocked(fetchManifest).mock.calls[0][0];
    expect(observer.manifestCacheScope()).toBe(loader.manifestCacheScope?.());
    observeManifestCommit(observer, { type: "resync", generation: 8 });
    await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "fixture-service",
    });

    expect(source.fetchJson).toHaveBeenCalledTimes(2);
  });

  it("skips data fetches for files lacking all requireAnyColumns", async () => {
    Object.assign(wholeReadLimits, DEFAULT_WHOLE_READ_LIMITS);
    const withCmcd = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 500,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 1000 }, (_, i) => new Date(Date.parse(from) + i * 1000)) },
        { name: "status", type: "INT32", data: Array.from({ length: 1000 }, () => 200) },
        { name: "url", type: "STRING", data: Array.from({ length: 1000 }, (_, i) => `/wide/${"x".repeat(200)}/${i}`) },
        { name: "cmcd_sid", type: "STRING", data: Array.from({ length: 1000 }, (_, i) => `s${i % 3}`) },
      ],
    });
    const withoutCmcd = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 500,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 1000 }, (_, i) => new Date(Date.parse(from) + i * 1000)) },
        { name: "status", type: "INT32", data: Array.from({ length: 1000 }, () => 200) },
        { name: "url", type: "STRING", data: Array.from({ length: 1000 }, (_, i) => `/p/${i}`) },
      ],
    });
    const files = [
      { ...file, key: "processed/request/has-cmcd.parquet", size: withCmcd.byteLength, etag: "c1" },
      { ...file, key: "processed/request/no-cmcd.parquet", size: withoutCmcd.byteLength, etag: "c2" },
    ];
    const buffers = new Map([
      [files[0].key, withCmcd],
      [files[1].key, withoutCmcd],
    ]);
    vi.mocked(fetchManifest).mockResolvedValue({ complete: true, files });
    const rangeKeys: string[] = [];
    source.fetchRange.mockImplementation(async (entry: { key: string }, start: number, end: number) => {
      rangeKeys.push(entry.key);
      return buffers.get(entry.key)!.slice(start, end);
    });
    const wholeKeys: string[] = [];
    source.fetchFile.mockImplementation(async (key: string) => {
      wholeKeys.push(key);
      return { bytes: buffers.get(key)!.slice(0), cached: false };
    });
    await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "fixture-service",
      projection: {
        columns: ["timestamp", "status", "cmcd_sid"],
        requireAnyColumns: ["cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot"],
      },
    });
    // no-cmcd.parquet only gets a single footer probe, never a whole-file fetch or data ranges.
    expect(wholeKeys).not.toContain(files[1].key);
    expect(rangeKeys.filter((k) => k === files[1].key)).toHaveLength(1);
    const rows = await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM requests");
    expect(rows).toEqual([{ count: 1000 }]);
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
      { name: "metric_name", type: "STRING", data: ["LCP", "CLS", "page_load"] },
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

  it("loads a RUM range of many small window files that would exceed the request file cap", async () => {
    // Nothing compacts RUM windows: a day at a 300 s RUM period lists ~576 files of a few KiB.
    const at = new Date(Date.parse(from) + 1000);
    const vitalBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [at] },
      { name: "metric_name", type: "STRING", data: ["LCP"] },
      { name: "metric_value", type: "DOUBLE", data: [2000] },
      ...["pathname", "browser", "os", "device", "cid", "req_id"].map((name, index) => ({
        name, type: "STRING" as const, data: [["/home", "Chrome", "Test OS", "desktop", "client-a", "beacon"][index]],
      })),
    ] });
    const files = Array.from({ length: 600 }, (_, i) => ({
      ...file, key: `processed/rum/client_vitals/w${i}.parquet`, size: vitalBytes.byteLength,
    }));
    vi.mocked(fetchManifest).mockResolvedValueOnce({ complete: true, files });
    source.fetchFile.mockImplementation(async () => ({ bytes: vitalBytes.slice(0), cached: false }));
    await ensureCloudRange(from, to, signal, ["rum"]);
    expect(await getDuckDBEngine().executeSql("SELECT count(*) AS count FROM client_vitals")).toEqual([{ count: 600 }]);
    vi.mocked(fetchManifest).mockResolvedValueOnce({
      complete: true, files: Array.from({ length: 2049 }, (_, i) => ({ ...files[0], key: `processed/rum/client_vitals/x${i}.parquet` })),
    });
    await expect(ensureCloudRange(from, to, signal, ["rum"])).rejects.toBeInstanceOf(CloudBudgetError);
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

  it("shares >=65% projected whole-file fetches across column subsets via wholeKey while isolating scope, serviceId, etag, and size", async () => {
    const denseParquet = (tag: string, rows = 800) => parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: 400,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(Date.parse(from) + i)) },
        { name: "url", type: "STRING", data: Array.from({ length: rows }, (_, i) => `/${tag}/item-${i}-${"x".repeat(80)}`) },
        { name: "ua", type: "STRING", data: Array.from({ length: rows }, (_, i) => `Agent-${tag}-${i}-${"y".repeat(80)}`) },
        { name: "status", type: "INT32", data: Array.from({ length: rows }, () => 200) },
      ],
    });
    const bytesV1 = denseParquet("svc-a-v1");
    const bytesSvcB = denseParquet("svc-b-v1");
    const bytesV2 = denseParquet("svc-a-v2", 820);
    expect(bytesV1.byteLength).toBeGreaterThan(64 * 1024);

    const wholeCache = new Map<string, ArrayBuffer>();
    let networkWholeFetches = 0;
    let activeBytes = bytesV1;
    source.fetchRange.mockImplementation(async (_entry: ManifestFile, start: number, end: number) => activeBytes.slice(start, end));
    source.fetchFile.mockImplementation(async (_key: string, _sig?: AbortSignal, mode?: { identity?: string }) => {
      const id = mode?.identity ?? "";
      const hit = wholeCache.get(id);
      if (hit) return { bytes: hit.slice(0), cached: true };
      networkWholeFetches++;
      wholeCache.set(id, activeBytes.slice(0));
      return { bytes: activeBytes.slice(0), cached: false };
    });

    const entryV1 = { ...file, key: "processed/request/shared.parquet", etag: "\"etag-v1\"", size: bytesV1.byteLength };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entryV1], complete: true });

    // 1. Different column subsets (>=65% of file) under the SAME (scope, serviceId, dataset, key, etag, size) share 1 fetchFile call!
    const expectedWholeKeyA = JSON.stringify(["fixture-session:svc-a", "request", entryV1.key, "\"etag-v1\"", bytesV1.byteLength, ""]);
    const read1 = await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "svc-a",
      projection: { columns: ["timestamp", "url", "ua"] },
    });
    expect(source.withOptions).toHaveBeenCalledWith({ serviceId: "svc-a" });
    expect(source.fetchFile).toHaveBeenLastCalledWith(entryV1.key, expect.any(AbortSignal), { identity: expectedWholeKeyA });
    expect(networkWholeFetches).toBe(1);
    read1.release?.();

    const read2 = await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "svc-a",
      projection: { columns: ["timestamp", "url", "ua", "status"] },
    });
    expect(networkWholeFetches).toBe(1);
    expect((await read2.executeSql("SELECT min(url) AS u FROM requests"))[0]).toMatchObject({
      u: expect.stringContaining("/svc-a-v1/"),
    });
    read2.release?.();

    // 2. Switching serviceId (svc-a -> svc-b) must NOT reuse svc-a's whole-file cache!
    activeBytes = bytesSvcB;
    const entrySvcB = { ...file, key: entryV1.key, etag: "\"etag-v1\"", size: bytesSvcB.byteLength };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entrySvcB], complete: true });
    const readB = await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "svc-b",
      projection: { columns: ["timestamp", "url", "ua"] },
    });
    expect(source.withOptions).toHaveBeenCalledWith({ serviceId: "svc-b" });
    expect(networkWholeFetches).toBe(2);
    expect((await readB.executeSql("SELECT min(url) AS u FROM requests"))[0]).toMatchObject({
      u: expect.stringContaining("/svc-b-v1/"),
    });
    readB.release?.();

    // 3. Rotating dashboardCacheScope (logout / user switch) must NOT reuse previous identity's whole-file cache!
    scopeState.value = "bob-analyst-no-pii-session";
    activeBytes = bytesV1;
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entryV1], complete: true });
    const readBob = await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "svc-a",
      projection: { columns: ["timestamp", "url", "ua"] },
    });
    expect(networkWholeFetches).toBe(3);
    readBob.release?.();

    // 4. Rotating ETag / size must NOT reuse stale ETag whole-file bytes, and byteLength !== file.size rejects!
    activeBytes = bytesV2;
    const entryV2 = { ...file, key: entryV1.key, etag: "\"etag-v2\"", size: bytesV2.byteLength };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entryV2], complete: true });
    const readV2 = await ensureCloudRange(from, to, signal, ["request"], {
      serviceId: "svc-a",
      projection: { columns: ["timestamp", "url", "ua"] },
    });
    expect(networkWholeFetches).toBe(4);
    expect((await readV2.executeSql("SELECT count(*) AS n FROM requests"))[0]).toEqual({ n: 820 });
    readV2.release?.();

    source.fetchFile.mockResolvedValueOnce({ bytes: new ArrayBuffer(12), cached: false });
    vi.mocked(fetchManifest).mockResolvedValueOnce({
      files: [{ ...entryV2, key: "processed/request/truncated.parquet" }],
      complete: true,
    });
    await expect(
      ensureCloudRange(from, to, signal, ["request"], {
        serviceId: "svc-a",
        projection: { columns: ["timestamp", "url", "ua"] },
      }),
    ).rejects.toThrow(/Whole-file read .* returned 12 bytes, not/);
  });

  it("skips writeDerivedFile across footer, whole-file, and range paths when signal aborts in flight", async () => {
    const oneGroupBytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: 400,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 400 }, (_, i) => new Date(Date.parse(from) + i)) },
        { name: "status", type: "INT32", data: Array.from({ length: 400 }, () => 200) },
        { name: "ua", type: "STRING", data: Array.from({ length: 400 }, (_, i) => `/ua-${i}-${"u".repeat(700)}`) },
      ],
    });
    expect(oneGroupBytes.byteLength).toBeGreaterThan(256 * 1024);

    // 1. Footer read (line 191): aborting during footer fetchRange must skip writeDerivedFile(footerKey)
    const footerEntry = { ...file, key: "processed/request/abort-footer.parquet", size: oneGroupBytes.byteLength };
    vi.mocked(fetchManifest).mockResolvedValueOnce({ files: [footerEntry], complete: true });
    const footerAbort = new AbortController();
    source.writeDerivedFile.mockClear();
    source.fetchRange.mockImplementationOnce(async (_entry: ManifestFile, start: number, end: number) => {
      footerAbort.abort(new DOMException("Logged out during footer", "AbortError"));
      return oneGroupBytes.slice(start, end);
    });
    await expect(
      ensureCloudRange(from, to, footerAbort.signal, ["request"], {
        serviceId: "svc-a",
        projection: { columns: ["timestamp", "status"] },
      }),
    ).rejects.toThrow();
    expect(source.writeDerivedFile).not.toHaveBeenCalled();

    // 2. Whole-file >=65% projection (line 260): aborting during fetchFile must skip writeDerivedFile(scopedKey)
    const wholeEntry = { ...file, key: "processed/request/abort-whole.parquet", size: oneGroupBytes.byteLength };
    vi.mocked(fetchManifest).mockResolvedValueOnce({ files: [wholeEntry], complete: true });
    const wholeAbort = new AbortController();
    source.fetchRange.mockImplementation(async (_entry: ManifestFile, start: number, end: number) =>
      oneGroupBytes.slice(start, end),
    );
    source.fetchFile.mockImplementationOnce(async () => {
      wholeAbort.abort(new DOMException("Logged out during whole-file", "AbortError"));
      return { bytes: oneGroupBytes.slice(0), cached: false };
    });
    await expect(
      ensureCloudRange(from, to, wholeAbort.signal, ["request"], {
        serviceId: "svc-a",
        projection: { columns: ["timestamp", "status", "ua"] },
      }),
    ).rejects.toThrow();
    expect(
      source.writeDerivedFile.mock.calls.filter(([, id]) => String(id).includes("\"projection\"")),
    ).toEqual([]);
    source.writeDerivedFile.mockClear();

    // 3. <65% range projection (line 275): aborting during the final projected range read must skip writeDerivedFile(scopedKey)
    const rangeEntry = { ...file, key: "processed/request/abort-range.parquet", size: oneGroupBytes.byteLength };
    vi.mocked(fetchManifest).mockResolvedValueOnce({ files: [rangeEntry], complete: true });
    const rangeAbort = new AbortController();
    let rangeCallIdx = 0;
    source.fetchRange.mockImplementation(async (_entry: ManifestFile, start: number, end: number) => {
      rangeCallIdx += 1;
      // Call 1 is footer tail; Call 2 is the single coalesced ["timestamp", "status"] range!
      if (rangeCallIdx === 2) {
        rangeAbort.abort(new DOMException("Logged out during range", "AbortError"));
      }
      return oneGroupBytes.slice(start, end);
    });
    await expect(
      ensureCloudRange(from, to, rangeAbort.signal, ["request"], {
        serviceId: "svc-a",
        projection: { columns: ["timestamp", "status"] },
      }),
    ).rejects.toThrow();
    const projCalls = source.writeDerivedFile.mock.calls.filter(
      ([, identity]) => String(identity).includes("\"projection\""),
    );
    expect(projCalls).toEqual([]);
  });

  it("prunes out-of-window and non-matching row groups during projected scans without poisoning wider windows", async () => {
    const startMs = Date.parse(from);
    const rgRows = 500;
    // 4 row groups spanning 40 minutes (10m each), each with a large unneeded column so ranges stay distinct.
    const pad = "x".repeat(320);
    const timestamps = Array.from({ length: rgRows * 4 }, (_, i) => {
      const rg = Math.floor(i / rgRows);
      return new Date(startMs + rg * 600_000 + (i % rgRows) * 1000);
    });
    const pops = Array.from({ length: rgRows * 4 }, (_, i) => ["AMS", "IAD", "FRA", "SJC"][Math.floor(i / rgRows)]);
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: timestamps },
        { name: "pop", type: "STRING", data: pops },
        { name: "wide", type: "STRING", data: Array.from({ length: rgRows * 4 }, (_, i) => `${i}-${pad}`) },
      ],
    });
    // Open-hour file above 256 KiB so it uses footer-planned range reads.
    expect(bytes.byteLength).toBeGreaterThan(512 * 1024);
    const openEnd = new Date(Date.now() + 60_000).toISOString();
    const entry = { ...file, key: "processed/request/pruned-rgs.parquet", end: openEnd, size: bytes.byteLength };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entry], complete: true });

    let dataRangeCalls = 0;
    let dataRangeBytes = 0;
    source.fetchRange.mockImplementation(async (_entry: ManifestFile, start: number, end: number) => {
      const chunk = bytes.slice(start, end);
      if (end < bytes.byteLength) {
        dataRangeCalls += 1;
        dataRangeBytes += chunk.byteLength;
      }
      return chunk;
    });

    // 1. Filter pop="IAD" (only RG 1 matches): should fetch far fewer ranges/bytes and return only IAD rows.
    const filteredRead = await ensureCloudRange(from, to, signal, ["request"], {
      projection: {
        columns: ["timestamp", "pop"],
        filters: [{ field: "pop", mode: "include", values: ["IAD"] }],
      },
    });
    expect(await filteredRead.executeSql("SELECT pop, count(*)::INT AS n FROM requests GROUP BY pop"))
      .toEqual([{ pop: "IAD", n: rgRows }]);
    expect(dataRangeCalls).toBe(1);
    const filteredBytes = dataRangeBytes;
    filteredRead.release?.();

    // 2. Filter matching ZERO row groups skips data range reads completely and returns 0 rows.
    dataRangeCalls = 0;
    const emptyRead = await ensureCloudRange(from, to, signal, ["request"], {
      projection: {
        columns: ["timestamp", "pop"],
        filters: [{ field: "pop", mode: "include", values: ["NRT"] }],
      },
    });
    expect(dataRangeCalls).toBe(0);
    expect(await emptyRead.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: 0 }]);
    emptyRead.release?.();

    // 3. Unfiltered wider read on the same file must NOT reuse the pruned RG-1-only cache entry.
    dataRangeCalls = 0;
    dataRangeBytes = 0;
    const fullRead = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "pop"] },
    });
    expect(await fullRead.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: rgRows * 4 }]);
    expect(dataRangeBytes).toBeGreaterThan(filteredBytes * 2);
    fullRead.release?.();
  });

  it("falls back to a single whole-file fetch when a multi-row-group projection needs 8+ ranges covering >=60% of the file", async () => {
    // 12 row groups where wanted columns (`timestamp`, `url`, `status`) + gaps cover ~65-75% of the file
    // while each unneeded `ua` chunk exceeds baseGap + SOFT_MERGE_MAX_GAP, leaving >= 8 ranges.
    const rgRows = 500;
    const rowGroups = 12;
    const totalRows = rgRows * rowGroups;
    let seed = 7;
    const randStr = (len: number) => Array.from({ length: len }, () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return String.fromCharCode(33 + (seed % 90));
    }).join("");
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(Date.parse(from) + i * 100)) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => `/${randStr(380)}`) },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => randStr(220)) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
      ],
    });
    const entry = { ...file, key: "processed/request/assets-compact.parquet", size: bytes.byteLength };
    const plan = planParquetProjection(await readWholeParquetFooter(bytes), new Set(["timestamp", "url", "status"]));
    const plannedRangeBytes = plan.ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
    expect(plan.ranges.length).toBeGreaterThanOrEqual(8);
    expect(plannedRangeBytes / bytes.byteLength).toBeGreaterThanOrEqual(0.6);
    expect(plannedRangeBytes / bytes.byteLength).toBeLessThan(0.85);
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entry], complete: true });

    let dataRangeCalls = 0;
    source.fetchRange.mockImplementation(async (_f: ManifestFile, start: number, end: number) => {
      if (end < bytes.byteLength) dataRangeCalls++;
      return bytes.slice(start, end);
    });
    source.fetchFile.mockImplementation(async () => ({ bytes: bytes.slice(0), cached: false }));

    const read = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "url", "status"] },
    });
    expect(source.fetchFile).toHaveBeenCalledTimes(1);
    expect(dataRangeCalls).toBe(0);
    expect(await read.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: totalRows }]);
    read.release?.();
  });

  it("shares a single whole-file fetch across two-pass projections on high-fan-out multi-row-group compacts", async () => {
    // Reproduces /sessions Pass 1 (ranking columns) + Pass 2 (enrich ua/url) on an open-hour catalog1
    // compact with 16+ row groups: instead of 16+ HTTP 206s per pass, both passes share 1 fetchFile download.
    const rgRows = 500;
    const rowGroups = 20;
    const totalRows = rgRows * rowGroups;
    let seed = 19;
    const randStr = (len: number) => Array.from({ length: len }, () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return String.fromCharCode(33 + (seed % 90));
    }).join("");
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(Date.parse(from) + i * 100)) },
        { name: "ip", type: "STRING", data: Array.from({ length: totalRows }, (_, i) => `198.51.100.${i % 250}`) },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => randStr(260)) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => `/${randStr(260)}`) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
      ],
    });
    const openEnd = new Date(Date.now() + 60_000).toISOString();
    const entry = {
      ...file,
      key: "processed/request/year=2026/month=11/day=25/hour=07/catalog1/sessions-compact.parquet",
      end: openEnd,
      size: bytes.byteLength,
      etag: "\"etag-sess\"",
    };
    const pass1Plan = planParquetProjection(await readWholeParquetFooter(bytes), new Set(["timestamp", "ip", "status"]));
    expect(pass1Plan.ranges.length).toBeGreaterThanOrEqual(20);
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entry], complete: true });

    const wholeCache = new Map<string, ArrayBuffer>();
    let networkWholeFetches = 0;
    let dataRangeCalls = 0;
    source.fetchRange.mockImplementation(async (_f: ManifestFile, start: number, end: number) => {
      if (end < bytes.byteLength) dataRangeCalls++;
      return bytes.slice(start, end);
    });
    source.fetchFile.mockImplementation(async (_key: string, _sig?: AbortSignal, mode?: { identity?: string }) => {
      const id = mode?.identity ?? "";
      const hit = wholeCache.get(id);
      if (hit) return { bytes: hit.slice(0), cached: true };
      networkWholeFetches++;
      wholeCache.set(id, bytes.slice(0));
      return { bytes: bytes.slice(0), cached: false };
    });

    const pass1 = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "ip", "status"] },
    });
    expect(await pass1.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: totalRows }]);
    pass1.release?.();

    const pass2 = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "ip", "ua", "url"] },
    });
    expect(await pass2.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: totalRows }]);
    pass2.release?.();

    expect(networkWholeFetches).toBe(1);
    expect(dataRangeCalls).toBe(0);
  });

  it("reuses a cached whole file across a second projection pass with zero footer or range reads", async () => {
    const rgRows = 300;
    const rowGroups = 10;
    const totalRows = rgRows * rowGroups;
    let seed = 17;
    const randStr = (len: number) => Array.from({ length: len }, () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return String.fromCharCode(33 + (seed % 90));
    }).join("");
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(Date.parse(from) + i * 100)) },
        { name: "ip", type: "STRING", data: Array.from({ length: totalRows }, (_, i) => `198.51.100.${i % 200}`) },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => randStr(200)) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => `/${randStr(200)}`) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
      ],
    });
    const openEnd = new Date(Date.now() + 60_000).toISOString();
    const entry = {
      ...file,
      key: "processed/request/year=2026/month=11/day=25/hour=07/catalog1/sessions-10rg.parquet",
      end: openEnd,
      size: bytes.byteLength,
      etag: "\"etag-10rg\"",
    };
    vi.mocked(fetchManifest).mockResolvedValue({ files: [entry], complete: true });

    const wholeCache = new Map<string, ArrayBuffer>();
    let networkWholeFetches = 0;
    let totalRangeCalls = 0;
    source.fetchRange.mockImplementation(async (_f: ManifestFile, start: number, end: number) => {
      totalRangeCalls++;
      return bytes.slice(start, end);
    });
    source.fetchFile.mockImplementation(async (_key: string, _sig?: AbortSignal, mode?: { identity?: string }) => {
      const id = mode?.identity ?? "";
      const hit = wholeCache.get(id);
      if (hit) return { bytes: hit.slice(0), cached: true };
      networkWholeFetches++;
      wholeCache.set(id, bytes.slice(0));
      return { bytes: bytes.slice(0), cached: false };
    });
    (source as unknown as { hasFile?: (key: string, mode?: { identity?: string }) => Promise<boolean> }).hasFile =
      async (_key: string, mode?: { identity?: string }) => wholeCache.has(mode?.identity ?? "");

    const pass1 = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "ip", "status"] },
    });
    expect(await pass1.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: totalRows }]);
    pass1.release?.();
    const rangesAfterPass1 = totalRangeCalls;

    const pass2 = await ensureCloudRange(from, to, signal, ["request"], {
      projection: { columns: ["timestamp", "ip", "ua", "url"] },
    });
    expect(await pass2.executeSql("SELECT count(*)::INT AS n FROM requests")).toEqual([{ n: totalRows }]);
    pass2.release?.();

    expect(networkWholeFetches).toBe(1);
    expect(totalRangeCalls).toBe(rangesAfterPass1);
  });
});
