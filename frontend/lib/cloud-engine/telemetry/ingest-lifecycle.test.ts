// @vitest-environment node
import { describe, expect, it, beforeEach } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  clearOpsDecodedCache,
  OPS_FETCH_CONCURRENCY,
  readIngestLifecycle,
  readLogs,
  type OpsSource,
} from "./engine";
import type { ManifestFile } from "../dashboard/manifest";
import { fetchIngestDeletedIntervals } from "../usage/summary";

const DAY_MS = 86_400_000;
const BASE_DAY = Date.UTC(2026, 10, 10, 0, 0, 0); // 2026-11-10T00:00:00Z

interface RawIngestRow {
  timestamp: number;
  event_id: string;
  raw_key?: string | null;
  raw_size?: number | null;
  status?: string | null;
  state_version?: number | null;
  output_keys_json?: string | null;
  raw_deleted?: boolean | null;
  raw_deleted_at?: number | null;
}

function makeIngestParquet(rows: RawIngestRow[]): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      {
        name: "timestamp",
        type: "TIMESTAMP",
        data: rows.map((r) => new Date(r.timestamp)),
      },
      { name: "event_id", type: "STRING", data: rows.map((r) => r.event_id) },
      { name: "raw_key", type: "STRING", data: rows.map((r) => r.raw_key ?? null) },
      { name: "raw_size", type: "DOUBLE", data: rows.map((r) => r.raw_size ?? null) },
      { name: "status", type: "STRING", data: rows.map((r) => r.status ?? null) },
      {
        name: "state_version",
        type: "INT32",
        data: rows.map((r) => r.state_version ?? null),
      },
      {
        name: "output_keys_json",
        type: "STRING",
        data: rows.map((r) => r.output_keys_json ?? null),
      },
      {
        name: "raw_deleted",
        type: "BOOLEAN",
        data: rows.map((r) => r.raw_deleted ?? null),
      },
      {
        name: "raw_deleted_at",
        type: "DOUBLE",
        data: rows.map((r) => r.raw_deleted_at ?? null),
      },
    ],
    codec: "SNAPPY",
  });
}

function makeSource(opts: {
  files: Record<string, ArrayBuffer>;
  manifestFiles: ManifestFile[];
  liveRows?: RawIngestRow[];
  cacheScope?: string;
  onFetchFile?: (key: string) => Promise<void> | void;
}) {
  const fetchCalls: string[] = [];
  const src: OpsSource = {
    cacheScope: opts.cacheScope,
    async fetchJson(url) {
      if (url.includes("/live")) {
        return {
          rows: opts.liveRows ?? [],
          next_cursor: null,
          status: null,
        };
      }
      return {
        files: opts.manifestFiles,
        next_cursor: null,
        complete: true,
      };
    },
    async fetchFile(key) {
      fetchCalls.push(key);
      await opts.onFetchFile?.(key);
      const bytes = opts.files[key];
      if (!bytes) throw new Error(`Missing test file: ${key}`);
      return { bytes, cached: false, cdn: "HIT" };
    },
    async cachedKeys() {
      return [];
    },
    async evict() {},
  };
  return { src, fetchCalls };
}

describe("readIngestLifecycle", () => {
  beforeEach(() => {
    clearOpsDecodedCache();
  });

  it("matches row-based fetchIngestDeletedIntervals across compacts, fallback windows, live rows, and duplicate output_keys_json", async () => {
    const day10 = BASE_DAY;
    const day11 = BASE_DAY + DAY_MS;
    const sharedOutputJson = JSON.stringify([
      { key: "processed/logs/year=2026/month=11/day=10/part-a.parquet", bytes: 4096 },
      { key: "processed/logs/year=2026/month=11/day=10/part-b.parquet", bytes: 2048 },
    ]);
    const deletedJson = JSON.stringify([
      { key: "processed/logs/year=2026/month=11/day=10/part-b.parquet" },
    ]);

    const compactKey =
      "processed/ops/ingest_files/year=2026/month=11/day=10/compact=29900000-29900060-c1-n4.parquet";
    const badCompactKey =
      "processed/ops/ingest_files/year=2026/month=11/day=11/compact=29901440-29901500-c1-n99.parquet";
    const fallbackWinKey =
      "processed/ops/ingest_files/year=2026/month=11/day=11/hour=00/window=29901440-29901444-ab12-n2.parquet";

    const compactRows: RawIngestRow[] = [
      {
        timestamp: day10 + 1000,
        event_id: "e1",
        raw_key: "raw/a.gz",
        raw_size: 1000,
        status: "converted",
        state_version: 1,
        output_keys_json: sharedOutputJson,
        raw_deleted: false,
      },
      {
        timestamp: day10 + 2000,
        event_id: "e2",
        raw_key: "raw/b.gz",
        raw_size: 2000,
        status: "converted",
        state_version: 1,
        output_keys_json: sharedOutputJson,
        raw_deleted: false,
      },
      {
        timestamp: day10 + 3000,
        event_id: "e3",
        raw_key: "raw/a.gz",
        raw_size: 1000,
        status: "raw_deleted",
        state_version: 2,
        raw_deleted: true,
        raw_deleted_at: day10 + 3600_000,
      },
      {
        timestamp: day10 + 4000,
        event_id: "e4",
        status: "parquet_deleted",
        output_keys_json: deletedJson,
      },
    ];

    const fallbackRows: RawIngestRow[] = [
      {
        timestamp: day11 + 1000,
        event_id: "e5",
        raw_key: "raw/b.gz",
        raw_size: 2000,
        status: "raw_deleted",
        state_version: 2,
        raw_deleted: true,
        raw_deleted_at: day11 + 5000,
      },
      {
        timestamp: day11 + 2000,
        event_id: "e6",
        raw_key: "raw/only-deleted.gz",
        raw_size: 500,
        status: "raw_deleted",
        state_version: 1,
        raw_deleted: true,
        raw_deleted_at: day11 + 2000,
      },
    ];

    const liveRows: RawIngestRow[] = [
      {
        timestamp: day11 + 10_000,
        event_id: "e7",
        raw_key: "raw/c.gz",
        raw_size: 3000,
        status: "converted",
        state_version: 1,
        output_keys_json: JSON.stringify([
          { key: "processed/logs/year=2026/month=11/day=11/part-c.parquet", bytes: 8192 },
        ]),
        raw_deleted: false,
      },
      {
        timestamp: day11 + 12_000,
        event_id: "e8",
        raw_key: "raw/c.gz",
        raw_size: 3000,
        status: "raw_deleted",
        state_version: 2,
        raw_deleted: true,
        raw_deleted_at: day11 + 12_000,
      },
    ];

    const files = {
      [compactKey]: makeIngestParquet(compactRows),
      // Wrong row count (1 != n99) so compact is ignored and fallbackWinKey is read instead
      [badCompactKey]: makeIngestParquet([fallbackRows[0]]),
      [fallbackWinKey]: makeIngestParquet(fallbackRows),
    };
    const manifestFiles: ManifestFile[] = [
      { key: compactKey, start: "", end: "", size: 100, rows: 4 },
      { key: badCompactKey, start: "", end: "", size: 100, rows: 99 },
      { key: fallbackWinKey, start: "", end: "", size: 100, rows: 2 },
    ];

    const fromIso = new Date(day10).toISOString();
    const toIso = new Date(day11 + DAY_MS).toISOString();

    const { src: srcColumnar } = makeSource({
      files,
      manifestFiles,
      liveRows,
      cacheScope: "parity-scope",
    });
    const columnar = await readIngestLifecycle(srcColumnar, {
      from: fromIso,
      to: toIso,
      minimumBilledDays: 30,
    });

    const { src: srcRow } = makeSource({
      files,
      manifestFiles,
      liveRows,
    });
    const rowBased = await fetchIngestDeletedIntervals(
      { from: day10, to: day11 + DAY_MS },
      { user: "alice", role: "admin" },
      async (kind, range, options) =>
        readLogs(srcRow, {
          kind,
          from: range.from,
          to: range.to,
          columns: options?.columns,
          dedupe: options?.dedupe,
          sort: options?.sort,
        }),
      { minimumBilledDays: 30 },
    );

    const byId = <T extends { id: string }>(arr: T[]) =>
      [...arr].sort((a, b) => a.id.localeCompare(b.id));
    const byKey = <T extends { key: string }>(arr: T[]) =>
      [...arr].sort((a, b) => a.key.localeCompare(b.key));

    expect(byId(columnar.intervals)).toEqual(byId(rowBased.intervals));
    expect(byKey(columnar.outputs)).toEqual(byKey(rowBased.outputs));
    expect([...columnar.deletedParquetKeys].sort()).toEqual(
      [...(rowBased.deletedParquetKeys ?? [])].sort(),
    );
    expect(columnar.bytes).toBe(rowBased.bytes);
    expect(columnar.complete).toBe(rowBased.complete);
    expect(columnar.provenance).toBe(rowBased.provenance);
    expect(columnar.stats?.ignoredCompact).toBe(1);
  });

  it("caches per-file folds for interior files while re-filtering boundary files", async () => {
    const day10 = BASE_DAY;
    const day11 = BASE_DAY + DAY_MS;
    const day12 = BASE_DAY + 2 * DAY_MS;

    const interiorKey =
      "processed/ops/ingest_files/year=2026/month=11/day=11/compact=29901440-29901500-c1-n2.parquet";
    const boundaryKey =
      "processed/ops/ingest_files/year=2026/month=11/day=10/compact=29900000-29900060-c1-n2.parquet";

    const files = {
      [interiorKey]: makeIngestParquet([
        {
          timestamp: day11 + 1000,
          event_id: "i1",
          raw_key: "raw/interior.gz",
          raw_size: 500,
          status: "converted",
          state_version: 1,
          raw_deleted: false,
        },
        {
          timestamp: day11 + 2000,
          event_id: "i2",
          raw_key: "raw/interior.gz",
          raw_size: 500,
          status: "raw_deleted",
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: day11 + 2000,
        },
      ]),
      [boundaryKey]: makeIngestParquet([
        {
          timestamp: day10 + 1000,
          event_id: "b1",
          raw_key: "raw/early.gz",
          raw_size: 700,
          status: "raw_deleted",
          state_version: 1,
          raw_deleted: true,
          raw_deleted_at: day10 + 1000,
        },
        {
          timestamp: day10 + 12 * 3_600_000,
          event_id: "b2",
          raw_key: "raw/late.gz",
          raw_size: 800,
          status: "raw_deleted",
          state_version: 1,
          raw_deleted: true,
          raw_deleted_at: day10 + 12 * 3_600_000,
        },
      ]),
    };
    const manifestFiles: ManifestFile[] = [
      { key: interiorKey, start: "", end: "", size: 100, rows: 2 },
      { key: boundaryKey, start: "", end: "", size: 100, rows: 2 },
    ];

    const { src, fetchCalls } = makeSource({
      files,
      manifestFiles,
      cacheScope: "fold-cache-scope",
    });

    // First query: [day10 + 6h, day12) -> day11 is interior, day10 is boundary
    const first = await readIngestLifecycle(src, {
      from: new Date(day10 + 6 * 3_600_000).toISOString(),
      to: new Date(day12).toISOString(),
    });
    expect(fetchCalls).toEqual([interiorKey, boundaryKey]);
    expect(first.intervals.map((i) => i.id).sort()).toEqual([
      "raw_request:raw/interior.gz",
      "raw_request:raw/late.gz",
    ]);

    // Second query shifts `from` inside day10 -> interiorKey hits fold cache (no fetch), boundaryKey is re-read
    const second = await readIngestLifecycle(src, {
      from: new Date(day10 + 18 * 3_600_000).toISOString(),
      to: new Date(day12).toISOString(),
    });
    expect(fetchCalls).toEqual([interiorKey, boundaryKey, boundaryKey]);
    expect(second.intervals.map((i) => i.id)).toEqual([
      "raw_request:raw/interior.gz",
    ]);
  });

  it("fetches ops files up to OPS_FETCH_CONCURRENCY (32) in parallel", async () => {
    expect(OPS_FETCH_CONCURRENCY).toBe(32);
    const totalFiles = 40;
    const files: Record<string, ArrayBuffer> = {};
    const manifestFiles: ManifestFile[] = [];
    const sampleBuf = makeIngestParquet([
      {
        timestamp: BASE_DAY + 1000,
        event_id: "c1",
        raw_key: "raw/k.gz",
        raw_size: 10,
        status: "converted",
        state_version: 1,
        raw_deleted: false,
      },
    ]);
    for (let i = 0; i < totalFiles; i++) {
      const key = `processed/ops/ingest_files/year=2026/month=11/day=10/hour=00/window=${29900000 + i * 5}-${29900004 + i * 5}-ab${String(i).padStart(2, "0")}-n1.parquet`;
      files[key] = sampleBuf;
      manifestFiles.push({ key, start: "", end: "", size: 100, rows: 1 });
    }

    let inFlight = 0;
    let maxInFlight = 0;
    const { src } = makeSource({
      files,
      manifestFiles,
      async onFetchFile() {
        inFlight += 1;
        if (inFlight > maxInFlight) maxInFlight = inFlight;
        await new Promise((r) => setTimeout(r, 15));
        inFlight -= 1;
      },
    });

    await readIngestLifecycle(src, {
      from: new Date(BASE_DAY).toISOString(),
      to: new Date(BASE_DAY + DAY_MS).toISOString(),
    });
    expect(maxInFlight).toBe(32);
  });
});
