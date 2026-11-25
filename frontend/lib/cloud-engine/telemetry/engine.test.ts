// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  clearOpsDecodedCache,
  fnv1a32,
  OPS_PROJECTED_MIN_BYTES,
  OpsRangeUnsupportedError,
  readIngestLifecycle,
  readLogs,
  type OpsSource,
} from "./engine";
import type { ManifestFile } from "../dashboard/manifest";
import type { RunProgress } from "../dashboard/engine";

const DAY_MS = Date.parse("2026-10-29T00:00:00Z");
const minute = (hour: number, extra = 0) => DAY_MS / 60_000 + hour * 60 + extra;
const DIR = "processed/ops/fos_usage/year=2026/month=10/day=29/";
const windowKey = (hour: number, start: number, n: number, txn = "ab12") =>
  `${DIR}hour=${String(hour).padStart(2, "0")}/window=${start}-${start + 4}-${txn}-n${n}.parquet`;
const compactKey = (a: number, b: number, c: number, n: number) =>
  `${DIR}compact=${a}-${b}-c${c}-n${n}.parquet`;

type Row = {
  timestamp: number;
  event_id: string;
  op?: string;
  snapshot_id?: string;
  complete?: boolean;
};
const row = (offsetMin: number, id: string): Row => ({
  timestamp: DAY_MS + 10 * 3_600_000 + offsetMin * 60_000,
  event_id: id,
  op: "GET",
});

function parquet(rows: Row[]) {
  return parquetWriteBuffer({
    columnData: [
      {
        name: "timestamp",
        type: "TIMESTAMP",
        data: rows.map((r) => new Date(r.timestamp)),
      },
      { name: "event_id", type: "STRING", data: rows.map((r) => r.event_id) },
      { name: "op", type: "STRING", data: rows.map((r) => r.op ?? null) },
    ],
    codec: "SNAPPY",
  });
}

const entry = (key: string, rows: number): ManifestFile => ({
  key,
  start: "2026-10-29T10:00:00Z",
  end: "2026-10-29T11:00:00Z",
  size: 1,
  rows,
});

function source(opts: {
  live?: { rows: Row[]; next_cursor: string | null }[];
  manifest?: { files: ManifestFile[]; next_cursor: string | null }[];
  files: Record<string, ArrayBuffer>;
  cached?: string[];
}) {
  const log = {
    json: [] as string[],
    files: [] as string[],
    evicted: [] as string[],
  };
  const src: OpsSource = {
    async fetchJson(url) {
      log.json.push(url);
      const params = new URL(url, "http://x").searchParams;
      const cursor = params.get("cursor");
      const parsedCursor = cursor !== null ? Number(cursor) : 0;
      const index = Number.isFinite(parsedCursor) ? parsedCursor : 0;
      if (url.startsWith("/api/logs/")) return opts.live![index];
      return { ...opts.manifest![index], complete: true };
    },
    async fetchFile(key) {
      log.files.push(key);
      return { bytes: opts.files[key], cached: false, cdn: "MISS" };
    },
    async cachedKeys() {
      return opts.cached ?? [];
    },
    async evict(key) {
      log.evicted.push(key);
    },
  };
  return { src, log };
}

const range = {
  from: "2026-10-29T00:00:00Z",
  to: "2026-10-30T00:00:00Z",
};
const noLive = [{ rows: [], next_cursor: null }];

describe("readLogs", () => {
  it("reports actual downloaded and cached file work before aggregation", async () => {
    const fetched = "processed/ops/fos_usage/download.parquet";
    const cached = "processed/ops/fos_usage/cached.parquet";
    const downloadedBytes = await parquet([row(1, "download")]);
    const cachedBytes = await parquet([row(2, "cached")]);
    const { src } = source({
      live: noLive,
      manifest: [{ files: [entry(fetched, 1), entry(cached, 1)], next_cursor: null }],
      files: {}, cached: [cached],
    });
    src.fetchFile = async (key) => ({
      bytes: key === cached ? cachedBytes : downloadedBytes,
      cached: key === cached,
    });
    const updates: RunProgress[] = [];
    await readLogs(src, { kind: "fos_usage", ...range, onProgress: (progress) => updates.push(progress) });
    expect(updates.some((p) => p.phase === "files" && p.done === 1)).toBe(true);
    expect(updates.at(-1)).toMatchObject({
      phase: "aggregate", toFetch: 1, toDecode: 1, done: 2, cached: 1,
      downloadedBytes: downloadedBytes.byteLength, cachedBytes: cachedBytes.byteLength,
    });
  });

  it("merges paginated live rows and manifest files, dedups on event_id, newest first", async () => {
    const k1 = windowKey(10, minute(10), 2);
    const { src, log } = source({
      live: [
        { rows: [row(9, "e9"), row(5, "e5")], next_cursor: "1" },
        { rows: [row(1, "e1")], next_cursor: null },
      ],
      manifest: [
        { files: [entry(k1, 2)], next_cursor: "1" },
        { files: [], next_cursor: null },
      ],
      files: { [k1]: await parquet([row(5, "e5"), row(2, "e2")]) },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(result.rows.map((r) => r.event_id)).toEqual([
      "e9",
      "e5",
      "e2",
      "e1",
    ]);
    expect(result.rows[0]).toMatchObject({ op: "GET" });
    expect(typeof result.rows[0].timestamp).toBe("number");
    const liveUrls = log.json.filter((u) =>
      u.startsWith("/api/logs/fos_usage/live"),
    );
    expect(liveUrls).toHaveLength(2);
    expect(liveUrls[1]).toContain("cursor=1");
    const manifestUrls = log.json.filter((u) => u.startsWith("/api/files"));
    expect(manifestUrls).toHaveLength(2);
    expect(manifestUrls[0]).toContain("dataset=ops%3Afos_usage");
    expect(result.stats).toMatchObject({ listed: 1, read: 1, liveRows: 3 });
    expect(result.liveTruncated).toBe(false);
  });

  it("propagates liveTruncated when the live endpoint reports truncation", async () => {
    const { src } = source({
      live: [{ rows: [row(9, "e9")], next_cursor: null } as unknown as {
        rows: Row[];
        next_cursor: string | null;
      }],
      manifest: [{ files: [], next_cursor: null }],
      files: {},
    });
    const origFetchJson = src.fetchJson.bind(src);
    src.fetchJson = async <T>(url: string): Promise<T> => {
      const res = (await origFetchJson(url)) as Record<string, unknown>;
      if (url.startsWith("/api/logs/")) {
        return { ...res, truncated: true } as unknown as T;
      }
      return res as unknown as T;
    };
    const result = await readLogs(src, "fos_usage", range);
    expect(result.liveTruncated).toBe(true);
  });

  it("pages outbox kinds through the source cursor instead of the budget-truncated newest-first read", async () => {
    // The live server: the default read drops older outbox sources at its backend-call
    // budget (truncated), whatever the range; `source-v1` pages every source to a terminal page.
    const older = [row(1, "c1"), row(2, "c2")];
    const newer = [row(8, "c8"), row(9, "c9")];
    const urls: string[] = [];
    const { src } = source({ manifest: [{ files: [], next_cursor: null }], files: {} });
    const manifest = src.fetchJson.bind(src);
    src.fetchJson = async <T,>(url: string): Promise<T> => {
      if (!url.startsWith("/api/logs/")) return (await manifest(url)) as T;
      urls.push(url);
      const cursor = new URL(url, "http://x").searchParams.get("cursor");
      const page = cursor === "source-v1"
        ? { rows: older, next_cursor: "source-v1:00ff", truncated: false }
        : cursor === "source-v1:00ff"
          ? { rows: newer, next_cursor: null, truncated: false }
          : { rows: newer, next_cursor: null, truncated: true };
      return page as T;
    };
    const result = await readLogs(src, "cron_runs", range);
    expect(result.liveTruncated).toBe(false);
    expect(result.rows.map((r) => r.event_id)).toEqual(["c9", "c8", "c2", "c1"]);
    expect(urls).toHaveLength(2);
    expect(urls[0]).toContain("cursor=source-v1");
  });

  it("restarts the source walk from the first page when a source changed under it", async () => {
    // The server answers 409 when an outbox record a cursor was mid-way through was
    // rewritten; the walk starts over instead of failing the whole live view.
    const urls: string[] = [];
    const { src } = source({ manifest: [{ files: [], next_cursor: null }], files: {} });
    const manifest = src.fetchJson.bind(src);
    let attempt = 0;
    src.fetchJson = async <T,>(url: string): Promise<T> => {
      if (!url.startsWith("/api/logs/")) return (await manifest(url)) as T;
      urls.push(url);
      const cursor = new URL(url, "http://x").searchParams.get("cursor");
      if (cursor === "source-v1") {
        attempt += 1;
        return { rows: [row(1, `c1-${attempt}`)], next_cursor: "source-v1:00ff", truncated: false } as T;
      }
      if (attempt === 1) throw new Error("Live log source changed. (409)");
      return { rows: [row(9, "c9")], next_cursor: null, truncated: false } as T;
    };
    const result = await readLogs(src, "cron_runs", range);
    expect(result.rows.map((r) => r.event_id)).toEqual(["c9", "c1-2"]);
    expect(urls).toHaveLength(4);
  });

  it("skips only windows covered by a valid compact file and reads late windows", async () => {
    const a = minute(10);
    const b = minute(10, 14);
    const covered1 = windowKey(10, a, 1);
    const covered2 = windowKey(10, a + 5, 1);
    const late = windowKey(10, b + 1, 1);
    const compact = compactKey(a, b, 2, 2);
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            entry(covered1, 1),
            entry(covered2, 1),
            entry(late, 1),
            entry(compact, 2),
          ],
          next_cursor: null,
        },
      ],
      files: {
        [compact]: await parquet([row(1, "a"), row(6, "b")]),
        [late]: await parquet([row(20, "late")]),
      },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect([...log.files].sort()).toEqual([compact, late].sort());
    expect(result.rows.map((r) => r.event_id)).toEqual(["late", "b", "a"]);
    expect(result.stats.skippedCovered).toBe(2);
  });

  it("ignores a compact file whose decoded rows differ from n{rows}", async () => {
    const a = minute(10);
    const b = minute(10, 9);
    const w1 = windowKey(10, a, 1);
    const w2 = windowKey(10, a + 5, 1);
    const compact = compactKey(a, b, 2, 5);
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [entry(w1, 1), entry(w2, 1), entry(compact, 5)],
          next_cursor: null,
        },
      ],
      files: {
        [compact]: await parquet([row(1, "a")]),
        [w1]: await parquet([row(1, "a")]),
        [w2]: await parquet([row(6, "b")]),
      },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(log.files).toContain(w1);
    expect(log.files).toContain(w2);
    expect(result.rows.map((r) => r.event_id)).toEqual(["b", "a"]);
    expect(result.stats.ignoredCompact).toBe(1);
  });

  it("ignores a compact file whose listed window count differs from c{nsrc}", async () => {
    const a = minute(10);
    const b = minute(10, 9);
    const w1 = windowKey(10, a, 1);
    const compact = compactKey(a, b, 2, 1);
    const { src, log } = source({
      live: noLive,
      manifest: [
        { files: [entry(w1, 1), entry(compact, 1)], next_cursor: null },
      ],
      files: {
        [w1]: await parquet([row(1, "a")]),
        [compact]: await parquet([row(1, "a")]),
      },
    });
    await readLogs(src, "fos_usage", range);
    expect(log.files).toEqual([w1]);
  });

  it("evicts cached files in range that are missing from the manifest", async () => {
    const live = windowKey(10, minute(10), 1);
    const gone = windowKey(10, minute(10, 30), 1);
    const otherDay = gone.replace("day=29", "day=01");
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(live, 1)], next_cursor: null }],
      files: { [live]: await parquet([row(1, "a")]) },
      cached: [live, gone, otherDay],
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(log.evicted).toEqual([gone]);
    expect(result.stats.evicted).toBe(1);
  });

  it("never evicts cached files that belong to another ops kind", async () => {
    const live = windowKey(10, minute(10), 1);
    const mine = windowKey(10, minute(10, 30), 1);
    const other = mine.replace("ops/fos_usage/", "ops/api_telemetry/");
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(live, 1)], next_cursor: null }],
      files: { [live]: await parquet([row(1, "a")]) },
      cached: [live, mine, other],
    });
    await readLogs(src, "fos_usage", range);
    expect(log.evicted).toEqual([mine]);
  });

  it("reuses decoded Parquet rows across reads when cacheScope is set and clears on clearOpsDecodedCache", async () => {
    clearOpsDecodedCache();
    const key = windowKey(10, minute(10), 2);
    const buf = await parquet([row(1, "a"), row(5, "b")]);
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(key, 2)], next_cursor: null }],
      files: { [key]: buf },
    });
    src.cacheScope = "epoch-1:svc:admin";

    const r1 = await readLogs(src, "fos_usage", range);
    const r2 = await readLogs(src, "fos_usage", range);
    expect(r1.rows.map((r) => r.event_id)).toEqual(["b", "a"]);
    expect(r2.rows.map((r) => r.event_id)).toEqual(["b", "a"]);
    expect(log.files).toEqual([key]);

    clearOpsDecodedCache();
    await readLogs(src, "fos_usage", range);
    expect(log.files).toEqual([key, key]);
  });
});

describe("tagged compact files (Rust parity)", () => {
  const a = minute(10);
  const t1 = "c0f408fa";
  const t2 = "ce64ab10";
  const tagged = (b: number, n: number, tags: string, c = 2) =>
    `${DIR}compact=${a}-${b}-c${c}-n${n}-${tags}.parquet`;

  it("hashes file names with FNV-1a 32", () => {
    expect(fnv1a32("a")).toBe(0xe40c292c);
    expect(fnv1a32("foobar")).toBe(0xbf9cf968);
    expect(fnv1a32(`window=${a}-${a + 4}-ab12-n1.parquet`)).toBe(0xc0f408fa);
  });

  it("covers exactly the tagged windows even when the range spans others", async () => {
    const w1 = windowKey(10, a, 1);
    const w2 = windowKey(10, a + 5, 1);
    const late = windowKey(10, a + 15, 1);
    const compact = tagged(a + 30, 2, t1 + t2);
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [entry(w1, 1), entry(w2, 1), entry(late, 1), entry(compact, 2)],
          next_cursor: null,
        },
      ],
      files: {
        [compact]: await parquet([row(1, "a"), row(6, "b")]),
        [late]: await parquet([row(20, "late")]),
      },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect([...log.files].sort()).toEqual([compact, late].sort());
    expect(result.stats.skippedCovered).toBe(2);
  });

  it("stays valid when a tagged source window is no longer listed", async () => {
    const w1 = windowKey(10, a, 1);
    const compact = tagged(a + 9, 2, t1 + t2);
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(w1, 1), entry(compact, 2)], next_cursor: null }],
      files: { [compact]: await parquet([row(1, "a"), row(6, "b")]) },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(log.files).toEqual([compact]);
    expect(result.stats.ignoredCompact).toBe(0);
  });

  it("ignores a compact whose tags are malformed and reads the windows", async () => {
    const w1 = windowKey(10, a, 1);
    const bad = tagged(a + 9, 1, t1, 2);
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(w1, 1), entry(bad, 1)], next_cursor: null }],
      files: { [w1]: await parquet([row(1, "a")]) },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(log.files).toEqual([w1]);
    expect(result.stats.ignoredCompact).toBe(1);
  });

  it("evicts a cached tagged compact that is missing from the manifest", async () => {
    const live = windowKey(10, a, 1);
    const gone = tagged(a + 9, 1, t1, 1);
    const { src, log } = source({
      live: noLive,
      manifest: [{ files: [entry(live, 1)], next_cursor: null }],
      files: { [live]: await parquet([row(1, "a")]) },
      cached: [gone],
    });
    await readLogs(src, "fos_usage", range);
    expect(log.evicted).toEqual([gone]);
  });

  it("stops downloading older storage_inventory snapshots once the latest complete snapshot is covered", async () => {
    const baseMs = Date.parse("2026-10-30T00:00:00.000Z");
    const snap1StartSec = Math.floor((baseMs + 2 * 3_600_000) / 1000);
    const snap2StartSec = Math.floor((baseMs + 8 * 3_600_000) / 1000);
    const snap3StartSec = Math.floor((baseMs + 14 * 3_600_000) / 1000);
    const invKey = (hour: number, min: number) =>
      `processed/ops/storage_inventory/year=2026/month=10/day=30/hour=${String(hour).padStart(2, "0")}/window=${Math.floor((baseMs + hour * 3_600_000 + min * 60_000) / 60_000)}-${Math.floor((baseMs + hour * 3_600_000 + (min + 4) * 60_000) / 60_000)}-ab12-n2.parquet`;
    const k1 = invKey(2, 0);
    const k2 = invKey(8, 0);
    const k3a = invKey(14, 0);
    const k3b = invKey(14, 5);
    const invEntry = (key: string, startMs: number, endMs: number) => ({
      key,
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
      size: 100,
      rows: 2,
    });
    const invRange = {
      from: "2026-10-30T00:00:00.000Z",
      to: "2026-10-30T23:59:59.000Z",
    };
    const inventoryParquet = (
      rows: {
        timestamp: number;
        event_id: string;
        snapshot_id: string;
        complete: boolean;
      }[],
    ) =>
      parquetWriteBuffer({
        columnData: [
          {
            name: "timestamp",
            type: "TIMESTAMP",
            data: rows.map((r) => new Date(r.timestamp)),
          },
          { name: "event_id", type: "STRING", data: rows.map((r) => r.event_id) },
          { name: "snapshot_id", type: "STRING", data: rows.map((r) => r.snapshot_id) },
          { name: "complete", type: "BOOLEAN", data: rows.map((r) => r.complete) },
        ],
        codec: "SNAPPY",
      });
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            invEntry(k1, baseMs + 2 * 3_600_000, baseMs + 2 * 3_600_000 + 240_000),
            invEntry(k2, baseMs + 8 * 3_600_000, baseMs + 8 * 3_600_000 + 240_000),
            invEntry(k3a, baseMs + 14 * 3_600_000, baseMs + 14 * 3_600_000 + 240_000),
            invEntry(k3b, baseMs + 14 * 3_600_000 + 300_000, baseMs + 14 * 3_600_000 + 540_000),
          ],
          next_cursor: null,
        },
      ],
      files: {
        [k1]: inventoryParquet([
          { event_id: "s1-obj", timestamp: snap1StartSec * 1000, snapshot_id: `snap-${snap1StartSec}-aaaa`, complete: false },
          { event_id: "s1-done", timestamp: snap1StartSec * 1000 + 1000, snapshot_id: `snap-${snap1StartSec}-aaaa`, complete: true },
        ]),
        [k2]: inventoryParquet([
          { event_id: "s2-obj", timestamp: snap2StartSec * 1000, snapshot_id: `snap-${snap2StartSec}-bbbb`, complete: false },
          { event_id: "s2-done", timestamp: snap2StartSec * 1000 + 1000, snapshot_id: `snap-${snap2StartSec}-bbbb`, complete: true },
        ]),
        [k3a]: inventoryParquet([
          { event_id: "s3-obj1", timestamp: snap3StartSec * 1000, snapshot_id: `snap-${snap3StartSec}-cccc`, complete: false },
        ]),
        [k3b]: inventoryParquet([
          { event_id: "s3-obj2", timestamp: snap3StartSec * 1000 + 300_000, snapshot_id: `snap-${snap3StartSec}-cccc`, complete: false },
          { event_id: "s3-done", timestamp: snap3StartSec * 1000 + 301_000, snapshot_id: `snap-${snap3StartSec}-cccc`, complete: true },
        ]),
      },
    });

    const result = await readLogs(src, {
      kind: "storage_inventory",
      ...invRange,
      latestCompleteSnapshotOnly: true,
    });
    // Must read k3b (newest) and k3a (overlaps snap3's requested_at), and skip k1 and k2!
    expect([...log.files].sort()).toEqual([k3a, k3b].sort());
    expect(result.rows.map((r) => r.event_id).sort()).toEqual(["s3-done", "s3-obj1", "s3-obj2"]);
  });

  it("skips all storage_inventory parquet files when live already contains the latest complete snapshot", async () => {
    const baseMs = Date.parse("2026-10-30T00:00:00.000Z");
    const snap1StartSec = Math.floor((baseMs + 2 * 3_600_000) / 1000);
    const snapLiveStartSec = Math.floor((baseMs + 14 * 3_600_000) / 1000);
    const k1 = `processed/ops/storage_inventory/year=2026/month=10/day=30/hour=02/window=${Math.floor(snap1StartSec / 60)}-${Math.floor(snap1StartSec / 60) + 4}-ab12-n2.parquet`;
    const { src, log } = source({
      live: [
        {
          rows: [
            { event_id: "live-obj", timestamp: snapLiveStartSec * 1000, snapshot_id: `snap-${snapLiveStartSec}-live`, complete: false },
            { event_id: "live-done", timestamp: snapLiveStartSec * 1000 + 1000, snapshot_id: `snap-${snapLiveStartSec}-live`, complete: true },
          ],
          next_cursor: null,
        },
      ],
      manifest: [
        {
          files: [
            {
              key: k1,
              start: new Date(baseMs + 2 * 3_600_000).toISOString(),
              end: new Date(baseMs + 2 * 3_600_000 + 240_000).toISOString(),
              size: 100,
              rows: 2,
            },
          ],
          next_cursor: null,
        },
      ],
      files: {},
    });

    const result = await readLogs(src, {
      kind: "storage_inventory",
      from: "2026-10-30T00:00:00.000Z",
      to: "2026-10-30T23:59:59.000Z",
      latestCompleteSnapshotOnly: true,
    });
    expect(log.files).toEqual([]);
    expect(result.rows.map((r) => r.event_id).sort()).toEqual(["live-done", "live-obj"]);
  });

  it("fetches uncovered window and plain files concurrently with valid compacts in one wave", async () => {
    const w1 = windowKey(10, a, 1);
    const w2 = windowKey(10, a + 5, 1);
    const late = windowKey(10, a + 15, 1);
    const plain = "processed/ops/fos_usage/plain.parquet";
    const compact = tagged(a + 30, 2, t1 + t2);

    const compactBytes = await parquet([row(1, "a"), row(6, "b")]);
    const lateBytes = await parquet([row(20, "late")]);
    const plainBytes = await parquet([row(25, "plain")]);

    let releaseCompact!: () => void;
    const compactGate = new Promise<void>((resolve) => {
      releaseCompact = resolve;
    });
    const started = new Set<string>();
    const { src } = source({
      live: noLive,
      manifest: [
        {
          files: [
            entry(w1, 1),
            entry(w2, 1),
            entry(late, 1),
            entry(plain, 1),
            entry(compact, 2),
          ],
          next_cursor: null,
        },
      ],
      files: {},
    });
    src.fetchFile = async (key) => {
      started.add(key);
      if (key === compact) {
        await compactGate;
        return { bytes: compactBytes, cached: false, cdn: "MISS" };
      }
      if (key === late) return { bytes: lateBytes, cached: false, cdn: "MISS" };
      if (key === plain) return { bytes: plainBytes, cached: false, cdn: "MISS" };
      throw new Error(`Unexpected fetch of covered window: ${key}`);
    };

    const pending = readLogs(src, "fos_usage", range);
    // Allow microtasks/manifest fetch to dispatch the first wave.
    for (let i = 0; i < 10 && !started.has(late); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    const startedBeforeCompactResolved = new Set(started);
    releaseCompact();
    const result = await pending;

    expect(startedBeforeCompactResolved.has(compact)).toBe(true);
    expect(startedBeforeCompactResolved.has(late)).toBe(true);
    expect(startedBeforeCompactResolved.has(plain)).toBe(true);
    expect(started.has(w1)).toBe(false);
    expect(started.has(w2)).toBe(false);
    expect(result.stats.skippedCovered).toBe(2);
    expect(result.rows.map((r) => r.event_id).sort()).toEqual(["a", "b", "late", "plain"]);
  });

  it("falls back to reading covered windows when a compact fails its row-count check", async () => {
    const w1 = windowKey(10, a, 1);
    const w2 = windowKey(10, a + 5, 1);
    const late = windowKey(10, a + 15, 1);
    // Claims n2 rows in key, but parquet actually only has 1 row -> must fall back to w1 + w2.
    const badCompact = tagged(a + 30, 2, t1 + t2);
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [entry(w1, 1), entry(w2, 1), entry(late, 1), entry(badCompact, 2)],
          next_cursor: null,
        },
      ],
      files: {
        [badCompact]: await parquet([row(1, "truncated")]),
        [w1]: await parquet([row(1, "w1")]),
        [w2]: await parquet([row(6, "w2")]),
        [late]: await parquet([row(20, "late")]),
      },
    });
    const result = await readLogs(src, "fos_usage", range);
    expect(result.stats.ignoredCompact).toBe(1);
    expect(result.stats.skippedCovered).toBe(0);
    expect([...log.files].sort()).toEqual([badCompact, late, w1, w2].sort());
    expect(result.rows.map((r) => r.event_id).sort()).toEqual(["late", "w1", "w2"]);
  });

  it("uses projected Range reads for large uncached ops Parquet files and skips full fetchFile", async () => {
    clearOpsDecodedCache();
    const rowCount = 16_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const bigParquet = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "event_id",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `evt-${String(i).padStart(6, "0")}-${(i * 2654435761).toString(16)}-${(i * 1597334677).toString(16)}`,
          ),
        },
        {
          name: "run_id",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `run-${String(i).padStart(6, "0")}-${(i * 1597334677).toString(16)}-${(i * 2246822519).toString(16)}`,
          ),
        },
        {
          name: "raw_key",
          type: "STRING",
          data: Array.from({ length: rowCount }, (_, i) => `raw/f-${i % 20}.log.gz`),
        },
        {
          name: "raw_size",
          type: "INT64",
          data: Array.from({ length: rowCount }, (_, i) => BigInt(1024 + (i % 10))),
        },
      ],
      codec: "SNAPPY",
    });
    expect(bigParquet.byteLength).toBeGreaterThan(OPS_PROJECTED_MIN_BYTES);

    const key = "processed/ops/ingest_files/year=2026/month=10/day=29/compact=100-200-c2-n16000.parquet";
    const rangeCalls: Array<{ start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: bigParquet.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: bigParquet },
    });
    src.fetchRange = async (_file, start, end) => {
      rangeCalls.push({ start, end });
      return { bytes: bigParquet.slice(start, end), cached: false, cdn: "HIT" };
    };

    const result = await readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "raw_key", "raw_size"],
      dedupe: false,
      sort: false,
    });

    expect(log.files).toEqual([]);
    expect(rangeCalls.length).toBeGreaterThan(0);
    const rangeBytes = rangeCalls.reduce((sum, r) => sum + (r.end - r.start), 0);
    expect(rangeBytes).toBeLessThan(bigParquet.byteLength * 0.65);
    expect(result.rows).toHaveLength(rowCount);
    expect(result.rows[0]).toMatchObject({
      timestamp: baseTs,
      raw_key: "raw/f-0.log.gz",
      raw_size: 1024,
    });
    expect(result.rows[0].event_id).toBeUndefined();
    expect(result.rows[0].run_id).toBeUndefined();
    expect(result.stats.read).toBe(1);
    expect(result.stats.cdnHits).toBe(1);
  });

  it("falls back to fetchFile when fetchRange throws OpsRangeUnsupportedError", async () => {
    clearOpsDecodedCache();
    const rowCount = 16_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const bigParquet = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "event_id",
          type: "STRING",
          data: Array.from({ length: rowCount }, (_, i) => `evt-${i}-${(i * 9973).toString(16)}-${(i * 2654435761).toString(16)}`),
        },
        {
          name: "raw_key",
          type: "STRING",
          data: Array.from({ length: rowCount }, (_, i) => `raw/f-${i}.log.gz`),
        },
      ],
      codec: "SNAPPY",
    });
    expect(bigParquet.byteLength).toBeGreaterThan(OPS_PROJECTED_MIN_BYTES);

    const key = "processed/ops/ingest_files/fallback.parquet";
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: bigParquet.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: bigParquet },
    });
    src.fetchRange = async () => {
      throw new OpsRangeUnsupportedError("server returned 200");
    };

    const result = await readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "raw_key"],
      dedupe: false,
      sort: false,
    });

    expect(log.files).toEqual([key]);
    expect(result.rows).toHaveLength(rowCount);
    expect(result.rows[0].raw_key).toBe("raw/f-0.log.gz");
  });

  it("isolates projected decoded cache entries from full reads while letting full reads serve projected reads", async () => {
    clearOpsDecodedCache();
    const key = "processed/ops/fos_usage/proj-cache.parquet";
    const bytes = parquet([row(1, "e1"), row(2, "e2")]);
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [{ ...entry(key, 2), size: bytes.byteLength }],
          next_cursor: null,
        },
      ],
      files: { [key]: bytes },
    });
    src.cacheScope = "scope-proj-test";

    // 1. Projected read populates projected cache entry only.
    const proj1 = await readLogs(src, {
      kind: "fos_usage",
      ...range,
      columns: ["timestamp"],
      dedupe: false,
      sort: false,
    });
    expect(log.files).toEqual([key]);
    expect(proj1.rows[0].op).toBeUndefined();

    // 2. Subsequent full read must NOT reuse the projected entry (must fetch & decode all columns).
    const full = await readLogs(src, {
      kind: "fos_usage",
      ...range,
    });
    expect(log.files).toEqual([key, key]);
    expect(full.rows[0].op).toBe("GET");
    expect(full.rows[0].event_id).toBe("e2");

    // 3. Subsequent projected read with different columns reuses the full "*" decoded entry (0 extra fetches).
    const proj2 = await readLogs(src, {
      kind: "fos_usage",
      ...range,
      columns: ["timestamp", "op"],
      dedupe: false,
      sort: false,
    });
    expect(log.files).toEqual([key, key]);
    expect(proj2.rows[0].op).toBe("GET");
  });

  it("ignores missing projected columns on older Parquet schemas for both ranged and full-file reads", async () => {
    clearOpsDecodedCache();
    const rowCount = 16_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const oldSchemaBig = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)) },
        { name: "event_id", type: "STRING", data: Array.from({ length: rowCount }, (_, i) => `evt-${i}-${(i * 9973).toString(16)}-${(i * 2654435761).toString(16)}`) },
        { name: "raw_key", type: "STRING", data: Array.from({ length: rowCount }, (_, i) => `raw/old-${i % 20}.log.gz`) },
        { name: "output_keys_json", type: "STRING", data: Array.from({ length: rowCount }, (_, i) => `["processed/requests/year=2026/month=10/day=29/hour=10/part-${i}-${(i * 7919).toString(16)}.parquet"]`) },
      ],
      codec: "SNAPPY",
    });
    const oldSchemaSmall = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(baseTs)] },
        { name: "raw_key", type: "STRING", data: ["raw/tiny-old.log.gz"] },
      ],
      codec: "SNAPPY",
    });
    expect(oldSchemaBig.byteLength).toBeGreaterThan(OPS_PROJECTED_MIN_BYTES);
    const bigKey = "processed/ops/ingest_files/old-big.parquet";
    const smallKey = "processed/ops/ingest_files/old-small.parquet";
    const fileMap: Record<string, ArrayBuffer> = { [bigKey]: oldSchemaBig, [smallKey]: oldSchemaSmall };
    const rangeCalls: Array<{ key: string; start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            { key: bigKey, start: "2026-10-29T10:00:00Z", end: "2026-10-29T11:00:00Z", size: oldSchemaBig.byteLength, rows: rowCount },
            { key: smallKey, start: "2026-10-29T11:00:00Z", end: "2026-10-29T12:00:00Z", size: oldSchemaSmall.byteLength, rows: 1 },
          ],
          next_cursor: null,
        },
      ],
      files: fileMap,
    });
    src.fetchRange = async (file, start, end) => {
      rangeCalls.push({ key: file.key, start, end });
      return { bytes: fileMap[file.key].slice(start, end), cached: false, cdn: "HIT" };
    };

    const res = await readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "raw_key", "raw_bytes", "nonexistent_future_col"],
      dedupe: false,
      sort: false,
    });
    expect(log.files).toEqual([smallKey]);
    expect(rangeCalls.some((c) => c.key === bigKey)).toBe(true);
    expect(res.rows).toHaveLength(rowCount + 1);
    expect(res.rows.some((r) => r.raw_key === "raw/old-0.log.gz")).toBe(true);
    expect(res.rows.some((r) => r.raw_key === "raw/tiny-old.log.gz")).toBe(true);
  });

  it("fetches medium ops Parquet files (<= 512 KiB) whole in one request instead of multi-round-trip Range reads", async () => {
    clearOpsDecodedCache();
    const rowCount = 2_500;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const mediumParquet = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "event_id",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `evt-${String(i).padStart(6, "0")}-${(i * 2654435761).toString(16)}`,
          ),
        },
        {
          name: "raw_key",
          type: "STRING",
          data: Array.from({ length: rowCount }, (_, i) => `raw/f-${i % 20}.log.gz`),
        },
        {
          name: "raw_size",
          type: "INT64",
          data: Array.from({ length: rowCount }, (_, i) => BigInt(1024 + (i % 10))),
        },
      ],
      codec: "SNAPPY",
    });
    expect(mediumParquet.byteLength).toBeGreaterThan(48 * 1024);
    expect(mediumParquet.byteLength).toBeLessThanOrEqual(512 * 1024);

    const key = "processed/ops/storage_inventory/year=2026/month=10/day=29/hour=10/window=100-105-ab12-n2500.parquet";
    const rangeCalls: Array<{ start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: mediumParquet.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: mediumParquet },
    });
    src.fetchRange = async (_file, start, end) => {
      rangeCalls.push({ start, end });
      return { bytes: mediumParquet.slice(start, end), cached: false, cdn: "HIT" };
    };

    const result = await readLogs(src, {
      kind: "storage_inventory",
      ...range,
      columns: ["timestamp", "raw_key", "raw_size"],
      dedupe: false,
      sort: false,
    });

    expect(rangeCalls).toEqual([]);
    expect(log.files).toEqual([key]);
    expect(result.rows).toHaveLength(rowCount);
    expect(result.rows[0].event_id).toBeUndefined();
    expect(result.rows[0].raw_key).toBe("raw/f-0.log.gz");
  });

  it("falls back to whole-file fetchFile when projected bytes exceed 50% of a >512 KiB ops Parquet file", async () => {
    clearOpsDecodedCache();
    const rowCount = 16_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const largeParquet = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "event_id",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `evt-${String(i).padStart(6, "0")}-${(i * 2654435761).toString(16)}`,
          ),
        },
        {
          name: "output_keys_json",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `["processed/requests/year=2026/month=10/day=29/hour=10/part-${i}-${(i * 7919).toString(16)}.parquet"]`,
          ),
        },
      ],
      codec: "SNAPPY",
    });
    expect(largeParquet.byteLength).toBeGreaterThan(512 * 1024);

    const key = "processed/ops/ingest_files/year=2026/month=10/day=29/compact=100-200-c2-n16000.parquet";
    const rangeCalls: Array<{ start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: largeParquet.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: largeParquet },
    });
    src.fetchRange = async (_file, start, end) => {
      rangeCalls.push({ start, end });
      return { bytes: largeParquet.slice(start, end), cached: false, cdn: "HIT" };
    };

    // Requesting timestamp + output_keys_json reads > 50% of the file; should fall back to fetchFile
    // after the footer probe instead of issuing data range requests.
    const result = await readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "output_keys_json"],
      dedupe: false,
      sort: false,
    });

    expect(rangeCalls).toHaveLength(1);
    expect(log.files).toEqual([key]);
    expect(result.rows).toHaveLength(rowCount);
    expect(result.rows[0].event_id).toBeUndefined();
  });

  it("coalesces concurrent in-flight Parquet loads for the same cacheScope, key, and columns with abort isolation", async () => {
    clearOpsDecodedCache();
    const key = "processed/ops/ingest_files/shared-inflight.parquet";
    const bytes = parquet([row(1, "e1"), row(2, "e2")]);
    let fetchCount = 0;
    let releaseFetch!: () => void;
    const fetchGate = new Promise<void>((resolve) => {
      releaseFetch = resolve;
    });

    const { src } = source({
      live: noLive,
      manifest: [
        {
          files: [{ ...entry(key, 2), size: bytes.byteLength }],
          next_cursor: null,
        },
      ],
      files: { [key]: bytes },
    });
    src.cacheScope = "scope-inflight-test";
    src.fetchFile = async (_k, signal) => {
      fetchCount += 1;
      await fetchGate;
      signal?.throwIfAborted();
      return { bytes: bytes.slice(0), cached: false, cdn: "HIT" };
    };

    const abortFirst = new AbortController();
    const p1 = readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "op"],
      dedupe: false,
      sort: false,
      signal: abortFirst.signal,
    });
    const p2 = readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "op"],
      dedupe: false,
      sort: false,
    });
    const p3 = readLogs(src, {
      kind: "ingest_files",
      ...range,
      columns: ["timestamp", "op"],
      dedupe: false,
      sort: false,
    });

    // Wait a tick for all three readLogs calls to reach load(file).
    await new Promise((r) => setTimeout(r, 10));
    expect(fetchCount).toBe(1);

    // Aborting caller 1 must not abort callers 2 and 3.
    abortFirst.abort();
    await expect(p1).rejects.toThrow();

    releaseFetch();
    const [r2, r3] = await Promise.all([p2, p3]);
    expect(fetchCount).toBe(1);
    expect(r2.rows).toHaveLength(2);
    expect(r3.rows).toHaveLength(2);
    expect(r2.stats.cached).toBe(1);
    expect(r3.stats.cached).toBe(1);
  });

  it("skips HTTP Range footer probes when readLogs sets rangeProject: false", async () => {
    clearOpsDecodedCache();
    const rowCount = 26_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const bigParquet = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "event_id",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `evt-${String(i).padStart(6, "0")}-${(i * 9973).toString(16)}-${(i * 2654435761).toString(16)}`,
          ),
        },
        {
          name: "op",
          type: "STRING",
          data: Array.from({ length: rowCount }, () => "GET"),
        },
      ],
      codec: "SNAPPY",
    });
    expect(bigParquet.byteLength).toBeGreaterThan(OPS_PROJECTED_MIN_BYTES);

    const key = `processed/ops/fos_usage/year=2026/month=10/day=29/compact=100-200-c2-n${rowCount}.parquet`;
    const rangeCalls: Array<{ start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: bigParquet.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: bigParquet },
    });
    src.fetchRange = async (_file, start, end) => {
      rangeCalls.push({ start, end });
      return { bytes: bigParquet.slice(start, end), cached: false, cdn: "HIT" };
    };

    const result = await readLogs(src, {
      kind: "fos_usage",
      ...range,
      columns: ["timestamp", "op"],
      rangeProject: false,
      dedupe: false,
      sort: false,
    });

    expect(rangeCalls).toHaveLength(0);
    expect(log.files).toEqual([key]);
    expect(result.rows).toHaveLength(rowCount);
    expect(result.rows[0].event_id).toBeUndefined();
  });

  it("readIngestLifecycle fetches large files directly without Range footer probes", async () => {
    clearOpsDecodedCache();
    const rowCount = 18_000;
    const baseTs = DAY_MS + 10 * 3_600_000;
    const bigIngest = parquetWriteBuffer({
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000)),
        },
        {
          name: "raw_key",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `raw/logs/year=2026/month=10/day=29/raw-${i}-${(i * 9973).toString(16)}-${(i * 2654435761).toString(16)}.log.gz`,
          ),
        },
        {
          name: "raw_size",
          type: "INT64",
          data: Array.from({ length: rowCount }, () => BigInt(1024)),
        },
        {
          name: "status",
          type: "STRING",
          data: Array.from({ length: rowCount }, () => "converted"),
        },
        {
          name: "state_version",
          type: "INT64",
          data: Array.from({ length: rowCount }, () => BigInt(2)),
        },
        {
          name: "output_keys_json",
          type: "STRING",
          data: Array.from(
            { length: rowCount },
            (_, i) => `["processed/requests/year=2026/month=10/day=29/part-${i}.parquet"]`,
          ),
        },
        {
          name: "raw_deleted",
          type: "BOOLEAN",
          data: Array.from({ length: rowCount }, () => true),
        },
        {
          name: "raw_deleted_at",
          type: "TIMESTAMP",
          data: Array.from({ length: rowCount }, (_, i) => new Date(baseTs + i * 1000 + 60_000)),
        },
      ],
      codec: "SNAPPY",
    });
    expect(bigIngest.byteLength).toBeGreaterThan(OPS_PROJECTED_MIN_BYTES);

    const key = `processed/ops/ingest_files/year=2026/month=10/day=29/compact=100-200-c2-n${rowCount}.parquet`;
    const rangeCalls: Array<{ start: number; end: number }> = [];
    const { src, log } = source({
      live: noLive,
      manifest: [
        {
          files: [
            {
              key,
              start: "2026-10-29T10:00:00Z",
              end: "2026-10-29T11:00:00Z",
              size: bigIngest.byteLength,
              rows: rowCount,
            },
          ],
          next_cursor: null,
        },
      ],
      files: { [key]: bigIngest },
    });
    src.fetchRange = async (_file, start, end) => {
      rangeCalls.push({ start, end });
      return { bytes: bigIngest.slice(start, end), cached: false, cdn: "HIT" };
    };

    const result = await readIngestLifecycle(src, {
      ...range,
      minimumBilledDays: 30,
    });

    expect(rangeCalls).toHaveLength(0);
    expect(log.files).toEqual([key]);
    expect(result.intervals).toHaveLength(rowCount);
    expect(result.complete).toBe(true);
  });
});
