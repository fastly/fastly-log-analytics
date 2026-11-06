// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { fnv1a32, readLogs, type OpsSource } from "./engine";
import type { ManifestFile } from "../dashboard/manifest";
import type { RunProgress } from "../dashboard/engine";

const DAY_MS = Date.parse("2026-10-29T00:00:00Z");
const minute = (hour: number, extra = 0) => DAY_MS / 60_000 + hour * 60 + extra;
const DIR = "processed/ops/fos_usage/year=2026/month=10/day=29/";
const windowKey = (hour: number, start: number, n: number, txn = "ab12") =>
  `${DIR}hour=${String(hour).padStart(2, "0")}/window=${start}-${start + 4}-${txn}-n${n}.parquet`;
const compactKey = (a: number, b: number, c: number, n: number) =>
  `${DIR}compact=${a}-${b}-c${c}-n${n}.parquet`;

type Row = { timestamp: number; event_id: string; op?: string };
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
      const index = params.get("cursor") ? Number(params.get("cursor")) : 0;
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
});
