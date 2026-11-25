// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetMetadata, type SchemaElement } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { createNodeBackend } from "./duckdb";
import {
  assembleProjectedParquet, fetchProjectedRanges, FOOTER_PROBE_BYTES, MAX_RANGES_PER_FILE, parquetQuerySchema,
  planParquetProjection, readParquetFooter, RANGE_MERGE_GAP, RANGE_REQUEST_COST_BYTES,
} from "./parquet-projection";
import { projectedReader } from "./projected-read";

const tableRows = (table: unknown) => (table as { toArray(): Record<string, unknown>[] }).toArray();

const rows = 5000;
let seed = 1;
const noise = (length: number) => Array.from({ length }, () => {
  seed = (seed * 1103515245 + 12345) % 2147483648;
  return String.fromCharCode(33 + seed % 90);
}).join("");
const fixture = () => parquetWriteBuffer({
  codec: "SNAPPY",
  rowGroupSize: 1500,
  columnData: [
    { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(1700000000000 + i * 1000)) },
    { name: "wide", type: "STRING", data: Array.from({ length: rows }, () => noise(400)) },
    { name: "url", type: "STRING", data: Array.from({ length: rows }, (_, i) => `/path/${i % 7}`) },
    { name: "status", type: "INT32", data: Array.from({ length: rows }, (_, i) => i % 11 ? 200 : 503) },
    { name: "elapsed", type: "DOUBLE", data: Array.from({ length: rows }, (_, i) => i % 13 ? i / 3 : null) },
    { name: "other", type: "STRING", data: Array.from({ length: rows }, () => noise(300)) },
    { name: "city", type: "STRING", data: Array.from({ length: rows }, (_, i) => i % 5 ? `c${i % 3}` : null) },
  ],
});

function ranged(bytes: ArrayBuffer) {
  const reads: [number, number][] = [];
  return {
    reads,
    fetchRange: async (start: number, end: number) => {
      expect(start).toBeGreaterThanOrEqual(0);
      expect(end).toBeLessThanOrEqual(bytes.byteLength);
      reads.push([start, end]);
      return bytes.slice(start, end);
    },
  };
}

async function query(buffers: Record<string, ArrayBuffer>, sql: string) {
  const backend = await createNodeBackend();
  try {
    for (const [name, bytes] of Object.entries(buffers)) await backend.registerFileBuffer(name, new Uint8Array(bytes));
    return tableRows(await backend.query(sql)).map((row) => ({ ...row }));
  } finally {
    await backend.dispose();
  }
}

describe("column-projected Parquet", () => {
  it("rebuilds only the needed column chunks and answers them exactly", async () => {
    const bytes = fixture();
    const columns = new Set(["timestamp", "url", "status", "elapsed", "city"]);
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.metadata.row_groups.length).toBeGreaterThan(3);
    const plan = planParquetProjection(footer, columns);
    expect(plan.presentColumns).toEqual(["timestamp", "wide", "url", "status", "elapsed", "other", "city"]);
    const groups = footer.metadata.row_groups;
    const needed = groups.flatMap((group) => group.columns)
      .filter((chunk) => columns.has(chunk.meta_data!.path_in_schema[0]))
      .reduce((sum, chunk) => sum + Number(chunk.meta_data!.total_compressed_size), 0);
    expect(plan.neededBytes).toBe(needed);
    expect(needed).toBeLessThan(bytes.byteLength / 3);

    const footerReads = source.reads.length;
    const ranges = await fetchProjectedRanges(plan, source.fetchRange);
    const dataReads = source.reads.slice(footerReads);
    // The wide columns sit between needed chunks; they are never fetched.
    expect(dataReads.reduce((sum, [start, end]) => sum + end - start, 0)).toBeLessThan(bytes.byteLength / 2);
    const projected = assembleProjectedParquet(plan, ranges);
    expect(projected.byteLength).toBeLessThanOrEqual(plan.projectedBytes);

    const select = (file: string) => `SELECT epoch_ms("timestamp") AS ts, url, status, elapsed, city FROM read_parquet('${file}') ORDER BY ts`;
    const aggregate = (file: string) => `SELECT url, count(*) AS n, quantile_cont(elapsed, 0.95) AS p95,
      count(*) FILTER (WHERE status >= 500) AS errors, count(DISTINCT city) AS cities
      FROM read_parquet('${file}') GROUP BY url ORDER BY url`;
    const backend = await createNodeBackend();
    let full, projectedRows, fullAggregate, projectedAggregate, schema;
    try {
      await backend.registerFileBuffer("full.parquet", new Uint8Array(bytes));
      await backend.registerFileBuffer("projected.parquet", new Uint8Array(projected));
      const run = async (sql: string) => tableRows(await backend.query(sql)).map((row) => ({ ...row }));
      full = await run(select("full.parquet"));
      projectedRows = await run(select("projected.parquet"));
      fullAggregate = await run(aggregate("full.parquet"));
      projectedAggregate = await run(aggregate("projected.parquet"));
      schema = await run("DESCRIBE SELECT * FROM read_parquet('projected.parquet')");
    } finally {
      await backend.dispose();
    }
    expect(projectedRows).toHaveLength(rows);
    expect(projectedRows).toEqual(full);
    expect(projectedAggregate).toEqual(fullAggregate);
    expect(schema.map((row) => row.column_name)).toEqual(["timestamp", "url", "status", "elapsed", "city"]);
  });

  it("serves needed chunks inside the footer probe from it instead of fetching them again", async () => {
    const small = parquetWriteBuffer({ codec: "SNAPPY", rowGroupSize: 100, columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 300 }, (_, i) => new Date(1700000000000 + i)) },
      { name: "status", type: "INT32", data: Array.from({ length: 300 }, (_, i) => 200 + i % 3) },
    ] });
    expect(small.byteLength).toBeLessThan(FOOTER_PROBE_BYTES);
    const smallSource = ranged(small);
    const smallPlan = planParquetProjection(await readParquetFooter(small.byteLength, smallSource.fetchRange), new Set(["status"]));
    const smallParts = await fetchProjectedRanges(smallPlan, smallSource.fetchRange);
    expect(smallSource.reads).toHaveLength(1);
    expect(await query({ "p.parquet": assembleProjectedParquet(smallPlan, smallParts) },
      "SELECT CAST(sum(status) AS INTEGER) AS total FROM read_parquet('p.parquet')")).toEqual([{ total: 100 * 603 }]);

    const bytes = fixture();
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    const plan = planParquetProjection(footer, new Set(["timestamp", "url", "status", "elapsed", "city"]));
    const parts = await fetchProjectedRanges(plan, source.fetchRange);
    const probeStart = bytes.byteLength - FOOTER_PROBE_BYTES;
    for (const [start, end] of source.reads.slice(1)) expect(start < probeStart && end <= probeStart).toBe(true);
    parts.forEach((part, index) => expect(new Uint8Array(part))
      .toEqual(new Uint8Array(bytes.slice(plan.ranges[index].start, plan.ranges[index].end))));
  });

  it("reads a footer longer than the tail probe with one more range", async () => {
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 2,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 4000 }, (_, i) => new Date(1700000000000 + i)) },
        ...Array.from({ length: 6 }, (_, c) => ({ name: `c${c}`, type: "INT32" as const, data: Array.from({ length: 4000 }, (_, i) => i + c) })),
      ],
    });
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.footerLength).toBeGreaterThan(64 * 1024);
    expect(source.reads).toHaveLength(2);
    expect(footer.metadata.row_groups).toHaveLength(2000);
    // The second read fetches only the footer bytes the probe missed.
    const probeStart = bytes.byteLength - FOOTER_PROBE_BYTES;
    expect(source.reads[1]).toEqual([bytes.byteLength - footer.footerLength - 8, probeStart]);
    expect(footer.tail!.start).toBe(source.reads[1][0]);
    expect(new Uint8Array(footer.tail!.bytes)).toEqual(new Uint8Array(bytes.slice(footer.tail!.start)));
  });

  it("does not fetch unneeded columns of compacted files with many small row groups", async () => {
    // Like compacted Cloud files: many row groups, each column chunk a few KiB.
    const count = 6000;
    const bytes = parquetWriteBuffer({ codec: "SNAPPY", rowGroupSize: 500, columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i)) },
      { name: "rid", type: "STRING", data: Array.from({ length: count }, () => noise(24)) },
      { name: "status", type: "INT32", data: Array.from({ length: count }, (_, i) => i % 11 ? 200 : 503) },
      { name: "referer", type: "STRING", data: Array.from({ length: count }, () => noise(24)) },
      { name: "city", type: "STRING", data: Array.from({ length: count }, (_, i) => `c${i % 3}`) },
    ] });
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(12);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status", "city"]));
    expect(plan.ranges.reduce((sum, range) => sum + range.end - range.start, 0)).toBeLessThan(bytes.byteLength);
    const parts = await fetchProjectedRanges(plan, source.fetchRange);
    expect(await query({ "p.parquet": assembleProjectedParquet(plan, parts) },
      "SELECT count(*) AS n, count(*) FILTER (WHERE status = 503) AS errors FROM read_parquet('p.parquet')"))
      .toEqual([{ n: BigInt(count), errors: BigInt(546) }]);
  });

  it("merges needed chunks separated by small gaps and keeps large gaps apart", async () => {
    const bytes = fixture();
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status"]));
    for (let i = 1; i < plan.ranges.length; i++)
      expect(plan.ranges[i].start - plan.ranges[i - 1].end).toBeGreaterThan(RANGE_MERGE_GAP);
    for (const chunk of plan.chunks)
      expect(plan.ranges.some((range) => range.start <= chunk.start && chunk.start + chunk.length <= range.end)).toBe(true);
  });

  it("trades a few unneeded columns for far fewer requests on a file of many narrow row groups", async () => {
    // Shaped like a live compact: many columns, a few KiB per chunk, dozens of row groups.
    const count = 24000;
    const columnData = Array.from({ length: 60 }, (_, c) => ({
      name: c % 4 === 0 ? `want_${c}` : `skip_${c}`, type: "STRING" as const,
      data: Array.from({ length: count }, () => noise(6)),
    }));
    const bytes = parquetWriteBuffer({ codec: "SNAPPY", rowGroupSize: 1000, columnData });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(24);
    const wanted = new Set(columnData.map((column) => column.name).filter((name) => name.startsWith("want_")));
    const plan = planParquetProjection(footer, wanted);
    const tight: { start: number; end: number }[] = [];
    for (const chunk of [...plan.chunks].sort((a, b) => a.start - b.start)) {
      const last = tight[tight.length - 1];
      if (last && chunk.start - last.end <= RANGE_MERGE_GAP) last.end = Math.max(last.end, chunk.start + chunk.length);
      else tight.push({ start: chunk.start, end: chunk.start + chunk.length });
    }
    const fetched = (ranges: { start: number; end: number }[]) => ranges.reduce((sum, range) => sum + range.end - range.start, 0);
    expect(tight.length).toBeGreaterThan(100);
    expect(plan.ranges.length).toBeLessThanOrEqual(tight.length / 4);
    expect(fetched(plan.ranges) + plan.ranges.length * RANGE_REQUEST_COST_BYTES)
      .toBeLessThan(fetched(tight) + tight.length * RANGE_REQUEST_COST_BYTES);
    for (const chunk of plan.chunks)
      expect(plan.ranges.some((range) => range.start <= chunk.start && chunk.start + chunk.length <= range.end)).toBe(true);
  });

  it("keeps only the selected row groups and the file's key-value metadata", async () => {
    const count = 3000;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 1000,
      kvMetadata: [{ key: "flp_summary", value: "3" }, { key: "flp_summary_layout", value: "{}" }],
      columnData: [
        { name: "a", type: "INT32", data: Array.from({ length: count }, (_, i) => i) },
        { name: "wide", type: "STRING", data: Array.from({ length: count }, () => noise(200)) },
        { name: "b", type: "STRING", data: Array.from({ length: count }, (_, i) => `b${i % 17}`) },
        { name: "c", type: "INT32", data: Array.from({ length: count }, (_, i) => i * 2) },
      ],
    });
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(3);
    const plan = planParquetProjection(footer, new Set(["a", "b"]), (rowGroup) => rowGroup === 1);
    expect(new Set(plan.chunks.map((chunk) => chunk.rowGroup))).toEqual(new Set([1]));
    const group = footer.metadata.row_groups[1].columns;
    const chunkBytes = [0, 2].reduce((sum, c) => sum + Number(group[c].meta_data!.total_compressed_size), 0);
    expect(plan.neededBytes).toBe(chunkBytes);

    const parts = await fetchProjectedRanges(plan, source.fetchRange);
    // The wide chunk between `a` and `b` is far past the merge gap, so exactly the two chunks are read.
    expect(plan.ranges).toHaveLength(2);
    expect(parts.reduce((sum, part) => sum + part.byteLength, 0)).toBe(chunkBytes);

    const projected = assembleProjectedParquet(plan, parts);
    const metadata = parquetMetadata(projected);
    expect(metadata.row_groups).toHaveLength(1);
    expect(Number(metadata.num_rows)).toBe(1000);
    expect(metadata.key_value_metadata).toEqual([
      { key: "flp_summary", value: "3" }, { key: "flp_summary_layout", value: "{}" },
    ]);
    expect(await query({ "p.parquet": projected }, "SELECT a, b FROM read_parquet('p.parquet') ORDER BY a"))
      .toEqual(Array.from({ length: 1000 }, (_, i) => ({ a: 1000 + i, b: `b${(1000 + i) % 17}` })));
  });

  it("avoids collapsing uniform multi-row-group compacts into a single whole-file range", async () => {
    // 40 uniform row groups (> MAX_RANGES_PER_FILE = 32), each with a large (~80 KiB)
    // unneeded payload column between narrow wanted columns. Power-of-2 uniform gap doubling
    // previously jumped past the uniform gap and collapsed all 40 row groups into 1 range
    // covering ~98% of the file, triggering whole-file fallback in range-loader.
    const rowGroupSize = 500;
    const rowGroups = 40;
    const count = rowGroupSize * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "status", type: "INT32", data: Array.from({ length: count }, (_, i) => i % 10 === 0 ? 503 : 200) },
        { name: "wide_unneeded", type: "STRING", data: Array.from({ length: count }, () => noise(600)) },
      ],
    });
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(rowGroups);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status"]));
    const plannedBytes = plan.ranges.reduce((sum, range) => sum + (range.end - range.start), 0);
    expect(plan.ranges.length).toBeLessThanOrEqual(MAX_RANGES_PER_FILE);
    expect(plan.ranges.length).toBeGreaterThan(1);
    expect(plannedBytes).toBeLessThan(bytes.byteLength * 0.55);
    for (const chunk of plan.chunks) {
      expect(plan.ranges.some((range) => range.start <= chunk.start && chunk.start + chunk.length <= range.end)).toBe(true);
    }
    const parts = await fetchProjectedRanges(plan, source.fetchRange);
    expect(await query({ "p.parquet": assembleProjectedParquet(plan, parts) },
      "SELECT count(*) AS n, count(*) FILTER (WHERE status = 503) AS errors FROM read_parquet('p.parquet')"))
      .toEqual([{ n: BigInt(count), errors: BigInt(count / 10) }]);
  });

  it("avoids swallowing wide unneeded columns on multi-row-group Cloud-shaped compacts", async () => {
    const rowGroupSize = 500;
    const rowGroups = 36;
    const count = rowGroupSize * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "cache", type: "STRING", data: Array.from({ length: count }, (_, i) => i % 2 ? "HIT" : "MISS") },
        { name: "url", type: "STRING", data: Array.from({ length: count }, () => noise(60)) },
        { name: "proto", type: "STRING", data: Array.from({ length: count }, () => "HTTP/3") },
        { name: "ua", type: "STRING", data: Array.from({ length: count }, () => noise(80)) },
        { name: "referer", type: "STRING", data: Array.from({ length: count }, () => noise(60)) },
        { name: "digest", type: "STRING", data: Array.from({ length: count }, () => noise(64)) },
        { name: "edge", type: "STRING", data: Array.from({ length: count }, () => "1") },
        { name: "rid", type: "STRING", data: Array.from({ length: count }, () => noise(48)) },
        { name: "waf", type: "STRING", data: Array.from({ length: count }, () => "allow") },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(rowGroups);
    const plan = planParquetProjection(footer, new Set(["timestamp", "cache", "proto", "edge", "waf"]));
    const plannedBytes = plan.ranges.reduce((sum, range) => sum + (range.end - range.start), 0);
    expect(plan.ranges.length).toBeLessThanOrEqual(MAX_RANGES_PER_FILE);
    expect(plannedBytes).toBeLessThan(bytes.byteLength * 0.45);
  });

  it("rejects a footer whose chunk offsets fall outside the file", async () => {
    const bytes = fixture();
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(() => planParquetProjection({ ...footer, size: footer.footerLength + 100 }, new Set(["url"])))
      .toThrow(/outside the file/);
  });
});

describe("whole-file reads of small finished-hour files", () => {
  const smallFile = () => parquetWriteBuffer({
    codec: "SNAPPY",
    rowGroupSize: 200,
    columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 4000 }, (_, i) => new Date(1700000000000 + i * 1000)) },
      { name: "wide", type: "STRING", data: Array.from({ length: 4000 }, () => noise(400)) },
      { name: "status", type: "INT32", data: Array.from({ length: 4000 }, (_, i) => i % 11 ? 200 : 503) },
    ],
  });
  const reader = (bytes: ArrayBuffer) => {
    const calls = { ranges: 0, files: 0 };
    const read = projectedReader({
      cacheScope: "t",
      fetchRange: async (_file, start, end) => {
        calls.ranges++;
        return bytes.slice(start, end);
      },
      fetchFile: async () => {
        calls.files++;
        return { bytes: bytes.slice(0), cached: false };
      },
    }, ["timestamp", "status"]);
    return { calls, read };
  };
  const entry = (bytes: ArrayBuffer, endMs: number) => ({
    key: "processed/request/f.parquet",
    start: new Date(endMs - 600_000).toISOString(),
    end: new Date(endMs).toISOString(),
    size: bytes.byteLength,
    rows: 4000,
  });

  it("fetches a closed-hour file below the threshold once, with no footer or column ranges", async () => {
    const bytes = smallFile();
    expect(bytes.byteLength).toBeGreaterThan(256 * 1024);
    const { calls, read } = reader(bytes);
    const projected = await (await read(entry(bytes, Date.now() - 2 * 3_600_000))).read();
    expect(calls).toEqual({ ranges: 0, files: 1 });
    expect(projected.downloaded).toBe(bytes.byteLength);
    expect(await query({ "p.parquet": projected.bytes },
      "SELECT count(*)::INT AS n, count(*) FILTER (WHERE status = 503)::INT AS errors FROM 'p.parquet'"))
      .toEqual([{ n: 4000, errors: 364 }]);
    expect(parquetMetadata(projected.bytes).schema.slice(1).map((field) => field.name)).toEqual(["timestamp", "status"]);
  });

  it("keeps footer-planned ranges for an open-hour file above the small-file size", async () => {
    const bytes = smallFile();
    const { calls, read } = reader(bytes);
    await (await read(entry(bytes, Date.now() + 60_000))).read();
    expect(calls.files).toBe(0);
    expect(calls.ranges).toBeGreaterThan(1);
  });
});

describe("query schemas from footers", () => {
  const typed = (timestamp: Record<string, unknown>, n = 1) => parquetWriteBuffer({
    rowGroupSize: 100,
    schema: [
      { name: "root", num_children: 3 },
      { name: "timestamp", repetition_type: "OPTIONAL", ...timestamp },
      { name: "status", type: "INT32", repetition_type: "OPTIONAL" },
      { name: "wide", type: "BYTE_ARRAY", repetition_type: "OPTIONAL", logical_type: { type: "STRING" } },
    ] as SchemaElement[],
    columnData: [
      { name: "timestamp", data: Array.from({ length: n }, (_, i) => timestamp.type === "BYTE_ARRAY" ? `t${i}`
        : timestamp.logical_type ? new Date(1700000000000 + i * 1000) : BigInt(1700000000000 + i * 1000)) },
      { name: "status", data: Array.from({ length: n }, (_, i) => (i % 5 ? 200 : 503)) },
      { name: "wide", data: Array.from({ length: n }, () => noise(300)) },
    ],
  });
  const describeFile = async (bytes: ArrayBuffer) =>
    (await query({ "f.parquet": bytes }, "DESCRIBE SELECT * FROM read_parquet('f.parquet')"))
      .map((row) => [row.column_name, row.column_type]);

  it("names DuckDB's timestamp type when the footer settles it, and nothing otherwise", async () => {
    const logical = (isAdjustedToUTC: boolean, unit: string) =>
      ({ type: "INT64", logical_type: { type: "TIMESTAMP", isAdjustedToUTC, unit } });
    for (const timestamp of [logical(true, "MILLIS"), logical(false, "MILLIS"), logical(true, "MICROS"), logical(false, "MICROS")]) {
      const bytes = typed(timestamp);
      const described = await describeFile(bytes);
      const schema = parquetQuerySchema(parquetMetadata(bytes).schema);
      expect(schema?.columns).toEqual(described.map(([name]) => name));
      expect(schema?.timestampType, JSON.stringify(timestamp)).toBe(described[0][1]);
    }
    for (const timestamp of [
      logical(true, "NANOS"),
      { type: "INT64", converted_type: "TIMESTAMP_MILLIS" },
      { type: "INT64" },
      { type: "BYTE_ARRAY", logical_type: { type: "STRING" } },
    ]) {
      expect(parquetQuerySchema(parquetMetadata(typed(timestamp)).schema), JSON.stringify(timestamp)).toBeNull();
    }
    const utc = parquetQuerySchema(parquetMetadata(typed(logical(true, "MILLIS"))).schema)!;
    expect(parquetQuerySchema(parquetMetadata(typed(logical(true, "MILLIS"), 3)).schema)!.signature).toBe(utc.signature);
    expect(parquetQuerySchema(parquetMetadata(typed(logical(true, "MICROS"))).schema)!.signature).not.toBe(utc.signature);
  });

  it("describes the file a projected read rebuilds, by ranges or from a whole read", async () => {
    const bytes = typed({ type: "INT64", logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" } }, 2000);
    expect(bytes.byteLength).toBeGreaterThan(256 * 1024);
    const read = projectedReader({
      cacheScope: "t",
      fetchRange: async (_file, start, end) => bytes.slice(start, end),
      fetchFile: async () => ({ bytes: bytes.slice(0), cached: false }),
    }, ["timestamp", "status"]);
    for (const endMs of [Date.now() - 2 * 3_600_000, Date.now() + 60_000]) {
      const projected = await (await read({
        key: "processed/request/f.parquet",
        start: new Date(endMs - 600_000).toISOString(),
        end: new Date(endMs).toISOString(),
        size: bytes.byteLength,
        rows: 2000,
      })).read();
      expect(projected.schema).toEqual(parquetQuerySchema(parquetMetadata(projected.bytes).schema));
      expect(projected.schema?.columns).toEqual(["timestamp", "status"]);
      expect(projected.schema?.timestampType).toBe("TIMESTAMP WITH TIME ZONE");
    }
  });

  it("passes canonical wholeFileIdentity to fetchFile on whole-first reads so range-loader shares the cache entry", async () => {
    const bytes = typed({ type: "INT64", logical_type: { type: "TIMESTAMP", isAdjustedToUTC: true, unit: "MILLIS" } }, 200);
    const seenModes: Array<{ identity?: string } | undefined> = [];
    const read = projectedReader({
      cacheScope: "fixture-session:svc-a",
      fetchRange: async (_file, start, end) => bytes.slice(start, end),
      fetchFile: async (_key, _sig, mode) => {
        seenModes.push(mode);
        return { bytes: bytes.slice(0), cached: false };
      },
    }, ["timestamp", "status"]);
    const entry = {
      key: "processed/request/f.parquet",
      start: new Date(Date.now() - 600_000).toISOString(),
      end: new Date(Date.now() - 60_000).toISOString(),
      size: bytes.byteLength,
      rows: 200,
      etag: "\"etag-shared\"",
    };
    await (await read(entry)).read();
    expect(seenModes).toEqual([
      { identity: JSON.stringify(["fixture-session:svc-a", "request", entry.key, "\"etag-shared\"", bytes.byteLength, ""]) },
    ]);
  });
});
