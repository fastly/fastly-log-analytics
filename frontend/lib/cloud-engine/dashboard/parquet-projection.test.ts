// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetMetadata, type SchemaElement } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { createNodeBackend } from "./duckdb";
import {
  assembleProjectedParquet, defaultFooterProbeBytes, fetchProjectedRanges, filterRowGroupFilter, FOOTER_PROBE_BYTES,
  LARGE_FOOTER_PROBE_BYTES, MAX_RANGES_PER_FILE, parquetQuerySchema, planParquetProjection, readParquetFooter,
  readsWholeFirst, RANGE_MERGE_GAP, RANGE_REQUEST_COST_BYTES, shouldFallbackToWhole, SMALL_FOOTER_PROBE_BYTES,
  TARGET_RANGES_PER_FILE, wholeReadLimits,
} from "./parquet-projection";
import { effectiveProjectedColumnCount, prefersWholeForWideProjection, projectedReader } from "./projected-read";
import { valueScanColumns } from "../value";
import { fetchLanes } from "../fetch-lanes";

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

  it("reads a ~100 KiB multi-row-group compact footer in a single range request", async () => {
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 10,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 2000 }, (_, i) => new Date(1700000000000 + i)) },
        ...Array.from({ length: 12 }, (_, c) => ({ name: `c${c}`, type: "INT32" as const, data: Array.from({ length: 2000 }, (_, i) => i + c) })),
      ],
    });
    const source = ranged(bytes);
    const footer = await readParquetFooter(bytes.byteLength, source.fetchRange);
    expect(footer.footerLength).toBeGreaterThan(64 * 1024);
    expect(footer.footerLength).toBeLessThan(FOOTER_PROBE_BYTES);
    expect(source.reads).toHaveLength(1);
    expect(footer.metadata.row_groups).toHaveLength(200);
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

  it("coalesces medium inter-chunk gaps down to TARGET_RANGES_PER_FILE to cut HTTP 206s", async () => {
    // 18 row groups where each unneeded column chunk is ~50 KiB (above baseGap, below SOFT_MERGE_MAX_GAP),
    // shaped like /assets-shield compact files: soft coalescing should merge down toward TARGET_RANGES_PER_FILE
    // (4) rather than leaving 18 separate HTTP 206 range requests.
    const rowGroupSize = 500;
    const rowGroups = 18;
    const count = rowGroupSize * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "status", type: "INT32", data: Array.from({ length: count }, () => 200) },
        { name: "medium_unneeded", type: "STRING", data: Array.from({ length: count }, () => noise(110)) },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(rowGroups);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status"]));
    expect(TARGET_RANGES_PER_FILE).toBe(4);
    expect(plan.ranges.length).toBeLessThan(rowGroups);
    expect(plan.ranges.length).toBeLessThanOrEqual(4);
  });

  it("coalesces multi-row-group compact= files down to 4 ranges within 45% of file size", async () => {
    const rowGroupSize = 500;
    const rowGroups = 12;
    const count = rowGroupSize * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "gap", type: "STRING", data: Array.from({ length: count }, () => noise(190)) },
        { name: "status", type: "INT32", data: Array.from({ length: count }, () => 200) },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(rowGroups);
    // Boundary compact= slice keeping 8 of 12 row groups merges 4 gaps (~36% of file) down to 4 ranges.
    const plan = planParquetProjection(footer, new Set(["timestamp", "status"]), (rg) => rg >= 4);
    const plannedBytes = plan.ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
    expect(plan.ranges.length).toBeLessThanOrEqual(TARGET_RANGES_PER_FILE);
    expect(plannedBytes).toBeLessThanOrEqual(Math.floor(bytes.byteLength * 0.45));
  });

  it("prunes row groups using Parquet column chunk statistics for include/exclude filters", async () => {
    const rowGroupSize = 200;
    // 4 row groups with distinct pop, status, edge, and nullable tag values.
    const pops = [
      ...Array.from({ length: rowGroupSize }, () => "AMS"),
      ...Array.from({ length: rowGroupSize }, () => "IAD"),
      ...Array.from({ length: rowGroupSize }, () => "SJC"),
      ...Array.from({ length: rowGroupSize }, () => "SJC"),
    ];
    const statuses = [
      ...Array.from({ length: rowGroupSize }, () => 200),
      ...Array.from({ length: rowGroupSize }, () => 503),
      ...Array.from({ length: rowGroupSize }, () => 200),
      ...Array.from({ length: rowGroupSize }, () => 404),
    ];
    const edges = [
      ...Array.from({ length: rowGroupSize }, () => true),
      ...Array.from({ length: rowGroupSize }, () => true),
      ...Array.from({ length: rowGroupSize }, () => false),
      ...Array.from({ length: rowGroupSize }, () => true),
    ];
    const tags = [
      ...Array.from({ length: rowGroupSize }, () => null),
      ...Array.from({ length: rowGroupSize }, () => "canary"),
      ...Array.from({ length: rowGroupSize }, () => null),
      ...Array.from({ length: rowGroupSize }, () => "prod"),
    ];
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rowGroupSize * 4 }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "pop", type: "STRING", data: pops },
        { name: "status", type: "INT32", data: statuses },
        { name: "edge", type: "BOOLEAN", data: edges },
        { name: "tag", type: "STRING", data: tags },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(4);

    // String include: only RG 1 has pop = "IAD"
    const byPop = planParquetProjection(
      footer,
      new Set(["timestamp", "pop"]),
      filterRowGroupFilter(footer.metadata, [{ field: "pop", mode: "include", values: ["IAD"] }]),
    );
    expect(byPop.rowGroups).toEqual([1]);

    // Numeric include: only RG 1 has status = 503
    const byStatus = planParquetProjection(
      footer,
      new Set(["timestamp", "status"]),
      filterRowGroupFilter(footer.metadata, [{ field: "status", mode: "include", values: ["503"] }]),
    );
    expect(byStatus.rowGroups).toEqual([1]);

    // Boolean include: RG 2 is all-false so edge=true prunes RG 2
    const byEdge = planParquetProjection(
      footer,
      new Set(["timestamp", "edge"]),
      filterRowGroupFilter(footer.metadata, [{ field: "edge", mode: "include", values: ["true"] }]),
    );
    expect(byEdge.rowGroups).toEqual([0, 1, 3]);

    // String exclude on constant non-null RG: exclude SJC prunes RG 2 and RG 3
    const excludeSjc = planParquetProjection(
      footer,
      new Set(["timestamp", "pop"]),
      filterRowGroupFilter(footer.metadata, [{ field: "pop", mode: "exclude", values: ["SJC"] }]),
    );
    expect(excludeSjc.rowGroups).toEqual([0, 1]);

    // All-null RG pruned by include filter on nullable column
    const byTag = planParquetProjection(
      footer,
      new Set(["timestamp", "tag"]),
      filterRowGroupFilter(footer.metadata, [{ field: "tag", mode: "include", values: ["prod"] }]),
    );
    expect(byTag.rowGroups).toEqual([3]);

    // Wildcard, missing column, and empty value list conservatively keep all row groups
    const wildcard = planParquetProjection(
      footer,
      new Set(["timestamp", "pop"]),
      filterRowGroupFilter(footer.metadata, [
        { field: "pop", mode: "include", values: ["IA*"] },
        { field: "missing_col", mode: "include", values: ["x"] },
      ]),
    );
    expect(wildcard.rowGroups).toEqual([0, 1, 2, 3]);
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

  it("shouldFallbackToWhole caps high HTTP 206 fan-out while preserving narrow range reads", () => {
    const planWith = (rangeCount: number) => ({
      ranges: Array.from({ length: rangeCount }, (_, i) => ({ start: i * 100, end: i * 100 + 50 })),
    });
    const fileSize = 4 * 1024 * 1024;
    const largeFile = 20 * 1024 * 1024;
    expect(shouldFallbackToWhole(planWith(0), fileSize, 0)).toBe(false);
    expect(shouldFallbackToWhole(planWith(1), fileSize, Math.ceil(fileSize * 0.85))).toBe(true);
    expect(shouldFallbackToWhole(planWith(7), fileSize, Math.floor(fileSize * 0.45))).toBe(false);
    // Medium compacts (<= 8 MiB) stay ranged at 8–15 ranges unless planned bytes >= 60%,
    // preventing multi-second whole-file downloads after footer probes on 4–8 MiB compacts.
    expect(shouldFallbackToWhole(planWith(8), fileSize, Math.floor(fileSize * 0.35))).toBe(false);
    expect(shouldFallbackToWhole(planWith(8), fileSize, Math.floor(fileSize * 0.55))).toBe(false);
    expect(shouldFallbackToWhole(planWith(8), fileSize, Math.ceil(fileSize * 0.60))).toBe(true);
    expect(shouldFallbackToWhole(planWith(8), fileSize, Math.floor(fileSize * 0.10))).toBe(false);
    expect(shouldFallbackToWhole(planWith(13), 1_650_000, Math.floor(1_650_000 * 0.37))).toBe(false);
    expect(shouldFallbackToWhole(planWith(16), fileSize, Math.floor(fileSize * 0.25))).toBe(true);
    // Larger files (> 8 MiB) stay ranged unless byte share or extreme fan-out (<= 16 MiB) justifies a whole download.
    const eightMibFile = 8 * 1024 * 1024;
    const mediumLargeFile = 12 * 1024 * 1024;
    expect(shouldFallbackToWhole(planWith(8), largeFile, Math.ceil(largeFile * 0.60))).toBe(true);
    expect(shouldFallbackToWhole(planWith(8), largeFile, Math.floor(largeFile * 0.55))).toBe(false);
    expect(shouldFallbackToWhole(planWith(12), mediumLargeFile, Math.floor(mediumLargeFile * 0.35))).toBe(false);
    expect(shouldFallbackToWhole(planWith(16), largeFile, Math.ceil(largeFile * 0.45))).toBe(true);
    expect(shouldFallbackToWhole(planWith(16), largeFile, Math.floor(largeFile * 0.35))).toBe(false);
    expect(shouldFallbackToWhole(planWith(24), eightMibFile, Math.floor(eightMibFile * 0.20))).toBe(true);
    expect(shouldFallbackToWhole(planWith(24), mediumLargeFile, Math.floor(mediumLargeFile * 0.20))).toBe(true);
    expect(shouldFallbackToWhole(planWith(24), largeFile, Math.ceil(largeFile * 0.30))).toBe(true);
    expect(shouldFallbackToWhole(planWith(24), largeFile, Math.floor(largeFile * 0.20))).toBe(false);
  });

  it("falls back to shared whole-file fetch in projectedReader on high-fan-out multi-row-group compacts", async () => {
    // Reproduces /security bundle + threat-intel concurrently projecting the same residual catalog1 file:
    // both projections should fall back to fetchFile with canonical wholeFileIdentity and share 1 download.
    const rgRows = 500;
    const rowGroups = 20;
    const totalRows = rgRows * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(260)) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(260)) },
        { name: "ja4", type: "STRING", data: Array.from({ length: totalRows }, (_, i) => `t13d${i % 19}`) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, (_, i) => (i % 10 === 0 ? 503 : 200)) },
      ],
    });
    const entry = {
      key: "processed/request/year=2026/month=11/day=24/hour=07/catalog1/compact.parquet",
      start: new Date(1700000000000).toISOString(),
      end: new Date(Date.now() + 60_000).toISOString(),
      size: bytes.byteLength,
      rows: totalRows,
      etag: "\"etag-sec\"",
    };
    expect(readsWholeFirst(entry)).toBe(false);

    const wholeCache = new Map<string, ArrayBuffer>();
    let networkWholeFetches = 0;
    let dataRangeCalls = 0;
    const source = {
      cacheScope: "fixture-session:svc-sec",
      fetchRange: async (_file: unknown, start: number, end: number) => {
        if (end < bytes.byteLength) dataRangeCalls++;
        return bytes.slice(start, end);
      },
      fetchFile: async (_key: string, _sig?: AbortSignal, mode?: { identity?: string }) => {
        const id = mode?.identity ?? "";
        const hit = wholeCache.get(id);
        if (hit) return { bytes: hit.slice(0), cached: true };
        networkWholeFetches++;
        wholeCache.set(id, bytes.slice(0));
        return { bytes: bytes.slice(0), cached: false };
      },
    };

    const bundleRead = projectedReader(source, ["timestamp", "ua", "status"]);
    const threatRead = projectedReader(source, ["timestamp", "ja4", "status"]);

    const bundleProj = await (await bundleRead(entry)).read();
    const threatProj = await (await threatRead(entry)).read();

    expect(networkWholeFetches).toBe(1);
    expect(dataRangeCalls).toBe(0);
    expect(threatProj.cached).toBe(true);
    expect(await query({ "b.parquet": bundleProj.bytes }, "SELECT count(*)::INT AS n FROM 'b.parquet'"))
      .toEqual([{ n: totalRows }]);
    expect(await query({ "t.parquet": threatProj.bytes }, "SELECT count(*)::INT AS n FROM 't.parquet'"))
      .toEqual([{ n: totalRows }]);
  });

  it("coalesces a single pruned row group in a large compact into one HTTP 206 range", async () => {
    // 10 row groups where only the last row group matches the window; intra-row-group gaps
    // (~50 KiB) are above baseGap (32 KiB) but below SOFT_MERGE_MAX_GAP (128 KiB), and the
    // pruned row group is < 15% of the file, so Pass 2 collapses all chunks into 1 range.
    const rowGroupSize = 500;
    const rowGroups = 10;
    const count = rowGroupSize * rowGroups;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: count }, (_, i) => new Date(1700000000000 + i * 1000)) },
        { name: "gap1", type: "STRING", data: Array.from({ length: count }, () => noise(110)) },
        { name: "status", type: "INT32", data: Array.from({ length: count }, () => 200) },
        { name: "gap2", type: "STRING", data: Array.from({ length: count }, () => noise(110)) },
        { name: "edge", type: "STRING", data: Array.from({ length: count }, () => "1") },
        { name: "wide_unneeded", type: "STRING", data: Array.from({ length: count }, () => noise(300)) },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(footer.metadata.row_groups).toHaveLength(rowGroups);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status", "edge"]), (rg) => rg === rowGroups - 1);
    expect(plan.rowGroups).toEqual([rowGroups - 1]);
    expect(plan.ranges).toHaveLength(1);
    const plannedBytes = plan.ranges[0].end - plan.ranges[0].start;
    expect(plannedBytes).toBeLessThan(bytes.byteLength * 0.15);
  });

  it("prefersWholeForWideProjection gates on column count, file size, and window overlap", () => {
    const startMs = Date.parse("2026-11-25T04:04:00Z");
    const endMs = Date.parse("2026-11-25T04:32:00Z");
    const compact = { size: 20 * 1024 * 1024, start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString() };

    expect(prefersWholeForWideProjection(compact, 20, { fromMs: startMs + 9_000, toMs: endMs + 3_600_000 })).toBe(true);
    expect(prefersWholeForWideProjection(compact, 16)).toBe(true);
    expect(prefersWholeForWideProjection(compact, 15, { fromMs: startMs + 9_000, toMs: endMs + 3_600_000 })).toBe(false);
    expect(prefersWholeForWideProjection(compact, 11, { fromMs: startMs + 9_000, toMs: endMs + 3_600_000 })).toBe(false);
    expect(prefersWholeForWideProjection(compact, 20, { fromMs: endMs - 4 * 60_000, toMs: endMs + 3_600_000 })).toBe(false);
    expect(prefersWholeForWideProjection({ ...compact, size: 40 * 1024 * 1024 }, 20)).toBe(false);
    expect(prefersWholeForWideProjection({ ...compact, size: 6 * 1024 * 1024 }, 20)).toBe(false);
    expect(prefersWholeForWideProjection({ ...compact, size: 7 * 1024 * 1024 }, 20)).toBe(true);
  });

  it("reads a high-overlap boundary compact whole-first on wide projections without footer probes", async () => {
    const prevClosedLimit = wholeReadLimits.closedHourUpToBytes;
    wholeReadLimits.closedHourUpToBytes = 2 * 1024 * 1024;
    try {
      const rgRows = 500;
      const rowGroups = 6;
      const totalRows = rgRows * rowGroups;
      const startMs = 1795579440_000;
      const stepMs = 500;
      const endMs = startMs + totalRows * stepMs;
      const bytes = parquetWriteBuffer({
        codec: "UNCOMPRESSED",
        rowGroupSize: rgRows,
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(startMs + i * stepMs)) },
          { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
          { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
          { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
        ],
      });
      expect(bytes.byteLength).toBeGreaterThan(2 * 1024 * 1024);
      const entry = {
        key: "processed/request/year=2026/month=11/day=25/hour=04/catalog1/compact-edge.parquet",
        start: new Date(startMs).toISOString(),
        end: new Date(endMs).toISOString(),
        size: bytes.byteLength,
        rows: totalRows,
        etag: "\"etag-edge\"",
      };
      const wideCols = [
        "timestamp", "status", "cache", "resp_bytes", "edge", "url",
        "ua", "referer", "ip", "host", "country", "asn",
        "pop", "method", "proto", "tls",
      ];
      let rangeCalls = 0;
      let fileCalls = 0;
      const source = {
        cacheScope: "fixture-session:svc-edge",
        fetchRange: async (_file: unknown, start: number, end: number) => {
          rangeCalls++;
          return bytes.slice(start, end);
        },
        fetchFile: async () => {
          fileCalls++;
          return { bytes: bytes.slice(0), cached: false };
        },
      };

      // High overlap (99%): skips footer probes and downloads once via fetchFile.
      const highOverlap = projectedReader(source, wideCols, undefined, { fromMs: startMs + 9_000, toMs: endMs + 3_600_000 });
      const highResult = await (await highOverlap(entry)).read();
      expect(fileCalls).toBe(1);
      expect(rangeCalls).toBe(0);
      expect(await query({ "h.parquet": highResult.bytes }, "SELECT count(*)::INT AS n FROM 'h.parquet'"))
        .toEqual([{ n: totalRows }]);

      // Low overlap (~15%): stays on footer + range reads and avoids downloading the whole file.
      rangeCalls = 0;
      fileCalls = 0;
      const lowOverlap = projectedReader(source, wideCols, undefined, {
        fromMs: endMs - Math.floor((endMs - startMs) * 0.15),
        toMs: endMs + 3_600_000,
      });
      const lowResult = await (await lowOverlap(entry)).read();
      expect(fileCalls).toBe(0);
      expect(rangeCalls).toBeGreaterThan(0);
      const [{ n: lowRows }] = await query({ "l.parquet": lowResult.bytes }, "SELECT count(*)::INT AS n FROM 'l.parquet'") as [{ n: number }];
      expect(lowRows).toBeGreaterThan(0);
      expect(lowRows).toBeLessThan(totalRows);
    } finally {
      wholeReadLimits.closedHourUpToBytes = prevClosedLimit;
    }
  });

  it("uses 512 KiB tail probes for >= 4 MiB files and reads compact= files up to 6 MiB whole-first", () => {
    expect(defaultFooterProbeBytes(100 * 1024)).toBe(SMALL_FOOTER_PROBE_BYTES);
    expect(defaultFooterProbeBytes(1024 * 1024)).toBe(FOOTER_PROBE_BYTES);
    expect(defaultFooterProbeBytes(4 * 1024 * 1024)).toBe(LARGE_FOOTER_PROBE_BYTES);

    const nowMs = Date.parse("2026-11-25T10:53:00Z");
    const openEnd = "2026-11-25T10:24:00Z";
    expect(readsWholeFirst({
      key: "processed/request/year=2026/month=11/day=25/hour=10/compact=1795601880-1795602240-c16-n8012-abc.parquet",
      end: openEnd,
      size: 1_200_000,
    }, nowMs)).toBe(true);
    expect(readsWholeFirst({
      key: "processed/request/year=2026/month=11/day=25/hour=10/compact=1795601520-1795602120-c32-n42943-abc.parquet",
      end: openEnd,
      size: 5 * 1024 * 1024,
    }, nowMs)).toBe(true);
    expect(readsWholeFirst({
      key: "processed/request/year=2026/month=11/day=25/hour=10/compact=1795601520-1795602120-c32-n42943-abc.parquet",
      end: openEnd,
      size: 6 * 1024 * 1024,
    }, nowMs)).toBe(true);
    expect(readsWholeFirst({
      key: "processed/request/year=2026/month=11/day=25/hour=10/compact=1795601520-1795602120-c32-n42943-abc.parquet",
      end: openEnd,
      size: 7 * 1024 * 1024,
    }, nowMs)).toBe(false);
    expect(readsWholeFirst({
      key: "processed/request/year=2026/month=11/day=25/hour=10/window=1795602240.parquet",
      end: openEnd,
      size: 1_200_000,
    }, nowMs)).toBe(false);
  });

  it("projects 11-15 columns via ranges on a ~12 MiB compact= file while reading 20+ columns with >=50% overlap whole-first", async () => {
    const rgRows = 500;
    const rowGroups = 16;
    const totalRows = rgRows * rowGroups;
    const startMs = Date.parse("2026-11-25T04:00:00Z");
    const stepMs = 1000;
    const endMs = startMs + totalRows * stepMs;
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(startMs + i * stepMs)) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
        { name: "cache", type: "STRING", data: Array.from({ length: totalRows }, () => "HIT") },
        { name: "resp_bytes", type: "INT32", data: Array.from({ length: totalRows }, () => 1024) },
        { name: "edge", type: "BOOLEAN", data: Array.from({ length: totalRows }, () => true) },
        { name: "pop", type: "STRING", data: Array.from({ length: totalRows }, () => "IAD") },
        { name: "country", type: "STRING", data: Array.from({ length: totalRows }, () => "US") },
        { name: "proto", type: "STRING", data: Array.from({ length: totalRows }, () => "HTTP/2") },
        { name: "tls", type: "STRING", data: Array.from({ length: totalRows }, () => "TLSv1.3") },
        { name: "method", type: "STRING", data: Array.from({ length: totalRows }, () => "GET") },
        { name: "host", type: "STRING", data: Array.from({ length: totalRows }, () => "example.com") },
        { name: "wide_url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(500)) },
        { name: "wide_ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(500)) },
        { name: "wide_referer", type: "STRING", data: Array.from({ length: totalRows }, () => noise(500)) },
      ],
    });
    expect(bytes.byteLength).toBeGreaterThan(11 * 1024 * 1024);
    expect(bytes.byteLength).toBeLessThan(14 * 1024 * 1024);
    const entry = {
      key: "processed/request/year=2026/month=11/day=25/hour=04/compact=1795579200-1795587200-c16-n8000-abc.parquet",
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
      size: bytes.byteLength,
      rows: totalRows,
      etag: "\"etag-12mib\"",
    };
    const narrow11Cols = [
      "timestamp", "status", "cache", "resp_bytes", "edge",
      "pop", "country", "proto", "tls", "method", "host",
    ];
    let rangeCalls = 0;
    let fileCalls = 0;
    const source = {
      cacheScope: "fixture-session:svc-12mib",
      fetchRange: async (_file: unknown, start: number, end: number) => {
        rangeCalls++;
        return bytes.slice(start, end);
      },
      fetchFile: async () => {
        fileCalls++;
        return { bytes: bytes.slice(0), cached: false };
      },
    };

    const read11 = projectedReader(source, narrow11Cols, undefined, { fromMs: startMs, toMs: endMs });
    const projected11 = await (await read11(entry)).read();
    expect(fileCalls).toBe(0);
    expect(rangeCalls).toBeGreaterThan(0);
    expect(projected11.downloaded).toBeLessThan(bytes.byteLength * 0.45);
    expect(await query({ "p11.parquet": projected11.bytes }, "SELECT count(*)::INT AS n FROM 'p11.parquet'"))
      .toEqual([{ n: totalRows }]);

    rangeCalls = 0;
    fileCalls = 0;
    const mid15Cols = [...narrow11Cols, "col12", "col13", "col14", "col15"];
    const read15 = projectedReader(source, mid15Cols, undefined, { fromMs: startMs, toMs: endMs });
    const projected15 = await (await read15(entry)).read();
    expect(fileCalls).toBe(0);
    expect(rangeCalls).toBeGreaterThan(0);
    expect(projected15.downloaded).toBeLessThan(bytes.byteLength * 0.45);

    rangeCalls = 0;
    fileCalls = 0;
    const wide20Cols = [
      ...mid15Cols, "col16", "col17", "col18", "col19", "col20",
    ];
    const read20 = projectedReader(source, wide20Cols, undefined, {
      fromMs: startMs + Math.floor((endMs - startMs) * 0.25),
      toMs: endMs,
    });
    const projected20 = await (await read20(entry)).read();
    expect(fileCalls).toBe(1);
    expect(rangeCalls).toBe(0);
    expect(projected20.downloaded).toBe(bytes.byteLength);
    expect(await query({ "p20.parquet": projected20.bytes }, "SELECT count(*)::INT AS n FROM 'p20.parquet'"))
      .toEqual([{ n: 6000 }]);
  });

  it("holds >2 MiB whole-file reads in projectedReader behind active summaries and caps concurrency at 2", async () => {
    const rgRows = 500;
    const rowGroups = 6;
    const totalRows = rgRows * rowGroups;
    const startMs = Date.parse("2026-11-25T11:00:00Z");
    const stepMs = 500;
    const endMs = startMs + totalRows * stepMs;
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(startMs + i * stepMs)) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
      ],
    });
    expect(bytes.byteLength).toBeGreaterThan(2 * 1024 * 1024);

    let inFlight = 0;
    let peak = 0;
    let startedWhileSummary = 0;
    let summaryActive = true;
    const priorities: (RequestPriority | undefined)[] = [];
    const source = {
      cacheScope: "fixture-session:svc-lanes",
      fetchRange: async (_file: unknown, start: number, end: number) => bytes.slice(start, end),
      fetchFile: async (_key: string, _signal?: AbortSignal, mode?: { priority?: RequestPriority }) => {
        if (summaryActive) startedWhileSummary++;
        priorities.push(mode?.priority);
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise((r) => setTimeout(r, 15));
        inFlight--;
        return { bytes: bytes.slice(0), cached: false };
      },
    };
    const entries = Array.from({ length: 4 }, (_, i) => ({
      key: `processed/request/year=2026/month=11/day=25/hour=11/compact=c4-n${i}.parquet`,
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
      size: bytes.byteLength,
      rows: totalRows,
      etag: `"etag-${i}"`,
    }));

    let releaseSummary!: () => void;
    const summaryPromise = fetchLanes.run("summary", () => new Promise<void>((resolve) => {
      releaseSummary = () => {
        summaryActive = false;
        resolve();
      };
    }));
    const reader = projectedReader(source, ["timestamp", "status"]);
    const readsPromise = Promise.all(entries.map(async (entry) => (await reader(entry)).read()));

    await new Promise((r) => setTimeout(r, 20));
    expect(startedWhileSummary).toBe(0);
    releaseSummary();
    await summaryPromise;
    await readsPromise;

    expect(peak).toBe(2);
    expect(priorities).toEqual(["low", "low", "low", "low"]);
  });

  it("effectiveProjectedColumnCount collapses mutually-aliased schema names while keeping raw names for projection", () => {
    expect(effectiveProjectedColumnCount([
      "timestamp", "ts_ms",
      "cache_status", "cache",
      "resp_body_size", "resp_body_bytes", "resp_bytes",
      "time_elapsed_usec", "elapsed",
      "fastly_is_shield", "is_shield", "edge",
      "protocol", "proto",
      "is_ssl", "tls_version", "tls",
      "is_ipv6", "ipv6",
      "content_type", "resp_header_content_type",
      "waf_action", "ngwaf_decision",
      "bot_verified", "verified_bot",
      "_bot_name", "ngwaf_bot_name", "bot_category",
      "io_output_format", "ofmt",
      "io_input_format", "ifmt",
    ])).toBe(14);

    const summaryFilteredCols = valueScanColumns({
      sections: ["overview", "caching", "network"],
      filters: { status: { mode: "include", values: ["200"] } },
    })!;
    expect(summaryFilteredCols.length).toBe(25);
    expect(effectiveProjectedColumnCount(summaryFilteredCols)).toBe(13);

    const splitSummaryRawCols = valueScanColumns({
      sections: ["caching", "network"],
      filters: { status: { mode: "include", values: ["200"] } },
    })!;
    expect(splitSummaryRawCols.length).toBe(19);
    expect(effectiveProjectedColumnCount(splitSummaryRawCols)).toBe(9);

    const fullSevenSectionCols = valueScanColumns({})!;
    expect(fullSevenSectionCols.length).toBe(41);
    expect(effectiveProjectedColumnCount(fullSevenSectionCols)).toBe(23);

    const startMs = Date.parse("2026-11-25T04:00:00Z");
    const endMs = Date.parse("2026-11-25T05:00:00Z");
    const compact20Mib = {
      size: 20 * 1024 * 1024,
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
    };
    const window = { fromMs: startMs, toMs: endMs };
    expect(prefersWholeForWideProjection(compact20Mib, effectiveProjectedColumnCount(summaryFilteredCols), window)).toBe(false);
    expect(prefersWholeForWideProjection(compact20Mib, effectiveProjectedColumnCount(splitSummaryRawCols), window)).toBe(false);
    expect(prefersWholeForWideProjection(compact20Mib, effectiveProjectedColumnCount(fullSevenSectionCols), window)).toBe(true);
  });

  it("keeps >6 MiB compacts projected via ranges for filtered Summary valueScanColumns while preserving aliases", async () => {
    const prevClosedLimit = wholeReadLimits.closedHourUpToBytes;
    wholeReadLimits.closedHourUpToBytes = 2 * 1024 * 1024;
    try {
      const rgRows = 500;
      const rowGroups = 6;
      const totalRows = rgRows * rowGroups;
      const startMs = Date.parse("2026-11-25T04:00:00Z");
      const endMs = startMs + totalRows * 1000;
      const bytes = parquetWriteBuffer({
        codec: "UNCOMPRESSED",
        rowGroupSize: rgRows,
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(startMs + i * 1000)) },
          { name: "status", type: "INT32", data: Array.from({ length: totalRows }, () => 200) },
          { name: "cache_status", type: "STRING", data: Array.from({ length: totalRows }, () => "HIT") },
          { name: "resp_body_size", type: "INT32", data: Array.from({ length: totalRows }, () => 512) },
          { name: "time_elapsed_usec", type: "INT32", data: Array.from({ length: totalRows }, () => 1200) },
          { name: "protocol", type: "STRING", data: Array.from({ length: totalRows }, () => "HTTP/2") },
          { name: "tls_version", type: "STRING", data: Array.from({ length: totalRows }, () => "TLSv1.3") },
          { name: "ipv6", type: "BOOLEAN", data: Array.from({ length: totalRows }, () => false) },
          { name: "fastly_is_shield", type: "BOOLEAN", data: Array.from({ length: totalRows }, () => false) },
          { name: "waf_action", type: "STRING", data: Array.from({ length: totalRows }, () => "ALLOW") },
          { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
          { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(400)) },
        ],
      });
      expect(bytes.byteLength).toBeGreaterThan(2 * 1024 * 1024);
      const entry = {
        key: "processed/request/year=2026/month=11/day=25/hour=04/compact=summary-filtered.parquet",
        start: new Date(startMs).toISOString(),
        end: new Date(endMs).toISOString(),
        size: bytes.byteLength,
        rows: totalRows,
        etag: "\"etag-summary\"",
      };
      const summaryCols = valueScanColumns({
        sections: ["overview", "caching", "network"],
        filters: { status: { mode: "include", values: ["200"] } },
      })!;
      let rangeCalls = 0;
      let fileCalls = 0;
      const source = {
        cacheScope: "fixture-session:svc-value-summary",
        fetchRange: async (_file: unknown, start: number, end: number) => {
          rangeCalls++;
          return bytes.slice(start, end);
        },
        fetchFile: async () => {
          fileCalls++;
          return { bytes: bytes.slice(0), cached: false };
        },
      };

      const reader = projectedReader(source, summaryCols, undefined, { fromMs: startMs, toMs: endMs });
      const projected = await (await reader(entry)).read();
      expect(fileCalls).toBe(0);
      expect(rangeCalls).toBeGreaterThan(0);
      expect(projected.downloaded).toBeLessThan(bytes.byteLength * 0.45);
      // Legacy alias columns in the file must still be retained in the projection.
      expect(await query({ "v.parquet": projected.bytes },
        "SELECT count(*)::INT AS n, any_value(cache_status) AS cs, any_value(protocol) AS proto FROM 'v.parquet'"))
        .toEqual([{ n: totalRows, cs: "HIT", proto: "HTTP/2" }]);
    } finally {
      wholeReadLimits.closedHourUpToBytes = prevClosedLimit;
    }
  });

  it("avoids 10-15 range HTTP 206 fragmentation on >8 MiB multi-row-group compact= files", async () => {
    const mediumLargeFile = 10 * 1024 * 1024;
    expect(shouldFallbackToWhole(
      { ranges: Array.from({ length: 14 }, (_, i) => ({ start: i * 100, end: i * 100 + 50 })) },
      mediumLargeFile,
      Math.ceil(mediumLargeFile * 0.40),
    )).toBe(true);

    // 14 row groups in a >8 MiB compact= file with ~45% needed columns and >256 KiB gaps:
    // previously 14 < WHOLE_FALLBACK_HIGH_FANOUT_MIN (16) and size > 8 MiB left 14 HTTP 206s;
    // now shouldFallbackToWhole triggers a single shared fetchFile download.
    const rgRows = 650;
    const rowGroups = 14;
    const totalRows = rgRows * rowGroups;
    const startMs = Date.parse("2026-11-26T04:00:00Z");
    const bytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: rgRows,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: totalRows }, (_, i) => new Date(startMs + i * 1000)) },
        { name: "status", type: "INT32", data: Array.from({ length: totalRows }, (_, i) => (i % 10 === 0 ? 503 : 200)) },
        { name: "host", type: "STRING", data: Array.from({ length: totalRows }, () => noise(440)) },
        { name: "cache", type: "STRING", data: Array.from({ length: totalRows }, () => "HIT") },
        { name: "ua", type: "STRING", data: Array.from({ length: totalRows }, () => noise(240)) },
        { name: "url", type: "STRING", data: Array.from({ length: totalRows }, () => noise(240)) },
      ],
    });
    expect(bytes.byteLength).toBeGreaterThan(8 * 1024 * 1024);
    expect(bytes.byteLength).toBeLessThan(16 * 1024 * 1024);

    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status", "host", "cache"]));
    const plannedBytes = plan.ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
    expect(plan.ranges.length).toBe(14);
    expect(shouldFallbackToWhole(plan, bytes.byteLength, plannedBytes)).toBe(true);

    const entry = {
      key: "processed/request/year=2026/month=11/day=26/hour=04/compact=1795665600-1795669200-c14-n9100-abc.parquet",
      start: new Date(startMs).toISOString(),
      end: new Date(Date.now() + 60_000).toISOString(),
      size: bytes.byteLength,
      rows: totalRows,
      etag: "\"etag-14range\"",
    };
    let dataRangeCalls = 0;
    let fileCalls = 0;
    const source = {
      cacheScope: "fixture-session:svc-14range",
      fetchRange: async (_file: unknown, start: number, end: number) => {
        if (end < bytes.byteLength) dataRangeCalls++;
        return bytes.slice(start, end);
      },
      fetchFile: async () => {
        fileCalls++;
        return { bytes: bytes.slice(0), cached: false };
      },
    };
    const read = projectedReader(source, ["timestamp", "status", "host", "cache"]);
    const projected = await (await read(entry)).read();
    expect(fileCalls).toBe(1);
    expect(dataRangeCalls).toBe(0);
    expect(await query({ "c.parquet": projected.bytes },
      "SELECT count(*)::INT AS n, count(*) FILTER (WHERE status = 503)::INT AS errors FROM 'c.parquet'"))
      .toEqual([{ n: totalRows, errors: totalRows / 10 }]);
  });
});
