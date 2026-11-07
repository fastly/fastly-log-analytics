// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { createNodeBackend } from "./duckdb";
import {
  assembleProjectedParquet, fetchProjectedRanges, FOOTER_PROBE_BYTES, planParquetProjection, readParquetFooter,
  RANGE_MERGE_GAP,
} from "./parquet-projection";

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

  it("rejects a footer whose chunk offsets fall outside the file", async () => {
    const bytes = fixture();
    const footer = await readParquetFooter(bytes.byteLength, ranged(bytes).fetchRange);
    expect(() => planParquetProjection({ ...footer, size: footer.footerLength + 100 }, new Set(["url"])))
      .toThrow(/outside the file/);
  });
});
