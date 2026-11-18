// @vitest-environment node
import { parquetMetadata } from "hyparquet";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { ManifestFile } from "./manifest";
import { readColumns } from "./parquet";
import { loadSummaryFixtures } from "./summary-fixtures";
import { LAYOUT_B, withSummaryLayout } from "./summary-layout-fixture";
import {
  defineSummarySpec, hasSummaryProjection, readSummaryColumns, readSummaryProjection, summaryLayout,
  summaryProjectionLimits, type SummarySpec,
} from "./summary-projection";

const DASH = loadSummaryFixtures("../dashboard/__fixtures__/summaries");
const SOURCES = [...DASH.files("hour"), ...DASH.files("day")].map((file) => DASH.buffers[file.key]);
const KEY = "processed/summary/request/year=2027/month=01/day=15/hour=08/hour_bundle=08-c9-n9-h0.parquet";
const DEFAULT_LIMITS = { ...summaryProjectionLimits };

let layoutBytes: ArrayBuffer;
let legacyBytes: ArrayBuffer;
beforeAll(async () => {
  layoutBytes = await withSummaryLayout(SOURCES);
  legacyBytes = await withSummaryLayout(SOURCES, null);
}, 60_000);
afterEach(() => Object.assign(summaryProjectionLimits, DEFAULT_LIMITS));

/** Projection without the small-file and share rules, which these few-KiB fixtures would trip. */
const anySize = () => Object.assign(summaryProjectionLimits, { wholeBelowBytes: 0, wholeAboveShare: 1 });

const manifestFile = (bytes: ArrayBuffer): ManifestFile => ({
  key: KEY, start: "2027-01-15T08:00:00.000Z", end: "2027-01-15T09:00:00.000Z", size: bytes.byteLength, rows: 1,
});

/** A source with the browser source's caches: whole files, derived files, and the network beneath them. */
function fakeSource(bytes: ArrayBuffer) {
  const net = { whole: [] as string[], ranges: [] as [number, number][], priorities: [] as (string | undefined)[] };
  const wholeCache = new Map<string, ArrayBuffer>();
  const derived = new Map<string, { bytes: ArrayBuffer; meta: string }>();
  return {
    net,
    derived,
    cacheScope: "test",
    async fetchJson(): Promise<unknown> {
      throw new Error("unused");
    },
    async fetchFile(key: string) {
      const hit = wholeCache.get(key);
      if (hit) return { bytes: hit.slice(0), cached: true };
      net.whole.push(key);
      wholeCache.set(key, bytes.slice(0));
      return { bytes: bytes.slice(0), cached: false };
    },
    async hasFile(key: string) {
      return wholeCache.has(key);
    },
    async fetchRange(file: ManifestFile, start: number, end: number, _signal?: AbortSignal, priority?: string) {
      expect(file.key).toBe(KEY);
      net.ranges.push([start, end]);
      net.priorities.push(priority);
      return bytes.slice(start, end);
    },
    async readDerivedFile(key: string, identity: string) {
      const hit = derived.get(`${key}|${identity}`);
      return hit ? { byteLength: hit.bytes.byteLength, meta: hit.meta, bytes: async () => hit.bytes.slice(0) } : null;
    },
    async writeDerivedFile(key: string, identity: string, data: ArrayBuffer, meta: string) {
      derived.set(`${key}|${identity}`, { bytes: data.slice(0), meta });
    },
  };
}

const text = (value: unknown) => (value instanceof Uint8Array ? new TextDecoder().decode(value) : String(value));
const metaOf = (bytes: ArrayBuffer) => parquetMetadata(bytes);
const names = (bytes: ArrayBuffer) => metaOf(bytes).schema.slice(1).map((element) => element.name);

/** Byte span of one row group, and of one of its column chunks. */
function spans(bytes: ArrayBuffer, rowGroup: number) {
  const chunks = metaOf(bytes).row_groups[rowGroup].columns.map((chunk) => {
    const meta = chunk.meta_data!;
    const data = Number(meta.data_page_offset);
    const dict = meta.dictionary_page_offset === undefined ? NaN : Number(meta.dictionary_page_offset);
    const start = dict >= 4 && dict < data ? dict : data;
    return { name: meta.path_in_schema[0], start, end: start + Number(meta.total_compressed_size) };
  });
  return {
    start: Math.min(...chunks.map((c) => c.start)),
    end: Math.max(...chunks.map((c) => c.end)),
    chunk: (name: string) => chunks.find((c) => c.name === name)!,
  };
}

const overlaps = ([start, end]: [number, number], span: { start: number; end: number }) =>
  start < span.end && span.start < end;

/** Ranges past the footer probe (the probe is the read ending at the file's end). */
const dataRanges = (net: { ranges: [number, number][] }, size: number) => net.ranges.filter(([, end]) => end !== size);

/** The rows of `spec`'s groups (a `dim_totals` row by the group it names), as plain objects. */
async function groupRows(parts: readonly ArrayBuffer[], spec: SummarySpec, columns: string[]) {
  const { rows, columns: decoded } = await readSummaryColumns(parts, columns);
  const out: Record<string, unknown>[] = [];
  for (let i = 0; i < rows; i++) {
    const row = Object.fromEntries(columns.map((name) => [name, decoded.get(name)?.[i] ?? null]));
    const group = String(row.bundle === "dim_totals" ? row.value : row.bundle);
    if (spec.groups.has(String(row.bundle)) && (row.bundle !== "dim_totals" || spec.groups.has(group))) out.push(row);
  }
  return out;
}

const HEAD = defineSummarySpec({ groups: ["overview", "time_series"], columns: ["requests"] });
const DIMS = defineSummarySpec({ groups: ["dims", "dim_totals"], columns: ["requests", "count"] });
const FIELDS = defineSummarySpec({ groups: ["all_fields", "field_totals", "series5", "dim_totals"], columns: ["count", "requests"] });

describe("readSummaryProjection", () => {
  it("(a) fetches only the chunks of the row groups and columns a spec reads", async () => {
    anySize();
    const source = fakeSource(layoutBytes);
    const read = await readSummaryProjection(source, manifestFile(layoutBytes), HEAD);
    expect(read.whole).toBe(false);
    expect(read.parts).toHaveLength(1);
    expect(metaOf(read.parts[0]).row_groups).toHaveLength(1);
    expect(names(read.parts[0])).toEqual(["bundle", "bucket", "field", "value", "truncated", "requests"]);
    const head = spans(layoutBytes, 0);
    const data = dataRanges(source.net, layoutBytes.byteLength);
    expect(data.length).toBeGreaterThan(0);
    for (const range of data) expect(range[0] >= head.start && range[1] <= head.end).toBe(true);
    const fetched = data.reduce((sum, [start, end]) => sum + end - start, 0);
    expect(fetched).toBeLessThan((head.end - head.start) / 2);
    expect(source.net.whole).toEqual([]);
    expect(new Set(source.net.priorities)).toEqual(new Set(["high"]));
    expect(read.downloadedBytes).toBe(source.net.ranges.reduce((sum, [start, end]) => sum + end - start, 0));
  });

  it("(b) projects only columns from a file without a layout, keeping every row", async () => {
    anySize();
    const source = fakeSource(legacyBytes);
    const read = await readSummaryProjection(source, manifestFile(legacyBytes), DIMS);
    expect(read.whole).toBe(false);
    expect(read.parts).toHaveLength(1);
    expect(Number(metaOf(read.parts[0]).num_rows)).toBe(Number(metaOf(legacyBytes).num_rows));
    expect(names(read.parts[0])).toEqual(["bundle", "bucket", "field", "value", "count", "truncated", "requests"]);
    const sketch = spans(legacyBytes, 0).chunk("ip_sketch");
    for (const range of dataRanges(source.net, legacyBytes.byteLength)) expect(overlaps(range, sketch)).toBe(false);
    expect(await groupRows(read.parts, DIMS, ["bundle", "field", "value", "requests"]))
      .toEqual(await groupRows([legacyBytes], DIMS, ["bundle", "field", "value", "requests"]));
  });

  it("(c) serves a cached whole file without any network call", async () => {
    anySize();
    const source = fakeSource(layoutBytes);
    await source.fetchFile(KEY);
    source.net.whole.length = 0;
    const read = await readSummaryProjection(source, manifestFile(layoutBytes), HEAD);
    expect(read).toMatchObject({ whole: true, cached: true, downloadedBytes: 0 });
    expect(new Uint8Array(read.parts[0])).toEqual(new Uint8Array(layoutBytes));
    expect(source.net).toEqual({ whole: [], ranges: [], priorities: [] });
  });

  it("(d) never stores a projection where the whole file is looked up", async () => {
    anySize();
    const source = fakeSource(layoutBytes);
    await readSummaryProjection(source, manifestFile(layoutBytes), HEAD);
    expect(await source.hasFile(KEY)).toBe(false);
    const whole = await source.fetchFile(KEY);
    expect(source.net.whole).toEqual([KEY]);
    expect(whole.cached).toBe(false);
    expect(new Uint8Array(whole.bytes)).toEqual(new Uint8Array(layoutBytes));
  });

  it("(e) reads spec A then spec B of one file exactly as the whole file, with one footer read and warm repeats", async () => {
    anySize();
    const source = fakeSource(layoutBytes);
    const file = manifestFile(layoutBytes);
    const columns = ["bundle", "bucket", "field", "value", "truncated", "count", "requests"];
    for (const spec of [DIMS, FIELDS]) {
      const read = await readSummaryProjection(source, file, spec);
      expect(read.whole).toBe(false);
      const want = await groupRows([layoutBytes], spec, columns.filter((name) => name !== "count" || spec.columns.has(name)));
      expect(want.length).toBeGreaterThan(10);
      expect(await groupRows(read.parts, spec, columns.filter((name) => name !== "count" || spec.columns.has(name))))
        .toEqual(want);
    }
    expect(source.net.ranges.filter(([, end]) => end === layoutBytes.byteLength)).toHaveLength(1);
    const before = source.net.ranges.length;
    const again = await readSummaryProjection(source, file, DIMS);
    expect(again).toMatchObject({ whole: false, cached: true, downloadedBytes: 0 });
    expect(source.net.ranges).toHaveLength(before);
  });

  it("reads a column for the row groups of the groups that need it only", async () => {
    anySize();
    const spec = defineSummarySpec({
      groups: ["overview", "time_series", "field_totals", "all_fields"],
      columns: ["count", "requests", "ip_sketch"],
      only: { ip_sketch: ["overview"] },
    });
    const source = fakeSource(layoutBytes);
    const read = await readSummaryProjection(source, manifestFile(layoutBytes), spec);
    expect(read.parts.map((part) => names(part).includes("ip_sketch"))).toEqual([true, false]);
    const fieldsSketch = spans(layoutBytes, LAYOUT_B.findIndex((groups) => groups.includes("all_fields"))).chunk("ip_sketch");
    for (const range of dataRanges(source.net, layoutBytes.byteLength)) expect(overlaps(range, fieldsSketch)).toBe(false);
    const projected = await readSummaryColumns(read.parts, ["bundle", "ip_sketch"], false);
    const whole = await readColumns(layoutBytes, ["bundle", "ip_sketch"], false);
    const sketches = (columns: Map<string, ArrayLike<unknown>>, rows: number) =>
      Array.from({ length: rows }, (_, i) => i).filter((i) => columns.get("bundle")![i] !== undefined)
        .flatMap((i) => (text(columns.get("bundle")![i]) === "overview" ? [columns.get("ip_sketch")![i]] : []));
    expect(sketches(projected.columns, projected.rows)).toEqual(sketches(whole.columns, whole.rows));
    expect(sketches(projected.columns, projected.rows).length).toBeGreaterThan(0);
  });

  it("knows a spec of a file is local once projected or cached whole, and only then", async () => {
    anySize();
    const file = manifestFile(layoutBytes);
    const source = fakeSource(layoutBytes);
    expect(await hasSummaryProjection(source, file, HEAD)).toBe(false);
    await readSummaryProjection(source, file, HEAD);
    expect(await hasSummaryProjection(source, file, HEAD)).toBe(true);
    expect(await hasSummaryProjection(source, file, DIMS)).toBe(false);
    expect(await hasSummaryProjection(source, { ...file, size: file.size + 1 }, HEAD)).toBe(false);
    await source.fetchFile(KEY);
    expect(await hasSummaryProjection(source, file, DIMS)).toBe(true);
  });

  it("reads small files, and files a projection would mostly cover, whole", async () => {
    const small = fakeSource(DASH.buffers[DASH.files("hour")[0].key]);
    const smallFile = { ...manifestFile(SOURCES[0]), size: SOURCES[0].byteLength };
    expect((await readSummaryProjection(small, smallFile, HEAD)).whole).toBe(true);
    expect(small.net.ranges).toEqual([]);
    Object.assign(summaryProjectionLimits, { wholeBelowBytes: 0 });
    const all = names(layoutBytes).filter((name) => !["bundle", "bucket", "field", "value", "truncated"].includes(name));
    const source = fakeSource(layoutBytes);
    const read = await readSummaryProjection(source, manifestFile(layoutBytes),
      defineSummarySpec({ groups: LAYOUT_B.flat(), columns: all }));
    expect(read.whole).toBe(true);
    expect(source.net.whole).toEqual([KEY]);
  });
});

describe("summaryLayout", () => {
  const meta = (value: string | undefined, rowGroups = 2) => ({
    key_value_metadata: value === undefined ? [] : [{ key: "flp_summary_layout", value }],
    row_groups: Array.from({ length: rowGroups }, () => ({})),
  });

  it("reads the groups of each row group, and treats anything else as one row group of every group", () => {
    expect(summaryLayout(meta(JSON.stringify({ v: 1, rg: [["overview"], ["dims", "x"]] }))))
      .toEqual([new Set(["overview"]), new Set(["dims", "x"])]);
    for (const bad of [undefined, "nope", "{}", JSON.stringify({ v: 2, rg: [["a"], ["b"]] }),
      JSON.stringify({ v: 1, rg: [["a"]] }), JSON.stringify({ v: 1, rg: [["a"], [1]] })]) {
      expect(summaryLayout(meta(bad))).toBeNull();
    }
  });
});
