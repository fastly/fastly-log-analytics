// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { runDashboard, type DashboardResult, type EngineOptions, type FileSource } from "./engine";
import { getDuckDBEngine } from "./duckdb";
import type { DashboardFilter } from "./filters";
import type { FilteredFileResult } from "./filtered-cache";
import type { ManifestFile } from "./manifest";
import { fixtureSource, parquetFixture, type FixtureRow } from "./test-fixtures";

const T0 = Date.parse("2026-10-29T10:00:00Z");
const at = (minutes: number) => new Date(T0 + minutes * 60_000).toISOString();
const row = (seconds: number, extra: Partial<FixtureRow> = {}): FixtureRow => ({
  timestamp: T0 + seconds * 1000,
  status: 200,
  cache: "HIT",
  resp_bytes: 100,
  host: "a.example",
  country: "US",
  ...extra,
});
const us: DashboardFilter = { mode: "include", field: "country", value: "US" };

type Window = { key: string; start: number; end: number; rows: FixtureRow[] };

const windows: Window[] = [
  {
    key: "a",
    start: 0,
    end: 1,
    rows: [
      row(10),
      row(20, { country: "DE", status: 404 }),
      row(50, { host: "b.example", status: 503, cache: "MISS", tenant: "acme" }),
    ],
  },
  {
    key: "b",
    start: 1,
    end: 2,
    rows: [
      row(70, { tenant: "acme" }),
      row(80, { country: "DE", tenant: "globex", resp_bytes: null }),
      // Logged late: outside the file's own window.
      row(150, { host: "c.example" }),
    ],
  },
  {
    key: "c",
    start: 2,
    end: 3,
    rows: [row(130, { status: 404, tenant: "acme" }), row(170, { country: null })],
  },
  {
    key: "d",
    start: 3,
    end: 4,
    rows: [row(190, { host: "b.example", tenant: "globex" }), row(200, { country: "DE" })],
  },
];

async function buffersFor(list: readonly Window[]) {
  return Object.fromEntries(
    await Promise.all(list.map(async (w) => [w.key, await parquetFixture(w.rows)] as const)),
  ) as Record<string, ArrayBuffer>;
}

type Store = Map<string, FilteredFileResult>;

async function selectiveSource(list: readonly Window[], store?: Store) {
  const buffers = await buffersFor(list);
  const files: ManifestFile[] = list.map((w) => ({
    key: w.key,
    start: at(w.start),
    end: at(w.end),
    rows: w.rows.length,
    size: buffers[w.key].byteLength,
    etag: `"${w.key}-v1"`,
  }));
  const base = fixtureSource([{ files, next_cursor: null, complete: true }], buffers);
  const opened: string[] = [];
  const source: FileSource & { opened: string[] } = {
    ...base,
    opened,
    readRollup: undefined,
    writeRollup: undefined,
    openParquet: async (file) => {
      opened.push(file.key);
      return { kind: "buffer", bytes: buffers[file.key], cached: false };
    },
    ...(store
      ? {
          readFilteredResult: async (id: string) => store.get(id) ?? null,
          writeFilteredResult: async (id: string, result: FilteredFileResult) => {
            store.set(id, structuredClone(result));
          },
        }
      : {}),
  };
  return source;
}

// The map's value list is unordered; everything else must match exactly.
const withoutStats = ({ stats: _stats, ...rest }: DashboardResult) => ({
  ...rest,
  cards: Object.fromEntries(
    Object.entries(rest.cards).map(([field, card]) => [
      field,
      card.all
        ? { ...card, all: [...card.all].sort((a, b) => String(a.value).localeCompare(String(b.value))) }
        : card,
    ]),
  ),
});

async function freshScan(list: readonly Window[], options: EngineOptions) {
  return withoutStats(await runDashboard(await selectiveSource(list), options));
}

const range = (from: number, to: number, extra: Partial<EngineOptions> = {}): EngineOptions => ({
  from: at(from),
  to: at(to),
  filters: [us],
  ...extra,
});

describe("filtered result cache", () => {
  it("scans only files without a cached filtered result and merges to the fresh scan", async () => {
    const store: Store = new Map();
    const cold = await selectiveSource(windows.slice(0, 3), store);
    const first = await runDashboard(cold, range(0, 10));
    expect(cold.opened.sort()).toEqual(["a", "b", "c"]);
    expect(withoutStats(first)).toEqual(await freshScan(windows.slice(0, 3), range(0, 10)));
    expect(store.size).toBe(3);

    // A new page load: same filter and range, one new file.
    const warm = await selectiveSource(windows, store);
    const query = vi.spyOn(getDuckDBEngine(), "queryParquetPerFile");
    try {
      const second = await runDashboard(warm, range(0, 10));
      expect(warm.opened).toEqual(["d"]);
      expect(query.mock.calls.flatMap(([files]) => files)).toHaveLength(1);
      const fresh = await freshScan(windows, range(0, 10));
      expect(withoutStats(second)).toEqual(fresh);
      expect(second.totalRequests).toBe(6);
      expect(second.stats).toMatchObject({ cached: 3, fetched: 1 });
    } finally {
      query.mockRestore();
    }
  });

  it("still coalesces filtered files that arrive while DuckDB is busy", async () => {
    const source = await selectiveSource(windows, new Map());
    const query = vi.spyOn(getDuckDBEngine(), "queryParquetPerFile");
    try {
      await runDashboard(source, range(0, 10));
      expect(query.mock.calls.flatMap(([files]) => files)).toHaveLength(windows.length);
      expect(query.mock.calls.length).toBeLessThan(windows.length);
    } finally {
      query.mockRestore();
    }
  });

  it.each([
    ["another filter value", range(0, 10, { filters: [{ ...us, value: "DE" }] })],
    ["an exclude filter", range(0, 10, { filters: [{ ...us, mode: "exclude" }] })],
    ["edge-only mode", range(0, 10, { edgeOnly: true })],
    ["another custom card", range(0, 10, { customFields: [{ id: "tenant", type: "string" }] })],
  ])("never reuses a cached result for %s", async (_name, options) => {
    const store: Store = new Map();
    await runDashboard(await selectiveSource(windows, store), range(0, 10));
    const again = await selectiveSource(windows, store);
    const result = await runDashboard(again, options);
    expect(again.opened.sort()).toEqual(["a", "b", "c", "d"]);
    expect(withoutStats(result)).toEqual(await freshScan(windows, options));
  });

  it("keys filters on non-card custom fields, so a no-match value never answers a match", async () => {
    const store: Store = new Map();
    const tenant = (value: string) =>
      range(0, 10, {
        filters: [{ mode: "include", field: "tenant", value }],
        filterFields: [{ id: "tenant", type: "string" }],
      });
    const none = await runDashboard(await selectiveSource(windows, store), tenant("nope"));
    expect(none.totalRequests).toBe(0);
    const again = await selectiveSource(windows, store);
    const acme = await runDashboard(again, tenant("acme"));
    expect(again.opened.sort()).toEqual(["a", "b", "c", "d"]);
    expect(acme.totalRequests).toBe(3);
    expect(withoutStats(acme)).toEqual(await freshScan(windows, tenant("acme")));
    const warm = await selectiveSource(windows, store);
    expect(withoutStats(await runDashboard(warm, tenant("acme")))).toEqual(withoutStats(acme));
    expect(warm.opened).toEqual([]);
  });

  it("clips files that straddle the range and reuses whole-file results only inside it", async () => {
    const store: Store = new Map();
    // [0:30, 10:00): "a" straddles the start, the rest lie inside.
    const shifted = range(0.5, 10);
    const first = await selectiveSource(windows, store);
    expect(withoutStats(await runDashboard(first, shifted))).toEqual(await freshScan(windows, shifted));
    const second = await selectiveSource(windows, store);
    expect(withoutStats(await runDashboard(second, shifted))).toEqual(await freshScan(windows, shifted));
    expect(second.opened).toEqual(["a"]);

    // [1:30, 10:00): "b" now straddles, so its cached whole-file result is not used.
    const later = range(1.5, 10);
    const third = await selectiveSource(windows, store);
    expect(withoutStats(await runDashboard(third, later))).toEqual(await freshScan(windows, later));
    expect(third.opened).toEqual(["b"]);
  });

  it("rescans a file whose cached rows run past the range end", async () => {
    const store: Store = new Map();
    await runDashboard(await selectiveSource(windows, store), range(0, 10));
    // "b" lies inside [0, 2:00) by its window, but logged a row at 2:30.
    const ending = range(0, 2);
    const warm = await selectiveSource(windows, store);
    const result = await runDashboard(warm, ending);
    expect(withoutStats(result)).toEqual(await freshScan(windows, ending));
    expect(warm.opened).toEqual(["b"]);
  });
});
