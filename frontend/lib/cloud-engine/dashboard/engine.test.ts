// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { Aggregator, runDashboard, searchFieldValues } from "./engine";
import { getDuckDBEngine } from "./duckdb";
import {
  discoverPresets,
  discoverPresetsWithManifest,
  buildSeries,
  presetRange,
} from "./range";
import { fetchManifest } from "./manifest";
import { readColumns } from "./parquet";
import { parseFilters, serializeFilter, type DashboardFilter } from "./filters";
import {
  fixtureSource,
  manifestEntry,
  parquetFixture,
  type FixtureRow,
} from "./test-fixtures";

const T0 = Date.parse("2026-10-29T10:00:00Z");
const row = (offsetS: number, extra: Partial<FixtureRow> = {}): FixtureRow => ({
  timestamp: T0 + offsetS * 1000,
  status: 200,
  cache: "HIT",
  resp_bytes: 100,
  host: "a.example",
  country: "US",
  ...extra,
});

describe("runDashboard", () => {
  it("hands the complete manifest and range to the source's cache pruner", async () => {
    const page = { files: [], next_cursor: null, complete: true };
    const source = fixtureSource([page], {});
    const pruneCache = vi.fn(async () => {});
    await runDashboard(
      { ...source, pruneCache },
      { from: "2026-10-29T10:00:00Z", to: "2026-10-29T11:00:00Z" },
    );
    expect(pruneCache).toHaveBeenCalledWith(
      { files: [], complete: true },
      Date.parse("2026-10-29T10:00:00Z"),
      Date.parse("2026-10-29T11:00:00Z"),
    );
  });

  const rows = [
    row(5),
    row(10, { cache: "MISS", host: "b.example", resp_bytes: 50 }),
    row(70, { status: 404, country: null }),
    row(75, { status: 503, cache: null, resp_bytes: null }),
    row(7200),
  ];

  it("decodes real Parquet bytes and aggregates exactly within the range", async () => {
    const key = "pq/a.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              key,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              5,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      { [key]: await parquetFixture(rows) },
    );
    const result = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
    });
    expect(result.totalRequests).toBe(4);
    expect(result.buckets).toEqual([
      {
        t: T0,
        requests: 2,
        bytes: 150,
        s4xx: 0,
        s5xx: 0,
        hits: 1,
        cacheRows: 2,
      },
      {
        t: T0 + 60_000,
        requests: 2,
        bytes: 100,
        s4xx: 1,
        s5xx: 1,
        hits: 1,
        cacheRows: 1,
      },
    ]);
    expect(result.cards.host).toEqual({
      total: 4,
      distinct: 2,
      top: [
        { value: "a.example", count: 3 },
        { value: "b.example", count: 1 },
      ],
    });
    expect(result.cards.country.total).toBe(3);
    expect(result.cards.edge.total).toBe(0);
    expect(result.cards.url.total).toBe(0);
    expect(result.stats).toMatchObject({
      listed: 1,
      fetched: 1,
      cached: 0,
      rows: 5,
    });
  });

  it("keeps 16 file fetches in flight by default", async () => {
    const keys = Array.from({ length: 20 }, (_, i) => `pq/${i}.parquet`);
    const bytes = await parquetFixture([row(5)]);
    const source = fixtureSource(
      [
        {
          files: keys.map((k) =>
            manifestEntry(k, "2026-10-29T10:00:00Z", "2026-10-29T10:05:00Z", 1),
          ),
          next_cursor: null,
          complete: true,
        },
      ],
      Object.fromEntries(keys.map((k) => [k, bytes])),
    );
    let inFlight = 0;
    let peak = 0;
    const fetchFile: typeof source.fetchFile = async (...args) => {
      peak = Math.max(peak, ++inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight--;
      return source.fetchFile(...args);
    };
    await runDashboard(
      { ...source, fetchFile },
      { from: "2026-10-29T10:00:00Z", to: "2026-10-29T10:03:00Z" },
    );
    expect(peak).toBe(16);
  });

  it("treats a missing column as no samples and counts cached files", async () => {
    const buffers = {
      a: await parquetFixture(rows, ["host"]),
      b: await parquetFixture([row(20)]),
    };
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              "a",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              5,
            ),
            manifestEntry(
              "b",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              1,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      buffers,
    );
    const first = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
      concurrency: 1,
    });
    expect(first.cards.host.total).toBe(1);
    const second = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
    });
    expect(second.stats).toMatchObject({ fetched: 0, cached: 2 });
  });

  it("counts every HIT* CDN cache state as a CDN hit", async () => {
    const states = ["HIT", "HIT-STALE", "MISS", null];
    const keys = states.map((_, i) => `cdn-${i}`);
    const bytes = await parquetFixture([row(20)]);
    const source = fixtureSource(
      [
        {
          files: keys.map((key) =>
            manifestEntry(
              key,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              1,
            ),
          ),
          next_cursor: null,
          complete: true,
        },
      ],
      Object.fromEntries(keys.map((key) => [key, bytes])),
    );
    const fetchFile = source.fetchFile.bind(source);
    source.fetchFile = async (key, signal) => ({
      ...(await fetchFile(key, signal)),
      cdn: states[keys.indexOf(key)],
    });
    const result = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
    });
    expect(result.stats.cdnHits).toBe(2);
  });

  it("reuses persisted per-file rollups for interior files and only fetches newly added files", async () => {
    const buffers = {
      f1: await parquetFixture([
        row(10, { host: "a.example", country: "US", resp_bytes: 120 }),
        row(20, {
          host: "b.example",
          country: "DE",
          status: 404,
          cache: "MISS",
          resp_bytes: 80,
        }),
      ]),
      f2: await parquetFixture([
        row(75, {
          host: "a.example",
          country: "CA",
          status: 503,
          resp_bytes: 200,
        }),
      ]),
      f3: await parquetFixture([
        row(130, { host: "c.example", country: "US", resp_bytes: 300 }),
      ]),
    };
    const pages = [
      {
        files: [
          manifestEntry(
            "f1",
            "2026-10-29T10:00:00Z",
            "2026-10-29T10:01:00Z",
            2,
          ),
          manifestEntry(
            "f2",
            "2026-10-29T10:01:00Z",
            "2026-10-29T10:02:00Z",
            1,
          ),
        ],
        next_cursor: null,
        complete: true,
      },
    ];
    const source = fixtureSource(pages, buffers);
    const first = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    expect(first.stats).toMatchObject({
      listed: 2,
      fetched: 2,
      cached: 0,
      rows: 3,
    });
    expect(source.log.files).toEqual(["f1", "f2"]);

    pages[0] = {
      files: [
        manifestEntry("f1", "2026-10-29T10:00:00Z", "2026-10-29T10:01:00Z", 2),
        manifestEntry("f2", "2026-10-29T10:01:00Z", "2026-10-29T10:02:00Z", 1),
        manifestEntry("f3", "2026-10-29T10:02:00Z", "2026-10-29T10:03:00Z", 1),
      ],
      next_cursor: null,
      complete: true,
    };
    const second = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    expect(second.stats).toMatchObject({
      listed: 3,
      fetched: 1,
      cached: 2,
      rows: 4,
    });
    // f1 and f2 were satisfied directly from per-file rollups without calling fetchFile again.
    expect(source.log.files).toEqual(["f1", "f2", "f3"]);

    const coldSource = fixtureSource(pages, buffers);
    const cold = await runDashboard(coldSource, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    expect(second.totalRequests).toBe(cold.totalRequests);
    expect(second.hasBytes).toBe(cold.hasBytes);
    expect(second.buckets).toEqual(cold.buckets);
    expect(second.cards).toEqual(cold.cards);
  });

  it("combines boundary-file DuckDB queries with interior-file rollups without re-fetching", async () => {
    const buffers = {
      boundary: await parquetFixture([
        row(10, { host: "edge.example", country: "US" }),
        row(240, { host: "late.example", country: "FR" }),
      ]),
      interior: await parquetFixture([
        row(30, { host: "core.example", country: "US" }),
      ]),
    };
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              "boundary",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              2,
            ),
            manifestEntry(
              "interior",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:01:00Z",
              1,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      buffers,
    );
    const narrow = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
      concurrency: 1,
    });
    expect(narrow.totalRequests).toBe(2);
    expect(narrow.stats).toMatchObject({
      listed: 2,
      fetched: 2,
      cached: 0,
      rows: 3,
    });
    expect(source.log.files).toEqual(["boundary", "interior"]);

    const wide = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    expect(wide.totalRequests).toBe(3);
    expect(wide.stats).toMatchObject({
      listed: 2,
      fetched: 0,
      cached: 2,
      rows: 3,
    });
    expect(source.log.files).toEqual(["boundary", "interior"]);

    const reloadProgress = vi.fn();
    const reloadedSource = {
      ...source,
      cacheScope: "reloaded-worker",
    };
    const narrowReload = await runDashboard(reloadedSource, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
      concurrency: 1,
      onProgress: reloadProgress,
    });
    expect(narrowReload.totalRequests).toBe(2);
    expect(narrowReload.stats).toMatchObject({
      listed: 2,
      fetched: 0,
      cached: 2,
      rows: 3,
    });
    expect(reloadProgress).not.toHaveBeenCalled();

    let fakeClock = 1_000;
    const slowCachedProgress = vi.fn();
    await runDashboard(reloadedSource, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
      now: () => {
        const current = fakeClock;
        fakeClock += 300;
        return current;
      },
      onProgress: slowCachedProgress,
    });
    expect(slowCachedProgress).toHaveBeenCalled();
    for (const [progress] of slowCachedProgress.mock.calls) {
      expect(progress).toMatchObject({ toFetch: 0, toDecode: 0, done: 0 });
    }
    expect(slowCachedProgress).toHaveBeenLastCalledWith(
      expect.objectContaining({ cached: 2, toFetch: 0, toDecode: 0, done: 0 }),
    );
  });

  it("reports no file work on a warm run where every rollup is cached", async () => {
    const buffers = {
      a: await parquetFixture([row(10)]),
      b: await parquetFixture([row(70)]),
      c: await parquetFixture([row(130)]),
    };
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              "a",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:01:00Z",
              1,
            ),
            manifestEntry(
              "b",
              "2026-10-29T10:01:00Z",
              "2026-10-29T10:02:00Z",
              1,
            ),
            manifestEntry(
              "c",
              "2026-10-29T10:02:00Z",
              "2026-10-29T10:03:00Z",
              1,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      buffers,
    );
    const range = { from: "2026-10-29T10:00:00Z", to: "2026-10-29T10:05:00Z" };
    const coldProgress = vi.fn();
    let coldClock = 0;
    await runDashboard(source, {
      ...range,
      concurrency: 1,
      now: () => (coldClock += 300),
      onProgress: coldProgress,
    });
    const allBytes = Object.values(buffers).reduce(
      (sum, b) => sum + b.byteLength,
      0,
    );
    expect(coldProgress).toHaveBeenCalledWith({
      phase: "files",
      cached: 0,
      toFetch: 3,
      toDecode: 0,
      done: 0,
      downloadedBytes: 0,
      cachedBytes: 0,
    });
    expect(coldProgress).toHaveBeenLastCalledWith({
      phase: "aggregate",
      cached: 0,
      toFetch: 3,
      toDecode: 0,
      done: 3,
      downloadedBytes: allBytes,
      cachedBytes: 0,
    });

    const warmProgress = vi.fn();
    let warmClock = 0;
    const warm = await runDashboard(
      { ...source, cacheScope: "warm-reload" },
      {
        ...range,
        concurrency: 1,
        now: () => (warmClock += 300),
        onProgress: warmProgress,
      },
    );
    expect(warm.totalRequests).toBe(3);
    expect(source.log.files).toEqual(["a", "b", "c"]);
    for (const [progress] of warmProgress.mock.calls) {
      expect(progress.toFetch + progress.toDecode).toBe(0);
    }
    expect(warmProgress).toHaveBeenLastCalledWith(
      expect.objectContaining({
        phase: "aggregate",
        downloadedBytes: 0,
        cachedBytes: allBytes,
      }),
    );
  });

  it("counts boundary files whose bytes are local as decode work, not fetches", async () => {
    const buffers = {
      edge: await parquetFixture([row(10), row(250)]),
      inner: await parquetFixture([row(70)]),
    };
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              "edge",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              2,
            ),
            manifestEntry(
              "inner",
              "2026-10-29T10:01:00Z",
              "2026-10-29T10:02:00Z",
              1,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      buffers,
    );
    await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    const progress = vi.fn();
    const result = await runDashboard(
      { ...source, cacheScope: "decode-reload" },
      {
        from: "2026-10-29T10:00:00Z",
        to: "2026-10-29T10:03:00Z",
        concurrency: 1,
        now: () => 0,
        onProgress: progress,
      },
    );
    expect(result.totalRequests).toBe(2);
    expect(progress).not.toHaveBeenCalled();

    const slowProgress = vi.fn();
    let clock = 0;
    await runDashboard(
      { ...source, cacheScope: "decode-reload-slow" },
      {
        from: "2026-10-29T10:00:00Z",
        to: "2026-10-29T10:03:00Z",
        concurrency: 1,
        now: () => (clock += 300),
        onProgress: slowProgress,
      },
    );
    expect(slowProgress).toHaveBeenLastCalledWith(
      expect.objectContaining({ cached: 1, toFetch: 0, toDecode: 1, done: 1 }),
    );
  });

  it("counts custom card fields, keeping their rollups apart from built-in ones", async () => {
    const buffers = {
      f1: await parquetFixture([
        row(10, { tenant: "acme" }),
        row(20, { tenant: "acme" }),
        row(30, { tenant: "globex" }),
      ]),
      f2: await parquetFixture([row(75, { tenant: null })]),
    };
    const pages = [
      {
        files: [
          manifestEntry(
            "f1",
            "2026-10-29T10:00:00Z",
            "2026-10-29T10:01:00Z",
            3,
          ),
          manifestEntry(
            "f2",
            "2026-10-29T10:01:00Z",
            "2026-10-29T10:02:00Z",
            1,
          ),
        ],
        next_cursor: null,
        complete: true,
      },
    ];
    const source = fixtureSource(pages, buffers);
    const range = {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    };
    const plain = await runDashboard(source, range);
    expect(plain.cards.tenant).toBeUndefined();

    const customFields = [{ id: "tenant", type: "string" as const }];
    const custom = await runDashboard(source, { ...range, customFields });
    expect(custom.cards.tenant).toMatchObject({
      total: 3,
      top: [
        { value: "acme", count: 2 },
        { value: "globex", count: 1 },
      ],
    });
    // Rollups cached without the custom field can't answer it, so files are read again.
    expect(source.log.files).toEqual(["f1", "f2", "f1", "f2"]);

    const again = await runDashboard(source, { ...range, customFields });
    expect(again.cards.tenant).toEqual(custom.cards.tenant);
    expect(source.log.files).toHaveLength(4);

    expect(
      await searchFieldValues(source, {
        ...range,
        column: "tenant",
        query: "GLO",
        customFields,
      }),
    ).toEqual([{ value: "globex", count: 1 }]);

    const filtered = await runDashboard(fixtureSource(pages, buffers), {
      ...range,
      customFields,
      filters: parseFilters(["+tenant:acme"], ["tenant"]),
    });
    expect(filtered.totalRequests).toBe(2);
    expect(filtered.cards.tenant.top).toEqual([{ value: "acme", count: 2 }]);
  });

  it("refuses an incomplete manifest", async () => {
    const source = fixtureSource(
      [{ files: [], next_cursor: null, complete: false }],
      {},
    );
    await expect(
      runDashboard(source, {
        from: "2026-10-29T10:00:00Z",
        to: "2026-10-29T11:00:00Z",
      }),
    ).rejects.toThrow(/incomplete/);
  });

  it("aggregates top-10 on high-cardinality columns without sorting every unique key", () => {
    const rowCount = 40_000;
    const timestamps = new Float64Array(rowCount);
    const urls = new Array<string>(rowCount);
    const digests = new Array<string>(rowCount);
    for (let i = 0; i < rowCount; i++) {
      timestamps[i] = T0 + 1000;
      const hash = ((i + 1) * 2654435761) >>> 0;
      urls[i] = `/item/${hash}`;
      digests[i] = `digest-${hash.toString(16).padStart(8, "0")}`;
    }
    // Give two URLs a higher count and test numeric tie-breaking among count=2 and count=1.
    urls[0] = "/item/10";
    urls[1] = "/item/10";
    urls[2] = "/item/2";
    urls[3] = "/item/2";

    const agg = new Aggregator("2026-10-29T10:00:00Z", "2026-10-29T10:05:00Z");
    agg.add(
      rowCount,
      new Map<string, ArrayLike<unknown>>([
        ["timestamp", timestamps],
        ["url", urls],
        ["digest", digests],
      ]),
    );

    const started = performance.now();
    const res = agg.result();
    const elapsedMs = performance.now() - started;

    expect(res.cards.url.total).toBe(rowCount);
    expect(res.cards.url.top.slice(0, 2)).toEqual([
      { value: "/item/2", count: 2 },
      { value: "/item/10", count: 2 },
    ]);
    expect(res.cards.url.top).toHaveLength(10);
    expect(res.cards.digest.top).toHaveLength(10);
    expect(elapsedMs).toBeLessThan(300);
  });

  it("bounds high-cardinality rankings without quadratic tie comparisons", () => {
    const hosts = [
      ...Array.from({ length: 8192 }, (_, i) => `host-${8191 - i}`),
      ...Array<string>(9).fill("hot"),
      "host-04094",
    ];
    const agg = new Aggregator(T0, T0 + 60_000, ["host"]);
    agg.add(
      hosts.length,
      new Map<string, ArrayLike<unknown>>([
        ["timestamp", new Float64Array(hosts.length).fill(T0 + 1)],
        ["host", hosts],
      ]),
    );
    const compare = new Intl.Collator(undefined, { numeric: true }).compare;
    const expected = [...agg.counts.get("host")!]
      .sort((a, b) => b[1] - a[1] || compare(String(a[0]), String(b[0])))
      .slice(0, 4096);
    const descriptor = Object.getOwnPropertyDescriptor(
      Intl.Collator.prototype,
      "compare",
    )!;
    let comparisons = 0;
    Object.defineProperty(Intl.Collator.prototype, "compare", {
      ...descriptor,
      get() {
        comparisons++;
        return descriptor.get!.call(this);
      },
    });
    try {
      agg.boundRankings();
    } finally {
      Object.defineProperty(Intl.Collator.prototype, "compare", descriptor);
    }

    expect(comparisons).toBeLessThan(400_000);
    expect([...agg.counts.get("host")!]).toEqual(expected);
    expect(agg.totalRequests).toBe(hosts.length);
    expect(agg.result().cards.host).toMatchObject({
      total: hosts.length,
      approximate: true,
      top: expected.slice(0, 10).map(([value, count]) => ({ value, count })),
    });
  }, 30_000);
});

describe("manifest paging and ranges", () => {
  const file = (n: number) =>
    manifestEntry(`k${n}`, "2026-10-29T10:00:00Z", "2026-10-29T10:01:00Z", 1);

  it("follows cursors until none remains and takes the final page completeness", async () => {
    const source = fixtureSource(
      [
        { files: [file(1), file(2)], next_cursor: "1", complete: true },
        { files: [file(2), file(3)], next_cursor: null, complete: false },
      ],
      {},
    );
    const manifest = await fetchManifest(
      source.fetchJson,
      "2026-10-29T10:00:00Z",
      "2026-10-29T11:00:00Z",
    );
    expect(manifest.files.map((f) => f.key)).toEqual(["k1", "k2", "k3"]);
    expect(manifest.complete).toBe(false);
    expect(source.log.json[1]).toContain("cursor=1");
  });

  it("reports a paged listing complete when the final page is", async () => {
    const source = fixtureSource(
      [
        { files: [file(1)], next_cursor: "1", complete: false },
        { files: [file(2)], next_cursor: null, complete: true },
      ],
      {},
    );
    const manifest = await fetchManifest(
      source.fetchJson,
      "2026-10-29T10:00:00Z",
      "2026-10-29T11:00:00Z",
    );
    expect(manifest.complete).toBe(true);
  });

  it("rejects a repeating cursor", async () => {
    const source = fixtureSource(
      [{ files: [], next_cursor: "0", complete: true }],
      {},
    );
    await expect(
      fetchManifest(
        source.fetchJson,
        "2026-10-29T10:00:00Z",
        "2026-10-29T11:00:00Z",
      ),
    ).rejects.toThrow(/repeated/);
  });

  it("disables presets only when no larger listing is complete", async () => {
    const anchorMs = Date.parse("2026-10-29T10:30:00Z");
    const calls: string[] = [];
    const available = await discoverPresets(async (url) => {
      calls.push(url);
      const from = Date.parse(
        new URL(url, "http://x").searchParams.get("from")!,
      );
      const to = Date.parse(new URL(url, "http://x").searchParams.get("to")!);
      return {
        files: [],
        next_cursor: null,
        complete: to - from <= 6 * 3_600_000,
      };
    }, anchorMs);
    expect(available).toEqual({
      "1h": true,
      "3h": true,
      "6h": true,
      "12h": false,
      "24h": false,
    });
    expect(calls).toHaveLength(3);
  });

  it("reuses the manifest discovered by discoverPresetsWithManifest without re-fetching /api/files", async () => {
    const anchorMs = Date.parse("2026-10-29T10:01:30Z");
    const buffers = {
      "pq/1.parquet": await parquetFixture([
        row(10, { host: "edge.example", country: "US" }),
      ]),
    };
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              "pq/1.parquet",
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:01:00Z",
              1,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      buffers,
    );
    const discovery = await discoverPresetsWithManifest(
      source.fetchJson,
      anchorMs,
    );
    expect(discovery.availability["24h"]).toBe(true);
    expect(discovery.manifest?.manifest.complete).toBe(true);
    expect(source.log.json).toHaveLength(1);

    const range = presetRange(24, anchorMs);
    const result = await runDashboard(source, {
      from: range.from,
      to: range.to,
      manifest: discovery.manifest?.manifest,
    });
    expect(result.totalRequests).toBe(1);
    expect(source.log.json).toHaveLength(1);
  });

  it("zero-fills quiet minutes and hours and keeps unsampled hit ratio null", () => {
    const range = presetRange(1, Date.parse("2026-10-29T10:02:30Z"));
    expect(range.to).toBe("2026-10-29T10:02:00.000Z");
    const minute = buildSeries(
      [
        {
          t: Date.parse("2026-10-29T10:01:00Z"),
          requests: 3,
          bytes: 9,
          s4xx: 0,
          s5xx: 0,
          hits: 1,
          cacheRows: 2,
        },
      ],
      range,
      "minute",
    );
    expect(minute).toHaveLength(60);
    expect(minute.filter((p) => p.requests === 0)).toHaveLength(59);
    expect(minute.find((p) => p.requests === 3)?.chr).toBe(50);
    expect(minute[0].chr).toBeNull();
    const hours = buildSeries(
      [],
      { from: "2026-10-29T10:30:00Z", to: "2026-10-29T13:00:00Z" },
      "hour",
    );
    expect(hours.map((p) => p.requests)).toEqual([0, 0, 0]);
  });
});

describe("dashboard filters", () => {
  const rows = [
    row(1, { host: "a.example", country: "US", status: 200 }),
    row(2, { host: "a.example", country: "DE", status: 404 }),
    row(3, { host: "b.example", country: "US", status: 200 }),
    row(4, { host: "api.b.example", country: null, status: 503 }),
  ];

  async function run(filters: DashboardFilter[]) {
    const key = "pq/f.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              key,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              4,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      { [key]: await parquetFixture(rows) },
    );
    return runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      filters,
    });
  }
  const f = (
    mode: DashboardFilter["mode"],
    field: string,
    value: string,
  ): DashboardFilter => ({ mode, field, value });

  it("includes OR within a field and AND across fields", async () => {
    expect((await run([f("include", "country", "US")])).totalRequests).toBe(2);
    expect(
      (
        await run([
          f("include", "country", "US"),
          f("include", "country", "DE"),
        ])
      ).totalRequests,
    ).toBe(3);
    const both = await run([
      f("include", "country", "US"),
      f("include", "host", "b.example"),
    ]);
    expect(both.totalRequests).toBe(1);
    expect(both.cards.host.top).toEqual([{ value: "b.example", count: 1 }]);
  });

  it("excludes drop matches but keep null values", async () => {
    const result = await run([f("exclude", "country", "US")]);
    expect(result.totalRequests).toBe(2);
    expect(result.cards.country.top).toEqual([{ value: "DE", count: 1 }]);
  });

  it("matches numeric columns as text and supports wildcards", async () => {
    expect((await run([f("include", "status", "404")])).totalRequests).toBe(1);
    expect((await run([f("include", "host", "*.example")])).totalRequests).toBe(
      4,
    );
    expect(
      (await run([f("exclude", "host", "*b.example")])).totalRequests,
    ).toBe(2);
  });

  it("gives identical numeric filter counts on the rollup, hyparquet and DuckDB paths", async () => {
    const numericRows = [
      row(1, { ttl: 3600, age: 0 }),
      row(2, { ttl: 3600, age: 120 }),
      row(3, { ttl: 60, age: 120 }),
      row(4, { ttl: 3600.5, age: 7 }),
      row(5, { ttl: null, age: null }),
    ];
    const from = "2026-10-29T10:00:00Z";
    const to = "2026-10-29T10:05:00Z";
    const cases: [DashboardFilter[], number][] = [
      [[f("include", "ttl", "3600")], 2],
      [[f("include", "ttl", "3600.0")], 2],
      [[f("exclude", "ttl", "3600")], 3],
      [[f("include", "ttl", "3600.5")], 1],
      [[f("include", "ttl", "*00")], 2],
      [[f("include", "ttl", "36*")], 3],
      [[f("include", "age", "120")], 2],
      [[f("include", "age", "0")], 1],
      [[f("exclude", "age", "120"), f("include", "ttl", "3600")], 1],
    ];

    // One single-row file per row, so every cached rollup classifies as
    // all-or-none and the warm filtered run never re-reads Parquet.
    const files = await Promise.all(
      numericRows.map(async (r, i) => ({
        key: `pq/numeric-${i}.parquet`,
        bytes: await parquetFixture([r]),
      })),
    );
    const manifest = [
      {
        files: files.map(({ key }) => manifestEntry(key, from, to, 1)),
        next_cursor: null,
        complete: true,
      },
    ];
    const buffers = Object.fromEntries(files.map((x) => [x.key, x.bytes]));
    const warm = fixtureSource(manifest, buffers);
    await runDashboard(warm, { from, to });

    const decoded = await readColumns(await parquetFixture(numericRows), [
      "timestamp",
      "ttl",
      "age",
    ]);

    for (const [filters, expected] of cases) {
      const hyparquet = new Aggregator(from, to, ["ttl", "age"], filters);
      hyparquet.add(decoded.rows, decoded.columns);

      const duckdbRun = await runDashboard(fixtureSource(manifest, buffers), {
        from,
        to,
        filters,
      });

      const fetchedBefore = warm.log.files.length;
      const rollupRun = await runDashboard(warm, { from, to, filters });
      expect(warm.log.files.length).toBe(fetchedBefore);

      expect({
        filters,
        hyparquet: hyparquet.totalRequests,
        duckdb: duckdbRun.totalRequests,
        rollup: rollupRun.totalRequests,
      }).toEqual({
        filters,
        hyparquet: expected,
        duckdb: expected,
        rollup: expected,
      });
    }
  });

  it("executes repeated filtered queries in-memory via DuckDB-Wasm without re-fetching files", async () => {
    const key = "pq/f-persistent.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              key,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              4,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      { [key]: await parquetFixture(rows) },
    );
    const first = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      filters: [f("include", "country", "US")],
    });
    expect(first.totalRequests).toBe(2);
    expect(first.stats).toMatchObject({ fetched: 1, cached: 0 });

    const second = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      filters: [f("exclude", "country", "US")],
    });
    expect(second.totalRequests).toBe(2);
    expect(second.stats).toMatchObject({ fetched: 0, cached: 1 });

    const third = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      filters: [
        f("include", "host", "*.example"),
        f("include", "status", "503"),
      ],
    });
    expect(third.totalRequests).toBe(1);
    expect(third.stats).toMatchObject({ fetched: 0, cached: 1 });
    expect(source.log.files).toEqual([key]);
  });

  it("round-trips URL tokens and rejects unknown fields", () => {
    const filter = f("exclude", "ua", "a:b,c");
    expect(parseFilters([serializeFilter(filter), "+nope:x", "bad"])).toEqual([
      filter,
    ]);
  });

  it("filters to edge==true when edgeOnly is enabled and keeps files without an edge column", async () => {
    const kWithEdge = "pq/with-edge.parquet";
    const kWithoutEdge = "pq/no-edge.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              kWithEdge,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              3,
            ),
            manifestEntry(
              kWithoutEdge,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              2,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      {
        [kWithEdge]: await parquetFixture([
          row(5, { edge: true, host: "edge.example" }),
          row(10, { edge: false, host: "shield.example" }),
          row(15, { edge: null, host: "null.example" }),
        ]),
        [kWithoutEdge]: await parquetFixture(
          [
            row(20, { host: "legacy1.example" }),
            row(25, { host: "legacy2.example" }),
          ],
          ["edge"],
        ),
      },
    );

    const allRows = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      edgeOnly: false,
    });
    expect(allRows.totalRequests).toBe(5);

    const edgeRows = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      edgeOnly: true,
    });
    expect(edgeRows.totalRequests).toBe(3);
    expect(edgeRows.stats).toMatchObject({ fetched: 0, cached: 2 });
    expect(source.log.files).toEqual([kWithEdge, kWithoutEdge]);
    expect(edgeRows.cards.host.top.map((r) => r.value)).toEqual([
      "edge.example",
      "legacy1.example",
      "legacy2.example",
    ]);
  });

  it("prunes 0%-match files and merges 100%-match interior rollups without re-reading Parquet files", async () => {
    const kNone = "pq/none.parquet";
    const kAll = "pq/all.parquet";
    const kPartial = "pq/partial.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              kNone,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:01:00Z",
              2,
            ),
            manifestEntry(
              kAll,
              "2026-10-29T10:01:00Z",
              "2026-10-29T10:02:00Z",
              2,
            ),
            manifestEntry(
              kPartial,
              "2026-10-29T10:02:00Z",
              "2026-10-29T10:03:00Z",
              2,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      {
        [kNone]: await parquetFixture([
          row(5, { host: "other.example", country: "DE" }),
          row(10, { host: "other.example", country: "FR" }),
        ]),
        [kAll]: await parquetFixture([
          row(65, { host: "target.example", country: "US" }),
          row(70, { host: "target.example", country: "US" }),
        ]),
        [kPartial]: await parquetFixture([
          row(125, { host: "target.example", country: "CA" }),
          row(130, { host: "other.example", country: "GB" }),
        ]),
      },
    );

    const unfiltered = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      concurrency: 1,
    });
    expect(unfiltered.totalRequests).toBe(6);
    expect(source.log.files).toEqual([kNone, kAll, kPartial]);

    // Filter where kNone has 0 matches ("none"), kAll has 100% matches ("all"),
    // and only kPartial has mixed rows ("partial").
    const filtered = await runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      filters: [f("include", "host", "target.example")],
      concurrency: 1,
    });
    expect(filtered.totalRequests).toBe(3);
    expect(filtered.cards.country.top).toEqual([
      { value: "US", count: 2 },
      { value: "CA", count: 1 },
    ]);
    // Only kPartial needed to be read from source.fetchFile for DuckDB ingestion!
    expect(source.log.files).toEqual([kNone, kAll, kPartial, kPartial]);
  });

  it("searchFieldValues filters case-insensitively and ignores its own column filter while honoring other filters", async () => {
    const key = "pq/search.parquet";
    const source = fixtureSource(
      [
        {
          files: [
            manifestEntry(
              key,
              "2026-10-29T10:00:00Z",
              "2026-10-29T10:05:00Z",
              4,
            ),
          ],
          next_cursor: null,
          complete: true,
        },
      ],
      {
        [key]: await parquetFixture([
          row(5, { host: "Alpha.Example.com", country: "US" }),
          row(10, { host: "alpha.example.com", country: "US" }),
          row(15, { host: "beta.example.com", country: "US" }),
          row(20, { host: "alpha-eu.example.com", country: "DE" }),
        ]),
      },
    );

    const results = await searchFieldValues(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      column: "host",
      query: "ALPHA",
      filters: [
        f("include", "host", "beta.example.com"),
        f("include", "country", "US"),
      ],
    });
    expect(results).toEqual([
      { value: "alpha.example.com", count: 1 },
      { value: "Alpha.Example.com", count: 1 },
    ]);
  });

  it("finds rare values through the Cloud selective source without building every card's rollup", async () => {
    const key = "pq/selective-search.parquet";
    const rows = [
      ...Array.from({ length: 4200 }, (_, i) =>
        Array.from({ length: 3 }, () => row(5, { host: `common-${i}.example` }))).flat(),
      row(10, { host: "rare_100%.example" }),
      row(15, { host: "rare_100%.example" }),
      row(20, { host: "rare_100%.example", country: "DE" }),
      row(30, { host: "rare_100%.example" }),
    ];
    const bytes = await parquetFixture(rows);
    const source = fixtureSource([{
      files: [manifestEntry(key, "2026-10-29T10:00:00Z", "2026-10-29T10:05:00Z", rows.length)],
      next_cursor: null,
      complete: true,
    }], { [key]: bytes });
    const writeRollup = vi.spyOn(source, "writeRollup");
    const result = await searchFieldValues({
      ...source,
      openParquet: async () => ({ kind: "buffer", bytes }),
    }, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:00:30Z",
      column: "host",
      query: "RARE_100%",
      filters: [
        f("include", "host", "common-1.example"),
        f("include", "country", "US"),
      ],
    });
    expect(result).toEqual([{ value: "rare_100%.example", count: 2 }]);
    expect(writeRollup).not.toHaveBeenCalled();
  });

  it("scans only the searched field for a complete Cloud window", async () => {
    const key = "pq/field-only.parquet";
    const bytes = await parquetFixture([row(5)]);
    const source = fixtureSource([{
      files: [manifestEntry(key, "2026-10-29T10:00:00Z", "2026-10-29T10:05:00Z", 1)],
      next_cursor: null,
      complete: true,
    }], { [key]: bytes });
    const query = vi.spyOn(getDuckDBEngine(), "queryParquetIntoAggregator");
    const writeRollup = vi.spyOn(source, "writeRollup");
    try {
      expect(await searchFieldValues({
        ...source,
        openParquet: async () => ({ kind: "buffer", bytes }),
      }, {
        from: "2026-10-29T10:00:00Z",
        to: "2026-10-29T10:05:00Z",
        column: "host",
        query: "",
      })).toEqual([{ value: "a.example", count: 1 }]);
      expect(query.mock.calls[0][4].fields).toEqual(["host"]);
      expect(writeRollup).not.toHaveBeenCalled();
    } finally {
      query.mockRestore();
    }
  });

  it.each([
    ["ttl", "100000000000000000000", 1e20],
    ["host", "i\u0307stanbul", "İSTANBUL.example"],
  ] as const)("preserves JavaScript search text for %s on the selective path", async (column, query, value) => {
    const key = "pq/search-text.parquet";
    const bytes = await parquetFixture([row(5, { [column]: value })]);
    const source = fixtureSource([{
      files: [manifestEntry(key, "2026-10-29T10:00:00Z", "2026-10-29T10:05:00Z", 1)],
      next_cursor: null, complete: true,
    }], { [key]: bytes });
    expect(await searchFieldValues({
      ...source,
      openParquet: async () => ({ kind: "buffer", bytes }),
    }, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:05:00Z",
      column, query,
    })).toEqual([{ value, count: 1 }]);
  });
});

describe("query beacon", () => {
  const run = async (
    beacon: (body: Record<string, number>) => Promise<void>,
  ) => {
    const key = "pq/a.parquet";
    const source = {
      ...fixtureSource(
        [
          {
            files: [
              manifestEntry(
                key,
                "2026-10-29T10:00:00Z",
                "2026-10-29T10:05:00Z",
                1,
              ),
            ],
            next_cursor: null,
            complete: true,
          },
        ],
        { [key]: await parquetFixture([row(5)]) },
      ),
      beacon,
    };
    return runDashboard(source, {
      from: "2026-10-29T10:00:00Z",
      to: "2026-10-29T10:03:00Z",
    });
  };

  it("sends exactly one beacon per completed query", async () => {
    const beacon = vi.fn(async () => {});
    const result = await run(beacon);
    expect(beacon).toHaveBeenCalledTimes(1);
    expect(beacon).toHaveBeenCalledWith(
      expect.objectContaining({
        files_listed: 1,
        files_fetched: 1,
        files_cached: 0,
        cdn_hits: 0,
        rows_scanned: 1,
      }),
    );
    expect(result.totalRequests).toBe(1);
  });

  it("never fails the query when the beacon rejects or throws", async () => {
    await expect(
      run(async () => Promise.reject(new Error("x"))),
    ).resolves.toBeTruthy();
    await expect(
      run(() => {
        throw new Error("sync");
      }),
    ).resolves.toBeTruthy();
  });
});
