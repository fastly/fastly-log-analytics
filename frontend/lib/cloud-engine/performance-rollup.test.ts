// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type Rollup, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import {
  executePerformanceAggregates, type PerfScatterItem, type PerfTopAsnItem, type PerfTopUrlItem,
} from "./performance";
import {
  performanceAggregatesFromRollup,
  performanceFromRollup,
  performanceRollupEligible,
} from "./performance-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const { buffers: BUFFERS, files } = loadSummaryFixtures("origin-summaries");
const RAW = files("raw");
const WINDOWS = files("window");
const HOURS = files("hour").filter((f) => f.key.includes("-c4-"));
const TOPS = ["top_urls", "top_asns"];

const near = (actual: number, expected: number, rel = 0.15) =>
  expect(Math.abs(actual - expected), `${actual} vs ${expected}`).toBeLessThanOrEqual(Math.abs(expected) * rel + 0.5);

async function rollupOf(list: ManifestFile[], stepMs = 300_000): Promise<RollupResult> {
  const parts = await Promise.all(list.map((f) => decodeRollup(BUFFERS[f.key], stepMs)));
  return { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: T1 };
}

const body = (extra: Record<string, unknown> = {}) => ({
  start_time: new Date(T0).toISOString(),
  end_time: new Date(T1).toISOString(),
  sections: ["ttl_dist", "waterfall"],
  ...extra,
});

describe("performance rollup vs raw DuckDB over the same rows", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);

  beforeAll(async () => {
    for (const file of RAW) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, BUFFERS[file.key], undefined, undefined, "request", {
        deferView: true,
      });
    }
    await lease.publishView();
  }, 60_000);

  afterAll(async () => {
    lease.release();
    await engine.reset();
  });

  it("matches the raw waterfall averages and TTL buckets exactly from windows, hour and day bundles", async () => {
    const want = await executePerformanceAggregates(
      { start_time: body().start_time, end_time: body().end_time, sections: ["ttl_dist", "waterfall"] },
      lease.readContext(),
    );
    expect(want.waterfall?.avg).not.toBeNull();
    expect(want.ttl_dist?.length).toBe(4);
    const hours = files("hour").filter((f) => f.key.includes("-c4-"));
    expect(hours).toHaveLength(2);
    for (const [list, step] of [[WINDOWS, 300_000], [hours, 300_000], [files("day"), HOUR]] as const) {
      const got = performanceAggregatesFromRollup(await rollupOf([...list], step), body())!;
      expect(got.waterfall).toEqual(want.waterfall);
      expect(got.ttl_dist).toEqual(want.ttl_dist);
    }
  });

  const rawTops = (sort_by: string) => executePerformanceAggregates(
    { start_time: body().start_time, end_time: body().end_time, sections: TOPS, sort_by } as never,
    lease.readContext(),
  );

  it("ranks URLs and networks by average latency exactly like raw, from windows, hour and day bundles", async () => {
    const want = await rawTops("avg");
    expect(want.top_urls).toHaveLength(20);
    expect(want.top_asns!.length).toBeGreaterThan(2);
    for (const [list, step] of [[WINDOWS, 300_000], [HOURS, 300_000], [files("day"), HOUR]] as const) {
      const got = performanceAggregatesFromRollup(await rollupOf([...list], step), body({ sections: TOPS, sort_by: "avg" }))!;
      const exact = (rows: (PerfTopUrlItem | PerfTopAsnItem)[] | undefined) =>
        rows!.map((r) => ["url" in r ? r.url : `${r.asn} ${r.label}`, r.requests, r.avg]);
      expect(exact(got.top_urls)).toEqual(exact(want.top_urls));
      expect(exact(got.top_asns)).toEqual(exact(want.top_asns));
      const rawUrls = new Map(want.top_urls!.map((r) => [r.url, r]));
      for (const row of got.top_urls!) {
        for (const q of ["p50", "p95", "p99"] as const) near(row[q], rawUrls.get(row.url)![q]);
      }
      expect(got._approx).toBe(true);
    }
  });

  it("ranks by approximate p99 by default, keeping raw's slowest URLs and their exact counts", async () => {
    const want = await rawTops("p99");
    const got = performanceAggregatesFromRollup(await rollupOf(WINDOWS), body({ sections: TOPS }))!;
    const p99s = got.top_urls!.map((r) => r.p99);
    expect(p99s).toEqual(p99s.toSorted((a, b) => b - a));
    const rawUrls = new Map(want.top_urls!.map((r) => [r.url, r]));
    const shared = got.top_urls!.filter((r) => rawUrls.has(r.url));
    expect(shared.length).toBeGreaterThanOrEqual(15);
    for (const row of shared) {
      expect([row.requests, row.avg]).toEqual([rawUrls.get(row.url)!.requests, rawUrls.get(row.url)!.avg]);
      near(row.p99, rawUrls.get(row.url)!.p99);
    }
    expect(got.top_asns!.map((r) => r.asn).toSorted()).toEqual(want.top_asns!.map((r) => r.asn).toSorted());
  });

  const rawScatter = async (file: ManifestFile) => (await executePerformanceAggregates(
    { start_time: file.start, end_time: file.end, sections: ["scatter"] }, lease.readContext(),
  )).scatter!;
  const points = (rows: PerfScatterItem[]) => rows.map((r) => JSON.stringify([r.origin, r.edge, r.cache])).toSorted();

  it("samples the scatter from summary sample rows: every raw point of a window under the sample size", async () => {
    for (const [i, file] of WINDOWS.entries()) {
      const want = await rawScatter(RAW[i]);
      expect(want.length).toBeGreaterThan(20);
      expect(want.length).toBeLessThan(300);
      const got = performanceAggregatesFromRollup(await rollupOf([file]), body({ sections: ["scatter"] }))!;
      expect(got.rollup.approximate, file.key).toEqual(["scatter"]);
      expect(got._approx, file.key).toBe(false);
      expect(points(got.scatter!), file.key).toEqual(points(want));
    }
  });

  it("keeps up to 300 valid scatter points from windows, hour and day bundles, all raw points", async () => {
    const every = new Set(points((await Promise.all(RAW.map(rawScatter))).flat()));
    const page = body({ sections: ["scatter"] });
    const windows = performanceAggregatesFromRollup(await rollupOf(WINDOWS), page)!.scatter!;
    expect(windows).toHaveLength(300);
    const windowSet = new Set(points(windows));
    expect(points(windows).every((point) => every.has(point))).toBe(true);
    for (const [list, step] of [[HOURS, 300_000], [files("day"), HOUR]] as const) {
      const got = performanceAggregatesFromRollup(await rollupOf([...list], step), page)!.scatter!;
      expect(got.length).toBeGreaterThan(200);
      expect(got.length).toBeLessThanOrEqual(300);
      expect(points(got).every((point) => every.has(point) && windowSet.has(point))).toBe(true);
    }
  });
});

describe("performanceAggregatesFromRollup", () => {
  it("reports the data span and nothing approximate when no TTL was cut", async () => {
    const out = performanceAggregatesFromRollup(await rollupOf(WINDOWS), body())!;
    expect(out.rollup).toEqual({
      data_from: new Date(T0).toISOString(),
      data_through: new Date(T1).toISOString(),
      approximate: [],
      lower_bound: [],
    });
    expect(out._approx).toBe(false);
  });

  it("labels ranking percentiles approximate and counts lower bounds when URLs or networks were cut to a top K", async () => {
    const result = await rollupOf(WINDOWS);
    expect(performanceAggregatesFromRollup(result, body({ sections: TOPS }))!.rollup).toMatchObject({
      approximate: ["top_urls.p50", "top_urls.p95", "top_urls.p99", "top_asns.p50", "top_asns.p95", "top_asns.p99"],
      lower_bound: [],
    });
    const cut: Rollup = { ...result.rollup, truncated: new Set(["urls/url", "urls/asn"]) };
    expect(performanceAggregatesFromRollup({ ...result, rollup: cut }, body({ sections: TOPS }))!.rollup.lower_bound)
      .toEqual(["top_urls.requests", "top_asns.requests"]);
  });

  it("returns null for the rankings when a summary predates the URL rows, so raw answers", async () => {
    const result = await rollupOf(WINDOWS);
    for (const row of ["urls/url", "urls/asn"]) {
      const present = new Set(result.rollup.present);
      present.delete(row);
      expect(performanceAggregatesFromRollup({ ...result, rollup: { ...result.rollup, present } }, body({ sections: TOPS })), row)
        .toBeNull();
    }
    const present = new Set(result.rollup.present);
    present.delete("urls/url");
    expect(performanceAggregatesFromRollup({ ...result, rollup: { ...result.rollup, present } }, body())).not.toBeNull();
  });

  it("marks TTL buckets lower bounds when a summary cut TTL values to its top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["all_fields/ttl"]) };
    expect(performanceAggregatesFromRollup({ ...result, rollup: cut }, body())!.rollup.lower_bound).toEqual(["ttl_dist"]);
  });

  it("prefers exact pre-bucketed urls/ttl_bucket without lower_bound even when all_fields/ttl is truncated", async () => {
    const result = await rollupOf(WINDOWS);
    const ttlBuckets = new Map([
      ["> 1d", { requests: 400 } as never],
      ["< 60s", { requests: 120 } as never],
      ["1h - 1d", { requests: 300 } as never],
      ["1m - 1h", { requests: 200 } as never],
    ]);
    const urlsGroup = new Map(result.rollup.groups?.get("urls"));
    urlsGroup.set("ttl_bucket", ttlBuckets);
    const groups = new Map(result.rollup.groups);
    groups.set("urls", urlsGroup);
    const present = new Set(result.rollup.present);
    present.add("urls/ttl_bucket");
    const exactRollup: Rollup = {
      ...result.rollup,
      groups,
      present,
      truncated: new Set(["all_fields/ttl"]),
    };
    const out = performanceAggregatesFromRollup({ ...result, rollup: exactRollup }, body({ sections: ["ttl_dist"] }))!;
    expect(out.ttl_dist).toEqual([
      { bucket: "< 60s", count: 120 },
      { bucket: "1m - 1h", count: 200 },
      { bucket: "1h - 1d", count: 300 },
      { bucket: "> 1d", count: 400 },
    ]);
    expect(out.rollup.lower_bound).toEqual([]);
  });

  it("filters invalid and negative-edge scatter samples before slicing to 300", () => {
    const samples = [
      ...Array.from({ length: 40 }, (_, i) => ({
        key: BigInt(i),
        tsMs: T0 + i,
        cache: "MISS",
        ttfbS: 0.1,
        elapsedUs: null,
        rttUs: 1000,
      })),
      ...Array.from({ length: 40 }, (_, i) => ({
        key: BigInt(40 + i),
        tsMs: T0 + 40 + i,
        cache: "MISS",
        ttfbS: 0.5,
        elapsedUs: 100_000, // edge = 100ms - 500ms = -400ms < 0
        rttUs: 1000,
      })),
      ...Array.from({ length: 320 }, (_, i) => ({
        key: BigInt(80 + i),
        tsMs: T0 + 80 + i,
        cache: "HIT",
        ttfbS: 0.01,
        elapsedUs: 50_000 + i * 100, // edge >= 40ms
        rttUs: 1000,
      })),
    ];
    const rollup: Rollup = {
      dims: new Map(),
      series: new Map(),
      seriesStepMs: 300_000,
      fields: new Map(),
      truncated: new Set(),
      samples,
    };
    const out = performanceAggregatesFromRollup({ rollup, dataFromMs: T0, dataThroughMs: T1 }, body({ sections: ["scatter"] }))!;
    expect(out.scatter).toHaveLength(300);
    expect(out.scatter!.every((p) => p.edge >= 0)).toBe(true);
  });

  it("answers only the requested sections, and an empty rollup with no waterfall and no buckets", () => {
    const empty: Rollup = { dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set() };
    const result = { rollup: empty, dataFromMs: T0, dataThroughMs: T1 };
    const out = performanceAggregatesFromRollup(result, body());
    expect(out!.waterfall).toEqual({ avg: null });
    expect(out!.ttl_dist).toEqual([]);
    const tops = performanceAggregatesFromRollup(
      { ...result, rollup: { ...empty, present: new Set(["urls/url", "urls/asn"]) } }, body({ sections: TOPS }),
    );
    expect([tops!.top_urls, tops!.top_asns]).toEqual([[], []]);
    const ttlOnly = performanceAggregatesFromRollup(result, body({ sections: ["ttl_dist"] }));
    expect("waterfall" in ttlOnly!).toBe(false);
    expect(performanceAggregatesFromRollup(result, body({ sections: ["scatter"] }))!.scatter).toEqual([]);
    expect("scatter" in ttlOnly!).toBe(false);
  });
});

describe("performanceFromRollup", () => {
  const source = () => fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(WINDOWS)]);

  it("answers unfiltered waterfall and TTL reads from summaries without reading raw files", async () => {
    const s = source();
    const served = await performanceFromRollup("/api/performance/aggregates", body(), s);
    expect(served).toEqual(performanceAggregatesFromRollup(await rollupOf(WINDOWS), body()));
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
    expect(await performanceFromRollup("/api/performance", body(), source())).not.toBeNull();
    const page = body({ sections: ["ttl_dist", "waterfall", ...TOPS, "scatter"], sort_by: "p95" });
    expect(await performanceFromRollup("/api/performance/aggregates", page, s))
      .toEqual(performanceAggregatesFromRollup(await rollupOf(WINDOWS), page));
    expect(await performanceFromRollup("/api/performance/aggregates", body({ filters: { pop: { values: [] } } }), source()))
      .not.toBeNull();
  });

  it("leaves filtered reads, unknown sections, unnamed sections and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { pop: ["IAD"] } }),
      body({ sections: ["waterfall", "nope"] }),
      body({ filters: { pop: { values: ["IAD"], mode: "include" } }, sections: ["scatter"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
    ]) {
      expect(performanceRollupEligible("/api/performance/aggregates", params) &&
        await performanceFromRollup("/api/performance/aggregates", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(performanceRollupEligible("/api/performance/other", body())).toBe(false);
    expect(performanceRollupEligible("/api/performance/aggregates", body({
      start_time: Math.floor(T0 / 3_600_000) * 3_600_000,
      end_time: Math.floor(T0 / 3_600_000) * 3_600_000 + 3_600_000,
      sections: ["waterfall"],
      filters: { status: { mode: "include", values: ["200"] } },
    }))).toBe(false);
  });
});
