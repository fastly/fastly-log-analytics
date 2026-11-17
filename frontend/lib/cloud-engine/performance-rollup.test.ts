// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type Rollup, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { executePerformanceAggregates } from "./performance";
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
      const got = performanceAggregatesFromRollup(await rollupOf([...list], step), body());
      expect(got.waterfall).toEqual(want.waterfall);
      expect(got.ttl_dist).toEqual(want.ttl_dist);
    }
  });
});

describe("performanceAggregatesFromRollup", () => {
  it("reports the data span and nothing approximate when no TTL was cut", async () => {
    const out = performanceAggregatesFromRollup(await rollupOf(WINDOWS), body());
    expect(out.rollup).toEqual({
      data_from: new Date(T0).toISOString(),
      data_through: new Date(T1).toISOString(),
      approximate: [],
      lower_bound: [],
    });
    expect(out._approx).toBe(false);
  });

  it("marks TTL buckets lower bounds when a summary cut TTL values to its top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["all_fields/ttl"]) };
    expect(performanceAggregatesFromRollup({ ...result, rollup: cut }, body()).rollup.lower_bound).toEqual(["ttl_dist"]);
  });

  it("answers only the requested sections, and an empty rollup with no waterfall and no buckets", () => {
    const empty: Rollup = { dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set() };
    const result = { rollup: empty, dataFromMs: T0, dataThroughMs: T1 };
    const out = performanceAggregatesFromRollup(result, body());
    expect(out.waterfall).toEqual({ avg: null });
    expect(out.ttl_dist).toEqual([]);
    const ttlOnly = performanceAggregatesFromRollup(result, body({ sections: ["ttl_dist"] }));
    expect("waterfall" in ttlOnly).toBe(false);
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
    expect(await performanceFromRollup("/api/performance/aggregates", body({ filters: { pop: { values: [] } } }), source()))
      .not.toBeNull();
  });

  it("leaves filtered reads, row sections, unnamed sections and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { pop: ["IAD"] } }),
      body({ sections: ["waterfall", "scatter"] }),
      body({ sections: ["top_urls"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
    ]) {
      expect(performanceRollupEligible("/api/performance/aggregates", params) &&
        await performanceFromRollup("/api/performance/aggregates", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(performanceRollupEligible("/api/performance/other", body())).toBe(false);
  });
});
