// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { queryAssetsAggregates } from "./assets";
import { assetsAggregatesFromRollup, assetsFromRollup, assetsRollupEligible } from "./assets-rollup";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type Rollup, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
// Origin rows: HIT/MISS/PASS/HIT-STALE, TTFB nulls that fall back to elapsed. Dashboard rows: lower-case and empty cache.
const FIXTURES = {
  origin: loadSummaryFixtures("origin-summaries"),
  dashboard: loadSummaryFixtures("../dashboard/__fixtures__/summaries"),
};
type Fixture = keyof typeof FIXTURES;

async function rollupOf(fixture: Fixture, list: ManifestFile[], stepMs = 300_000): Promise<RollupResult> {
  const parts = await Promise.all(list.map((f) => decodeRollup(FIXTURES[fixture].buffers[f.key], stepMs)));
  return { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: T1 };
}

const body = (extra: Record<string, unknown> = {}) => ({
  start_time: new Date(T0).toISOString(),
  end_time: new Date(T1).toISOString(),
  sections: ["summary"],
  ...extra,
});

// Ties in request counts keep the input's group order, which neither side fixes.
const canonical = (out: Record<string, unknown>) => Object.fromEntries(Object.entries(out).map(([key, value]) =>
  [key, Array.isArray(value) ? [...value].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))) : value]));

describe("assets rollup vs raw DuckDB over the same rows", () => {
  // One engine per fixture: leases on one engine share its request view.
  const engines = {} as Record<Fixture, ParquetEngine>;
  const leases = {} as Record<Fixture, ReturnType<ParquetEngine["lease"]>>;

  beforeAll(async () => {
    for (const fixture of Object.keys(FIXTURES) as Fixture[]) {
      engines[fixture] = new ParquetEngine();
      const lease = engines[fixture].lease([]);
      leases[fixture] = lease;
      for (const file of FIXTURES[fixture].files("raw")) {
        await lease.ingestFile(file.key, `${fixture}-${file.key}.parquet`, FIXTURES[fixture].buffers[file.key],
          undefined, undefined, "request", { deferView: true });
      }
      await lease.publishView();
    }
  }, 60_000);

  afterAll(async () => {
    for (const lease of Object.values(leases)) lease.release();
    for (const engine of Object.values(engines)) await engine.reset();
  });

  const raw = (fixture: Fixture, filters?: unknown) => queryAssetsAggregates({
    start_time: body().start_time,
    end_time: body().end_time,
    sections: ["summary"],
    filters: filters as never,
    engine: leases[fixture].readContext(),
  });

  const bundles = (fixture: Fixture) => {
    const { files } = FIXTURES[fixture];
    const hours = files("hour").filter((f) => f.key.includes("-c4-"));
    expect(hours).toHaveLength(2);
    return [[files("window"), 300_000], [hours, 300_000], [files("day"), HOUR]] as const;
  };

  it("matches the raw totals and breakdowns exactly from windows, hour and day bundles", async () => {
    for (const fixture of Object.keys(FIXTURES) as Fixture[]) {
      const want = await raw(fixture);
      expect(want.summary?.total_requests).toBeGreaterThan(1000);
      expect(Object.keys(want).sort()).toEqual(
        ["asset_type_breakdown", "cache_performance", "compression_performance", "missing_fields", "summary"]);
      for (const [list, step] of bundles(fixture)) {
        const { rollup, ...got } = assetsAggregatesFromRollup(await rollupOf(fixture, [...list], step), body())!;
        expect(canonical(got), `${fixture} ${list[0].key}`).toEqual(canonical(want));
        expect(rollup).toEqual({
          data_from: new Date(T0).toISOString(), data_through: new Date(T1).toISOString(), approximate: [], lower_bound: [],
        });
      }
    }
  });

  it("applies an exact cache filter to the keys like the raw scan's filter on cache", async () => {
    for (const filters of [
      { cache: { mode: "include", values: ["MISS", "HIT-STALE", "OTHER"] } },
      { cache: { mode: "exclude", values: ["HIT", "PASS", "OTHER"] } },
      { cache: { mode: "include", values: ["hit"] } },
      { cache: { mode: "exclude", values: ["MISS"] }, pop: { mode: "include", values: [] } },
    ]) {
      const want = await raw("origin", filters);
      for (const [list, step] of bundles("origin")) {
        const { rollup: _meta, ...got } = assetsAggregatesFromRollup(await rollupOf("origin", [...list], step), body({ filters }))!;
        expect(canonical(got), JSON.stringify(filters)).toEqual(canonical(want));
      }
    }
  });

  it("leaves a cache filter to the raw scan when the logged cache values aren't all upper case", async () => {
    const filters = { cache: { mode: "include", values: ["HIT-STALE"] } };
    const want = await raw("dashboard", filters);
    expect(want.summary?.total_requests).toBe(0);
    expect(assetsAggregatesFromRollup(await rollupOf("dashboard", FIXTURES.dashboard.files("window")), body({ filters })))
      .toBeNull();
  });
});

describe("assetsAggregatesFromRollup", () => {
  const empty = (): Rollup => ({ dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set() });
  const result = (rollup: Rollup): RollupResult => ({ rollup, dataFromMs: T0, dataThroughMs: T1 });

  it("falls back when a summary cut the keys, lacks them, or folds non-ASCII text", async () => {
    const full = await rollupOf("origin", FIXTURES.origin.files("window"));
    const cut = { ...full.rollup, truncated: new Set(["assets/asset"]) };
    expect(assetsAggregatesFromRollup(result(cut), body())).toBeNull();
    expect(assetsAggregatesFromRollup(result({ ...full.rollup, assets: undefined }), body())).toBeNull();
    const asset = { requests: 1, respBytes: 2, ttfbSum: 3, originWaitSum: 0, originRequests: 0 };
    const accented: Rollup = {
      ...empty(), overview: { ...full.rollup.overview!, requests: 1 },
      assets: new Map([[JSON.stringify(["API/Dynamic", "HÍT", "uncompressed", 0, 0]), asset]]),
    };
    expect(assetsAggregatesFromRollup(result(accented), body())).toBeNull();
    const caught: Rollup = { ...full.rollup, fields: new Map([...full.rollup.fields, ["cache_status", new Map([["HIT", 1]])]]) };
    expect(assetsAggregatesFromRollup(result(caught), body({ filters: { cache: { mode: "include", values: ["HIT"] } } })))
      .toBeNull();
    expect(assetsAggregatesFromRollup(result(caught), body())).not.toBeNull();
  });

  it("treats the OTHER key as a null cache, which an include drops and an exclude keeps", async () => {
    const asset = (requests: number) => ({ requests, respBytes: 10 * requests, ttfbSum: 0, originWaitSum: 0, originRequests: 0 });
    const rollup: Rollup = {
      ...empty(), overview: { ...(await rollupOf("origin", FIXTURES.origin.files("window"))).rollup.overview!, requests: 5 },
      fields: new Map([["cache", new Map([["HIT", 2]])]]),
      assets: new Map([[JSON.stringify(["API/Dynamic", "HIT", "uncompressed", 0, 0]), asset(2)], [JSON.stringify(["API/Dynamic", "OTHER", "uncompressed", 0, 0]), asset(3)]]),
    };
    const total = (filters: unknown) => assetsAggregatesFromRollup(result(rollup), body({ filters }))?.summary.total_requests;
    expect(total(undefined)).toBe(5);
    expect(total({ cache: { mode: "include", values: ["HIT", "OTHER"] } })).toBe(2);
    expect(total({ cache: { mode: "exclude", values: ["OTHER"] } })).toBe(5);
    expect(total({ cache: { mode: "exclude", values: ["HIT"] } })).toBe(3);
    // High-Scale shows a request with no cache value as MISS.
    expect(assetsAggregatesFromRollup(result(rollup), body())?.cache_performance)
      .toEqual(expect.arrayContaining([expect.objectContaining({ cache_status: "MISS", requests: 3 })]));
    const logsOther = { ...rollup, fields: new Map([["cache", new Map([["HIT", 2], ["OTHER", 1]])]]) };
    expect(assetsAggregatesFromRollup(result(logsOther), body({ filters: { cache: { mode: "exclude", values: ["HIT"] } } })))
      .toBeNull();
  });

  it("leaves summaries keyed before High-Scale's rules to the raw scan", async () => {
    const asset = { requests: 1, respBytes: 2, ttfbSum: 3, originWaitSum: 0, originRequests: 0 };
    const full = await rollupOf("origin", FIXTURES.origin.files("window"));
    const older: Rollup = {
      ...empty(), overview: { ...full.rollup.overview!, requests: 1 },
      assets: new Map([[JSON.stringify(["Other", "HIT", "unknown"]), asset]]),
    };
    expect(assetsAggregatesFromRollup(result(older), body())).toBeNull();
  });

  it("answers an empty range like the raw scan's empty response", () => {
    const out = assetsAggregatesFromRollup(result(empty()), body())!;
    expect(out.summary).toEqual({
      total_requests: 0, total_bytes: 0, overall_hit_ratio: 0, overall_hit_ratio_pct: 0, avg_ttfb_ms: 0, avg_origin_ms: 0,
    });
    expect(Object.keys(out).sort())
      .toEqual(["asset_type_breakdown", "cache_performance", "compression_performance", "rollup", "summary"]);
  });
});

describe("assetsFromRollup", () => {
  const { buffers, files } = FIXTURES.origin;
  const source = () => fixtureSource([fixturePage(files("raw"))], buffers, { json: [], files: [] }, [fixturePage(files("window"))]);

  it("answers the summary section from summaries without reading raw files", async () => {
    const s = source();
    const served = await assetsFromRollup("/api/assets/aggregates", body(), s);
    expect(served).toEqual(assetsAggregatesFromRollup(await rollupOf("origin", files("window")), body()));
    expect(s.log.files.every((key) => files("window").some((f) => f.key === key))).toBe(true);
    const filtered = body({ filters: { cache: { mode: "include", values: ["HIT"] } } });
    expect(await assetsFromRollup("/api/assets/aggregates", filtered, source())).not.toBeNull();
  });

  it("leaves other filters, URL sections, unnamed sections and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { cache: { values: ["HIT"], mode: "include" }, pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { cache: { values: ["HIT*"], mode: "include" } } }),
      body({ filters: { cache: { values: [""], mode: "include" } } }),
      body({ filters: [{ column: "cache_status", operator: "=", value: "HIT" }] }),
      body({ sections: ["summary", "low_ttl_assets"] }),
      body({ sections: ["large_uncompressed_assets"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
    ]) {
      expect(assetsRollupEligible("/api/assets/aggregates", params) &&
        await assetsFromRollup("/api/assets/aggregates", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(assetsRollupEligible("/api/assets/other", body())).toBe(false);
  });
});

describe("queryAssetsAggregates sections", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  const { buffers, files } = FIXTURES.origin;

  beforeAll(async () => {
    for (const file of files("raw")) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, buffers[file.key], undefined, undefined, "request", { deferView: true });
    }
    await lease.publishView();
  }, 60_000);

  afterAll(async () => {
    lease.release();
    await engine.reset();
  });

  it("answers only the named sections, each as the full response has it", async () => {
    const range = { start_time: body().start_time, end_time: body().end_time, engine: lease.readContext() };
    const full = await queryAssetsAggregates(range) as Record<string, unknown>;
    expect(Object.keys(full).sort()).toEqual([
      "asset_type_breakdown", "cache_performance", "compression_performance", "large_uncompressed_assets",
      "low_ttl_assets", "missing_fields", "summary",
    ]);
    // The fixture URLs carry no extension, so High-Scale's rules find no static low-TTL hotspot.
    expect(full.low_ttl_assets).toEqual([]);
    const hotspots = await queryAssetsAggregates({ ...range, sections: ["large_uncompressed_assets", "low_ttl_assets"] });
    expect(hotspots).toEqual({ large_uncompressed_assets: full.large_uncompressed_assets, low_ttl_assets: full.low_ttl_assets });
    const summary = await queryAssetsAggregates({ ...range, sections: ["summary"] }) as Record<string, unknown>;
    expect(Object.keys(summary).sort())
      .toEqual(["asset_type_breakdown", "cache_performance", "compression_performance", "missing_fields", "summary"]);
    for (const key of Object.keys(summary)) expect(summary[key]).toEqual(full[key]);
    expect(await queryAssetsAggregates({ ...range, sections: [] })).toEqual(full);
    const none = { ...range, end_time: range.start_time };
    expect(await queryAssetsAggregates({ ...none, sections: ["low_ttl_assets"] })).toEqual({ low_ttl_assets: [] });
  });
});
