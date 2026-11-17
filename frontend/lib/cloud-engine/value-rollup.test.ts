// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { queryValueSummary } from "./value";
import { valueFromRollup, valueRollupEligible, valueSummaryFromRollup } from "./value-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
// Network rows: protocols with nulls, bytes, no latency. Origin rows: HIT/MISS/PASS/HIT-STALE with
// elapsed. Dashboard rows: lower-case `hit-stale` and empty cache, IPv6 flags, no protocol.
const FIXTURES = {
  network: loadSummaryFixtures("network-summaries"),
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
  sections: ["overview", "network"] as ("overview" | "network")[],
  chart_interval: "1 day",
  ...extra,
});

const bundles = (fixture: Fixture) => {
  const { files } = FIXTURES[fixture];
  const hours = files("hour").filter((f) => f.key.includes("-c4-"));
  expect(hours).toHaveLength(2);
  return [[files("window"), 300_000], [hours, 300_000], [files("day"), HOUR]] as const;
};

describe("value rollup vs raw DuckDB over the same rows", () => {
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

  const raw = (fixture: Fixture, params: ReturnType<typeof body>) =>
    queryValueSummary({ ...params, engine: leases[fixture].readContext() });

  it("matches the raw network section exactly from windows, hour and day bundles", async () => {
    for (const fixture of Object.keys(FIXTURES) as Fixture[]) {
      for (const chart_interval of ["1 day", "1 hour", "5 minutes"]) {
        const params = body({ sections: ["network"], chart_interval });
        const want = await raw(fixture, params);
        expect(want.network!.total_requests).toBeGreaterThan(1000);
        for (const [list, step] of bundles(fixture)) {
          const got = valueSummaryFromRollup(await rollupOf(fixture, [...list], step), params);
          const label = `${fixture} ${chart_interval} ${list[0].key}`;
          // Hourly day-bundle series can't split into 5-minute buckets; only a protocol series needs them.
          if (chart_interval === "5 minutes" && step === HOUR && want.network!.protocol_time_series!.length) {
            expect(got, label).toBeNull();
            continue;
          }
          const { rollup, ...sections } = got!;
          expect(sections, label).toEqual(want);
          expect(rollup.data_through).toBe(new Date(T1).toISOString());
        }
      }
    }
    const network = await raw("network", body({ sections: ["network"] }));
    expect(network.network!.http3_pct).toBeGreaterThan(0);
    expect(network.network!.protocol_time_series!.map((p) => p.category))
      .toEqual(expect.arrayContaining(["HTTP/3", "HTTP/2", "HTTP/1.1", "Other"]));
    expect((await raw("dashboard", body({ sections: ["network"] }))).network!.ipv6_pct).toBeGreaterThan(0);
  });

  it("matches the raw overview cards exactly where every cache state is one the summaries' latency sums take", async () => {
    for (const fixture of ["network", "origin"] as const) {
      const want = await raw(fixture, body());
      expect(want.overview!.edge_hit_requests).toBeGreaterThan(0);
      for (const [list, step] of bundles(fixture)) {
        const { rollup: _meta, ...got } = valueSummaryFromRollup(await rollupOf(fixture, [...list], step), body())!;
        expect(got, `${fixture} ${list[0].key}`).toEqual(want);
      }
    }
    expect((await raw("origin", body())).overview!.cache_acceleration_factor).toBeGreaterThan(0);
  });

  it("leaves the overview to the raw scan when a cache state's case or variant isn't in the summaries' latency sums", async () => {
    const result = await rollupOf("dashboard", FIXTURES.dashboard.files("window"));
    expect(valueSummaryFromRollup(result, body())).toBeNull();
    expect(valueSummaryFromRollup(result, body({ sections: ["network"] }))).not.toBeNull();
    const variant = await rollupOf("origin", FIXTURES.origin.files("window"));
    const cache = new Map(variant.rollup.fields.get("cache"));
    cache.set("MISS-CLUSTER", 1);
    expect(valueSummaryFromRollup({ ...variant, rollup: { ...variant.rollup, fields: new Map([["cache", cache]]) } }, body()))
      .toBeNull();
  });
});

describe("valueSummaryFromRollup", () => {
  it("falls back on cut value counts or series, alias columns, unknown byte totals, or no overview row", async () => {
    const result = await rollupOf("network", FIXTURES.network.files("window"));
    const { rollup } = result;
    const with_ = (patch: Partial<typeof rollup>) => ({ ...result, rollup: { ...rollup, ...patch } });
    expect(valueSummaryFromRollup(result, body())).not.toBeNull();
    for (const cut of ["all_fields/proto", "all_fields/tls", "all_fields/is_ipv6", "series5/proto"]) {
      expect(valueSummaryFromRollup(with_({ truncated: new Set([cut]) }), body({ sections: ["network"] })), cut).toBeNull();
      expect(valueSummaryFromRollup(with_({ truncated: new Set([cut]) }), body({ sections: ["overview"] })), cut).not.toBeNull();
    }
    expect(valueSummaryFromRollup(with_({ truncated: new Set(["all_fields/cache"]) }), body({ sections: ["overview"] }))).toBeNull();
    const aliased = new Map([...rollup.fields, ["protocol", new Map([["HTTP/2", 1]])]]);
    expect(valueSummaryFromRollup(with_({ fields: aliased }), body())).toBeNull();
    expect(valueSummaryFromRollup(with_({ overview: { ...rollup.overview!, totalBandwidth: 0 } }), body({ sections: ["overview"] })))
      .toBeNull();
    expect(valueSummaryFromRollup(with_({ overview: undefined }), body())).toBeNull();
  });

  it("answers an empty range with the raw scan's empty sections", () => {
    const overview = { requests: 0, totalBandwidth: 0, hitElapsedSum: 0, hitElapsedCount: 0, missElapsedSum: 0, missElapsedCount: 0 };
    const out = valueSummaryFromRollup({
      rollup: { dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set(), overview },
      dataFromMs: T0, dataThroughMs: T1,
    }, body())!;
    expect(out.overview?.total_requests).toBe(0);
    expect(out.network?.protocol_time_series).toEqual([]);
    expect(Object.keys(out).sort()).toEqual(["network", "overview", "rollup"]);
  });
});

describe("valueFromRollup", () => {
  const { buffers, files } = FIXTURES.network;
  const source = () => fixtureSource([fixturePage(files("raw"))], buffers, { json: [], files: [] }, [fixturePage(files("window"))]);

  it("answers the overview and network sections from summaries without reading raw files", async () => {
    const s = source();
    const served = await valueFromRollup("/api/value/summary", body(), s);
    expect(served).toEqual(valueSummaryFromRollup(await rollupOf("network", files("window")), body()));
    expect(s.log.files.every((key) => files("window").some((f) => f.key === key))).toBe(true);
  });

  it("leaves filters, other sections, unnamed sections and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ custom_filters: { tenant: { values: ["a"], mode: "include" } } }),
      body({ sections: ["overview", "caching"] }),
      body({ sections: ["performance"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
    ]) {
      expect(valueRollupEligible("/api/value/summary", params) &&
        await valueFromRollup("/api/value/summary", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(valueRollupEligible("/api/value/summary", body({ filters: { pop: { values: [], mode: "include" } } }))).toBe(true);
    expect(valueRollupEligible("/api/value/other", body())).toBe(false);
  });
});
