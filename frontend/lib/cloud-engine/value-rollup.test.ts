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
  // Bot names, WAF signals, Image Optimizer formats and bytes, SYNTH/ERROR/'' cache states, shield rows.
  securityValue: loadSummaryFixtures("security-value-summaries"),
};
type Fixture = keyof typeof FIXTURES;

async function rollupOf(fixture: Fixture, list: ManifestFile[], stepMs = 300_000): Promise<RollupResult> {
  const parts = await Promise.all(list.map((f) => decodeRollup(FIXTURES[fixture].buffers[f.key], stepMs)));
  return { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: T1 };
}

const ALL_SECTIONS = ["overview", "caching", "performance", "security", "bots", "network", "io"] as const;

const body = (extra: Record<string, unknown> = {}) => ({
  start_time: new Date(T0).toISOString(),
  end_time: new Date(T1).toISOString(),
  sections: ["overview", "network"] as (typeof ALL_SECTIONS)[number][],
  chart_interval: "1 day",
  ...extra,
});

type Summary = Awaited<ReturnType<typeof queryValueSummary>>;

/**
 * A summary with the parts that may differ from the raw scan set aside: the p99 latency (an
 * estimate, checked apart), latency averages' last float bits, and the order of tied counts.
 */
function comparable(summary: Summary) {
  const out = structuredClone(summary);
  const byCount = <T extends { count: number }>(rows: T[] | undefined, key: (row: T) => string) =>
    rows?.sort((a, b) => b.count - a.count || (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
  if (out.performance) {
    out.performance.p99_latency_ms = null;
    for (const point of out.performance.latency_time_series ?? []) point.value = Number(point.value.toFixed(6));
  }
  byCount(out.security?.top_waf_signals, (row) => String(row.signal));
  byCount(out.bots?.top_bots, (row) => String(row.name));
  byCount(out.io?.format_distribution, (row) => row.format);
  byCount(out.io?.io_format_conversion_pairs, (row) => `${row.input_format}|${row.output_format}`);
  return out;
}

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

  it("matches every raw section from the per-day value rows, with the p99 latency estimated", async () => {
    for (const fixture of Object.keys(FIXTURES) as Fixture[]) {
      const params = body({ sections: [...ALL_SECTIONS] });
      const want = await raw(fixture, params);
      for (const [list, step] of bundles(fixture)) {
        const label = `${fixture} ${list[0].key}`;
        const got = valueSummaryFromRollup(await rollupOf(fixture, [...list], step), params);
        expect(got, label).not.toBeNull();
        const { rollup: meta, ...sections } = got!;
        expect(comparable(sections), label).toEqual(comparable(want));
        expect(meta.lower_bound, label).toEqual([]);
        const p99 = want.performance!.p99_latency_ms;
        if (p99 == null) {
          expect(sections.performance!.p99_latency_ms, label).toBeNull();
          expect(meta.approximate, label).toEqual([]);
        } else {
          expect(meta.approximate, label).toEqual(["performance.p99_latency_ms"]);
          expect(Math.abs(sections.performance!.p99_latency_ms! - p99) / p99, label).toBeLessThan(0.1);
        }
      }
    }
    const want = await raw("securityValue", body({ sections: [...ALL_SECTIONS] }));
    expect(want.caching!.shield_effectiveness_pct).not.toBeNull();
    expect(want.caching!.cache_state_time_series!.map((p) => p.category)).toContain("OTHER");
    expect(want.performance!.latency_time_series!.length).toBeGreaterThan(1);
    expect(want.performance!.p99_latency_ms).not.toBeNull();
    expect(want.security!.top_waf_signals!.length).toBeGreaterThan(2);
    expect(want.bots!.top_bots!.length).toBeGreaterThan(1);
    expect(want.io!.io_format_conversion_pairs!.some((pair) => pair.avg_ratio !== null)).toBe(true);
    expect(want.io!.format_distribution!.length).toBeGreaterThan(1);
    expect(want.io!.io_actual_compression_ratio).not.toBeNull();
    expect(want.io!.io_shield_bandwidth_bytes).not.toBeNull();
  });

  it("leaves day-series sections to the raw scan on a chart interval under a day", async () => {
    const result = await rollupOf("securityValue", FIXTURES.securityValue.files("window"));
    for (const section of ["caching", "performance", "io"] as const) {
      expect(valueSummaryFromRollup(result, body({ sections: [section], chart_interval: "1 hour" })), section).toBeNull();
    }
    for (const section of ["security", "bots"] as const) {
      expect(valueSummaryFromRollup(result, body({ sections: [section], chart_interval: "1 hour" })), section).not.toBeNull();
    }
  });

  it("marks cut top lists as lower bounds and leaves summaries without value rows to the raw scan", async () => {
    const result = await rollupOf("securityValue", FIXTURES.securityValue.files("window"));
    const { rollup } = result;
    const with_ = (patch: Partial<typeof rollup>) => ({ ...result, rollup: { ...rollup, ...patch } });
    const cut = new Set(["value/waf_signal", "value/bot_name", "value/io_pair"]);
    expect(valueSummaryFromRollup(with_({ truncated: cut }), body({ sections: ["security", "bots", "io"] }))!.rollup.lower_bound)
      .toEqual(["security.top_waf_signals", "bots.top_bots", "io.io_format_conversion_pairs"]);
    for (const field of ["cache", "io"]) {
      expect(valueSummaryFromRollup(with_({ truncated: new Set([`value/${field}`]) }), body({ sections: ["caching"] })), field)
        .toBeNull();
    }
    expect(valueSummaryFromRollup(with_({ truncated: new Set(["all_fields/io_output_format"]) }), body({ sections: ["io"] })))
      .toBeNull();
    const old = new Set([...rollup.present!].filter((key) => !key.startsWith("value/")));
    for (const section of ["caching", "performance", "security", "bots", "io"] as const) {
      expect(valueSummaryFromRollup(with_({ present: old }), body({ sections: [section] })), section).toBeNull();
    }
    for (const alias of ["fastly_is_shield", "is_bot", "ngwaf_bot_name", "ofmt", "content_type"]) {
      const aliased = new Map([...rollup.fields, [alias, new Map([["x", 1]])]]);
      expect(valueSummaryFromRollup(with_({ fields: aliased }), body({ sections: ["bots"] })), alias).toBeNull();
    }
  });

  it("leaves the overview to the raw scan in summaries without value rows when a cache state's case or variant isn't in the overview row's latency sums", async () => {
    const full = await rollupOf("dashboard", FIXTURES.dashboard.files("window"));
    const present = new Set([...full.rollup.present!].filter((key) => !key.startsWith("value/")));
    const result = { ...full, rollup: { ...full.rollup, present } };
    expect(valueSummaryFromRollup(result, body())).toBeNull();
    expect(valueSummaryFromRollup(result, body({ sections: ["network"] }))).not.toBeNull();
    const variant = await rollupOf("origin", FIXTURES.origin.files("window"));
    const cache = new Map(variant.rollup.fields.get("cache"));
    cache.set("MISS-CLUSTER", 1);
    expect(valueSummaryFromRollup({ ...variant, rollup: { ...variant.rollup, present, fields: new Map([["cache", cache]]) } }, body()))
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
    const old = new Set([...rollup.present!].filter((key) => !key.startsWith("value/")));
    expect(valueSummaryFromRollup(with_({ truncated: new Set(["all_fields/cache"]) }), body({ sections: ["overview"] }))).not.toBeNull();
    expect(valueSummaryFromRollup(with_({ present: old, truncated: new Set(["all_fields/cache"]) }), body({ sections: ["overview"] })))
      .toBeNull();
    const aliased = new Map([...rollup.fields, ["protocol", new Map([["HTTP/2", 1]])]]);
    expect(valueSummaryFromRollup(with_({ fields: aliased }), body())).toBeNull();
    expect(valueSummaryFromRollup(with_({ present: old, overview: { ...rollup.overview!, totalBandwidth: 0 } }), body({ sections: ["overview"] })))
      .toBeNull();
    expect(valueSummaryFromRollup(with_({ overview: undefined }), body())).toBeNull();
  });

  it("carries the overview's logged requests and bytes per UTC hour for the Fastly Stats comparison", async () => {
    const result = await rollupOf("network", FIXTURES.network.files("window"));
    const out = valueSummaryFromRollup(result, body())!;
    const hours = out.rollup.logged_hours!;
    expect(hours.length).toBeGreaterThan(0);
    expect(hours.every(([at]) => at % HOUR === 0)).toBe(true);
    expect(hours.reduce((n, [, requests]) => n + requests, 0)).toBe(out.overview!.total_requests);
    expect(hours.reduce((n, [, , bytes]) => n + bytes, 0)).toBeCloseTo(out.overview!.total_bandwidth_bytes!, 1);
    expect(valueSummaryFromRollup(result, body({ sections: ["network"] }))!.rollup).not.toHaveProperty("logged_hours");
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

  it("answers every section from summaries without reading raw files", async () => {
    const s = source();
    const all = body({ sections: [...ALL_SECTIONS] });
    const served = await valueFromRollup("/api/value/summary", all, s);
    expect(served).toEqual(valueSummaryFromRollup(await rollupOf("network", files("window")), all));
    expect(served).not.toBeNull();
    expect(s.log.files.every((key) => files("window").some((f) => f.key === key))).toBe(true);
  });

  it("leaves filters, other sections, unnamed sections and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ custom_filters: { tenant: { values: ["a"], mode: "include" } } }),
      body({ sections: ["overview", "caching"], chart_interval: "1 hour" }),
      body({ sections: ["overview", "assets" as "overview"] }),
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
