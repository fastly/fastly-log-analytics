// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { editSummaryRows } from "./dashboard/summary-layout-fixture";
import { fixtureSource } from "./dashboard/test-fixtures";
import { queryValueSummary } from "./value";
import { VALUE_ROLLUP_SECTIONS } from "./rollup-sections";
import {
  VALUE_FILTERED_SUMMARY_SPEC,
  VALUE_SUMMARY_SPEC,
  VALUE_SUMMARY_TAB_SPEC,
  valueFromRollup,
  valueRollupEligible,
  valueSummaryFromRollup,
} from "./value-rollup";

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
    const overview = { requests: 0, totalBandwidth: 0, hitElapsedSum: 0, hitElapsedCount: 0, missElapsedSum: 0, missElapsedCount: 0, threatsBlocked: 0 };
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

  it("every value section is served from rollups when value is covered", async () => {
    expect([...VALUE_ROLLUP_SECTIONS]).toEqual([...ALL_SECTIONS]);
    const sv = FIXTURES.securityValue;
    const s = fixtureSource([fixturePage(sv.files("raw"))], sv.buffers, { json: [], files: [] }, [fixturePage(sv.files("window"))]);
    const all = body({ sections: [...VALUE_ROLLUP_SECTIONS] });
    const served = await valueFromRollup("/api/value/summary", all, s);
    expect(served).not.toBeNull();
    expect(s.log.files.every((key) => sv.files("window").some((f) => f.key === key))).toBe(true);
    expect(served!.performance!.p99_latency_ms).not.toBeNull();
    expect(served!.rollup.approximate).toContain("performance.p99_latency_ms");
    expect(served!.io!.format_distribution!.length).toBeGreaterThan(1);
    expect(served!.io!.optimization_opportunities).toEqual([]);
    expect(served!.io!.image_request_count).toBe(0);
    expect(served!.io!.image_bandwidth_bytes).toBeNull();
    expect(served!.security!.waf_blocked).toBeNull();
    expect(served!.security!.waf_logged).toBeNull();
    expect(served!.security!.waf_passed).toBeNull();
    expect(served!.security!.threat_time_series).toEqual([]);
    expect(served!.bots!.verified_bots).toBe(1028);

    const full = await rollupOf("securityValue", sv.files("window"));
    for (const [alias, section] of [
      ["content_type", "io"],
      ["resp_header_content_type", "io"],
      ["waf_action", "security"],
      ["ngwaf_decision", "security"],
      ["bot_verified", "bots"],
      ["verified_bot", "bots"],
    ] as const) {
      const aliased = new Map([...full.rollup.fields, [alias, new Map([["x", 1]])]]);
      expect(valueSummaryFromRollup({ ...full, rollup: { ...full.rollup, fields: aliased } }, body({ sections: [section] })), alias)
        .toBeNull();
    }
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

  it("serves filtered /api/value/summary overview from filtered_rollups for hour-aligned single low-card filters", async () => {
    const hour = FIXTURES.origin.files("hour")[0];
    const hourStart = Date.parse(hour.start);
    const t = new Date(hourStart);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["overview", "dim_totals", "filtered_rollups"], type: "STRING" },
        { name: "bucket", data: [t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["", "status", "status"], type: "STRING" },
        { name: "value", data: ["", "filtered_rollups", "200"], type: "STRING" },
        { name: "requests", data: [BigInt(100), BigInt(100), BigInt(80)], type: "INT64" },
        { name: "hit_requests", data: [null, null, BigInt(60)], type: "INT64" },
        { name: "miss_requests", data: [null, null, BigInt(15)], type: "INT64" },
        { name: "pass_requests", data: [null, null, BigInt(5)], type: "INT64" },
        { name: "resp_bytes_sum", data: [BigInt(10000), null, BigInt(8000)], type: "INT64" },
        { name: "truncated", data: [null, false, null], type: "BOOLEAN" },
      ],
    });
    const filteredSource = fixtureSource(
      [fixturePage(FIXTURES.origin.files("raw"))],
      { [hour.key]: summaryBuf },
      { json: [], files: [] },
      [fixturePage([hour])],
    );
    const reqBody = {
      start_time: hour.start,
      end_time: hour.end,
      sections: ["overview"] as ("overview" | "caching")[],
      filters: { status: { mode: "include", values: ["200"] } },
    };
    expect(valueRollupEligible("/api/value/summary", reqBody)).toBe(true);
    const served = await valueFromRollup("/api/value/summary", reqBody, filteredSource);
    expect(served).not.toBeNull();
    expect(served!.overview).toEqual({
      total_requests: 80,
      total_bandwidth_bytes: 8000,
      origin_offload_pct: 75,
      threats_blocked: null,
      cache_acceleration_factor: null,
      edge_hit_requests: 60,
      origin_requests: 20,
    });
    expect(served!.rollup.lower_bound).toEqual([]);

    // Non-hour-aligned, multi-field, custom_filters, or non-overview sections must fall back.
    expect(valueRollupEligible("/api/value/summary", { ...reqBody, start_time: new Date(hourStart + 60_000).toISOString() })).toBe(false);
    expect(valueRollupEligible("/api/value/summary", { ...reqBody, sections: ["overview", "caching"] })).toBe(false);
    expect(valueRollupEligible("/api/value/summary", { ...reqBody, custom_filters: { tenant: { mode: "include", values: ["a"] } } })).toBe(false);
  });

  it("reads overview.threats_blocked and UA bots from rollups when waf/ua are logged, leaving active waf security to raw scan", async () => {
    const net = FIXTURES.network;
    const windowFiles = net.files("window");
    const editedBuffers: Record<string, ArrayBuffer> = { ...net.buffers };
    const editedWindows = await Promise.all(windowFiles.map(async (f, idx) => {
      const bytes = await editSummaryRows(net.buffers[f.key], (rows) => {
        const next = rows.map((row) =>
          row.bundle === "overview" ? { ...row, threats_blocked: BigInt(idx === 0 ? 7 : 0) } : { ...row },
        );
        const bucket = rows.find((row) => row.bundle === "overview")?.bucket ?? BigInt(T0 * 1000);
        next.push(
          { bundle: "all_fields", bucket, field: "waf", value: "true", count: BigInt(7) },
          { bundle: "all_fields", bucket, field: "waf", value: "false", count: BigInt(10) },
          { bundle: "all_fields", bucket, field: "ua", value: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", count: BigInt(5) },
          { bundle: "all_fields", bucket, field: "ua", value: "Mozilla/5.0", count: BigInt(20) },
        );
        return next;
      });
      editedBuffers[f.key] = bytes;
      return { ...f, size: bytes.byteLength };
    }));
    const src = fixtureSource([fixturePage(net.files("raw"))], editedBuffers, { json: [], files: [] }, [fixturePage(editedWindows)]);

    const ov = await valueFromRollup("/api/value/summary", body({ sections: ["overview"] }), src);
    expect(ov?.overview?.threats_blocked).toBe(7);

    const bots = await valueFromRollup("/api/value/summary", body({ sections: ["bots"] }), src);
    expect(bots?.bots).toMatchObject({
      bot_requests: 40,
      top_bots: [{ name: "Google Crawler", count: 40 }],
    });

    const sec = await valueFromRollup("/api/value/summary", body({ sections: ["security"] }), src);
    expect(sec).toBeNull();
  });

  it("scopes value summary spec columns to only their owning row groups", () => {
    expect([...(VALUE_SUMMARY_SPEC.only.get("elapsed_hist") ?? [])]).toEqual(["series5"]);
    expect([...(VALUE_SUMMARY_SPEC.only.get("io_in_sum") ?? [])]).toEqual(["value"]);
    expect([...(VALUE_SUMMARY_SPEC.only.get("total_bandwidth_bytes") ?? [])]).toEqual(["overview"]);
    expect([...(VALUE_SUMMARY_SPEC.only.get("count") ?? [])].sort()).toEqual(["all_fields", "value"]);
    expect([...(VALUE_FILTERED_SUMMARY_SPEC.only.get("requests") ?? [])]).toEqual(["filtered_rollups"]);
    expect([...(VALUE_FILTERED_SUMMARY_SPEC.only.get("resp_bytes_sum") ?? [])]).toEqual(["filtered_rollups"]);
    expect(VALUE_SUMMARY_SPEC.groups.has("assets")).toBe(true);
    expect([...(VALUE_SUMMARY_SPEC.only.get("resp_bytes_sum") ?? [])]).toContain("assets");
    expect([...(VALUE_SUMMARY_SPEC.only.get("requests") ?? [])]).toContain("assets");
    expect([...(VALUE_SUMMARY_SPEC.fields?.get("series5") ?? [])].sort()).toEqual(["overall", "proto"]);
    expect([...(VALUE_SUMMARY_SPEC.fields?.get("bins") ?? [])]).toEqual(["verified_bot"]);
    expect(VALUE_SUMMARY_SPEC.fields?.get("all_fields")?.has("ua")).toBe(true);

    expect(VALUE_SUMMARY_TAB_SPEC.columns.has("elapsed_hist")).toBe(false);
    expect(VALUE_SUMMARY_TAB_SPEC.groups.has("bins")).toBe(false);
    expect(VALUE_SUMMARY_TAB_SPEC.groups.has("assets")).toBe(false);
    expect(VALUE_SUMMARY_TAB_SPEC.fields?.get("all_fields")?.has("ua")).toBe(false);
    expect([...(VALUE_SUMMARY_TAB_SPEC.fields?.get("value") ?? [])].sort()).toEqual(["cache", "shield"]);
  });

  it("FV-07: falls back io section to raw scan when rollup.assets has Images or is truncated", async () => {
    const sv = await rollupOf("securityValue", FIXTURES.securityValue.files("window"));
    const withAssets = (assets: Map<string, { requests: number; respBytes: number; ttfbSum: number; originWaitSum: number; originRequests: number }>) => ({
      ...sv,
      rollup: { ...sv.rollup, assets },
    });
    expect(
      valueSummaryFromRollup(
        withAssets(new Map([['["Images","HIT","uncompressed",0,0]', { requests: 1, respBytes: 500, ttfbSum: 1, originWaitSum: 0, originRequests: 0 }]])),
        body({ sections: ["io"] }),
      ),
    ).toBeNull();
    expect(
      valueSummaryFromRollup(
        { ...sv, rollup: { ...sv.rollup, truncated: new Set([...sv.rollup.truncated, "assets/asset"]) } },
        body({ sections: ["io"] }),
      ),
    ).toBeNull();
    const zeroImages = valueSummaryFromRollup(
      withAssets(new Map([['["Images","HIT","uncompressed",0,0]', { requests: 0, respBytes: 0, ttfbSum: 0, originWaitSum: 0, originRequests: 0 }]])),
      body({ sections: ["io"] }),
    );
    expect(zeroImages?.io?.image_request_count).toBe(0);
    expect(zeroImages?.io?.image_bandwidth_bytes).toBeNull();
  });

  it("uses VALUE_SUMMARY_TAB_SPEC by default for unfiltered Summary-tab section subsets", async () => {
    const net = FIXTURES.network;
    const windowFiles = net.files("window");
    const editedBuffers: Record<string, ArrayBuffer> = { ...net.buffers };
    const editedWindows = await Promise.all(windowFiles.map(async (f) => {
      const bytes = await editSummaryRows(net.buffers[f.key], (rows) => {
        const bucket = rows.find((row) => row.bundle === "overview")?.bucket ?? BigInt(T0 * 1000);
        return [
          ...rows,
          { bundle: "all_fields", bucket, field: "ua", value: "Mozilla/5.0", count: BigInt(10) },
          { bundle: "assets", bucket, field: "asset", value: '["Images","HIT","uncompressed",0,0]', requests: BigInt(5), resp_bytes_sum: BigInt(1000) },
        ];
      });
      editedBuffers[f.key] = bytes;
      return { ...f, size: bytes.byteLength };
    }));
    const src = fixtureSource([fixturePage(net.files("raw"))], editedBuffers, { json: [], files: [] }, [fixturePage(editedWindows)]);
    const tabRes = await valueFromRollup("/api/value/summary", body({ sections: ["overview", "caching", "network"] }), src);
    expect(tabRes?.overview?.total_requests).toBeGreaterThan(0);
    // When io is requested and assets has Images > 0, valueFromRollup uses VALUE_SUMMARY_SPEC (which loads assets) and falls back to null.
    const ioRes = await valueFromRollup("/api/value/summary", body({ sections: ["io"] }), src);
    expect(ioRes).toBeNull();
  });
});
