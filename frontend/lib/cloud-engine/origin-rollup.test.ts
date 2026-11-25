// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => new Set(["edge", "rid", "prid", "pop"]),
  cloudServiceFlags: async () => ({
    serviceId: "default",
    activeIds: new Set(["edge", "rid", "prid", "pop"]),
    edgeOnly: false,
    cmcdEnabled: false,
  }),
}));
import { parquetWriteBuffer } from "hyparquet-writer";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { Histogram } from "./dashboard/histogram";
import { decodeRollup, mergeRollups, type Rollup, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { handleOriginRequest, type OriginAggregatesResponse, type OriginTimeseriesPoint } from "./origin";
import { originAggregatesFromRollup, originFromRollup, originRollupEligible } from "./origin-rollup";
import { originRollupSections } from "./rollup-sections";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const { buffers: BUFFERS, files } = loadSummaryFixtures("origin-summaries");
const RAW = files("raw");
const WINDOWS = files("window");
const HOURS = files("hour").filter((f) => f.key.includes("-c4-"));
const SPANS = [[WINDOWS, 300_000], [HOURS, 300_000], [files("day"), HOUR]] as const;
const PAGE_SECTIONS = ["summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"];

async function rollupOf(list: ManifestFile[], stepMs = 300_000): Promise<RollupResult> {
  const parts = await Promise.all(list.map((f) => decodeRollup(BUFFERS[f.key], stepMs)));
  return { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: T1 };
}

const near = (actual: unknown, expected: unknown, rel = 0.1) => {
  expect(actual).not.toBeNull();
  expect(Math.abs(Number(actual) - Number(expected))).toBeLessThanOrEqual(Math.abs(Number(expected)) * rel);
};

const body = (extra: Record<string, unknown> = {}) => ({
  start_time: new Date(T0).toISOString(),
  end_time: new Date(T1).toISOString(),
  bucket_minutes: 60,
  timeseries_metric: "ttlb",
  timeseries_percentile: "p95",
  ip_health_limit: 30,
  sections: ["ip_health", "timeseries"],
  ...extra,
});

describe("origin rollup vs raw DuckDB over the same rows", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  const raw = async (extra: Record<string, unknown> = {}) => {
    const request = new Request("http://localhost/api/origin/aggregates", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body(extra)),
    });
    const response = await handleOriginRequest(request, undefined, undefined, lease.readContext());
    return await response!.json() as OriginAggregatesResponse;
  };

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

  it("matches raw per-origin-IP request counts and error shares exactly, with approximate OTTFB", async () => {
    const want = (await raw()).ip_health;
    const got = originAggregatesFromRollup(await rollupOf(WINDOWS), body())!.ip_health;
    expect(want.has_data).toBe(true);
    expect(got.has_data).toBe(true);
    const exact = (rows: typeof want.rows) => rows.map((r) => [r.oip, r.requests, r.error_pct]).toSorted();
    expect(exact(got.rows)).toEqual(exact(want.rows));
    expect(got.rows.map((r) => r.oip)).not.toContain("-");
    const byIp = new Map(want.rows.map((r) => [r.oip, r]));
    for (const row of got.rows) {
      near(row.p50_ms, byIp.get(row.oip)!.p50_ms);
      near(row.p95_ms, byIp.get(row.oip)!.p95_ms);
    }
    expect(got.rows.map((r) => r.error_pct)).toEqual(got.rows.map((r) => r.error_pct).toSorted((a, b) => b! - a!));
  });

  it("matches raw OTTLB miss counts exactly per 5 minutes, hour and day, with approximate percentiles", async () => {
    for (const [bucket, list, step] of [[60, WINDOWS, 300_000], [60, files("day"), HOUR], [5, WINDOWS, 300_000]] as const) {
      for (const percentile of ["p50", "p95", "p99"]) {
        const want = (await raw({ bucket_minutes: bucket, timeseries_percentile: percentile })).timeseries;
        const got = originAggregatesFromRollup(await rollupOf([...list], step), body({
          bucket_minutes: bucket, timeseries_percentile: percentile,
        }))!.timeseries;
        expect(got.series.map((p) => [p.time, p.miss_count])).toEqual(want.series.map((p) => [p.time, p.miss_count]));
        // A 5-minute bucket holds ~35 rows, where one rank spans a wide gap of the fixture's spread latencies.
        if (bucket > 5) got.series.forEach((p, i) => near(p.value, want.series[i].value, 0.15));
      }
    }
  });

  it("matches raw per-minute series counts exactly from window summaries and hour bundles, never day bundles", async () => {
    for (const [list, step] of [[WINDOWS, 300_000], [HOURS, 300_000]] as const) {
      const rollup = await rollupOf([...list], step);
      for (const bucket of [1, 2, 3]) {
        for (const metric of ["ttfb", "ttlb"]) {
          for (const split of [false, true]) {
            const extra = { bucket_minutes: bucket, timeseries_metric: metric, split_by_leg: split, sections: ["timeseries"] };
            const want = (await raw(extra)).timeseries;
            const got = originAggregatesFromRollup(rollup, body(extra))?.timeseries;
            const what = JSON.stringify({ list: list[0].key, ...extra });
            expect(want.series.length, what).toBeGreaterThan(20);
            // Raw orders by time only, so legs within a bucket come in any order.
            const points = (series: OriginTimeseriesPoint[]) =>
              series.map((p) => [p.time, String(p.edge), p.miss_count]).toSorted((x, y) => (x.join() < y.join() ? -1 : 1));
            expect(points(got?.series ?? []), what).toEqual(points(want.series));
          }
        }
      }
    }
    const day = await rollupOf(files("day"), HOUR);
    expect(originAggregatesFromRollup(day, body({ bucket_minutes: 1, sections: ["timeseries"] }))).toBeNull();
  });

  it("matches raw origin status code counts and shares exactly from window summaries and day bundles", async () => {
    const want = (await raw({ sections: ["status_codes"] })).status_codes;
    expect(want.rows.length).toBeGreaterThan(2);
    for (const [list, step] of [[WINDOWS, 300_000], [files("day"), HOUR]] as const) {
      const got = originAggregatesFromRollup(await rollupOf([...list], step), body({ sections: ["status_codes"] }))!;
      expect(got.status_codes.has_data).toBe(true);
      expect(got.status_codes.rows.toSorted((a, b) => a.status - b.status))
        .toEqual(want.rows.toSorted((a, b) => a.status - b.status));
      const counts = got.status_codes.rows.map((r) => r.count);
      expect(counts).toEqual(counts.toSorted((a, b) => b - a));
      expect(got.rollup.approximate).toEqual([]);
    }
  });

  it("matches the raw origin summary's MISS/PASS counts and error rate exactly, with approximate percentiles", async () => {
    const want = (await raw({ sections: ["summary"] })).summary;
    expect(want.has_data).toBe(true);
    for (const [list, step] of SPANS) {
      const got = originAggregatesFromRollup(await rollupOf([...list], step), body({ sections: ["summary"] }))!.summary;
      expect([got.has_data, got.total_misses, got.total_passes, got.origin_error_rate])
        .toEqual([true, want.total_misses, want.total_passes, want.origin_error_rate]);
      for (const key of ["ottfb_p50_ms", "ottfb_p75_ms", "ottfb_p95_ms", "ottfb_p99_ms", "ottlb_p50_ms", "ottlb_p95_ms",
        "cdn_overhead_p50_ms", "obytes_p50"] as const) near(got[key], want[key], 0.15);
    }
  });

  it("matches raw slow URLs' request counts exactly, with approximate percentiles, ranked by p95", async () => {
    for (const min of [10, 50]) {
      const extra = { sections: ["slow_urls"], slow_urls_min_requests: min, slow_urls_limit: 100 };
      const want = (await raw(extra)).slow_urls;
      expect(want.rows.length).toBeGreaterThan(20);
      for (const [list, step] of SPANS) {
        const got = originAggregatesFromRollup(await rollupOf([...list], step), body(extra))!;
        const exact = (rows: typeof want.rows) => rows.map((r) => [r.url, r.requests]).toSorted();
        expect(exact(got.slow_urls.rows)).toEqual(exact(want.rows));
        const byUrl = new Map(want.rows.map((r) => [r.url, r]));
        for (const row of got.slow_urls.rows) near(row.p95_ms, byUrl.get(row.url)!.p95_ms, 0.2);
        const p95s = got.slow_urls.rows.map((r) => r.p95_ms!);
        expect(p95s).toEqual(p95s.toSorted((a, b) => b - a));
        expect(got.rollup.lower_bound).toEqual([]);
      }
    }
    const top = originAggregatesFromRollup(await rollupOf(WINDOWS), body({ sections: ["slow_urls"] }))!;
    expect(top.slow_urls.rows).toHaveLength(20);
  });

  it("matches raw path-leg and POP request counts exactly, with approximate percentiles", async () => {
    const extra = { sections: ["path_breakdown", "pop_latency"], pop_latency_limit: 100 };
    const want = await raw(extra);
    expect(want.path_breakdown.rows.length).toBe(3);
    for (const [list, step] of SPANS) {
      const got = originAggregatesFromRollup(await rollupOf([...list], step), body(extra))!;
      const legs = (rows: typeof want.path_breakdown.rows) => rows.map((r) => [String(r.edge), r.requests]).toSorted();
      expect(legs(got.path_breakdown.rows)).toEqual(legs(want.path_breakdown.rows));
      expect(got.path_breakdown.shielding_detected).toBe(want.path_breakdown.shielding_detected);
      for (const row of got.path_breakdown.rows) {
        near(row.p95_ms, want.path_breakdown.rows.find((r) => r.edge === row.edge)!.p95_ms, 0.15);
      }
      const pops = (rows: typeof want.pop_latency.rows) => rows.map((r) => [r.pop, r.requests]).toSorted();
      expect(pops(got.pop_latency.rows)).toEqual(pops(want.pop_latency.rows));
      expect(got.pop_latency.requires_group_c).toBe(want.pop_latency.requires_group_c);
      near(got.pop_latency.median_p95_ms, want.pop_latency.median_p95_ms, 0.15);
      const p95s = got.pop_latency.rows.map((r) => r.p95_ms!);
      expect(p95s).toEqual(p95s.toSorted((a, b) => b - a));
    }
  });

  it("matches raw TTFB and per-leg miss counts exactly per bucket, with approximate percentiles", async () => {
    for (const [metric, split] of [["ttfb", false], ["ttfb", true], ["ttlb", true]] as const) {
      for (const [list, step] of SPANS) {
        const extra = { sections: ["timeseries"], timeseries_metric: metric, split_by_leg: split };
        const want = (await raw(extra)).timeseries;
        const got = originAggregatesFromRollup(await rollupOf([...list], step), body(extra))!.timeseries;
        expect(want.series.length).toBeGreaterThan(split ? 2 : 1);
        const counts = (series: typeof want.series) => series.map((p) => [p.time, String(p.edge), p.miss_count]).toSorted();
        expect(counts(got.series)).toEqual(counts(want.series));
        for (const p of got.series) {
          near(p.value, want.series.find((w) => w.time === p.time && w.edge === p.edge)!.value, 0.2);
        }
      }
    }
  });
});

describe("originAggregatesFromRollup", () => {
  it("labels histogram percentiles approximate and reports the data span", async () => {
    const result = { ...(await rollupOf(WINDOWS)), dataThroughMs: T0 + HOUR + 45 * 60_000 };
    const out = originAggregatesFromRollup(result, body())!;
    expect(out.rollup).toEqual({
      data_from: new Date(T0).toISOString(),
      data_through: new Date(T0 + HOUR + 45 * 60_000).toISOString(),
      approximate: ["ip_health.p50_ms", "ip_health.p95_ms", "timeseries.value"],
      lower_bound: [],
    });
    expect(out.has_data).toBe(true);
  });

  it("marks origin-IP counts lower bounds and their error shares approximate when the IPs were cut to a top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["dims/oip"]) };
    const meta = originAggregatesFromRollup({ ...result, rollup: cut }, body())!.rollup;
    expect(meta.lower_bound).toEqual(["ip_health.requests"]);
    expect(meta.approximate).toContain("ip_health.error_pct");
  });

  it("returns null when an origin IP's latency mixes OTTFB with client TTFB, which no histogram holds", async () => {
    const result = await rollupOf(WINDOWS);
    const oips = new Map(result.rollup.dims.get("oip"));
    const [ip, agg] = [...oips].find(([key]) => key !== "-")!;
    oips.set(ip, { ...agg, latUsCount: agg.latUsCount + 1 });
    const mixed: Rollup = { ...result.rollup, dims: new Map([...result.rollup.dims, ["oip", oips]]) };
    expect(originAggregatesFromRollup({ ...result, rollup: mixed }, body({ sections: ["ip_health"] }))).toBeNull();
  });

  it("returns null when the series bucket is not a whole number of rollup steps", async () => {
    const day = await rollupOf(files("day"), HOUR);
    expect(originAggregatesFromRollup(day, body({ bucket_minutes: 5 }))).toBeNull();
    expect(originAggregatesFromRollup(day, body({ sections: ["ip_health"], bucket_minutes: 5 }))).not.toBeNull();
  });

  it("folds origin statuses outside 100-599 into -1 and orders by count, then status", async () => {
    const result = await rollupOf(WINDOWS);
    const fields = new Map(result.rollup.fields);
    fields.set("ost", new Map([["200", 5], ["0", 1], ["999", 2], ["302", 1], ["404", 1]]));
    const out = originAggregatesFromRollup({ ...result, rollup: { ...result.rollup, fields } }, body({ sections: ["status_codes"] }))!;
    expect(out.status_codes).toEqual({
      has_data: true,
      rows: [
        { status: 200, count: 5, pct: 50 },
        { status: -1, count: 3, pct: 30 },
        { status: 302, count: 1, pct: 10 },
        { status: 404, count: 1, pct: 10 },
      ],
    });
  });

  it("returns null when origin statuses were cut to a top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["all_fields/ost"]) };
    expect(originAggregatesFromRollup({ ...result, rollup: cut }, body({ sections: ["ip_health", "status_codes"] }))).toBeNull();
    expect(originAggregatesFromRollup({ ...result, rollup: cut }, body({ sections: ["ip_health"] }))).not.toBeNull();
  });

  it("returns null when a summary predates a section's rows, so raw answers", async () => {
    const result = await rollupOf(WINDOWS);
    for (const [section, row, extra] of [
      ["summary", "origin/overall", {}],
      ["path_breakdown", "origin/edge", {}],
      ["pop_latency", "origin/pop", {}],
      ["slow_urls", "urls/url", {}],
      ["timeseries", "series5/origin", { timeseries_metric: "ttfb" }],
      ["timeseries", "series5/origin", { split_by_leg: true }],
    ] as const) {
      const present = new Set(result.rollup.present);
      expect(present.delete(row), row).toBe(true);
      const params = body({ sections: [section], ...extra });
      expect(originAggregatesFromRollup(result, params), row).not.toBeNull();
      expect(originAggregatesFromRollup({ ...result, rollup: { ...result.rollup, present } }, params), row).toBeNull();
    }
  });

  it("returns null when any merged summary predates the rows, so a mixed range scans raw", async () => {
    const parts = await Promise.all(WINDOWS.map((f) => decodeRollup(BUFFERS[f.key], 300_000)));
    const old = { ...parts[0]!, present: new Set([...parts[0]!.present!].filter((row) => !row.startsWith("origin/"))) };
    const result = { rollup: mergeRollups([old, ...parts.slice(1).map((p) => p!)]), dataFromMs: T0, dataThroughMs: T1 };
    expect(originAggregatesFromRollup(result, body({ sections: ["summary"] }))).toBeNull();
    expect(originAggregatesFromRollup(result, body({ sections: ["slow_urls"] }))).not.toBeNull();
  });

  it("returns null when the median CDN overhead falls among negative values, which a histogram can't place", async () => {
    const result = await rollupOf(WINDOWS);
    const withOverhead = (under: number, min: number): RollupResult => {
      const overall = result.rollup.groups!.get("origin")!.get("overall")!;
      const agg = overall.get("")!;
      const overhead = new Histogram();
      Object.assign(overhead, { under, min, max: 5_000 });
      overhead.buckets[10] = 3;
      const groups = new Map(result.rollup.groups);
      groups.set("origin", new Map([...groups.get("origin")!, ["overall", new Map([["", { ...agg, hists: { ...agg.hists, overhead } }]])]]));
      return { ...result, rollup: { ...result.rollup, groups } };
    };
    const params = body({ sections: ["summary"] });
    expect(originAggregatesFromRollup(withOverhead(4, -50_000), params)).toBeNull();
    expect(originAggregatesFromRollup(withOverhead(2, -50_000), params)!.summary.cdn_overhead_p50_ms).toBeGreaterThan(0);
    expect(originAggregatesFromRollup(withOverhead(4, 20), params)!.summary.cdn_overhead_p50_ms).not.toBeNull();
  });

  it("marks slow-URL request counts lower bounds when the URLs were cut to a top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["urls/url"]) };
    const meta = originAggregatesFromRollup({ ...result, rollup: cut }, body({ sections: ["slow_urls"] }))!.rollup;
    expect(meta.lower_bound).toEqual(["slow_urls.requests"]);
    expect(meta.approximate).toEqual(["slow_urls.p50_ms", "slow_urls.p95_ms", "slow_urls.p99_ms"]);
  });

  it("answers an empty rollup with empty sections", () => {
    const empty: Rollup = { dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set() };
    const out = originAggregatesFromRollup({ rollup: empty, dataFromMs: T0, dataThroughMs: T1 },
      body({ sections: ["ip_health", "timeseries", "status_codes"] }))!;
    expect(out.has_data).toBe(false);
    expect(out.status_codes).toEqual({ has_data: false, rows: [] });
    expect(out.ip_health).toEqual({ has_data: false, rows: [] });
    expect(out.timeseries).toEqual({ has_data: false, series: [] });
  });
});

describe("originRollupSections", () => {
  it("offers every table always and the latency series only on whole-minute steps that tile a day", () => {
    const tables = PAGE_SECTIONS.filter((s) => s !== "timeseries");
    expect(originRollupSections({ metric: "ttlb", bucketMinutes: 1 })).toContain("timeseries");
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 2, splitByLeg: true })).toContain("timeseries");
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 35 })).toEqual(tables);
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 7 })).toEqual(tables);
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 0.5 })).toEqual(tables);
    expect(originRollupSections({ metric: "other", bucketMinutes: 60 })).toEqual(tables);
    for (const metric of ["ttfb", "ttlb"]) {
      for (const splitByLeg of [false, true]) {
        expect(originRollupSections({ metric, bucketMinutes: 60, splitByLeg }).toSorted()).toEqual(PAGE_SECTIONS.toSorted());
      }
    }
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 1440 })).toContain("timeseries");
  });
});

describe("originFromRollup", () => {
  const source = (summaries: ManifestFile[] = WINDOWS, buffers = BUFFERS) =>
    fixtureSource([fixturePage(RAW)], buffers, { json: [], files: [] }, [fixturePage(summaries)]);

  it("answers unfiltered origin IP health, status codes and OTTLB series from summaries without reading raw files", async () => {
    const s = source();
    const params = body({ sections: ["ip_health", "status_codes", "timeseries"] });
    const served = await originFromRollup("/api/origin/aggregates", params, s) as OriginAggregatesResponse;
    const direct = originAggregatesFromRollup(await rollupOf(WINDOWS), params)!;
    expect(served.ip_health).toEqual(direct.ip_health);
    expect(served.status_codes).toEqual(direct.status_codes);
    expect(served.status_codes.has_data).toBe(true);
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
  });

  it("answers the page's full unfiltered TTFB read from summaries without reading raw files", async () => {
    const s = source();
    const params = body({ sections: PAGE_SECTIONS, timeseries_metric: "ttfb", slow_urls_min_requests: 50 });
    const served = await originFromRollup("/api/origin/aggregates", params, s) as OriginAggregatesResponse;
    expect(served).toEqual(originAggregatesFromRollup(await rollupOf(WINDOWS), params));
    expect([served.summary, served.timeseries, served.slow_urls, served.path_breakdown, served.pop_latency]
      .every((section) => section.has_data)).toBe(true);
    expect(s.log.files.length).toBeGreaterThan(0);
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
  });

  it("answers the page's 1-minute read from window summaries and hour bundles without reading raw files", async () => {
    for (const summaries of [WINDOWS, HOURS]) {
      const s = source(summaries);
      const params = body({ sections: PAGE_SECTIONS, timeseries_metric: "ttfb", bucket_minutes: 1 });
      const served = await originFromRollup("/api/origin/aggregates", params, s) as OriginAggregatesResponse;
      expect(served).toEqual(originAggregatesFromRollup(await rollupOf(summaries), params));
      expect(served.timeseries.series.length).toBeGreaterThan(60);
      expect(s.log.files.length).toBeGreaterThan(0);
      expect(s.log.files.every((key) => summaries.some((f) => f.key === key))).toBe(true);
    }
  });

  it("leaves filtered reads, raw-only sections and options and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ custom_filters: { tier: { values: ["a"], mode: "include" } } }),
      body({ sections: ["ip_health", "drilldown"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ timeseries_metric: "ttfb", bucket_minutes: 7 }),
      body({ end_time: "not a time" }),
    ]) {
      expect(originRollupEligible("/api/origin/aggregates", params) &&
        await originFromRollup("/api/origin/aggregates", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(originRollupEligible("/api/origin/ip-health", body())).toBe(false);
    expect(await originFromRollup("/api/origin/aggregates", body({ filters: {} }), source())).not.toBeNull();
  });

  it("answers /api/origin/shielding-analysis from shield_routes when covered", async () => {
    const h = new Histogram();
    h.min = 30_000;
    h.max = 30_000;
    h.buckets[52] = 5;
    const hBytes = h.toBytes()!;
    const t = new Date(T0);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["overview", "dim_totals", "shield_routes"], type: "STRING" },
        { name: "bucket", data: [t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["", "route", "route"], type: "STRING" },
        { name: "value", data: ["", "shield_routes", JSON.stringify(["EWR", "IAD"])], type: "STRING" },
        { name: "requests", data: [BigInt(5), BigInt(5), BigInt(5)], type: "INT64" },
        { name: "errors", data: [null, null, BigInt(0)], type: "INT64" },
        { name: "truncated", data: [null, false, null], type: "BOOLEAN" },
        { name: "overhead_hist", data: [null, null, hBytes], type: "BYTE_ARRAY" },
      ],
    });
    const win = WINDOWS[0];
    const s = source([win], { [win.key]: summaryBuf });
    const served = await originFromRollup(
      "/api/origin/shielding-analysis",
      { start_time: win.start, end_time: win.end, limit: 50 },
      s,
    ) as {
      has_data: boolean;
      total_routes: number;
      _approx: boolean;
      _approx_note: string;
      rows: Array<{ edge_pop: string; shield_pop: string; requests: number }>;
    };
    expect(served.has_data).toBe(true);
    expect(served.total_routes).toBe(1);
    expect(served._approx).toBe(true);
    expect(served._approx_note).toBe("≈ (window-local join)");
    expect(served.rows[0]).toMatchObject({ edge_pop: "EWR", shield_pop: "IAD", requests: 5 });
    expect(s.log.files).toEqual([win.key]);
    expect(originRollupEligible("/api/origin/shielding-analysis", { filters: { pop: { mode: "include", values: ["IAD"] } } })).toBe(false);
  });

  it("serves filtered /api/origin/aggregates summary from filtered_rollups for hour-aligned single low-card filters", async () => {
    const ottfbHist = new Histogram();
    ottfbHist.min = 10_000;
    ottfbHist.max = 10_000;
    ottfbHist.buckets[44] = 3;
    const ottlbHist = new Histogram();
    ottlbHist.min = 20_000;
    ottlbHist.max = 20_000;
    ottlbHist.buckets[49] = 3;
    const hourStart = Math.floor(T0 / 3_600_000) * 3_600_000;
    const hourEnd = hourStart + 3_600_000;
    const t = new Date(hourStart);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["overview", "dim_totals", "filtered_rollups"], type: "STRING" },
        { name: "bucket", data: [t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["", "pop", "pop"], type: "STRING" },
        { name: "value", data: ["", "filtered_rollups", "IAD"], type: "STRING" },
        { name: "requests", data: [BigInt(10), BigInt(10), BigInt(3)], type: "INT64" },
        { name: "miss_requests", data: [null, null, BigInt(2)], type: "INT64" },
        { name: "pass_requests", data: [null, null, BigInt(1)], type: "INT64" },
        { name: "ost_5xx_count", data: [null, null, BigInt(1)], type: "INT64" },
        { name: "ost_total_count", data: [null, null, BigInt(3)], type: "INT64" },
        { name: "truncated", data: [null, false, null], type: "BOOLEAN" },
        { name: "ottfb_hist", data: [null, null, ottfbHist.toBytes()!], type: "BYTE_ARRAY" },
        { name: "ottlb_hist", data: [null, null, ottlbHist.toBytes()!], type: "BYTE_ARRAY" },
      ],
    });
    const hour = HOURS[0];
    const s = source([hour], { [hour.key]: summaryBuf });
    const reqBody = {
      start_time: hour.start,
      end_time: hour.end,
      sections: ["summary"],
      filters: { pop: { mode: "include", values: ["IAD"] } },
    };
    expect(originRollupEligible("/api/origin/aggregates", reqBody)).toBe(true);
    const served = await originFromRollup("/api/origin/aggregates", reqBody, s) as {
      has_data: boolean;
      summary: { total_misses: number; total_passes: number; origin_error_rate: number | null; ottfb_p50_ms: number | null };
      rollup: { approximate: string[]; lower_bound: string[] };
    };
    expect(served.has_data).toBe(true);
    expect(served.summary.total_misses).toBe(2);
    expect(served.summary.total_passes).toBe(1);
    expect(served.summary.origin_error_rate).toBeCloseTo(1 / 3);
    expect(served.summary.ottfb_p50_ms).toBeGreaterThan(5);
    expect(served.rollup.lower_bound).toEqual([]);

    // Non-hour-aligned, multi-field, custom_filters, or non-summary sections must fall back.
    expect(originRollupEligible("/api/origin/aggregates", { ...reqBody, start_time: hourStart + 60_000 })).toBe(false);
    expect(originRollupEligible("/api/origin/aggregates", { ...reqBody, sections: ["summary", "slow_urls"] })).toBe(false);
    expect(originRollupEligible("/api/origin/aggregates", { ...reqBody, custom_filters: [{ field: "x", value: "y" }] })).toBe(false);
  });
});
