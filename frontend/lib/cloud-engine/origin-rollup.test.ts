// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type Rollup, type RollupResult } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { handleOriginRequest, type OriginAggregatesResponse } from "./origin";
import { originAggregatesFromRollup, originFromRollup, originRollupEligible } from "./origin-rollup";
import { originRollupSections } from "./rollup-sections";

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
  it("offers origin IP health and status codes always and the series only for OTTLB on whole 5-minute steps", () => {
    const always = ["ip_health", "status_codes"];
    expect(originRollupSections({ metric: "ttfb", bucketMinutes: 60 })).toEqual(always);
    expect(originRollupSections({ metric: "ttlb", bucketMinutes: 1 })).toEqual(always);
    expect(originRollupSections({ metric: "ttlb", bucketMinutes: 60, splitByLeg: true })).toEqual(always);
    expect(originRollupSections({ metric: "ttlb", bucketMinutes: 60 })).toEqual([...always, "timeseries"]);
    expect(originRollupSections({ metric: "ttlb", bucketMinutes: 1440 })).toEqual([...always, "timeseries"]);
  });
});

describe("originFromRollup", () => {
  const source = (summaries: ManifestFile[] = WINDOWS) =>
    fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(summaries)]);

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

  it("leaves filtered reads, raw-only sections and options and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ custom_filters: { tier: { values: ["a"], mode: "include" } } }),
      body({ sections: ["ip_health", "summary"] }),
      body({ sections: ["pop_latency"] }),
      body({ sections: ["status_codes", "slow_urls"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ timeseries_metric: "ttfb" }),
      body({ bucket_minutes: 1 }),
      body({ split_by_leg: true }),
      body({ end_time: "not a time" }),
    ]) {
      expect(originRollupEligible("/api/origin/aggregates", params) &&
        await originFromRollup("/api/origin/aggregates", params, source()), JSON.stringify(params)).toBeFalsy();
    }
    expect(originRollupEligible("/api/origin/ip-health", body())).toBe(false);
    expect(await originFromRollup("/api/origin/aggregates", body({ filters: {} }), source())).not.toBeNull();
  });
});
