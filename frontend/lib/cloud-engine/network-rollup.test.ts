// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import type { ManifestFile } from "./dashboard/manifest";
import {
  decodeRollup,
  mergeRollups,
  type Rollup,
  type RollupResult,
} from "./dashboard/rollup-query";
import {
  FIXTURE_START,
  SUMMARY_FIXTURE_BUFFERS as BUFFERS,
  fixtureFiles,
  fixturePage,
} from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { handleNetworkRequest, queryNetworkHealth, queryPopHealth } from "./network";
import {
  canServeHealthFromRollup,
  networkFromRollup,
  networkHealthFromRollup,
  popHealthFromRollup,
  transportFromRollup,
} from "./network-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const RAW = fixtureFiles("raw");
const WINDOWS = fixtureFiles("window");
const HOURS = fixtureFiles("hour").filter((f) => f.key.includes("-c4-"));
const SECTIONS = ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets"];

async function rollupOf(files: ManifestFile[], stepMs = 300_000): Promise<RollupResult> {
  const parts = await Promise.all(files.map((f) => decodeRollup(BUFFERS[f.key], stepMs)));
  return {
    rollup: mergeRollups(parts.map((p) => p!)),
    dataFromMs: T0,
    dataThroughMs: T1,
  };
}

const near = (actual: unknown, expected: number, rel = 0.1) => {
  expect(actual).not.toBeNull();
  expect(Math.abs(Number(actual) - expected)).toBeLessThanOrEqual(Math.abs(expected) * rel);
};

const byKey = <T extends Record<string, unknown>>(rows: readonly T[], key: (row: T) => string) =>
  new Map(rows.map((row) => [key(row), row]));

describe("network rollup vs raw DuckDB over the same rows", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  let raw: Awaited<ReturnType<typeof queryNetworkHealth>>;
  let rawPops: Awaited<ReturnType<typeof queryPopHealth>>;
  let rawBundle: Record<string, unknown>;
  let rawTransport: Record<string, unknown>;

  beforeAll(async () => {
    for (const file of RAW) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, BUFFERS[file.key], undefined, undefined, "request", {
        deferView: true,
      });
    }
    await lease.publishView();
    const params = { engine: lease.readContext(), start_time: T0, end_time: T1, bucket_seconds: 900, top_n: 30 };
    raw = await queryNetworkHealth({ ...params, sections: SECTIONS });
    rawPops = await queryPopHealth(params);
    rawBundle = await handleNetworkRequest("/api/network/bundle", { ...params, sections: [...SECTIONS, "pop_health"] });
    rawTransport = (await handleNetworkRequest("/api/network/bundle", { ...params, sections: ["transport"] }) as {
      transport: Record<string, unknown>;
    }).transport;
  }, 60_000);

  afterAll(async () => {
    lease.release();
    await engine.reset();
  });

  it("matches the raw summary counts exactly and its RTT quantiles approximately", async () => {
    const health = networkHealthFromRollup(await rollupOf(WINDOWS), { bucket_seconds: 900, top_n: 30 })!;
    expect(raw.has_data).toBe(true);
    expect(health.summary.total_requests).toBe(raw.summary.total_requests);
    expect(health.summary.avg_rtt_ms).toBe(raw.summary.avg_rtt_ms);
    expect(health.summary.avg_ploss_pct).toBe(raw.summary.avg_ploss_pct);
    near(health.summary.median_rtt_ms, raw.summary.median_rtt_ms);
    near(health.summary.p95_rtt_ms, raw.summary.p95_rtt_ms);
    // The score moves with the histogram median and p10, so allow a few points either way.
    expect(Math.abs(health.summary.global_health_score! - raw.summary.global_health_score!)).toBeLessThanOrEqual(5);
  });

  it("matches raw per-country, per-ASN and per-metro counts, loss and 5xx shares", async () => {
    const health = networkHealthFromRollup(await rollupOf(WINDOWS), { bucket_seconds: 900, top_n: 30 })!;
    const exact = (row: Record<string, unknown>) => [row.reqs, row.avg_ploss, row.error_pct];

    const countries = byKey(health.by_country, (r) => String(r.country));
    expect([...countries.keys()].toSorted()).toEqual(raw.by_country.map((r) => String(r.country)).toSorted());
    expect(countries.has("Unknown")).toBe(true);
    for (const row of raw.by_country) {
      const got = countries.get(String(row.country))!;
      expect(exact(got), String(row.country)).toEqual(exact(row));
      near(got.rtt_med_us, Number(row.rtt_med_us));
    }

    const asns = byKey(health.by_asn, (r) => String(r.asn));
    expect([...asns.keys()].toSorted()).toEqual(raw.by_asn.map((r) => String(r.asn)).toSorted());
    expect(asns.has("0")).toBe(true);
    for (const row of raw.by_asn) {
      const got = asns.get(String(row.asn))!;
      expect([...exact(got), got.as_name], String(row.asn)).toEqual([...exact(row), row.as_name]);
    }
    expect(health.leaderboard.map((r) => [r.asn, r.total_reqs]).toSorted())
      .toEqual(raw.leaderboard.map((r) => [r.asn, r.total_reqs]).toSorted());

    const metroKey = (r: Record<string, unknown>) => `${r.country}|${r.city}`;
    const metros = byKey(health.metro_leaderboard as Record<string, unknown>[], metroKey);
    const rawMetros = raw.metro_leaderboard as Record<string, unknown>[];
    expect([...metros.keys()].toSorted()).toEqual(rawMetros.map(metroKey).toSorted());
    for (const row of rawMetros) {
      expect(exact(metros.get(metroKey(row))!), metroKey(row)).toEqual(exact(row));
    }
    expect(health.has_metro).toBe(true);
  });

  it("matches the raw ASN heatmap cell counts and buckets", async () => {
    const health = networkHealthFromRollup(await rollupOf(WINDOWS), { bucket_seconds: 900, top_n: 30 })!;
    expect(health.buckets).toEqual(raw.buckets);
    expect(health.buckets).toHaveLength(8);
    type Row = { asn: number; label: string; total_reqs: number; buckets: Record<string, unknown>[] };
    const cells = (rows: readonly Row[]) =>
      Object.fromEntries(rows.map((row) => [
        row.asn,
        // Loss averages differ from DuckDB's only in float summation order.
        [row.label, row.total_reqs, row.buckets.map((b) => [
          b.bucket, b.bucket_idx, b.reqs, b.avg_ploss == null ? null : Number(Number(b.avg_ploss).toFixed(12)), b.error_pct,
        ])],
      ]));
    expect(cells(health.heatmap as Row[])).toEqual(cells(raw.heatmap as Row[]));
  });

  it("matches raw PoP counts, bytes and 5xx rates, with approximate latency", async () => {
    const pops = popHealthFromRollup(await rollupOf(WINDOWS));
    const exact = (row: Record<string, unknown>) =>
      [row.requests, row.errors, row.error_rate, row.error_pct, row.bandwidth_bytes, row.cache_hit_rate];
    const rawByPop = byKey(rawPops.data, (r) => String(r.pop));
    expect(pops.data.map((r) => String(r.pop)).toSorted()).toEqual([...rawByPop.keys()].toSorted());
    for (const row of pops.data) {
      const want = rawByPop.get(String(row.pop))!;
      expect(exact(row), String(row.pop)).toEqual(exact(want));
      near(row.p50_rtt_us, Number(want.p50_rtt_us));
      near(row.p95_ttfb_ms, Number(want.p95_ttfb_ms));
    }
    expect(pops.total_pops).toBe(rawPops.total_pops);
  });

  it("answers a health and PoP bundle like the raw bundle, from summaries alone", async () => {
    const source = fixtureSource([fixturePage(RAW)], BUFFERS, { json: [], files: [] }, [fixturePage(WINDOWS)]);
    const served = await networkFromRollup("/api/network/bundle", {
      start_time: T0, end_time: T1, bucket_seconds: 900, top_n: 30, sections: [...SECTIONS, "pop_health"],
    }, source) as Record<string, unknown>;
    expect(source.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
    const health = served.network_health as typeof raw;
    const rawHealth = rawBundle.network_health as typeof raw;
    expect(Object.keys(served).toSorted()).toEqual(Object.keys(rawBundle).toSorted());
    expect([served.available, served.quality, served.transport, served.network_quality])
      .toEqual([rawBundle.available, rawBundle.quality, rawBundle.transport, rawBundle.network_quality]);
    // Summaries only report the TCP fields they count as missing.
    expect(served.missing_tcp_fields).toEqual(health.missing_tcp_fields);
    expect(rawBundle.missing_tcp_fields).toEqual(expect.arrayContaining(served.missing_tcp_fields as string[]));
    expect(served.health).toBe(served.network_health);
    expect([health.summary.total_requests, health.summary.avg_rtt_ms, health.summary.avg_ploss_pct])
      .toEqual([rawHealth.summary.total_requests, rawHealth.summary.avg_rtt_ms, rawHealth.summary.avg_ploss_pct]);
    near(health.summary.median_rtt_ms, rawHealth.summary.median_rtt_ms);
    expect(health.by_asn.map((r) => [r.asn, r.reqs, r.error_pct]).toSorted())
      .toEqual(rawHealth.by_asn.map((r) => [r.asn, r.reqs, r.error_pct]).toSorted());
    const pops = served.pop_health as typeof rawPops;
    const rawBundlePops = rawBundle.pop_health as typeof rawPops;
    expect(pops.data.map((r) => [r.pop, r.requests, r.errors, r.bandwidth_bytes]).toSorted())
      .toEqual(rawBundlePops.data.map((r) => [r.pop, r.requests, r.errors, r.bandwidth_bytes]).toSorted());
  });

  it("matches the raw transport counts, bytes, loss, delivery rate and protocol shares, with approximate RTT", async () => {
    expect(rawTransport.has_transport_data).toBe(true);
    expect((rawTransport.protocol_distribution as unknown[]).length).toBeGreaterThan(2);
    for (const [list, step] of [[WINDOWS, 300_000], [HOURS, 300_000], [fixtureFiles("day"), HOUR]] as const) {
      const got = transportFromRollup(await rollupOf([...list], step))! as Record<string, unknown>;
      const exact = ["total_requests", "has_transport_data", "missing_fields"];
      expect(Object.fromEntries(exact.map((k) => [k, got[k]]))).toEqual(Object.fromEntries(exact.map((k) => [k, rawTransport[k]])));
      // Raw SUM is a HUGEINT that comes back as a quoted string; summaries answer the number the schema declares.
      expect(got.bandwidth_bytes).toBe(Number(String(rawTransport.bandwidth_bytes).replaceAll('"', "")));
      // Raw orders protocols by requests only, so equal counts may come in either order.
      const shares = got.protocol_distribution as { protocol: string; requests: number }[];
      const byProtocol = (rows: unknown) => (rows as { protocol: string }[]).toSorted((x, y) => x.protocol.localeCompare(y.protocol));
      expect(byProtocol(shares)).toEqual(byProtocol(rawTransport.protocol_distribution));
      expect(shares.map((r) => r.requests)).toEqual(shares.map((r) => r.requests).toSorted((x, y) => y - x));
      expect(got.packet_loss_pct).toBeCloseTo(rawTransport.packet_loss_pct as number, 9);
      expect(got.delivery_rate_mbps).toBeCloseTo(rawTransport.delivery_rate_mbps as number, 9);
      for (const q of ["p50", "p90", "p95", "p99"]) near(got[`tcp_rtt_${q}_ms`], rawTransport[`tcp_rtt_${q}_ms`] as number);
      expect([got._approx, typeof got._approx_note]).toEqual([true, "string"]);
      const keys = (o: Record<string, unknown>) => Object.keys(o).filter((k) => !k.startsWith("_") && k !== "rollup").toSorted();
      expect(keys(got)).toEqual(keys(rawTransport));
    }
  });
});

describe("transportFromRollup", () => {
  it("returns null when a summary predates the transport rows, so raw answers", async () => {
    const result = await rollupOf(WINDOWS);
    for (const row of ["transport/overall", "transport/proto"]) {
      const present = new Set(result.rollup.present);
      present.delete(row);
      expect(transportFromRollup({ ...result, rollup: { ...result.rollup, present } }), row).toBeNull();
    }
  });

  it("labels RTT approximate and protocol counts lower bounds when protocols were cut to a top K", async () => {
    const result = await rollupOf(WINDOWS);
    const cut: Rollup = { ...result.rollup, truncated: new Set(["transport/proto"]) };
    expect(transportFromRollup(result)!.rollup.lower_bound).toEqual([]);
    expect(transportFromRollup({ ...result, rollup: cut })!.rollup).toMatchObject({
      approximate: ["tcp_rtt_p50_ms", "tcp_rtt_p90_ms", "tcp_rtt_p95_ms", "tcp_rtt_p99_ms"],
      lower_bound: ["protocol_distribution"],
    });
  });

  it("answers an empty span like the raw scan of no rows", () => {
    const empty: Rollup = {
      dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set(),
      present: new Set(["transport/overall", "transport/proto"]),
    };
    expect(transportFromRollup({ rollup: empty, dataFromMs: T0, dataThroughMs: T1 })).toMatchObject({
      total_requests: 0, bandwidth_bytes: null, tcp_rtt_p50_ms: null, packet_loss_pct: null, delivery_rate_mbps: null,
      has_transport_data: false, missing_fields: ["tcp_rtt"], protocol_distribution: [],
    });
  });
});

describe("networkHealthFromRollup", () => {
  it("labels histogram percentiles and scores approximate and reports the data span", async () => {
    const health = networkHealthFromRollup(
      { ...(await rollupOf(WINDOWS)), dataThroughMs: T0 + HOUR + 45 * 60_000 },
      { bucket_seconds: 900 },
    )!;
    expect(health.rollup).toEqual({
      data_from: new Date(T0).toISOString(),
      data_through: new Date(T0 + HOUR + 45 * 60_000).toISOString(),
      approximate: expect.arrayContaining(["median_rtt_ms", "p95_rtt_ms", "health_score", "global_health_score"]),
      lower_bound: [],
    });
    expect(health.rollup.approximate).not.toContain("avg_rtt_ms");
  });

  it("marks the counts of top-K-truncated ASN and metro tables as lower bounds", async () => {
    const result = await rollupOf(WINDOWS);
    const truncated: Rollup = { ...result.rollup, truncated: new Set(["dims/asn", "dims/city", "series5/asn"]) };
    const health = networkHealthFromRollup({ ...result, rollup: truncated }, { bucket_seconds: 900 })!;
    expect(health.rollup.lower_bound.toSorted()).toEqual(["by_asn", "heatmap", "leaderboard", "metro_leaderboard"]);
  });

  it("returns null when the heatmap bucket is not a whole number of rollup steps", async () => {
    expect(networkHealthFromRollup(await rollupOf(WINDOWS), { bucket_seconds: 60 })).toBeNull();
    const day = await rollupOf(fixtureFiles("day"), HOUR);
    expect(networkHealthFromRollup(day, { bucket_seconds: 900 })).toBeNull();
    const hourly = networkHealthFromRollup(day, { bucket_seconds: 3600 })!;
    expect(hourly.buckets).toEqual([new Date(T0).toISOString(), new Date(T0 + HOUR).toISOString()]);
  });

  it("answers an empty rollup with the empty response, still carrying its span", () => {
    const empty: Rollup = { dims: new Map(), series: new Map(), seriesStepMs: 300_000, fields: new Map(), truncated: new Set() };
    const health = networkHealthFromRollup({ rollup: empty, dataFromMs: T0, dataThroughMs: T1 }, {})!;
    expect(health.has_data).toBe(false);
    expect(health.summary.total_requests).toBe(0);
    expect(health.rollup.data_through).toBe(new Date(T1).toISOString());
  });
});

describe("popHealthFromRollup", () => {
  it("labels latency approximate and the HIT/HIT-STALE hit rate a lower bound", async () => {
    const pops = popHealthFromRollup(await rollupOf(WINDOWS));
    expect(pops.rollup.approximate).toEqual(expect.arrayContaining(["p50_rtt_us", "p95_ttfb_ms"]));
    expect(pops.rollup.lower_bound).toEqual(["cache_hit_rate"]);
  });
});

describe("canServeHealthFromRollup", () => {
  it("accepts only the rollup-served sections, never the map or shielding", () => {
    expect(canServeHealthFromRollup(SECTIONS)).toBe(true);
    expect(canServeHealthFromRollup(["summary"])).toBe(true);
    expect(canServeHealthFromRollup([...SECTIONS, "cities"])).toBe(false);
    expect(canServeHealthFromRollup(["map_buckets"])).toBe(false);
    expect(canServeHealthFromRollup(["shielding_analysis"])).toBe(false);
    expect(canServeHealthFromRollup(undefined)).toBe(false);
    expect(canServeHealthFromRollup([])).toBe(false);
  });
});

describe("networkFromRollup", () => {
  const source = (summaries: ManifestFile[] = WINDOWS, buffers = BUFFERS) =>
    fixtureSource([fixturePage(RAW)], buffers, { json: [], files: [] }, [fixturePage(summaries)]);
  const health = { start_time: new Date(T0).toISOString(), end_time: new Date(T1).toISOString(), sections: SECTIONS };

  it("answers unfiltered health and PoP reads from summaries without reading raw files", async () => {
    const s = source();
    const served = await networkFromRollup("/api/network-health", health, s) as { summary: { total_requests: number } };
    const direct = networkHealthFromRollup(await rollupOf(WINDOWS), {})!;
    expect(served.summary.total_requests).toBe(direct.summary.total_requests);
    const pops = await networkFromRollup("/api/network/pop-health", { start_time: T0, end_time: T1 }, s);
    expect(pops).toMatchObject({ total_pops: popHealthFromRollup(await rollupOf(WINDOWS)).total_pops });
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
  });

  it("answers the transport card from summaries without reading raw files", async () => {
    const s = source();
    const served = await networkFromRollup("/api/network/bundle", { ...health, sections: ["transport"] }, s) as Record<string, unknown>;
    expect(served.transport).toEqual(transportFromRollup(await rollupOf(WINDOWS)));
    expect([served.network_health, served.pop_health, served.quality]).toEqual([null, null, null]);
    expect(s.log.files.length).toBeGreaterThan(0);
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
    const both = await networkFromRollup("/api/network/bundle", { ...health, sections: ["summary", "transport"] }, source()) as
      Record<string, { total_requests?: number; summary?: { total_requests: number } }>;
    expect(both.transport.total_requests).toBe(both.network_health.summary!.total_requests);
  });

  it("omits the open tail newer than the newest summary and says how far the data goes", async () => {
    const served = await networkFromRollup("/api/network-health", health, source(WINDOWS.slice(0, 7))) as {
      summary: { total_requests: number }; rollup: { data_through: string };
    };
    const covered = networkHealthFromRollup(await rollupOf(WINDOWS.slice(0, 7)), {})!;
    expect(served.summary.total_requests).toBe(covered.summary.total_requests);
    expect(served.summary.total_requests).toBeLessThan(networkHealthFromRollup(await rollupOf(WINDOWS), {})!.summary.total_requests);
    expect(served.rollup.data_through).toBe(WINDOWS[6].end);
  });

  it("leaves filtered reads, raw-only sections and non-v3 windows to the raw scan", async () => {
    for (const params of [
      { ...health, filter_pop: "IAD" },
      { ...health, filter_country: "US" },
      { ...health, filter_asn: 64512 },
      { ...health, filters: { proto: { values: ["2"], mode: "include" as const } } },
      { ...health, sections: [...SECTIONS, "map_buckets"] },
      { ...health, sections: undefined },
      { ...health, end_time: "not a time" },
    ]) {
      const s = source();
      expect(await networkFromRollup("/api/network-health", params, s), JSON.stringify(params)).toBeNull();
      expect(s.log.json).toEqual([]);
    }
    expect(await networkFromRollup("/api/network-quality", health, source())).toBeNull();
    // Quality needs per-row throughput.
    for (const sections of [["quality"], ["transport", "quality"], ["map_buckets"], ["pop_health", "cities"], [], undefined]) {
      const s = source();
      expect(await networkFromRollup("/api/network/bundle", { ...health, sections }, s), String(sections)).toBeNull();
      expect(s.log.json).toEqual([]);
    }
    expect(await networkFromRollup("/api/network/bundle", { ...health, sections: ["pop_health"], filter_pop: "IAD" }, source()))
      .toBeNull();
    const stale = { ...BUFFERS, [WINDOWS[3].key]: BUFFERS[RAW[3].key] };
    expect(await networkFromRollup("/api/network-health", health, source(WINDOWS, stale))).toBeNull();
  });
});
