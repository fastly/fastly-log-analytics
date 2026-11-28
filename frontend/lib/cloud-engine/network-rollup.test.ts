// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => new Set(["client_rtt_us", "client_ploss", "client_tcpi_rtt", "client_tcpi_rttvar"]),
}));
import { parquetWriteBuffer } from "hyparquet-writer";
import { Histogram } from "./dashboard/histogram";
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
import { handleNetworkRequest, queryNetworkHealth, queryNetworkQuality, queryPopHealth } from "./network";
import {
  canServeHealthFromRollup,
  isNetworkPartial,
  mapFromRollup,
  mergeNetworkPartial,
  networkFromRollup,
  networkHealthFromRollup,
  networkRollupEligible,
  popHealthFromRollup,
  qualityFromRollup,
  transportFromRollup,
} from "./network-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const RAW = fixtureFiles("raw");
const WINDOWS = fixtureFiles("window");
const HOURS = fixtureFiles("hour").filter((f) => f.key.includes("-c4-"));
const SECTIONS = ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets"];
const MAP = ["buckets", "cities", "map_buckets"];

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

const withoutRows = (result: RollupResult, ...rows: string[]): RollupResult => {
  const present = new Set(result.rollup.present);
  for (const row of rows) present.delete(row);
  return { ...result, rollup: { ...result.rollup, present } };
};

type MapAnswer = NonNullable<ReturnType<typeof mapFromRollup>>;
/** Map cells keyed by bucket, country and city, since raw orders (and so indexes) equal counts arbitrarily. */
const mapCells = (map: Pick<MapAnswer, "cities" | "map_buckets">) => new Map(map.map_buckets.flatMap((b) =>
  b.cities.map(({ city_idx, ...c }) => [`${b.bucket}|${b.bucket_idx}|${c.country}|${map.cities[city_idx as number].name}`, c]),
));

const byKey = <T extends Record<string, unknown>>(rows: readonly T[], key: (row: T) => string) =>
  new Map(rows.map((row) => [key(row), row]));

describe("network rollup vs raw DuckDB over the same rows", () => {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  let raw: Awaited<ReturnType<typeof queryNetworkHealth>>;
  let rawPops: Awaited<ReturnType<typeof queryPopHealth>>;
  let rawBundle: Record<string, unknown>;
  let rawTransport: Record<string, unknown>;
  let rawMap: Awaited<ReturnType<typeof queryNetworkHealth>>;
  type QualityAsn = { asn: number; avg_ploss_pct: number; avg_rttvar_ms: number };
  let rawQuality: Awaited<ReturnType<typeof queryNetworkQuality>>;
  const rawAsns = () => rawQuality.by_asn as QualityAsn[];
  let rawParts: Record<string, unknown>;

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
    rawMap = await queryNetworkHealth({ ...params, sections: [...SECTIONS, "cities", "map_buckets"] });
    rawQuality = await queryNetworkQuality({ ...params, region_country: "US" });
    rawParts = await handleNetworkRequest("/api/network-quality", {
      ...params, region_country: "US", quality_parts: ["throughput", "region", "asn_client"],
      quality_asns: rawAsns().map((row) => row.asn),
    }) as Record<string, unknown>;
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
    expect(asns.has("0")).toBe(false);
    for (const row of raw.by_asn) {
      const got = asns.get(String(row.asn))!;
      expect([...exact(got), got.as_name], String(row.asn)).toEqual([...exact(row), row.as_name]);
      if (row.p95_rtt_us != null) expect(got.p95_rtt_us, String(row.asn)).toEqual(expect.any(Number));
      if (row.p99_rtt_us != null) expect(got.p99_rtt_us, String(row.asn)).toEqual(expect.any(Number));
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
    const pops = served.pop_health as typeof rawPops.data;
    const rawBundlePops = rawBundle.pop_health as typeof rawPops.data;
    expect(pops.map((r) => [r.pop, r.requests, r.errors, r.bandwidth_bytes]).toSorted())
      .toEqual(rawBundlePops.map((r) => [r.pop, r.requests, r.errors, r.bandwidth_bytes]).toSorted());
  });

  it("answers the map alone from a map-only raw pass exactly as the full pass does", async () => {
    const only = await queryNetworkHealth({ engine: lease.readContext(), start_time: T0, end_time: T1, bucket_seconds: 900, sections: MAP });
    expect(only.buckets).toEqual(rawMap.buckets);
    expect(only.cities.toSorted((a, b) => a.name.localeCompare(b.name)))
      .toEqual(rawMap.cities.toSorted((a, b) => a.name.localeCompare(b.name)));
    expect(mapCells(only as unknown as MapAnswer)).toEqual(mapCells(rawMap as unknown as MapAnswer));
    expect([only.heatmap, only.by_asn, only.summary.total_requests]).toEqual([[], [], 0]);
    expect(only.has_data).toBe(true);
  });

  it("matches the raw city map's counts, loss and 5xx shares, with approximate RTT and coordinates", async () => {
    for (const files of [WINDOWS, HOURS]) {
      const map = mapFromRollup(await rollupOf(files), { bucket_seconds: 900 })!;
      expect(map.buckets).toEqual(rawMap.buckets);
      const got = mapCells(map);
      const want = mapCells(rawMap as unknown as MapAnswer);
      expect(want.size).toBeGreaterThan(20);
      expect([...got.keys()].toSorted()).toEqual([...want.keys()].toSorted());
      for (const [key, row] of want) {
        const cell = got.get(key)!;
        const exact = (c: Record<string, unknown>) =>
          [c.reqs, c.error_pct, c.avg_ploss == null ? null : Number(Number(c.avg_ploss).toFixed(12)), c.country, c.metro_code];
        expect(exact(cell), key).toEqual(exact(row));
        // Histogram medians of a handful of rows can land a bin away from the interpolated raw median.
        if (row.rtt_med_us == null) expect(cell.rtt_med_us, key).toBeNull();
        else if (Number(row.reqs) >= 20) near(cell.rtt_med_us, Number(row.rtt_med_us), 0.15);
        else expect(cell.rtt_med_us, key).toEqual(expect.any(Number));
      }
      for (const city of rawMap.cities) {
        const mine = map.cities.find((c) => c.name === city.name)!;
        expect(mine.lat).toBeCloseTo(city.lat!, 9);
        expect(mine.lon).toBeCloseTo(city.lon!, 9);
      }
      // Each bucket's cells come busiest first, like raw.
      for (const bucket of map.map_buckets) {
        const reqs = bucket.cities.map((c) => c.reqs as number);
        expect(reqs).toEqual(reqs.toSorted((x, y) => y - x));
      }
    }
  });

  it("matches the raw quality tables and summary, with an approximate scatter", async () => {
    const pick = (q: Record<string, unknown>) => {
      const rows = (name: string, key: string) =>
        (q[name] as Record<string, unknown>[]).map((row) => JSON.stringify(row, Object.keys(row).toSorted())).toSorted()
          .map((row) => JSON.parse(row)).toSorted((a, b) => String(a[key]).localeCompare(String(b[key])));
      return {
        available: q.available,
        summary: q.summary,
        by_country: rows("by_country", "value"),
        by_asn: rows("by_asn", "value"),
        by_pop: rows("by_pop", "value"),
        by_region: rows("by_region", "value"),
        countries: (q.countries as string[]).toSorted(),
        protocol_distribution: q.protocol_distribution,
        region_country: q.region_country,
      };
    };
    expect((rawQuality.by_region as unknown[]).length).toBeGreaterThan(1);
    expect(rawQuality.summary.avg_throughput_mbps).toBeGreaterThan(0);
    expect(rawAsns().some((row) => row.avg_ploss_pct > 0 && row.avg_rttvar_ms > 0)).toBe(true);
    for (const [files, step] of [[WINDOWS, 300_000], [HOURS, 300_000], [fixtureFiles("day"), HOUR]] as const) {
      const got = qualityFromRollup(await rollupOf([...files], step), { region_country: "US" }) as Record<string, unknown>;
      expect(isNetworkPartial(got)).toBe(false);
      expect(pick(got)).toEqual(pick(rawQuality as Record<string, unknown>));
      // Summaries only report the TCP fields they count as missing.
      expect(rawQuality.missing_tcp_fields).toEqual(expect.arrayContaining(got.missing_tcp_fields as string[]));
      const scatter = got.scatter as { rtt_ms: number; ttfb_ms: number; cache: string }[];
      expect(scatter.length).toBeGreaterThan(100);
      expect(scatter.every((p) => p.rtt_ms > 0 && p.ttfb_ms >= 0 && typeof p.cache === "string")).toBe(true);
      expect((got.rollup as { approximate: string[] }).approximate).toEqual(["scatter"]);
    }
  });

  it("fills a summary answer that predates the quality rows from a raw read of only the missing parts", async () => {
    const full = qualityFromRollup(await rollupOf(WINDOWS), { region_country: "US" });
    for (const [rows, parts] of [
      [["quality/overall"], ["throughput"]],
      [["quality/region"], ["region"]],
      [["quality/asn"], ["asn_client"]],
      [["quality/overall", "quality/region", "quality/asn"], ["throughput", "region", "asn_client"]],
    ] as const) {
      const partial = qualityFromRollup(withoutRows(await rollupOf(WINDOWS), ...rows), { region_country: "US" });
      if (!isNetworkPartial(partial)) throw new Error(`not partial: ${rows}`);
      expect(partial.residual).toEqual({
        quality_parts: parts,
        ...parts.includes("asn_client" as never) ? { quality_asns: expect.any(Array) } : {},
        start_time: new Date(T0).toISOString(),
        end_time: new Date(T1).toISOString(),
      });
      const raw = await handleNetworkRequest("/api/network-quality", {
        engine: lease.readContext(), region_country: "US", ...partial.residual,
      });
      expect(Object.keys(raw)).toEqual(["quality_parts"]);
      expect(mergeNetworkPartial(partial, raw as never)).toEqual(full);
    }
    expect(Object.keys(rawParts.quality_parts as object).toSorted()).toEqual(["asn_client", "region", "throughput"]);
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
  it("accepts the rollup-served sections, shielding and the all-ASN map, never one ASN's map", () => {
    expect(canServeHealthFromRollup(SECTIONS)).toBe(true);
    expect(canServeHealthFromRollup(["summary"])).toBe(true);
    expect(canServeHealthFromRollup([...SECTIONS, "cities"])).toBe(true);
    expect(canServeHealthFromRollup(MAP, "all")).toBe(true);
    expect(canServeHealthFromRollup(MAP, "13335")).toBe(false);
    expect(canServeHealthFromRollup(["shielding_analysis"])).toBe(true);
    expect(canServeHealthFromRollup([...MAP, "shielding_analysis"])).toBe(true);
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

  it("answers the all-ASN map and quality from summaries without reading raw files", async () => {
    const s = source();
    const map = await networkFromRollup("/api/network-health", { ...health, sections: MAP, map_asn: "all", bucket_seconds: 3600 }, s) as
      Record<string, unknown>;
    expect(map).toMatchObject(mapFromRollup(await rollupOf(WINDOWS), { bucket_seconds: 3600 })!);
    expect([map.heatmap, map.by_asn]).toEqual([[], []]);
    const both = await networkFromRollup("/api/network-health", { ...health, sections: [...SECTIONS, "cities", "map_buckets"] }, s) as
      Record<string, unknown>;
    expect(both.cities).toEqual(mapFromRollup(await rollupOf(WINDOWS), {})!.cities);
    expect((both.summary as { total_requests: number }).total_requests).toBeGreaterThan(0);
    const quality = await networkFromRollup("/api/network-quality", { ...health, region_country: "US" }, s);
    expect(quality).toEqual(qualityFromRollup(await rollupOf(WINDOWS), { region_country: "US" }));
    expect(s.log.files.every((key) => WINDOWS.some((f) => f.key === key))).toBe(true);
  });

  it("leaves the map to raw when summaries predate its rows, and names only the missing quality parts", async () => {
    const result = await rollupOf(WINDOWS);
    expect(mapFromRollup(withoutRows(result, "series5/city"), {})).toBeNull();
    expect(mapFromRollup(result, { bucket_seconds: 60 })).toBeNull();
    expect(qualityFromRollup(withoutRows(result, "quality/region"), {})).toMatchObject({
      network_partial: true, residual: { quality_parts: ["region"] },
    });
    expect(networkRollupEligible("/api/network-quality", { network_residual: true })).toBe(false);
  });

  it("rolls up unsummarized tail raw files via NETWORK_RESIDUAL to match full summaries", async () => {
    const served = await networkFromRollup("/api/network-health", health, source(WINDOWS.slice(0, 6))) as {
      summary: { total_requests: number }; rollup: { data_through: string };
    };
    const full = networkHealthFromRollup(await rollupOf(WINDOWS), {})!;
    expect(served.summary.total_requests).toBe(full.summary.total_requests);
    expect(served.rollup.data_through).toBe(WINDOWS[7].end);
  });

  it("leaves filtered reads, raw-only sections and non-v3 windows to the raw scan", async () => {
    for (const params of [
      { ...health, filter_pop: "IAD" },
      { ...health, filter_country: "US" },
      { ...health, filter_asn: 64512 },
      { ...health, filters: { proto: { values: ["2"], mode: "include" as const } } },
      { ...health, sections: MAP, map_asn: "13335" },
      { ...health, sections: undefined },
      { ...health, end_time: "not a time" },
    ]) {
      const s = source();
      expect(await networkFromRollup("/api/network-health", params, s), JSON.stringify(params)).toBeNull();
      expect(s.log.json).toEqual([]);
    }
    for (const params of [
      { ...health, filter_pop: "IAD" },
      { ...health, network_residual: true },
      { ...health, quality_parts: ["throughput" as const] },
    ]) {
      expect(await networkFromRollup("/api/network-quality", params, source()), JSON.stringify(params)).toBeNull();
    }
    // The bundle never carries quality or the map: the page reads them alone.
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

  it("serves shielding from shield_routes when covered", async () => {
    const h = new Histogram();
    h.min = 30_000;
    h.max = 30_000;
    h.buckets[52] = 10;
    const hBytes = h.toBytes()!;
    const t = new Date(T0);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["overview", "dim_totals", "shield_routes"], type: "STRING" },
        { name: "bucket", data: [t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["", "route", "route"], type: "STRING" },
        { name: "value", data: ["", "shield_routes", JSON.stringify(["EWR", "IAD"])], type: "STRING" },
        { name: "requests", data: [BigInt(10), BigInt(10), BigInt(10)], type: "INT64" },
        { name: "errors", data: [null, null, BigInt(0)], type: "INT64" },
        { name: "truncated", data: [null, false, null], type: "BOOLEAN" },
        { name: "overhead_hist", data: [null, null, hBytes], type: "BYTE_ARRAY" },
      ],
    });
    const win = WINDOWS[0];
    const s = source([win], { [win.key]: summaryBuf });
    const served = await networkFromRollup(
      "/api/network-health",
      { start_time: win.start, end_time: win.end, sections: ["shielding_analysis"] },
      s,
    ) as {
      shielding_analysis: {
        has_data: boolean;
        total_routes: number;
        _approx: boolean;
        _approx_note: string;
        rows: Array<{ edge_pop: string; shield_pop: string; requests: number; p50_ms: number }>;
      };
      rollup: { approximate: string[] };
    };
    expect(served.shielding_analysis.has_data).toBe(true);
    expect(served.shielding_analysis.total_routes).toBe(1);
    expect(served.shielding_analysis._approx).toBe(true);
    expect(served.shielding_analysis._approx_note).toBe("≈ (window-local join)");
    expect(served.shielding_analysis.rows[0]).toMatchObject({
      edge_pop: "EWR",
      shield_pop: "IAD",
      requests: 10,
    });
    expect(served.shielding_analysis.rows[0].p50_ms).toBeGreaterThan(20);
    expect(served.rollup.approximate).toContain("shielding_analysis");
    expect(s.log.files).toEqual([win.key]);
  });

  it("narrows quality residual start_time/end_time to only the uncovered sub-range", async () => {
    const makeSummary = (startIso: string, includeQuality: boolean) => {
      const t = new Date(startIso);
      const bundle = ["series5", ...(includeQuality ? ["dim_totals", "dim_totals", "dim_totals"] : [])];
      const bucket = bundle.map(() => t);
      const field = ["overall", ...(includeQuality ? ["overall", "region", "asn"] : [])];
      const value = ["", ...(includeQuality ? ["quality", "quality", "quality"] : [])];
      const requests = bundle.map(() => BigInt(10));
      const truncated = [null, ...(includeQuality ? [false, false, false] : [])];
      return parquetWriteBuffer({
        kvMetadata: [{ key: "flp_summary", value: "3" }],
        columnData: [
          { name: "bundle", data: bundle, type: "STRING" },
          { name: "bucket", data: bucket, type: "TIMESTAMP" },
          { name: "field", data: field, type: "STRING" },
          { name: "value", data: value, type: "STRING" },
          { name: "requests", data: requests, type: "INT64" },
          { name: "truncated", data: truncated, type: "BOOLEAN" },
        ],
      });
    };

    const span = WINDOWS.slice(0, 3);
    const partialBuffers = {
      [span[0].key]: makeSummary(span[0].start, true),
      [span[1].key]: makeSummary(span[1].start, false),
      [span[2].key]: makeSummary(span[2].start, true),
    };
    const partial = await networkFromRollup(
      "/api/network-quality",
      { start_time: span[0].start, end_time: span[2].end, region_country: "US" },
      source(span, partialBuffers),
    );
    if (!isNetworkPartial(partial)) throw new Error("expected NetworkPartial");
    expect(partial.residual.quality_parts).toEqual(["throughput", "region", "asn_client"]);
    expect(partial.residual.start_time).toBe(span[1].start);
    expect(partial.residual.end_time).toBe(span[1].end);

    const noneBuffers = {
      [span[0].key]: makeSummary(span[0].start, false),
      [span[1].key]: makeSummary(span[1].start, false),
      [span[2].key]: makeSummary(span[2].start, false),
    };
    const fullFallback = await networkFromRollup(
      "/api/network-quality",
      { start_time: span[0].start, end_time: span[2].end, region_country: "US" },
      source(span, noneBuffers),
    );
    if (!isNetworkPartial(fullFallback)) throw new Error("expected NetworkPartial");
    expect(fullFallback.residual.start_time).toBe(span[0].start);
    expect(fullFallback.residual.end_time).toBe(span[2].end);
  });

  it("emits metro_code on mapFromRollup cities from series5/city label and preserves it when re-bucketing", async () => {
    const t0 = new Date(T0);
    const t1 = new Date(T0 + 300_000);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["dim_totals", "dim_totals", "series5", "series5", "series5", "series5", "series5", "series5"], type: "STRING" },
        { name: "bucket", data: [t0, t0, t0, t1, t0, t1, t0, t0], type: "TIMESTAMP" },
        { name: "field", data: ["overall", "city", "overall", "overall", "city", "city", "city", "city"], type: "STRING" },
        {
          name: "value",
          data: [
            "series5",
            "series5",
            "",
            "",
            JSON.stringify(["US", "New York"]),
            JSON.stringify(["US", "New York"]),
            JSON.stringify(["GB", "London"]),
            JSON.stringify(["US", "BadMetro"]),
          ],
          type: "STRING",
        },
        { name: "requests", data: [10, 10, 6, 4, 3, 4, 2, 1].map((n) => BigInt(n)), type: "INT64" },
        { name: "status_5xx", data: [null, null, ...[0, 0, 0, 0, 0, 0].map((n) => BigInt(n))], type: "INT64" },
        { name: "lat_sum", data: [null, null, null, null, 122.13, 162.84, 103.02, 39.04], type: "DOUBLE" },
        { name: "lon_sum", data: [null, null, null, null, -222.03, -296.04, -0.26, -77.49], type: "DOUBLE" },
        { name: "geo_count", data: [null, null, null, null, ...[3, 4, 2, 1].map((n) => BigInt(n))], type: "INT64" },
        { name: "label", data: [null, null, null, null, null, "501", null, "0"], type: "STRING" },
        { name: "truncated", data: [false, false, null, null, null, null, null, null], type: "BOOLEAN" },
      ],
    });
    const win = WINDOWS[0];
    const served = await networkFromRollup(
      "/api/network-health",
      { start_time: win.start, end_time: win.end, bucket_seconds: 900, sections: ["buckets", "cities", "map_buckets"] },
      source([win], { [win.key]: summaryBuf }),
    ) as MapAnswer;
    const byCity = new Map(
      served.map_buckets[0].cities.map((c) => [served.cities[c.city_idx as number].name, c.metro_code]),
    );
    expect(byCity.get("New York")).toBe(501);
    expect(byCity.get("London")).toBeNull();
    expect(byCity.get("BadMetro")).toBeNull();
  });

  it("serves filtered /api/network-health summary from filtered_rollups for hour-aligned single low-card filters", async () => {
    const rttHist = new Histogram();
    rttHist.min = 12_000;
    rttHist.max = 12_000;
    rttHist.buckets[45] = 4;
    const rttBytes = rttHist.toBytes()!;
    const hourStart = Math.floor(T0 / 3_600_000) * 3_600_000;
    const hourEnd = hourStart + 3_600_000;
    const t = new Date(hourStart);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["overview", "dim_totals", "filtered_rollups"], type: "STRING" },
        { name: "bucket", data: [t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["", "content_type", "content_type"], type: "STRING" },
        { name: "value", data: ["", "filtered_rollups", "application/json"], type: "STRING" },
        { name: "requests", data: [BigInt(10), BigInt(10), BigInt(4)], type: "INT64" },
        { name: "errors", data: [null, null, BigInt(1)], type: "INT64" },
        { name: "status_5xx", data: [null, null, BigInt(1)], type: "INT64" },
        { name: "cache_hits", data: [null, null, BigInt(2)], type: "INT64" },
        { name: "resp_bytes_sum", data: [null, null, BigInt(2048)], type: "INT64" },
        { name: "rtt_sum", data: [null, null, BigInt(48_000)], type: "INT64" },
        { name: "rtt_count", data: [null, null, BigInt(4)], type: "INT64" },
        { name: "ploss_sum", data: [null, null, 0.04], type: "DOUBLE" },
        { name: "ploss_count", data: [null, null, BigInt(4)], type: "INT64" },
        { name: "cwnd_sum", data: [null, null, 40], type: "DOUBLE" },
        { name: "cwnd_count", data: [null, null, BigInt(4)], type: "INT64" },
        { name: "rttvar_sum", data: [null, null, 400], type: "DOUBLE" },
        { name: "rttvar_count", data: [null, null, BigInt(4)], type: "INT64" },
        { name: "retrans_count", data: [null, null, BigInt(0)], type: "INT64" },
        { name: "truncated", data: [null, false, null], type: "BOOLEAN" },
        { name: "rtt_hist", data: [null, null, rttBytes], type: "BYTE_ARRAY" },
      ],
    });
    const hour = HOURS[0];
    const s = source([hour], { [hour.key]: summaryBuf });
    const params = {
      start_time: hour.start,
      end_time: hour.end,
      sections: ["summary"],
      filters: { content_type: { mode: "include" as const, values: ["Application/JSON; charset=utf-8"] } },
    };
    expect(networkRollupEligible("/api/network-health", params)).toBe(true);
    const served = await networkFromRollup("/api/network-health", params, s) as {
      has_data: boolean;
      summary: { total_requests: number; avg_rtt_ms: number };
      rollup: { approximate: string[]; lower_bound: string[] };
    };
    expect(served.has_data).toBe(true);
    expect(served.summary.total_requests).toBe(4);
    expect(served.summary.avg_rtt_ms).toBe(12);
    expect(served.rollup.lower_bound).toEqual([]);

    // Non-hour-aligned, multi-field, exclude, high-card, or non-summary sections must fall back.
    expect(networkRollupEligible("/api/network-health", { ...params, start_time: new Date(hourStart + 60_000).toISOString() })).toBe(false);
    expect(networkRollupEligible("/api/network-health", { ...params, sections: ["summary", "heatmap"] })).toBe(false);
    expect(networkRollupEligible("/api/network-health", {
      ...params,
      filters: { content_type: { mode: "exclude" as const, values: ["text/html"] } },
    })).toBe(false);
    expect(networkRollupEligible("/api/network-health", {
      ...params,
      filters: { url: { mode: "include" as const, values: ["/foo"] } },
    })).toBe(false);
  });

  it("populates retrans_rate_pct and normalizes protocol_distribution in transportFromRollup plus leaderboard trend", async () => {
    const rttHist = new Histogram();
    rttHist.min = 20_000;
    rttHist.max = 20_000;
    rttHist.buckets[50] = 100;
    const rttBytes = rttHist.toBytes()!;
    const t = new Date(T0);
    const summaryBuf = parquetWriteBuffer({
      kvMetadata: [{ key: "flp_summary", value: "3" }],
      columnData: [
        { name: "bundle", data: ["dim_totals", "dim_totals", "dim_totals", "dim_totals", "transport", "transport", "transport", "dims", "dims", "series5", "series5"], type: "STRING" },
        { name: "bucket", data: [t, t, t, t, t, t, t, t, t, t, t], type: "TIMESTAMP" },
        { name: "field", data: ["overall", "proto", "country", "asn", "overall", "proto", "proto", "country", "asn", "overall", "asn"], type: "STRING" },
        { name: "value", data: ["transport", "transport", "dims", "dims", "", "1.1", "HTTP/1.1", "US", "64500", "", "64500"], type: "STRING" },
        { name: "requests", data: [BigInt(100), BigInt(100), BigInt(100), BigInt(100), BigInt(100), BigInt(60), BigInt(40), BigInt(100), BigInt(100), BigInt(100), BigInt(100)], type: "INT64" },
        { name: "resp_bytes_sum", data: [null, null, null, null, BigInt(50000), null, null, BigInt(50000), BigInt(50000), BigInt(50000), BigInt(50000)], type: "INT64" },
        { name: "rtt_sum", data: [null, null, null, null, null, null, null, BigInt(2_000_000), BigInt(2_000_000), BigInt(2_000_000), BigInt(2_000_000)], type: "INT64" },
        { name: "rtt_count", data: [null, null, null, null, null, null, null, BigInt(100), BigInt(100), BigInt(100), BigInt(100)], type: "INT64" },
        { name: "retrans_count", data: [null, null, null, null, null, null, null, BigInt(2), BigInt(2), null, null], type: "INT64" },
        { name: "truncated", data: [false, false, false, false, null, false, null, false, false, null, false], type: "BOOLEAN" },
        { name: "rtt_hist", data: [null, null, null, null, rttBytes, null, null, rttBytes, rttBytes, rttBytes, rttBytes], type: "BYTE_ARRAY" },
      ],
    });
    const decoded = await decodeRollup(summaryBuf, 300_000);
    const rollupRes: RollupResult = { rollup: decoded!, dataFromMs: T0, dataThroughMs: T0 + 300_000 };
    const tr = transportFromRollup(rollupRes)! as { retrans_rate_pct?: number; protocol_distribution: { protocol: string; requests: number; share_pct: number }[] };
    expect(tr.retrans_rate_pct).toBe(2);
    expect(tr.protocol_distribution).toEqual([{ protocol: "HTTP/1.1", requests: 100, share_pct: 100 }]);
    const nh = networkHealthFromRollup(rollupRes, { bucket_seconds: 300 })! as { leaderboard: { health_score_1h_ago: unknown; trend?: string }[] };
    expect(nh.leaderboard[0].health_score_1h_ago).toEqual(expect.any(Number));
    expect(nh.leaderboard[0].trend).toBe("stable");
  });
});
