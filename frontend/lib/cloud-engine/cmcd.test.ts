// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it, vi } from "vitest";
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => new Set(["cmcd_sid"]),
  cloudServiceFlags: async () => ({
    serviceId: "default",
    activeIds: new Set(["cmcd_sid"]),
    edgeOnly: false,
    cmcdEnabled: true,
  }),
}));
import {
  classifySessionDurationBucket,
  CMCD_REQUIRED_ANY_COLUMNS,
  CMCD_SESSION_DURATION_BUCKETS,
  cmcdScanColumns,
  emptyCmcdAggregates,
  handleCmcdRequest,
  queryCmcdAggregates,
} from "./cmcd";
import { clearCmcdRollupCache, cmcdFromRollup, getCmcdResidualSections, isCmcdPartial, MAX_CMCD_ROLLUP_FILES } from "./cmcd-rollup";
import * as cmcdModule from "./cmcd";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { BOUNDS, Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";
import {
  DuckDBAnalyticalEngine,
  fixtureSource,
  manifestEntry,
} from "./dashboard/test-fixtures";

describe("Feature F24 — CMCD Streaming Media cloud-engine", () => {
  it("projects CMCD columns plus filter columns without filter_mode and names required CMCD columns", () => {
    const cols = cmcdScanColumns({ country: { mode: "include", values: ["US"] }, filter_mode: "and" });
    expect(cols).toContain("country");
    expect(cols).not.toContain("filter_mode");
    expect(CMCD_REQUIRED_ANY_COLUMNS).toEqual(["cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot"]);
  });

  it("does not fabricate missing optional streaming measurements", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_ot)
        VALUES ('native', 1700000100000, 'manifest-session', 'm')`);
      const data = await queryCmcdAggregates({ engine });
      expect(data.overview).toMatchObject({
        rebuffer_rate: null, avg_bitrate: null, avg_buffer_length: null, median_throughput: null,
      });
      expect(data.bitrate_ts).toEqual([]);
    } finally {
      await engine.reset();
    }
  });

  it("returns the Streaming page contract with requested buckets and native filters", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, country, cmcd_sid, cmcd_br, cmcd_tb, cmcd_bl, cmcd_mtp, cmcd_bs, cmcd_ot, cmcd_sf)
        VALUES ('native', 1700000100000, 'US', 'a', 1000, 2000, 1000, 2000, false, 'v', 'h'),
          ('native', 1700000160000, 'US', 'a', 1000, 2000, 3000, 4000, true, 'v', 'h'),
          ('native', 1700000220000, 'CA', 'b', 2000, 4000, 5000, 6000, false, 'v', 'h')`);
      const data = await queryCmcdAggregates({ engine, bucket_seconds: 300 });
      expect(data).toMatchObject({
        available: true, has_data: true,
        overview: { active_sessions: 2, rebuffer_rate: 50, median_throughput: 4000 },
      });
      expect(data.sessions_ts).toHaveLength(1);
      expect(data.sessions_ts[0]).toMatchObject({
        bucket: new Date(1700000100000).toISOString(), concurrent_sessions: 2, new_sessions: 2,
      });
      expect(data.buffer_health_ts[0]).toMatchObject({ p50_buffer: 3000, p95_buffer: 4800 });
      expect(data.throughput_ts[0]).toMatchObject({ p50: 4000, p99: 5960 });
      expect(data.throughput_ts[0].p95).toBeCloseTo(5800);
      expect(data.object_type_dist[0]).toMatchObject({ object_type: "v", request_count: 3 });
      expect(data.streaming_format_dist[0]).toMatchObject({ streaming_format: "h", session_count: 2 });
      const filtered = await queryCmcdAggregates({
        engine, filters: { country: { mode: "exclude", values: ["CA"] } },
      });
      expect(filtered.overview.active_sessions).toBe(1);
    } finally {
      await engine.reset();
    }
  });

  it("omits sections whose CMCD keys the window never reported, like High-Scale", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, url, geo_country, client_asn, cmcd_sid, cmcd_ot)
        VALUES ('native', 1700000100000, '/seg-1.m4s', 'US', 15169, 'a', 'v'),
          ('native', 1700000101000, '/seg-2.m4s', 'US', 15169, 'a', NULL)`);
      const data = await queryCmcdAggregates({ engine });
      expect(data.sessions_ts).toHaveLength(1);
      for (const section of ["buffer_health_ts", "bitrate_ts", "throughput_ts", "startup_ts", "top_content",
        "rebuffer_by_country", "rebuffer_by_asn", "streaming_format_dist"] as const) {
        expect(data[section], section).toEqual([]);
      }
      expect(data.object_type_dist.map((row) => row.object_type)).toEqual(["v"]);
    } finally {
      await engine.reset();
    }
  });

  it("reports bitrate utilization as the mean br/tb fraction like High-Scale", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_br, cmcd_tb, cmcd_ot)
        VALUES ('native', 1700000100000, 'a', 1000, 4000, 'v'),
          ('native', 1700000101000, 'a', 3000, 3000, 'v')`);
      const data = await queryCmcdAggregates({ engine, bucket_seconds: 300 });
      expect(data.bitrate_ts[0].utilization_ratio).toBe(0.63);
    } finally {
      await engine.reset();
    }
  });

  it("averages bitrate, buffer and throughput over video segments only when object types exist", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, geo_country, client_asn, cmcd_sid, cmcd_cid, cmcd_br, cmcd_bl, cmcd_mtp, cmcd_bs, cmcd_ot)
        VALUES ('native', 1700000100000, 'US', 15169, 'a', 'movie', 1000, 1000, 1000, false, 'v'),
          ('native', 1700000101000, 'US', 15169, 'a', 'movie', 100, 9000, 100, false, 'a'),
          ('native', 1700000102000, 'US', 15169, 'a', 'movie', 100, 9000, 100, false, 'm')`);
      const data = await queryCmcdAggregates({ engine, bucket_seconds: 300 });
      expect(data.overview).toMatchObject({ avg_bitrate: 1000, avg_buffer_length: 1000, median_throughput: 1000 });
      expect(data.bitrate_ts[0]).toMatchObject({ avg_bitrate: 1000 });
      expect(data.buffer_health_ts[0]).toMatchObject({ p50_buffer: 1000, p95_buffer: 1000 });
      expect(data.throughput_ts[0]).toMatchObject({ p50: 1000, p95: 1000, p99: 1000 });
      expect(data.top_content[0]).toMatchObject({ avg_bitrate: 1000, avg_buffer_length: 1000 });
      expect(data.rebuffer_by_asn[0]).toMatchObject({ avg_bitrate: 1000, avg_buffer_length: 1000 });
      expect(data.rebuffer_by_country[0]).toMatchObject({ median_throughput: 1000 });
    } finally {
      await engine.reset();
    }
  });

  it("consolidates raw CMCD queries into at most 3 table scans (and skips breakdowns when absent)", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const executed: string[] = [];
    const counted = new Proxy(engine, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver);
        if (property !== "executeSql") return typeof value === "function" ? value.bind(target) : value;
        return (sql: string, ...rest: unknown[]) => {
          executed.push(sql);
          return (value as (...args: unknown[]) => unknown).call(target, sql, ...rest);
        };
      },
    });
    try {
      // Empty window stops after 1 scan.
      const empty = await queryCmcdAggregates({ engine: counted });
      expect(empty.has_data).toBe(false);
      expect(executed).toHaveLength(1);

      // Minimal window (no cid/bs/bl) skips the breakdown scan -> 2 scans.
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_ot)
        VALUES ('native', 1700000100000, 's1', 'v')`);
      executed.length = 0;
      const minimal = await queryCmcdAggregates({ engine: counted, bucket_seconds: 300 });
      expect(minimal.has_data).toBe(true);
      expect(minimal.object_type_dist).toEqual([{ object_type: "v", request_count: 1, requests: 1, pct: 100 }]);
      expect(executed).toHaveLength(2);

      // Full window (with cid, bs, bl, country, asn, sf) runs at most 3 scans instead of 8.
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, geo_country, client_asn, as_name, cmcd_sid, cmcd_cid, cmcd_br, cmcd_bl, cmcd_bs, cmcd_mtp, cmcd_ot, cmcd_sf, cmcd_su, ttfb_ms)
        VALUES ('native', 1700000100000, 'US', 15169, 'Google', 's2', 'movie-1', 2000, 3000, false, 5000, 'v', 'h', true, 40),
               ('native', 1700000160000, 'US', 15169, 'Google', 's2', 'movie-1', 4000, 1000, true, 3000, 'v', 'h', false, 20),
               ('native', 1700000220000, 'CA', 64500, 'Example', 's3', 'movie-2', 1500, 2000, false, 4000, 'v', 'd', false, 30)`);
      executed.length = 0;
      const full = await queryCmcdAggregates({ engine: counted, bucket_seconds: 300 });
      expect(executed).toHaveLength(3);
      expect(full.overview).toMatchObject({ active_sessions: 3, avg_session_duration: 60 });
      expect(full.top_content.map((r) => r.content_id)).toEqual(["movie-1", "movie-2"]);
      expect(full.rebuffer_by_country.map((r) => r.country)).toEqual(["CA", "US"]);
      expect(full.rebuffer_by_asn.map((r) => r.asn)).toEqual([15169, 64500]);
      expect(full.streaming_format_dist.map((r) => r.format)).toEqual(["d", "h"]);
      expect(full.session_duration_dist).toEqual([
        expect.objectContaining({ bucket: "<30s", session_count: 2 }),
        expect.objectContaining({ bucket: "30s–2m", session_count: 1 }),
      ]);
    } finally {
      await engine.reset();
    }
  });

  it("classifies session durations into the High-Scale buckets", () => {
    expect(CMCD_SESSION_DURATION_BUCKETS).toEqual(["<30s", "30s–2m", "2–5m", "5–10m", "10–30m", "30m–1h", ">1h"]);
    expect(classifySessionDurationBucket(10)).toBe("<30s");
    expect(classifySessionDurationBucket(90)).toBe("30s–2m");
    expect(classifySessionDurationBucket(120)).toBe("2–5m");
    expect(classifySessionDurationBucket(400)).toBe("5–10m");
    expect(classifySessionDurationBucket(1200)).toBe("10–30m");
    expect(classifySessionDurationBucket(2400)).toBe("30m–1h");
    expect(classifySessionDurationBucket(4000)).toBe(">1h");
  });

  it("emits only populated session duration buckets from first-to-last request time", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_d)
        VALUES ('native', 1700000000000, 'a', 4000), ('native', 1700000020000, 'a', 15000),
          ('native', 1700000000000, 'b', 4000), ('native', 1700000090000, 'b', 4000)`);
      const data = await queryCmcdAggregates({ engine });
      expect(data.session_duration_dist.map(({ duration_bucket, session_count }) => ({ duration_bucket, session_count })))
        .toEqual([{ duration_bucket: "<30s", session_count: 1 }, { duration_bucket: "30s–2m", session_count: 1 }]);
    } finally {
      await engine.reset();
    }
  });

  it("returns empty CMCD aggregates when no CMCD rows exist", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const res = await queryCmcdAggregates({ engine });
    expect(res).toEqual(emptyCmcdAggregates());
    await engine.reset();
  });

  it("computes CMCD KPIs, 5 time series, and 6 breakdowns from DuckDB", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", geo_country, client_asn, as_name,
        cmcd_sid, cmcd_cid, cmcd_br, cmcd_tb, cmcd_bl, cmcd_bs, cmcd_mtp,
        cmcd_ot, cmcd_sf, cmcd_st, cmcd_d, cmcd_su, ttfb_ms
      ) VALUES
        ('c1', 1700000040000, epoch_ms(1700000040000)::TIMESTAMPTZ, 'US', 15169, 'Google', 'sess-1', 'live-match', 4500, 6000, 12000, FALSE, 18000, 'v', 'h', 'l', 4000, TRUE, 18.0),
        ('c1', 1700000060000, epoch_ms(1700000060000)::TIMESTAMPTZ, 'US', 15169, 'Google', 'sess-1', 'live-match', 6000, 6000, 16000, FALSE, 22000, 'v', 'h', 'l', 4000, FALSE, 12.0),
        ('c1', 1700000080000, epoch_ms(1700000080000)::TIMESTAMPTZ, 'GB', 3356, 'Level3', 'sess-2', 'vod-movie', 3000, 6000, 500, TRUE, 2500, 'v', 'd', 'v', 4000, TRUE, 45.0)`,
    );

    const res = await handleCmcdRequest("/api/cmcd/aggregates", { engine });
    expect("summary" in res && res.summary.active_sessions).toBe(2);
    if ("summary" in res) {
      expect(res.summary.rebuffer_events).toBe(1);
      expect(res.summary.rebuffer_rate).toBeCloseTo(33.33, 1);
      expect(res.summary.peak_viewers).toBe(2);
      expect(res.summary.utilization_ratio).toBe(75);
      expect(res.sessions_ts).toHaveLength(1);
      expect(res.buffer_health_ts).toHaveLength(1);
      expect(res.bitrate_ts).toHaveLength(1);
      expect(res.throughput_ts).toHaveLength(1);
      expect(res.startup_ts).toHaveLength(1);
      expect(res.top_content).toHaveLength(2);
      expect(res.rebuffer_by_country).toHaveLength(2);
      expect(res.rebuffer_by_asn).toHaveLength(2);
      expect(res.object_type_dist).toHaveLength(1);
      expect(res.streaming_format_dist).toHaveLength(2);
      expect(res.session_duration_dist).toHaveLength(1);
      expect(res.bitrate_ladder).toEqual([
        { tier: "2.5–5 Mbps (1080p)", request_count: 2, session_count: 2, avg_bitrate: 3750, rebuffer_rate: 50 },
        { tier: "5–10 Mbps (1440p)", request_count: 1, session_count: 1, avg_bitrate: 6000, rebuffer_rate: 0 },
      ]);
      expect(res.stream_type_dist).toEqual([
        { stream_type: "l", session_count: 1, request_count: 2 },
        { stream_type: "v", session_count: 1, request_count: 1 },
      ]);
    }
    await engine.reset();
  });

  it("narrows cmcdScanColumns and skips heavy core quantiles for breakdown-only extended sections", async () => {
    const narrow = cmcdScanColumns(undefined, ["bitrate_ladder", "stream_type_dist"]);
    expect(narrow).toEqual([
      "timestamp", "ts_ms", "service_id", "cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot", "cmcd_bs", "cmcd_st",
    ]);
    const withFilter = cmcdScanColumns(
      { host: { mode: "include", values: ["cdn.example"] }, filter_mode: "and" },
      ["bitrate_ladder"],
      { filter_sf: "h" },
    );
    expect(withFilter).toEqual([
      "timestamp", "ts_ms", "service_id", "cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot", "cmcd_bs", "cmcd_sf", "host",
    ]);

    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp",
        cmcd_sid, cmcd_br, cmcd_bl, cmcd_bs, cmcd_ot, cmcd_st
      ) VALUES
        ('c1', 1700000040000, epoch_ms(1700000040000)::TIMESTAMPTZ, 'sess-1', 4500, 12000, FALSE, 'v', 'l'),
        ('c1', 1700000060000, epoch_ms(1700000060000)::TIMESTAMPTZ, 'sess-1', 6000, 16000, FALSE, 'v', 'l'),
        ('c1', 1700000080000, epoch_ms(1700000080000)::TIMESTAMPTZ, 'sess-2', 3000, 500, TRUE, 'v', 'v')`,
    );
    const seenSql: string[] = [];
    const readCtx = engine.readContext();
    const spyEngine: typeof readCtx = {
      ...readCtx,
      executeSql: async <T = Record<string, unknown>>(sql: string, params?: unknown[], signal?: AbortSignal) => {
        seenSql.push(sql);
        return readCtx.executeSql<T>(sql, params, signal);
      },
    };

    const res = await queryCmcdAggregates({
      engine: spyEngine,
      sections: ["bitrate_ladder", "stream_type_dist"],
    });
    expect(seenSql.some((q) => q.includes("QUANTILE_CONT"))).toBe(false);
    expect(res.bitrate_ladder).toEqual([
      { tier: "2.5–5 Mbps (1080p)", request_count: 2, session_count: 2, avg_bitrate: 3750, rebuffer_rate: 50 },
      { tier: "5–10 Mbps (1440p)", request_count: 1, session_count: 1, avg_bitrate: 6000, rebuffer_rate: 0 },
    ]);
    expect(res.stream_type_dist).toEqual([
      { stream_type: "l", session_count: 1, request_count: 2 },
      { stream_type: "v", session_count: 1, request_count: 1 },
    ]);
    await engine.reset();
  });

  it("aggregates from cmcd_summary", async () => {
    const hllOf = (items: string[]) => {
      const h = new Hll();
      for (const item of items) h.add(item);
      return h.toBytes();
    };
    const histOf = (values: number[]) => {
      const h = new Histogram();
      for (const raw of values) {
        const v = Math.round(raw * 1000);
        h.min = Math.min(h.min, v);
        h.max = Math.max(h.max, v);
        if (v < BOUNDS[0]) h.under++;
        else if (v >= BOUNDS[BOUNDS.length - 1]) h.over++;
        else {
          for (let s = 0; s < BOUNDS.length - 1; s++) {
            if (v >= BOUNDS[s] && v < BOUNDS[s + 1]) {
              h.buckets[s]++;
              break;
            }
          }
        }
      }
      return h.toBytes();
    };

    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const t2 = t0 + 600_000;
    const makeSummaryBuffer = (rows: Array<Record<string, unknown>>) => {
      const str = (k: string) => rows.map((r) => (r[k] as string | undefined) ?? null);
      const i64 = (k: string) =>
        rows.map((r) => (r[k] == null ? null : BigInt(r[k] as number)));
      const f64 = (k: string) => rows.map((r) => (r[k] as number | undefined) ?? null);
      const bool = (k: string) => rows.map((r) => (r[k] as boolean | undefined) ?? null);
      const bin = (k: string) =>
        rows.map((r) => (r[k] as Uint8Array | undefined) ?? null);
      return parquetWriteBuffer({
        codec: "SNAPPY",
        columnData: [
          { name: "kind", type: "STRING", data: str("kind") },
          { name: "bucket", type: "INT64", data: i64("bucket") },
          { name: "key", type: "STRING", data: str("key") },
          { name: "label", type: "STRING", data: str("label") },
          { name: "truncated", type: "BOOLEAN", data: bool("truncated") },
          { name: "reqs", type: "INT64", data: i64("reqs") },
          { name: "sid_hll", type: "BYTE_ARRAY", data: bin("sid_hll") },
          { name: "rebuffer_events", type: "INT64", data: i64("rebuffer_events") },
          { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: bin("rebuffer_sid_hll") },
          { name: "has_bs", type: "INT64", data: i64("has_bs") },
          { name: "has_su", type: "INT64", data: i64("has_su") },
          { name: "has_ot", type: "INT64", data: i64("has_ot") },
          { name: "has_bl", type: "INT64", data: i64("has_bl") },
          { name: "has_br", type: "INT64", data: i64("has_br") },
          { name: "has_mtp", type: "INT64", data: i64("has_mtp") },
          { name: "has_cid", type: "INT64", data: i64("has_cid") },
          { name: "br_sum", type: "DOUBLE", data: f64("br_sum") },
          { name: "br_n", type: "INT64", data: i64("br_n") },
          { name: "tb_sum", type: "DOUBLE", data: f64("tb_sum") },
          { name: "tb_n", type: "INT64", data: i64("tb_n") },
          { name: "bl_sum", type: "DOUBLE", data: f64("bl_sum") },
          { name: "bl_n", type: "INT64", data: i64("bl_n") },
          { name: "mtp_sum", type: "DOUBLE", data: f64("mtp_sum") },
          { name: "mtp_n", type: "INT64", data: i64("mtp_n") },
          { name: "v_br_sum", type: "DOUBLE", data: f64("v_br_sum") },
          { name: "v_br_n", type: "INT64", data: i64("v_br_n") },
          { name: "v_tb_sum", type: "DOUBLE", data: f64("v_tb_sum") },
          { name: "v_tb_n", type: "INT64", data: i64("v_tb_n") },
          { name: "v_bl_sum", type: "DOUBLE", data: f64("v_bl_sum") },
          { name: "v_bl_n", type: "INT64", data: i64("v_bl_n") },
          { name: "util_sum", type: "DOUBLE", data: f64("util_sum") },
          { name: "util_n", type: "INT64", data: i64("util_n") },
          { name: "util_all_sum", type: "DOUBLE", data: f64("util_all_sum") },
          { name: "util_all_n", type: "INT64", data: i64("util_all_n") },
          { name: "startup_requests", type: "INT64", data: i64("startup_requests") },
          { name: "su_bl_sum", type: "DOUBLE", data: f64("su_bl_sum") },
          { name: "su_bl_n", type: "INT64", data: i64("su_bl_n") },
          { name: "su_ttfb_sum", type: "DOUBLE", data: f64("su_ttfb_sum") },
          { name: "su_ttfb_n", type: "INT64", data: i64("su_ttfb_n") },
          { name: "v_bl_hist", type: "BYTE_ARRAY", data: bin("v_bl_hist") },
          { name: "bl_hist", type: "BYTE_ARRAY", data: bin("bl_hist") },
          { name: "v_mtp_hist", type: "BYTE_ARRAY", data: bin("v_mtp_hist") },
          { name: "mtp_hist", type: "BYTE_ARRAY", data: bin("mtp_hist") },
          { name: "min_ts", type: "INT64", data: i64("min_ts") },
          { name: "max_ts", type: "INT64", data: i64("max_ts") },
          { name: "first_ts", type: "INT64", data: i64("first_ts") },
          { name: "last_ts", type: "INT64", data: i64("last_ts") },
        ],
      });
    };

    const key1 = "processed/request/cmcd_summary/year=2027/month=01/day=15/hour=08/window=1800000000-1800000300-p1s0-n2.parquet";
    const key2 = "processed/request/cmcd_summary/year=2027/month=01/day=15/hour=08/window=1800000300-1800000600-p1s0-n1.parquet";
    const buffers: Record<string, ArrayBuffer> = {
      [key1]: makeSummaryBuffer([
        {
          kind: "bucket", bucket: t0, reqs: 2, sid_hll: hllOf(["s1", "span"]),
          rebuffer_events: 1, rebuffer_sid_hll: hllOf(["s1"]),
          has_bs: 2, has_su: 2, has_ot: 2, has_bl: 2, has_br: 2, has_mtp: 2, has_cid: 2,
          br_sum: 6000, br_n: 2, tb_sum: 8000, tb_n: 2, bl_sum: 4000, bl_n: 2, mtp_sum: 10000, mtp_n: 2,
          v_br_sum: 6000, v_br_n: 2, v_tb_sum: 8000, v_tb_n: 2, v_bl_sum: 4000, v_bl_n: 2,
          util_sum: 1.5, util_n: 2, util_all_sum: 1.5, util_all_n: 2,
          startup_requests: 1, su_bl_sum: 1000, su_bl_n: 1, su_ttfb_sum: 20, su_ttfb_n: 1,
          v_bl_hist: histOf([1000, 3000]), bl_hist: histOf([1000, 3000]),
          v_mtp_hist: histOf([4000, 6000]), mtp_hist: histOf([4000, 6000]),
          min_ts: t0 + 1000, max_ts: t0 + 10000,
        },
        { kind: "session", key: "s1", reqs: 1, first_ts: t0 + 1000, last_ts: t0 + 1000 },
        { kind: "session", key: "span", reqs: 1, first_ts: t0 + 10000, last_ts: t0 + 10000 },
        { kind: "content", key: "movie", reqs: 2, sid_hll: hllOf(["s1", "span"]), rebuffer_sid_hll: hllOf(["s1"]), has_bs: 2, v_br_sum: 6000, v_br_n: 2, br_sum: 6000, br_n: 2, v_bl_sum: 4000, v_bl_n: 2, bl_sum: 4000, bl_n: 2 },
        { kind: "country", key: "US", reqs: 2, sid_hll: hllOf(["s1", "span"]), rebuffer_events: 1, rebuffer_sid_hll: hllOf(["s1"]), has_bs: 2, v_mtp_hist: histOf([4000, 6000]), mtp_hist: histOf([4000, 6000]) },
        { kind: "asn", key: "15169", label: "Google", reqs: 2, sid_hll: hllOf(["s1", "span"]), rebuffer_events: 1, rebuffer_sid_hll: hllOf(["s1"]), has_bs: 2, v_br_sum: 6000, v_br_n: 2, br_sum: 6000, br_n: 2, v_bl_sum: 4000, v_bl_n: 2, bl_sum: 4000, bl_n: 2, mtp_sum: 10000, mtp_n: 2 },
        { kind: "ot", key: "v", reqs: 2 },
        { kind: "sf", key: "h", reqs: 2, sid_hll: hllOf(["s1", "span"]) },
      ]),
      [key2]: makeSummaryBuffer([
        {
          kind: "bucket", bucket: t1, reqs: 1, sid_hll: hllOf(["span"]),
          rebuffer_events: 0, has_bs: 1, has_su: 1, has_ot: 1, has_bl: 1, has_br: 1, has_mtp: 1, has_cid: 1,
          br_sum: 3000, br_n: 1, tb_sum: 6000, tb_n: 1, bl_sum: 2000, bl_n: 1, mtp_sum: 8000, mtp_n: 1,
          v_br_sum: 3000, v_br_n: 1, v_tb_sum: 6000, v_tb_n: 1, v_bl_sum: 2000, v_bl_n: 1,
          util_sum: 0.5, util_n: 1, util_all_sum: 0.5, util_all_n: 1,
          startup_requests: 0,
          v_bl_hist: histOf([2000]), bl_hist: histOf([2000]),
          v_mtp_hist: histOf([8000]), mtp_hist: histOf([8000]),
          min_ts: t1 + 10000, max_ts: t1 + 10000,
        },
        { kind: "session", key: "span", reqs: 1, first_ts: t1 + 10000, last_ts: t1 + 10000 },
        { kind: "content", key: "movie", reqs: 1, sid_hll: hllOf(["span"]), has_bs: 1, v_br_sum: 3000, v_br_n: 1, br_sum: 3000, br_n: 1, v_bl_sum: 2000, v_bl_n: 1, bl_sum: 2000, bl_n: 1 },
        { kind: "country", key: "US", reqs: 1, sid_hll: hllOf(["span"]), has_bs: 1, v_mtp_hist: histOf([8000]), mtp_hist: histOf([8000]) },
        { kind: "asn", key: "15169", label: "Google", reqs: 1, sid_hll: hllOf(["span"]), has_bs: 1, v_br_sum: 3000, v_br_n: 1, br_sum: 3000, br_n: 1, v_bl_sum: 2000, v_bl_n: 1, bl_sum: 2000, bl_n: 1, mtp_sum: 8000, mtp_n: 1 },
        { kind: "ot", key: "v", reqs: 1 },
        { kind: "sf", key: "h", reqs: 1, sid_hll: hllOf(["span"]) },
      ]),
    };

    const rawPages = [{
      files: [
        manifestEntry("processed/request/year=2027/month=01/day=15/hour=08/window=1800000000-1800000300-p1s0-n2.parquet", new Date(t0).toISOString(), new Date(t1).toISOString(), 2),
        manifestEntry("processed/request/year=2027/month=01/day=15/hour=08/window=1800000300-1800000600-p1s0-n1.parquet", new Date(t1).toISOString(), new Date(t2).toISOString(), 1),
      ],
      next_cursor: null,
      complete: true,
    }];
    const cmcdPage = {
      files: [
        manifestEntry(key1, new Date(t0).toISOString(), new Date(t1).toISOString(), 2),
        manifestEntry(key2, new Date(t1).toISOString(), new Date(t2).toISOString(), 1),
      ],
      next_cursor: null,
      complete: true,
    };
    const baseSource = fixtureSource(rawPages, buffers);
    const rollupSource = {
      ...baseSource,
      async fetchJson(url: string, signal?: AbortSignal) {
        const params = new URL(url, "http://x").searchParams;
        if (params.get("dataset") === "cmcd_summary") return cmcdPage;
        return baseSource.fetchJson(url, signal);
      },
    };

    const res = await queryCmcdAggregates({
      start_time: new Date(t0).toISOString(),
      end_time: new Date(t2).toISOString(),
      bucket_seconds: 300,
      rollupSource,
    });
    expect(res.overview).toMatchObject({
      active_sessions: 2,
      peak_viewers: 2,
      rebuffer_rate: 50,
      avg_bitrate: 3000,
      avg_buffer_length: 2000,
    });
    expect(res.summary).toMatchObject({
      active_sessions: 2,
      total_requests: 3,
      rebuffer_events: 1,
      peak_viewers: 2,
    });
    expect(res.sessions_ts).toHaveLength(2);
    expect(res.sessions_ts[0]).toMatchObject({ concurrent_sessions: 2, new_sessions: 2 });
    expect(res.sessions_ts[1]).toMatchObject({ concurrent_sessions: 1, new_sessions: 0 });
    expect(res.session_duration_dist.map((d) => ({ duration_bucket: d.duration_bucket, session_count: d.session_count })))
      .toEqual([
        { duration_bucket: "<30s", session_count: 1 },
        { duration_bucket: "5–10m", session_count: 1 },
      ]);
    expect(res.top_content[0]).toMatchObject({ content_id: "movie", sessions: 2, requests: 3 });
    expect(res.rebuffer_by_country[0]).toMatchObject({ country: "US", sessions: 2, rebuffer_events: 1 });
    expect(res.rebuffer_by_asn[0]).toMatchObject({ asn: 15169, as_name: "Google", sessions: 2 });
    expect(res.object_type_dist[0]).toMatchObject({ object_type: "v", requests: 3 });
    expect(res.streaming_format_dist[0]).toMatchObject({ streaming_format: "h", session_count: 2 });
  });

  it("bounds concurrent cmcd_summary file fetches across large ranges", async () => {
    const t0 = 1_800_000_000_000;
    const stepMs = 300_000;
    const fileCount = 48;
    const files = [];
    const buffers: Record<string, ArrayBuffer> = {};
    for (let i = 0; i < fileCount; i++) {
      const start = t0 + i * stepMs;
      const end = start + stepMs;
      const key = `processed/request/cmcd_summary/year=2027/month=01/day=15/hour=08/window=${Math.floor(start / 1000)}-${Math.floor(end / 1000)}-p1s0-n1.parquet`;
      files.push(manifestEntry(key, new Date(start).toISOString(), new Date(end).toISOString(), 1));
      buffers[key] = parquetWriteBuffer({
        codec: "SNAPPY",
        columnData: [
          { name: "kind", type: "STRING", data: ["bucket"] },
          { name: "bucket", type: "INT64", data: [BigInt(start)] },
          { name: "key", type: "STRING", data: [null] },
          { name: "label", type: "STRING", data: [null] },
          { name: "truncated", type: "BOOLEAN", data: [null] },
          { name: "reqs", type: "INT64", data: [BigInt(1)] },
          { name: "sid_hll", type: "BYTE_ARRAY", data: [null] },
          { name: "rebuffer_events", type: "INT64", data: [BigInt(0)] },
          { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: [null] },
          { name: "has_bs", type: "INT64", data: [BigInt(1)] },
          { name: "has_su", type: "INT64", data: [BigInt(0)] },
          { name: "has_ot", type: "INT64", data: [BigInt(1)] },
          { name: "has_bl", type: "INT64", data: [BigInt(1)] },
          { name: "has_br", type: "INT64", data: [BigInt(1)] },
          { name: "has_mtp", type: "INT64", data: [BigInt(0)] },
          { name: "has_cid", type: "INT64", data: [BigInt(0)] },
          { name: "br_sum", type: "DOUBLE", data: [2000] },
          { name: "br_n", type: "INT64", data: [BigInt(1)] },
          { name: "tb_sum", type: "DOUBLE", data: [4000] },
          { name: "tb_n", type: "INT64", data: [BigInt(1)] },
          { name: "bl_sum", type: "DOUBLE", data: [1000] },
          { name: "bl_n", type: "INT64", data: [BigInt(1)] },
          { name: "mtp_sum", type: "DOUBLE", data: [null] },
          { name: "mtp_n", type: "INT64", data: [BigInt(0)] },
          { name: "v_br_sum", type: "DOUBLE", data: [2000] },
          { name: "v_br_n", type: "INT64", data: [BigInt(1)] },
          { name: "v_tb_sum", type: "DOUBLE", data: [4000] },
          { name: "v_tb_n", type: "INT64", data: [BigInt(1)] },
          { name: "v_bl_sum", type: "DOUBLE", data: [1000] },
          { name: "v_bl_n", type: "INT64", data: [BigInt(1)] },
          { name: "util_sum", type: "DOUBLE", data: [0.5] },
          { name: "util_n", type: "INT64", data: [BigInt(1)] },
          { name: "util_all_sum", type: "DOUBLE", data: [0.5] },
          { name: "util_all_n", type: "INT64", data: [BigInt(1)] },
          { name: "startup_requests", type: "INT64", data: [BigInt(0)] },
          { name: "su_bl_sum", type: "DOUBLE", data: [null] },
          { name: "su_bl_n", type: "INT64", data: [BigInt(0)] },
          { name: "su_ttfb_sum", type: "DOUBLE", data: [null] },
          { name: "su_ttfb_n", type: "INT64", data: [BigInt(0)] },
          { name: "v_bl_hist", type: "BYTE_ARRAY", data: [null] },
          { name: "bl_hist", type: "BYTE_ARRAY", data: [null] },
          { name: "v_mtp_hist", type: "BYTE_ARRAY", data: [null] },
          { name: "mtp_hist", type: "BYTE_ARRAY", data: [null] },
          { name: "min_ts", type: "INT64", data: [BigInt(start)] },
          { name: "max_ts", type: "INT64", data: [BigInt(start)] },
          { name: "first_ts", type: "INT64", data: [null] },
          { name: "last_ts", type: "INT64", data: [null] },
        ],
      });
    }

    const cmcdPage = { files, next_cursor: null, complete: true };
    const baseSource = fixtureSource([], buffers);
    let inFlight = 0;
    let maxInFlight = 0;
    const rollupSource = {
      async fetchJson(url: string, signal?: AbortSignal) {
        const params = new URL(url, "http://x").searchParams;
        if (params.get("dataset") === "cmcd_summary") return cmcdPage;
        return baseSource.fetchJson(url, signal);
      },
      async fetchFile(key: string, signal?: AbortSignal) {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        try {
          await new Promise((resolve) => setTimeout(resolve, 2));
          return await baseSource.fetchFile(key, signal);
        } finally {
          inFlight--;
        }
      },
    };

    const res = await queryCmcdAggregates({
      start_time: new Date(t0).toISOString(),
      end_time: new Date(t0 + fileCount * stepMs).toISOString(),
      bucket_seconds: 300,
      rollupSource,
    });
    expect(res.summary.total_requests).toBe(fileCount);
    expect(maxInFlight).toBeGreaterThan(1);
    expect(maxInFlight).toBeLessThanOrEqual(16);
  });

  it("bails out of uncompacted cmcd_summary rollups on page 1 when file count exceeds cap so callers fall back to compacted raw Parquet", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const overCap = MAX_CMCD_ROLLUP_FILES + 50;
    const files = Array.from({ length: overCap }, (_, i) => ({
      key: `processed/request/cmcd_summary/window=${i}.parquet`,
      start: new Date(t0 + i * 60_000).toISOString(),
      end: new Date(t0 + (i + 1) * 60_000).toISOString(),
      size: 14_000,
      rows: 20,
    }));
    let jsonCalls = 0;
    let fileCalls = 0;
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t0 + 24 * 3_600_000).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson(url: string) {
          jsonCalls++;
          expect(new URL(url, "http://x").searchParams.get("view")).toBe("summary");
          return { files, next_cursor: "page-2", complete: false };
        },
        async fetchFile() {
          fileCalls++;
          throw new Error("should not download over-cap cmcd_summary files");
        },
      },
    );
    expect(res).toBeNull();
    expect(jsonCalls).toBe(1);
    expect(fileCalls).toBe(0);
  });

  function oneBucketSummary(bucketMs: number, reqs: number) {
    const ints = [
      "rebuffer_events", "has_bs", "has_su", "has_ot", "has_bl", "has_br", "has_mtp", "has_cid",
      "br_n", "tb_n", "bl_n", "mtp_n", "v_br_n", "v_tb_n", "v_bl_n", "util_n", "util_all_n",
      "startup_requests", "su_bl_n", "su_ttfb_n",
    ];
    const doubles = [
      "br_sum", "tb_sum", "bl_sum", "mtp_sum", "v_br_sum", "v_tb_sum", "v_bl_sum", "util_sum",
      "util_all_sum", "su_bl_sum", "su_ttfb_sum",
    ];
    const blobs = ["sid_hll", "rebuffer_sid_hll", "v_bl_hist", "bl_hist", "v_mtp_hist", "mtp_hist"];
    return parquetWriteBuffer({
      columnData: [
        { name: "kind", type: "STRING", data: ["bucket"] },
        { name: "bucket", type: "INT64", data: [BigInt(bucketMs)] },
        { name: "key", type: "STRING", data: [null] },
        { name: "label", type: "STRING", data: [null] },
        { name: "truncated", type: "BOOLEAN", data: [false] },
        { name: "reqs", type: "INT64", data: [BigInt(reqs)] },
        ...ints.map((name) => ({ name, type: "INT64" as const, data: [BigInt(0)] })),
        ...doubles.map((name) => ({ name, type: "DOUBLE" as const, data: [null] })),
        ...blobs.map((name) => ({ name, type: "BYTE_ARRAY" as const, data: [null] })),
        { name: "min_ts", type: "INT64", data: [BigInt(bucketMs)] },
        { name: "max_ts", type: "INT64", data: [BigInt(bucketMs)] },
        { name: "first_ts", type: "INT64", data: [null] },
        { name: "last_ts", type: "INT64", data: [null] },
      ],
    });
  }

  it("reads CMCD hour bundles from the summary view instead of the per-window summaries they cover", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const hours = 24;
    const windowsPerHour = 75;
    const bundles = Array.from({ length: hours }, (_, h) => ({
      kind: "hour",
      key: `processed/summary_bundle/cmcd_summary/hour_bundle=${h}.parquet`,
      start: new Date(t0 + h * 3_600_000).toISOString(),
      end: new Date(t0 + (h + 1) * 3_600_000).toISOString(),
      size: 20_000,
      rows: 1,
      sources: windowsPerHour,
    }));
    const windows = Array.from({ length: hours * windowsPerHour }, (_, i) => ({
      key: `processed/request/cmcd_summary/window=${i}.parquet`,
      start: new Date(t0 + i * 48_000).toISOString(),
      end: new Date(t0 + (i + 1) * 48_000).toISOString(),
      size: 22_000,
      rows: 1,
    }));
    const urls: string[] = [];
    const fetched: string[] = [];
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t0 + hours * 3_600_000).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson(url: string) {
          urls.push(url);
          const view = new URL(url, "http://x").searchParams.get("view");
          return { files: view === "summary" ? bundles : windows, next_cursor: null, complete: true };
        },
        async fetchFile(key: string) {
          fetched.push(key);
          const h = Number(key.match(/hour_bundle=(\d+)/)?.[1]);
          return { bytes: oneBucketSummary(t0 + h * 3_600_000, windowsPerHour), cached: false };
        },
      },
    );
    expect(urls).toHaveLength(1);
    expect(new URL(urls[0], "http://x").searchParams.get("dataset")).toBe("cmcd_summary");
    expect(fetched.sort()).toEqual(bundles.map((b) => b.key).sort());
    expect(res?.summary.total_requests).toBe(hours * windowsPerHour);
  });

  it("counts only per-window CMCD summaries against the cap, so hour bundles plus the open hour's windows stay on summaries", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const hours = 23;
    const openWindows = MAX_CMCD_ROLLUP_FILES - 10;
    const bundles = Array.from({ length: hours }, (_, h) => ({
      kind: "hour",
      key: `processed/summary_bundle/cmcd_summary/hour_bundle=${h}.parquet`,
      start: new Date(t0 + h * 3_600_000).toISOString(),
      end: new Date(t0 + (h + 1) * 3_600_000).toISOString(),
      size: 20_000,
      rows: 1,
      sources: 138,
    }));
    const open = t0 + hours * 3_600_000;
    const windows = Array.from({ length: openWindows }, (_, i) => ({
      kind: "window",
      key: `processed/request/cmcd_summary/window=${i}.parquet`,
      start: new Date(open + i * 10_000).toISOString(),
      end: new Date(open + (i + 1) * 10_000).toISOString(),
      size: 14_000,
      rows: 1,
    }));
    let fetched = 0;
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t0 + (hours + 1) * 3_600_000).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson() {
          return { files: [...bundles, ...windows], next_cursor: null, complete: true };
        },
        async fetchFile() {
          fetched++;
          return { bytes: oneBucketSummary(t0, 1), cached: false };
        },
      },
    );
    expect(fetched).toBe(hours + openWindows);
    expect(res?.summary.total_requests).toBe(hours + openWindows);
  });

  it("falls back to the per-window CMCD listing when the server rejects the summary view", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const key = "processed/request/cmcd_summary/window=only.parquet";
    const views: (string | null)[] = [];
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t0 + 3_600_000).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson(url: string) {
          const view = new URL(url, "http://x").searchParams.get("view");
          views.push(view);
          if (view === "summary") throw new Error("400 Bad Request");
          return {
            files: [{ key, start: new Date(t0).toISOString(), end: new Date(t0 + 60_000).toISOString(), size: 1, rows: 1 }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes: oneBucketSummary(t0, 7), cached: false };
        },
      },
    );
    expect(views).toEqual(["summary", null]);
    expect(res?.summary.total_requests).toBe(7);
  });

  it("answers from the per-window CMCD listing when a listed hour bundle's object is gone", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const file = (kind: string, key: string, from: number, to: number) =>
      ({ kind, key, start: new Date(from).toISOString(), end: new Date(to).toISOString(), size: 1, rows: 1 });
    const bundle = file("hour", "processed/summary_bundle/cmcd_summary/hour_bundle=0.parquet", t0, t0 + 3_600_000);
    const windows = [0, 1, 2].map((i) =>
      file("window", `processed/request/cmcd_summary/window=${i}.parquet`, t0 + i * 600_000, t0 + (i + 1) * 600_000));
    const views: (string | null)[] = [];
    const fetched: string[] = [];
    const res = await cmcdFromRollup(
      { start_time: new Date(t0).toISOString(), end_time: new Date(t0 + 3_600_000).toISOString(), bucket_seconds: 300 },
      {
        async fetchJson(url: string) {
          const view = new URL(url, "http://x").searchParams.get("view");
          views.push(view);
          return { files: view === "summary" ? [bundle] : windows, next_cursor: null, complete: true };
        },
        async fetchFile(key: string) {
          fetched.push(key);
          if (key === bundle.key) throw new MissingCloudFileError(key);
          return { bytes: oneBucketSummary(t0, 5), cached: false };
        },
      },
    );
    // The fresh summary view still names the lost bundle, so its windows answer.
    expect(views).toEqual(["summary", "summary", null]);
    expect(fetched.sort()).toEqual([bundle.key, ...windows.map((w) => w.key)].sort());
    expect(res?.summary.total_requests).toBe(15);
  });

  it("reads the open hour's CMCD open bundle and relists the summary view when a snapshot it names was replaced", async () => {
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const open = t0 + 3_600_000;
    const file = (kind: string, key: string, from: number, to: number) =>
      ({ kind, key, start: new Date(from).toISOString(), end: new Date(to).toISOString(), size: 1, rows: 1 });
    const hour = file("hour", "processed/summary_bundle/cmcd_summary/hour_bundle=00.parquet", t0, open);
    const stale = file("hour", "processed/summary_bundle/cmcd_summary/open_bundle=01-a.parquet", open, open + 1_200_000);
    const fresh = file("hour", "processed/summary_bundle/cmcd_summary/open_bundle=01-b.parquet", open, open + 1_500_000);
    const tail = [25, 26].map((m) =>
      file("window", `processed/request/cmcd_summary/window=${m}.parquet`, open + m * 60_000, open + (m + 1) * 60_000));
    // The per-window listing of the range is over the cap, so only the summary view can answer.
    const windows = Array.from({ length: MAX_CMCD_ROLLUP_FILES + 1 }, (_, i) =>
      file("window", `processed/request/cmcd_summary/window=all-${i}.parquet`, t0 + i * 1_000, t0 + (i + 1) * 1_000));
    const views: (string | null)[] = [];
    const fetched: string[] = [];
    const res = await cmcdFromRollup(
      { start_time: new Date(t0).toISOString(), end_time: new Date(open + 1_800_000).toISOString(), bucket_seconds: 300 },
      {
        async fetchJson(url: string) {
          const view = new URL(url, "http://x").searchParams.get("view");
          views.push(view);
          if (view !== "summary") return { files: windows, next_cursor: null, complete: true };
          const snapshot = views.length === 1 ? stale : fresh;
          return { files: [hour, snapshot, ...tail], next_cursor: null, complete: true };
        },
        async fetchFile(key: string) {
          fetched.push(key);
          if (key === stale.key) throw new MissingCloudFileError(key);
          const reqs = key === hour.key ? 100 : key === fresh.key ? 25 : 1;
          return { bytes: oneBucketSummary(t0, reqs), cached: false };
        },
      },
    );
    expect(views).toEqual(["summary", "summary"]);
    expect([...new Set(fetched)].sort()).toEqual([hour.key, stale.key, fresh.key, ...tail.map((w) => w.key)].sort());
    expect(res?.summary.total_requests).toBe(127);
  });

  it("caches decoded cmcd_summary files across repeat queries on the same source", async () => {
    clearCmcdRollupCache();
    const t0 = Date.parse("2026-01-01T00:00:00Z");
    const file = {
      kind: "hour" as const,
      key: "processed/summary_bundle/cmcd_summary/hour_bundle=cache.parquet",
      start: new Date(t0).toISOString(),
      end: new Date(t0 + 3_600_000).toISOString(),
      size: 1024,
      rows: 1,
    };
    let fetched = 0;
    const source = {
      async fetchJson() {
        return { files: [file], next_cursor: null, complete: true };
      },
      async fetchFile() {
        fetched++;
        return { bytes: oneBucketSummary(t0, 42), cached: false };
      },
    };
    const params = {
      start_time: new Date(t0).toISOString(),
      end_time: new Date(t0 + 3_600_000).toISOString(),
      bucket_seconds: 300,
    };
    const first = await cmcdFromRollup(params, source);
    const second = await cmcdFromRollup(params, source);
    expect(first?.summary.total_requests).toBe(42);
    expect(second?.summary.total_requests).toBe(42);
    expect(fetched).toBe(1);

    clearCmcdRollupCache();
    await cmcdFromRollup(params, source);
    expect(fetched).toBe(2);
  });

  it("filters out cmcd_summary buckets outside [fromMs, toMs) and reports has_data=false when none remain", async () => {
    const t0 = Date.parse("2026-01-01T01:00:00Z");
    const outOfRangeBucket = t0 - 300_000;
    const bytes = parquetWriteBuffer({
      columnData: [
        { name: "kind", type: "STRING", data: ["bucket"] },
        { name: "bucket", type: "INT64", data: [BigInt(outOfRangeBucket)] },
        { name: "key", type: "STRING", data: [null] },
        { name: "label", type: "STRING", data: [null] },
        { name: "truncated", type: "BOOLEAN", data: [false] },
        { name: "reqs", type: "INT64", data: [BigInt(5)] },
        { name: "sid_hll", type: "BYTE_ARRAY", data: [null] },
        { name: "rebuffer_events", type: "INT64", data: [BigInt(0)] },
        { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: [null] },
        { name: "has_bs", type: "INT64", data: [BigInt(0)] },
        { name: "has_su", type: "INT64", data: [BigInt(0)] },
        { name: "has_ot", type: "INT64", data: [BigInt(0)] },
        { name: "has_bl", type: "INT64", data: [BigInt(0)] },
        { name: "has_br", type: "INT64", data: [BigInt(0)] },
        { name: "has_mtp", type: "INT64", data: [BigInt(0)] },
        { name: "has_cid", type: "INT64", data: [BigInt(0)] },
        { name: "br_sum", type: "DOUBLE", data: [null] },
        { name: "br_n", type: "INT64", data: [BigInt(0)] },
        { name: "tb_sum", type: "DOUBLE", data: [null] },
        { name: "tb_n", type: "INT64", data: [BigInt(0)] },
        { name: "bl_sum", type: "DOUBLE", data: [null] },
        { name: "bl_n", type: "INT64", data: [BigInt(0)] },
        { name: "mtp_sum", type: "DOUBLE", data: [null] },
        { name: "mtp_n", type: "INT64", data: [BigInt(0)] },
        { name: "v_br_sum", type: "DOUBLE", data: [null] },
        { name: "v_br_n", type: "INT64", data: [BigInt(0)] },
        { name: "v_tb_sum", type: "DOUBLE", data: [null] },
        { name: "v_tb_n", type: "INT64", data: [BigInt(0)] },
        { name: "v_bl_sum", type: "DOUBLE", data: [null] },
        { name: "v_bl_n", type: "INT64", data: [BigInt(0)] },
        { name: "util_sum", type: "DOUBLE", data: [null] },
        { name: "util_n", type: "INT64", data: [BigInt(0)] },
        { name: "util_all_sum", type: "DOUBLE", data: [null] },
        { name: "util_all_n", type: "INT64", data: [BigInt(0)] },
        { name: "startup_requests", type: "INT64", data: [BigInt(0)] },
        { name: "su_bl_sum", type: "DOUBLE", data: [null] },
        { name: "su_bl_n", type: "INT64", data: [BigInt(0)] },
        { name: "su_ttfb_sum", type: "DOUBLE", data: [null] },
        { name: "su_ttfb_n", type: "INT64", data: [BigInt(0)] },
        { name: "v_bl_hist", type: "BYTE_ARRAY", data: [null] },
        { name: "bl_hist", type: "BYTE_ARRAY", data: [null] },
        { name: "v_mtp_hist", type: "BYTE_ARRAY", data: [null] },
        { name: "mtp_hist", type: "BYTE_ARRAY", data: [null] },
        { name: "min_ts", type: "INT64", data: [BigInt(outOfRangeBucket)] },
        { name: "max_ts", type: "INT64", data: [BigInt(outOfRangeBucket + 60_000)] },
        { name: "first_ts", type: "INT64", data: [null] },
        { name: "last_ts", type: "INT64", data: [null] },
      ],
    });
    const key = "processed/request/cmcd_summary/window=boundary.parquet";
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t0 + 3_600_000).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson() {
          return {
            files: [{ key, start: new Date(t0 - 60_000).toISOString(), end: new Date(t0 + 60_000).toISOString(), size: bytes.byteLength, rows: 1 }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes, cached: false };
        },
      },
    );
    expect(res).not.toBeNull();
    expect(res?.has_data).toBe(false);
    expect(res?.sessions_ts).toEqual([]);
  });

  it("excludes non-CMCD rows with empty-string cmcd_sid and cmcd_ot so they do not count as active viewers", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_ot)
        VALUES ('native', 1700000100000, '', ''),
          ('native', 1700000160000, '', '')`);
      const empty = await queryCmcdAggregates({ engine });
      expect(empty.has_data).toBe(false);
      expect(empty.overview.active_sessions).toBe(0);

      // Even when a real CMCD row has cmcd_br set and cmcd_sid = '', '' must never count as a session.
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_ot, cmcd_br, cmcd_bl)
        VALUES ('native', 1700000220000, '', 'v', 2000, 4000),
          ('native', 1700000280000, 'real-sess', 'v', 3000, 5000)`);
      const mixed = await queryCmcdAggregates({ engine, bucket_seconds: 300 });
      expect(mixed.has_data).toBe(true);
      expect(mixed.overview.active_sessions).toBe(1);
      expect(mixed.summary.total_requests).toBe(2);
    } finally {
      await engine.reset();
    }
  });

  it("reports zero rebuffer metrics when cmcd_bl is present and sparse cmcd_bs is NULL (raw DuckDB)", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, geo_country, client_asn, as_name, cmcd_sid, cmcd_cid, cmcd_br, cmcd_bl, cmcd_bs, cmcd_mtp, cmcd_ot, cmcd_su)
        VALUES
          ('native', 1700000100000, 'US', 15169, 'Google', 'sess-1', 'live-1', 2500, 4000, NULL, 12000, 'v', TRUE),
          ('native', 1700000400000, 'US', 15169, 'Google', 'sess-1', 'live-1', 2500, 5000, NULL, 14000, 'v', NULL)`);
      const data = await queryCmcdAggregates({ engine, bucket_seconds: 300 });
      expect(data.overview.rebuffer_rate).toBe(0);
      expect(data.sessions_ts).toHaveLength(2);
      expect(data.sessions_ts.map((b) => b.rebuffer_session_pct)).toEqual([0, 0]);
      expect(data.buffer_health_ts.map((b) => b.starvation_rate)).toEqual([0, 0]);
      expect(data.startup_ts.map((b) => b.startup_ratio)).toEqual([100, 0]);
      expect(data.top_content[0]).toMatchObject({ content_id: "live-1", rebuffer_rate: 0 });
      expect(data.rebuffer_by_country).toHaveLength(1);
      expect(data.rebuffer_by_country[0]).toMatchObject({ country: "US", rebuffer_rate: 0 });
      expect(data.rebuffer_by_asn).toHaveLength(1);
      expect(data.rebuffer_by_asn[0]).toMatchObject({ asn: 15169, rebuffer_rate: 0 });
    } finally {
      await engine.reset();
    }
  });

  it("reports zero rebuffer metrics when has_bl > 0 and sparse has_bs = 0 (cmcdFromRollup)", async () => {
    const hllOf = (items: string[]) => {
      const h = new Hll();
      for (const item of items) h.add(item);
      return h.toBytes();
    };
    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const t2 = t0 + 600_000;
    const rows: Array<Record<string, unknown>> = [
      {
        kind: "bucket", bucket: t0, reqs: 2, sid_hll: hllOf(["s1"]),
        rebuffer_events: 0, has_bs: 0, has_su: 1, has_ot: 2, has_bl: 2, has_br: 2, has_mtp: 2, has_cid: 2,
        br_sum: 5000, br_n: 2, bl_sum: 8000, bl_n: 2, mtp_sum: 20000, mtp_n: 2,
        v_br_sum: 5000, v_br_n: 2, v_bl_sum: 8000, v_bl_n: 2,
        startup_requests: 1, min_ts: t0 + 1000, max_ts: t0 + 2000,
      },
      {
        kind: "bucket", bucket: t1, reqs: 1, sid_hll: hllOf(["s1"]),
        rebuffer_events: 0, has_bs: 0, has_su: 0, has_ot: 1, has_bl: 1, has_br: 1, has_mtp: 1, has_cid: 1,
        br_sum: 2500, br_n: 1, bl_sum: 5000, bl_n: 1, mtp_sum: 12000, mtp_n: 1,
        v_br_sum: 2500, v_br_n: 1, v_bl_sum: 5000, v_bl_n: 1,
        startup_requests: 0, min_ts: t1 + 1000, max_ts: t1 + 1000,
      },
      { kind: "session", key: "s1", reqs: 3, first_ts: t0 + 1000, last_ts: t1 + 1000 },
      { kind: "content", key: "live-1", reqs: 3, sid_hll: hllOf(["s1"]), has_bs: 0, v_br_sum: 7500, v_br_n: 3, br_sum: 7500, br_n: 3, v_bl_sum: 13000, v_bl_n: 3, bl_sum: 13000, bl_n: 3 },
      { kind: "country", key: "US", reqs: 3, sid_hll: hllOf(["s1"]), rebuffer_events: 0, has_bs: 0 },
      { kind: "asn", key: "15169", label: "Google", reqs: 3, sid_hll: hllOf(["s1"]), rebuffer_events: 0, has_bs: 0, v_br_sum: 7500, v_br_n: 3, br_sum: 7500, br_n: 3, v_bl_sum: 13000, v_bl_n: 3, bl_sum: 13000, bl_n: 3, mtp_sum: 32000, mtp_n: 3 },
      { kind: "ot", key: "v", reqs: 3 },
    ];
    const str = (k: string) => rows.map((r) => (r[k] as string | undefined) ?? null);
    const i64 = (k: string) => rows.map((r) => (r[k] == null ? null : BigInt(r[k] as number)));
    const f64 = (k: string) => rows.map((r) => (r[k] as number | undefined) ?? null);
    const bool = (k: string) => rows.map((r) => (r[k] as boolean | undefined) ?? null);
    const bin = (k: string) => rows.map((r) => (r[k] as Uint8Array | undefined) ?? null);
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      columnData: [
        { name: "kind", type: "STRING", data: str("kind") },
        { name: "bucket", type: "INT64", data: i64("bucket") },
        { name: "key", type: "STRING", data: str("key") },
        { name: "label", type: "STRING", data: str("label") },
        { name: "truncated", type: "BOOLEAN", data: bool("truncated") },
        { name: "reqs", type: "INT64", data: i64("reqs") },
        { name: "sid_hll", type: "BYTE_ARRAY", data: bin("sid_hll") },
        { name: "rebuffer_events", type: "INT64", data: i64("rebuffer_events") },
        { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: bin("rebuffer_sid_hll") },
        { name: "has_bs", type: "INT64", data: i64("has_bs") },
        { name: "has_su", type: "INT64", data: i64("has_su") },
        { name: "has_ot", type: "INT64", data: i64("has_ot") },
        { name: "has_bl", type: "INT64", data: i64("has_bl") },
        { name: "has_br", type: "INT64", data: i64("has_br") },
        { name: "has_mtp", type: "INT64", data: i64("has_mtp") },
        { name: "has_cid", type: "INT64", data: i64("has_cid") },
        { name: "br_sum", type: "DOUBLE", data: f64("br_sum") },
        { name: "br_n", type: "INT64", data: i64("br_n") },
        { name: "tb_sum", type: "DOUBLE", data: f64("tb_sum") },
        { name: "tb_n", type: "INT64", data: i64("tb_n") },
        { name: "bl_sum", type: "DOUBLE", data: f64("bl_sum") },
        { name: "bl_n", type: "INT64", data: i64("bl_n") },
        { name: "mtp_sum", type: "DOUBLE", data: f64("mtp_sum") },
        { name: "mtp_n", type: "INT64", data: i64("mtp_n") },
        { name: "v_br_sum", type: "DOUBLE", data: f64("v_br_sum") },
        { name: "v_br_n", type: "INT64", data: i64("v_br_n") },
        { name: "v_tb_sum", type: "DOUBLE", data: f64("v_tb_sum") },
        { name: "v_tb_n", type: "INT64", data: i64("v_tb_n") },
        { name: "v_bl_sum", type: "DOUBLE", data: f64("v_bl_sum") },
        { name: "v_bl_n", type: "INT64", data: i64("v_bl_n") },
        { name: "util_sum", type: "DOUBLE", data: f64("util_sum") },
        { name: "util_n", type: "INT64", data: i64("util_n") },
        { name: "util_all_sum", type: "DOUBLE", data: f64("util_all_sum") },
        { name: "util_all_n", type: "INT64", data: i64("util_all_n") },
        { name: "startup_requests", type: "INT64", data: i64("startup_requests") },
        { name: "su_bl_sum", type: "DOUBLE", data: f64("su_bl_sum") },
        { name: "su_bl_n", type: "INT64", data: i64("su_bl_n") },
        { name: "su_ttfb_sum", type: "DOUBLE", data: f64("su_ttfb_sum") },
        { name: "su_ttfb_n", type: "INT64", data: i64("su_ttfb_n") },
        { name: "v_bl_hist", type: "BYTE_ARRAY", data: bin("v_bl_hist") },
        { name: "bl_hist", type: "BYTE_ARRAY", data: bin("bl_hist") },
        { name: "v_mtp_hist", type: "BYTE_ARRAY", data: bin("v_mtp_hist") },
        { name: "mtp_hist", type: "BYTE_ARRAY", data: bin("mtp_hist") },
        { name: "min_ts", type: "INT64", data: i64("min_ts") },
        { name: "max_ts", type: "INT64", data: i64("max_ts") },
        { name: "first_ts", type: "INT64", data: i64("first_ts") },
        { name: "last_ts", type: "INT64", data: i64("last_ts") },
      ],
    });
    const key = "processed/request/cmcd_summary/window=sparse-bs.parquet";
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t2).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson() {
          return {
            files: [{ key, start: new Date(t0).toISOString(), end: new Date(t2).toISOString(), size: bytes.byteLength, rows: rows.length }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes, cached: false };
        },
      },
    );
    expect(res).not.toBeNull();
    expect(res?.overview.rebuffer_rate).toBe(0);
    expect(res?.sessions_ts.map((b) => b.rebuffer_session_pct)).toEqual([0, 0]);
    expect(res?.buffer_health_ts.map((b) => b.starvation_rate)).toEqual([0, 0]);
    expect(res?.startup_ts.map((b) => b.startup_ratio)).toEqual([50, 0]);
    expect(res?.top_content[0]).toMatchObject({ content_id: "live-1", rebuffer_rate: 0 });
    expect(res?.rebuffer_by_country).toHaveLength(1);
    expect(res?.rebuffer_by_country[0]).toMatchObject({ country: "US", rebuffer_rate: 0 });
    expect(res?.rebuffer_by_asn).toHaveLength(1);
    expect(res?.rebuffer_by_asn[0]).toMatchObject({ asn: 15169, rebuffer_rate: 0 });
  });

  it("zero-fills idle buckets in sessions_ts and nulls quality metrics across gaps (raw DuckDB and cmcdFromRollup)", async () => {
    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const t2 = t0 + 600_000;
    const t3 = t0 + 900_000;

    // Raw DuckDB path: traffic at t0 and t2, idle at t1
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, cmcd_sid, cmcd_ot, cmcd_br, cmcd_bl, cmcd_mtp)
        VALUES
          ('native', ${t0 + 1000}, 's1', 'v', 2500, 4000, 12000),
          ('native', ${t0 + 2000}, 's2', 'v', 2500, 4000, 12000),
          ('native', ${t2 + 1000}, 's3', 'v', 3000, 5000, 14000)`);
      const raw = await queryCmcdAggregates({
        engine,
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t3).toISOString(),
        bucket_seconds: 300,
      });
      expect(raw.sessions_ts.map((b) => b.concurrent_sessions)).toEqual([2, 0, 1]);
      expect(raw.sessions_ts.map((b) => b.new_sessions)).toEqual([2, 0, 1]);
      expect(raw.buffer_health_ts.map((b) => b.p50_buffer)).toEqual([4000, null, 5000]);
      expect(raw.bitrate_ts.map((b) => b.avg_bitrate)).toEqual([2500, null, 3000]);
    } finally {
      await engine.reset();
    }

    // Rollup path: bucket at t0 and t2, idle at t1
    const hllOf = (items: string[]) => {
      const h = new Hll();
      for (const item of items) h.add(item);
      return h.toBytes();
    };
    const key = "processed/request/cmcd_summary/window=gap.parquet";
    const bytes = parquetWriteBuffer({
      columnData: [
        { name: "kind", type: "STRING", data: ["bucket", "bucket"] },
        { name: "bucket", type: "INT64", data: [BigInt(t0), BigInt(t2)] },
        { name: "key", type: "STRING", data: [null, null] },
        { name: "label", type: "STRING", data: [null, null] },
        { name: "truncated", type: "BOOLEAN", data: [false, false] },
        { name: "reqs", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "sid_hll", type: "BYTE_ARRAY", data: [hllOf(["s1", "s2"]), hllOf(["s3"])] },
        { name: "rebuffer_events", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: [null, null] },
        { name: "has_bs", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_su", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_ot", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "has_bl", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "has_br", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "has_mtp", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_cid", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "br_sum", type: "DOUBLE", data: [5000, 3000] },
        { name: "br_n", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "tb_sum", type: "DOUBLE", data: [null, null] },
        { name: "tb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "bl_sum", type: "DOUBLE", data: [8000, 5000] },
        { name: "bl_n", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "mtp_sum", type: "DOUBLE", data: [null, null] },
        { name: "mtp_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_br_sum", type: "DOUBLE", data: [5000, 3000] },
        { name: "v_br_n", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "v_tb_sum", type: "DOUBLE", data: [null, null] },
        { name: "v_tb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_bl_sum", type: "DOUBLE", data: [8000, 5000] },
        { name: "v_bl_n", type: "INT64", data: [BigInt(2), BigInt(1)] },
        { name: "util_sum", type: "DOUBLE", data: [null, null] },
        { name: "util_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "util_all_sum", type: "DOUBLE", data: [null, null] },
        { name: "util_all_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "startup_requests", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "su_bl_sum", type: "DOUBLE", data: [null, null] },
        { name: "su_bl_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "su_ttfb_sum", type: "DOUBLE", data: [null, null] },
        { name: "su_ttfb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_bl_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "bl_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "v_mtp_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "mtp_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "min_ts", type: "INT64", data: [BigInt(t0 + 1000), BigInt(t2 + 1000)] },
        { name: "max_ts", type: "INT64", data: [BigInt(t0 + 2000), BigInt(t2 + 1000)] },
        { name: "first_ts", type: "INT64", data: [null, null] },
        { name: "last_ts", type: "INT64", data: [null, null] },
      ],
    });
    const rollup = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t3).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson() {
          return {
            files: [{ key, start: new Date(t0).toISOString(), end: new Date(t3).toISOString(), size: bytes.byteLength, rows: 2 }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes, cached: false };
        },
      },
    );
    expect(rollup).not.toBeNull();
    expect(rollup?.sessions_ts.map((b) => b.concurrent_sessions)).toEqual([2, 0, 1]);
    expect(rollup?.sessions_ts[1]?.bucket).toBe(new Date(t1).toISOString());
    expect(rollup?.bitrate_ts.map((b) => b.avg_bitrate)).toEqual([2500, null, 3000]);
  });

  it("excludes zero-session countries and ASNs in cmcdFromRollup", async () => {
    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const hllOf = (items: string[]) => {
      const h = new Hll();
      for (const item of items) h.add(item);
      return h.toBytes();
    };
    const key = "processed/request/cmcd_summary/window=zero-sess.parquet";
    const bytes = parquetWriteBuffer({
      columnData: [
        { name: "kind", type: "STRING", data: ["bucket", "country", "country", "asn", "asn"] },
        { name: "bucket", type: "INT64", data: [BigInt(t0), null, null, null, null] },
        { name: "key", type: "STRING", data: [null, "US", "CA", "7922", "15169"] },
        { name: "label", type: "STRING", data: [null, null, null, "Comcast", "Google"] },
        { name: "truncated", type: "BOOLEAN", data: [false, false, false, false, false] },
        { name: "reqs", type: "INT64", data: [BigInt(5), BigInt(2), BigInt(3), BigInt(2), BigInt(3)] },
        { name: "sid_hll", type: "BYTE_ARRAY", data: [hllOf(["s1"]), hllOf(["s1"]), hllOf([]), hllOf(["s1"]), hllOf([])] },
        { name: "rebuffer_events", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: [null, null, null, null, null] },
        { name: "has_bs", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "has_su", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "has_ot", type: "INT64", data: [BigInt(5), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "has_bl", type: "INT64", data: [BigInt(5), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "has_br", type: "INT64", data: [BigInt(5), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "has_mtp", type: "INT64", data: [BigInt(5), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "has_cid", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "br_sum", type: "DOUBLE", data: [2000, 2000, 0, 2000, 0] },
        { name: "br_n", type: "INT64", data: [BigInt(2), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "tb_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "tb_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "bl_sum", type: "DOUBLE", data: [4000, 4000, 0, 4000, 0] },
        { name: "bl_n", type: "INT64", data: [BigInt(2), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "mtp_sum", type: "DOUBLE", data: [5000, 5000, 0, 5000, 0] },
        { name: "mtp_n", type: "INT64", data: [BigInt(2), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "v_br_sum", type: "DOUBLE", data: [2000, 2000, 0, 2000, 0] },
        { name: "v_br_n", type: "INT64", data: [BigInt(2), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "v_tb_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "v_tb_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "v_bl_sum", type: "DOUBLE", data: [4000, 4000, 0, 4000, 0] },
        { name: "v_bl_n", type: "INT64", data: [BigInt(2), BigInt(2), BigInt(0), BigInt(2), BigInt(0)] },
        { name: "util_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "util_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "util_all_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "util_all_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "startup_requests", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "su_bl_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "su_bl_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "su_ttfb_sum", type: "DOUBLE", data: [null, null, null, null, null] },
        { name: "su_ttfb_n", type: "INT64", data: [BigInt(0), BigInt(0), BigInt(0), BigInt(0), BigInt(0)] },
        { name: "v_bl_hist", type: "BYTE_ARRAY", data: [null, null, null, null, null] },
        { name: "bl_hist", type: "BYTE_ARRAY", data: [null, null, null, null, null] },
        { name: "v_mtp_hist", type: "BYTE_ARRAY", data: [null, null, null, null, null] },
        { name: "mtp_hist", type: "BYTE_ARRAY", data: [null, null, null, null, null] },
        { name: "min_ts", type: "INT64", data: [BigInt(t0 + 1000), null, null, null, null] },
        { name: "max_ts", type: "INT64", data: [BigInt(t0 + 2000), null, null, null, null] },
        { name: "first_ts", type: "INT64", data: [null, null, null, null, null] },
        { name: "last_ts", type: "INT64", data: [null, null, null, null, null] },
      ],
    });
    const res = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t1).toISOString(),
        bucket_seconds: 300,
      },
      {
        async fetchJson() {
          return {
            files: [{ key, start: new Date(t0).toISOString(), end: new Date(t1).toISOString(), size: bytes.byteLength, rows: 5 }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes, cached: false };
        },
      },
    );
    expect(res?.rebuffer_by_country.map((r) => r.country)).toEqual(["US"]);
    expect(res?.rebuffer_by_asn.map((r) => r.asn)).toEqual([7922]);
  });

  it("defaults bucket_seconds to 300, rounds floats to 2dp, drops queryCmcdStatus, and reports partial rollup sections", async () => {
    expect("queryCmcdStatus" in cmcdModule).toBe(false);

    const engine = new DuckDBAnalyticalEngine();
    try {
      const baseMs = 1_800_000_000_000;
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, country, cmcd_sid, cmcd_cid, cmcd_br, cmcd_tb, cmcd_bl, cmcd_mtp, cmcd_bs, cmcd_ot)
        VALUES
          ('native', ${baseMs + 10_000}, 'US', 's1', 'c1', 1001, 3000, 1111, 2222, true, 'v'),
          ('native', ${baseMs + 70_000}, 'US', 's2', 'c1', 2000, 3000, 2222, 3333, false, 'v'),
          ('native', ${baseMs + 130_000}, 'US', 's3', 'c1', 2500, 3000, 3333, 4444, false, 'v')`);
      const data = await queryCmcdAggregates({ engine });
      expect(data.sessions_ts).toHaveLength(1);
      expect(data.overview.avg_bitrate).toBe(1833.67);
      expect(data.overview.avg_buffer_length).toBe(2222);
      expect(data.overview.rebuffer_rate).toBe(33.33);
      expect(data.top_content[0].avg_bitrate).toBe(1833.67);
    } finally {
      await engine.reset();
    }

    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const hllOf = (items: string[]) => {
      const h = new Hll();
      for (const item of items) h.add(item);
      return h.toBytes();
    };
    const bytes = parquetWriteBuffer({
      columnData: [
        { name: "kind", type: "STRING", data: ["bucket", "sf"] },
        { name: "bucket", type: "INT64", data: [BigInt(t0), null] },
        { name: "key", type: "STRING", data: [null, "h"] },
        { name: "label", type: "STRING", data: [null, null] },
        { name: "truncated", type: "BOOLEAN", data: [false, false] },
        { name: "reqs", type: "INT64", data: [BigInt(3), BigInt(3)] },
        { name: "sid_hll", type: "BYTE_ARRAY", data: [hllOf(["s1"]), hllOf(["s1"])] },
        { name: "rebuffer_events", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "rebuffer_sid_hll", type: "BYTE_ARRAY", data: [null, null] },
        { name: "has_bs", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_su", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_ot", type: "INT64", data: [BigInt(3), BigInt(0)] },
        { name: "has_bl", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_br", type: "INT64", data: [BigInt(3), BigInt(0)] },
        { name: "has_mtp", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "has_cid", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "br_sum", type: "DOUBLE", data: [5501, 0] },
        { name: "br_n", type: "INT64", data: [BigInt(3), BigInt(0)] },
        { name: "tb_sum", type: "DOUBLE", data: [null, null] },
        { name: "tb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "bl_sum", type: "DOUBLE", data: [null, null] },
        { name: "bl_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "mtp_sum", type: "DOUBLE", data: [null, null] },
        { name: "mtp_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_br_sum", type: "DOUBLE", data: [5501, 0] },
        { name: "v_br_n", type: "INT64", data: [BigInt(3), BigInt(0)] },
        { name: "v_tb_sum", type: "DOUBLE", data: [null, null] },
        { name: "v_tb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_bl_sum", type: "DOUBLE", data: [null, null] },
        { name: "v_bl_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "util_sum", type: "DOUBLE", data: [null, null] },
        { name: "util_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "util_all_sum", type: "DOUBLE", data: [null, null] },
        { name: "util_all_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "startup_requests", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "su_bl_sum", type: "DOUBLE", data: [null, null] },
        { name: "su_bl_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "su_ttfb_sum", type: "DOUBLE", data: [null, null] },
        { name: "su_ttfb_n", type: "INT64", data: [BigInt(0), BigInt(0)] },
        { name: "v_bl_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "bl_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "v_mtp_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "mtp_hist", type: "BYTE_ARRAY", data: [null, null] },
        { name: "min_ts", type: "INT64", data: [BigInt(t0 + 1000), null] },
        { name: "max_ts", type: "INT64", data: [BigInt(t0 + 2000), null] },
        { name: "first_ts", type: "INT64", data: [null, null] },
        { name: "last_ts", type: "INT64", data: [null, null] },
      ],
    });
    const partialRollup = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t1).toISOString(),
      },
      {
        async fetchJson() {
          return {
            files: [{ key: "processed/request/cmcd_summary/window=partial.parquet", start: new Date(t0).toISOString(), end: new Date(t1).toISOString(), size: bytes.byteLength, rows: 2 }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          return { bytes, cached: false };
        },
      },
    );
    expect(partialRollup?.overview.avg_bitrate).toBe(1833.67);
    expect(isCmcdPartial(partialRollup)).toBe(true);
    expect(getCmcdResidualSections(partialRollup)).toEqual(["bitrate_ladder", "stream_type_dist"]);
  });

  it("answers emptyCmcdAggregates without raw fallback when cmcd_summary is empty and raw files are all cataloged", async () => {
    const t0 = 1_800_000_000_000;
    const t1 = t0 + 300_000;
    const hex64 = "a".repeat(64);
    const catalogedResult = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t1).toISOString(),
        available: true,
      },
      {
        async fetchJson(url) {
          if (url.includes("dataset=cmcd_summary")) {
            return { files: [], next_cursor: null, complete: true };
          }
          return {
            files: [
              {
                key: `processed/_catalog/catalog0/${hex64}/00001.parquet`,
                start: new Date(t0).toISOString(),
                end: new Date(t1).toISOString(),
                size: 1024,
                rows: 50,
              },
            ],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          throw new Error("should not fetch any file");
        },
      },
    );
    expect(catalogedResult).toEqual(emptyCmcdAggregates(true));

    const legacyResult = await cmcdFromRollup(
      {
        start_time: new Date(t0).toISOString(),
        end_time: new Date(t1).toISOString(),
        available: true,
      },
      {
        async fetchJson(url) {
          if (url.includes("dataset=cmcd_summary")) {
            return { files: [], next_cursor: null, complete: true };
          }
          return {
            files: [
              {
                key: "processed/request/window=legacy.parquet",
                start: new Date(t0).toISOString(),
                end: new Date(t1).toISOString(),
                size: 1024,
                rows: 50,
              },
            ],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile() {
          throw new Error("should not fetch any file");
        },
      },
    );
    expect(legacyResult).toBeNull();
  });
});
