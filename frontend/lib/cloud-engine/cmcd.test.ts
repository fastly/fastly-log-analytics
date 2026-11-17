// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  classifySessionDurationBucket,
  CMCD_SESSION_DURATION_BUCKETS,
  emptyCmcdAggregates,
  handleCmcdRequest,
  queryCmcdAggregates,
} from "./cmcd";
import { DuckDBAnalyticalEngine } from "./dashboard/test-fixtures";

describe("Feature F24 — CMCD Streaming Media cloud-engine", () => {
  it("does not fabricate missing optional streaming measurements", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests (scoped_key, ts_ms, cmcd_sid, cmcd_ot)
        VALUES ('native', 1700000100000, 'manifest-session', 'm')`);
      const data = await queryCmcdAggregates({ engine });
      expect(data.overview).toMatchObject({
        rebuffer_rate: null, avg_bitrate: null, avg_buffer_length: null, median_throughput: null,
      });
      expect(data.bitrate_ts[0].utilization_ratio).toBeNull();
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
    }
    await engine.reset();
  });
});
