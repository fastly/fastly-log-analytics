// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  classifySessionDurationBucket,
  CMCD_SESSION_DURATION_BUCKETS,
  emptyCmcdAggregates,
  handleCmcdRequest,
  queryCmcdAggregates,
} from "./cmcd";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";

describe("Feature F24 — CMCD Streaming Media cloud-engine", () => {
  it("classifies all 7 session duration buckets accurately", () => {
    expect(CMCD_SESSION_DURATION_BUCKETS).toEqual([
      "< 30s",
      "30s - 1m",
      "1-5 min",
      "5-15 min",
      "15-30 min",
      "30-60 min",
      "> 1h",
    ]);
    expect(classifySessionDurationBucket(10)).toBe("< 30s");
    expect(classifySessionDurationBucket(45)).toBe("30s - 1m");
    expect(classifySessionDurationBucket(120)).toBe("1-5 min");
    expect(classifySessionDurationBucket(600)).toBe("5-15 min");
    expect(classifySessionDurationBucket(1200)).toBe("15-30 min");
    expect(classifySessionDurationBucket(2400)).toBe("30-60 min");
    expect(classifySessionDurationBucket(4000)).toBe("> 1h");
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
      expect(res.session_duration_dist).toHaveLength(7);
    }
    await engine.reset();
  });
});
