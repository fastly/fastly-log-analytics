// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";
import {
  emptyValueSummary,
  handleValueRequest,
  queryValueSummary,
} from "./value";

describe("Feature F22 — Fastly Value cloud-engine", () => {
  it("serves populated page sections and chart buckets from physical short fields", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const queries = vi.spyOn(engine, "executeSql");
    try {
      await engine.executeSql(`
        INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", status, cache, resp_bytes, elapsed, ttfb,
          country, proto, tls, is_ipv6, resp_header_content_encoding, url
        ) VALUES
          ('short', 1700000000000, epoch_ms(1700000000000), 200, 'HIT', 1000, 10000, 0.005, 'US', 'HTTP/3', 'TLSv1.3', TRUE, 'br', '/a'),
          ('short', 1700000060000, epoch_ms(1700000060000), 200, 'MISS', 3000, 50000, 0.025, 'US', 'HTTP/2', 'TLSv1.2', FALSE, 'gzip', '/a')
      `);
      const result = await queryValueSummary({
        engine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        chart_interval: "1 minute",
      });
      expect(result.overview).toMatchObject({
        total_requests: 2, total_bandwidth_bytes: 4000, cache_acceleration_factor: 5,
      });
      expect(result.caching).toMatchObject({ hit_requests: 1, miss_requests: 1 });
      expect(result.caching?.offload_time_series).toHaveLength(2);
      const bucketSql = queries.mock.calls.find(([sql]) => sql.includes("time_bucket("))![0];
      expect(await engine.executeSql(`
        SELECT typeof(bucket) AS bucket_type, epoch_ms(bucket) AS bucket_ms FROM (${bucketSql})
      `)).toEqual([
        { bucket_type: "TIMESTAMP", bucket_ms: Date.parse("2023-11-14T22:13:00Z") },
        { bucket_type: "TIMESTAMP", bucket_ms: Date.parse("2023-11-14T22:14:00Z") },
      ]);
      expect(result.caching?.cache_state_time_series).toEqual(
        expect.arrayContaining([expect.objectContaining({ category: "HIT", value: 1 })]),
      );
      expect(result.performance).toMatchObject({
        avg_hit_latency_ms: 10, avg_miss_latency_ms: 50, p99_latency_ms: 49.6,
        cache_accel_factor: 5,
      });
      expect(result.performance?.latency_time_series).toEqual(
        expect.arrayContaining([expect.objectContaining({ category: "hit", value: 10 })]),
      );
      expect(result.network).toMatchObject({ http3_pct: 50, h2_pct: 50, tls_pct: 100, ipv6_pct: 50 });
      expect(result.network?.protocol_time_series).toEqual(
        expect.arrayContaining([expect.objectContaining({ category: "HTTP/3", value: 1 })]),
      );
      expect(result.security?.waf_blocked).toBeNull();
      expect(result.io?.io_actual_bandwidth_saved_bytes).toBeNull();
    } finally {
      queries.mockRestore();
      await engine.reset();
    }
  });

  it("applies include/exclude filters and emits only requested sections", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", country, cache)
        VALUES ('filtered', 1700000000000, epoch_ms(1700000000000), 'US', 'HIT'),
          ('filtered', 1700000000000, epoch_ms(1700000000000), 'GB', 'MISS')
      `);
      const result = await queryValueSummary({
        engine, sections: ["overview", "caching"],
        filters: { country: { mode: "exclude", values: ["GB"] } },
      });
      expect(result.overview?.total_requests).toBe(1);
      expect(result.caching?.hit_requests).toBe(1);
      expect(result.performance).toBeUndefined();
      const empty = await queryValueSummary({
        engine, filters: { country: { mode: "include", values: ["FR"] } },
      });
      expect(empty.overview?.total_requests).toBe(0);
      expect(result.overview?.cache_acceleration_factor).toBeNull();
    } finally {
      await engine.reset();
    }
  });

  it("returns 7-section empty value summary when no rows exist", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const res = await queryValueSummary({ engine });
    expect(res).toEqual(emptyValueSummary());
    await engine.reset();
  });

  it("computes real optional security, bot and IO metrics without estimating missing measurements", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", status, cache_status, resp_body_size,
        ttfb_ms, geo_country, fastly_pop, client_asn, tls_version, protocol,
        content_type, content_encoding, url, fastly_is_shield, bot_category, bot_verified,
        time_elapsed_usec, waf_action, waf_sig, io_input_format, io_output_format, io_input_bytes, io_output_bytes
      ) VALUES
        ('v1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, 200, 'HIT', 100000, 10.0, 'US', 'IAD', 15169, 'TLSv1.3', 'HTTP/3', 'image/webp', 'br', '/hero.webp?ifmt=webp', TRUE, NULL, FALSE, 10000, 'BLOCK', 'attack', 'jpeg', 'webp', 200000, 100000),
        ('v1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, 200, 'HIT', 80000, 10.0, 'US', 'IAD', 15169, 'TLSv1.3', 'HTTP/2', 'image/avif', 'br', '/banner.avif', FALSE, 'search_engine', TRUE, 10000, 'LOG', NULL, 'png', 'avif', 160000, 80000),
        ('v1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, 200, 'MISS', 120000, 50.0, 'GB', 'LHR', 3356, 'TLSv1.2', 'HTTP/1.1', 'image/jpeg', 'identity', '/photo.jpg', FALSE, NULL, FALSE, 50000, 'ALLOW', NULL, NULL, NULL, NULL, NULL)`,
    );

    const summary = await handleValueRequest("/api/value/summary", { engine });
    expect(summary.overview?.total_requests).toBe(3);
    expect(summary.caching?.hit_requests).toBe(2);
    expect(summary.caching?.miss_requests).toBe(1);
    expect(summary.caching?.origin_offload_pct).toBeCloseTo(66.67, 1);
    expect(summary.performance?.cache_accel_factor).toBe(5);
    expect(summary.caching?.bandwidth_saved_bytes).toBe(180000);
    expect(summary.security).toMatchObject({ waf_blocked: 1, waf_logged: 1, waf_passed: 1 });
    expect(summary.security?.top_waf_signals).toEqual([{ signal: "attack", count: 1 }]);
    expect(summary.security?.threat_time_series).toEqual(
      expect.arrayContaining([expect.objectContaining({ category: "blocked", value: 1 })]),
    );
    expect(summary.bots).toMatchObject({ bot_requests: 1, verified_bots: 1 });
    expect(summary.bots?.top_bots).toEqual([{ name: "search_engine", count: 1 }]);
    expect(summary.io).toMatchObject({
      image_request_count: 3, io_transforms: 2,
      io_actual_bandwidth_saved_bytes: 180000, io_actual_compression_ratio: 2,
      io_estimated_cost_usd: null, estimated_savings_bytes: null,
    });
    expect(summary.io?.optimization_opportunities?.[0]).toMatchObject({ url: "/photo.jpg", request_count: 1 });
    await engine.reset();
  });

  it("filters numeric custom columns and keeps absent measurements unknown", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql("ALTER TABLE requests ADD COLUMN custom_duration DOUBLE");
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", custom_duration)
        VALUES ('custom', 1700000000000, epoch_ms(1700000000000), 3600),
          ('custom', 1700000000000, epoch_ms(1700000000000), 7200)
      `);
      const result = await queryValueSummary({
        engine, filters: { custom_duration: { mode: "include", values: ["3600"] } },
      });
      expect(result.overview?.total_requests).toBe(1);
      expect(result.network?.tls_pct).toBeNull();
      expect(result.bots?.verified_bots).toBeNull();
      expect(result.performance?.avg_hit_latency_ms).toBeNull();
    } finally {
      await engine.reset();
    }
  });
});
