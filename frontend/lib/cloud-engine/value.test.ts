// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/test-fixtures";
import {
  emptyValueSummary,
  handleValueRequest,
  queryValueSummary,
  valueScanColumns,
} from "./value";

describe("Feature F22 — Fastly Value cloud-engine", () => {
  it("excludes wide columns and filter_mode from summary-section projections", () => {
    const cols = valueScanColumns({
      sections: ["overview", "caching", "network"],
      filters: { status: { mode: "include", values: ["200"] }, filter_mode: "and" },
    });
    expect(cols).not.toBeNull();
    expect(cols).toContain("status");
    for (const excluded of ["url", "ua", "waf_sig", "content_type", "filter_mode"]) {
      expect(cols).not.toContain(excluded);
    }
  });

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

  it("derives WAF outcomes from waf/waf_resp and classifies ua/verified-bot signals when legacy bot columns are absent", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", cache, resp_bytes, waf, waf_resp, waf_sig, ua
        ) VALUES
          ('waf-ua', 1700000000000, epoch_ms(1700000000000), 'MISS', 100, TRUE, 406, 'SQLI', 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'),
          ('waf-ua', 1700000001000, epoch_ms(1700000001000), 'MISS', 100, TRUE, NULL, 'XSS,VERIFIED-BOT.googlebot', 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'),
          ('waf-ua', 1700000002000, epoch_ms(1700000002000), 'HIT', 100, TRUE, 200, 'VERIFIED-BOT.bingbot', 'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)'),
          ('waf-ua', 1700000003000, epoch_ms(1700000003000), 'HIT', 100, FALSE, NULL, NULL, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)')
      `);
      const res = await queryValueSummary({
        engine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        sections: ["overview", "security", "bots"],
      });
      expect(res.overview?.threats_blocked).toBe(1);
      expect(res.security).toMatchObject({
        waf_blocked: 1,
        waf_logged: 1,
        waf_passed: 1,
      });
      expect(
        (res.security?.waf_blocked ?? 0) + (res.security?.waf_logged ?? 0) + (res.security?.waf_passed ?? 0),
      ).toBeLessThanOrEqual(3);
      expect(res.security?.threat_time_series).toEqual([
        { time: "2023-11-14T00:00:00.000Z", value: 1, category: "blocked" },
        { time: "2023-11-14T00:00:00.000Z", value: 1, category: "logged" },
      ]);
      expect(res.bots).toMatchObject({
        total_requests: 4,
        bot_requests: 3,
        verified_bots: 2,
        top_bots: [
          { name: "Google Crawler", count: 2 },
          { name: "Bing Crawler", count: 1 },
        ],
      });
    } finally {
      await engine.reset();
    }
  });

  it("runs a single pruned bucket scan without QUANTILE_CONT or bot regex when only caching and network are requested", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const sqls: string[] = [];
    const spyEngine = {
      executeSql: <T extends Record<string, unknown>>(sql: string, params?: unknown[], signal?: AbortSignal) => {
        sqls.push(sql);
        return engine.executeSql<T>(sql, params, signal);
      },
    } as unknown as DuckDBAnalyticalEngine;
    try {
      await engine.executeSql(`
        INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", cache, resp_bytes, proto, is_ssl, is_ipv6
        ) VALUES
          ('cn', 1700000000000, epoch_ms(1700000000000), 'HIT', 200, 'HTTP/3', TRUE, TRUE),
          ('cn', 1700000001000, epoch_ms(1700000001000), 'MISS', 100, 'HTTP/2', TRUE, FALSE)
      `);
      const res = await queryValueSummary({
        engine: spyEngine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        sections: ["caching", "network"],
      });
      expect(sqls).toHaveLength(1);
      expect(sqls[0]).not.toContain("QUANTILE_CONT");
      expect(sqls[0]).not.toContain("regexp_matches");
      expect(sqls[0]).not.toContain("io_output_format");
      expect(res.caching).toMatchObject({
        total_requests: 2,
        hit_requests: 1,
        miss_requests: 1,
        bandwidth_saved_bytes: 200,
      });
      expect(res.network).toMatchObject({
        total_requests: 2,
        http3_pct: 50,
        h2_pct: 50,
        tls_pct: 100,
        ipv6_pct: 50,
      });
    } finally {
      await engine.reset();
    }
  });

  it("scans once and skips waf_sig regexes when requesting summary sections without security", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const sqls: string[] = [];
    const spyEngine = {
      executeSql: <T extends Record<string, unknown>>(sql: string, params?: unknown[], signal?: AbortSignal) => {
        sqls.push(sql);
        return engine.executeSql<T>(sql, params, signal);
      },
    } as unknown as DuckDBAnalyticalEngine;
    try {
      await engine.executeSql(`
        INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", cache, resp_bytes, elapsed, proto, is_ssl, is_ipv6, waf, waf_resp
        ) VALUES
          ('sum', 1700000000000, epoch_ms(1700000000000), 'HIT', 200, 10000, 'HTTP/3', TRUE, TRUE, TRUE, 406),
          ('sum', 1700000001000, epoch_ms(1700000001000), 'HIT', 100, 20000, 'HTTP/2', TRUE, FALSE, FALSE, 200)
      `);
      const res = await queryValueSummary({
        engine: spyEngine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        sections: ["overview", "caching", "network"],
        filters: { cache: { mode: "include", values: ["HIT"] } },
      });
      const dataScans = sqls.filter((sql) => !sql.startsWith("DESCRIBE "));
      expect(dataScans).toHaveLength(1);
      expect(dataScans[0]).not.toContain("regexp_matches");
      expect(dataScans[0]).not.toContain("waf_sig");
      expect(res.overview).toMatchObject({
        total_requests: 2,
        total_bandwidth_bytes: 300,
        edge_hit_requests: 2,
        origin_requests: 0,
        threats_blocked: 1,
      });
    } finally {
      await engine.reset();
    }
  });

  it("FV-07: detects images by URL extension only when content_type is empty and normalizes jpg to jpeg", async () => {
    expect(valueScanColumns({ sections: ["io"] })).toContain("url");
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", resp_bytes, content_type, url, io_output_format
        ) VALUES
          ('fv07', 1700000000000, epoch_ms(1700000000000), 70000, NULL, '/hero.JPG', NULL),
          ('fv07', 1700000001000, epoch_ms(1700000001000), 20000, '', '/thumb.jpeg', NULL),
          ('fv07', 1700000002000, epoch_ms(1700000002000), 60000, '', '/diagram.svg', NULL),
          ('fv07', 1700000003000, epoch_ms(1700000003000), 10000, NULL, '/badge.png', NULL),
          ('fv07', 1700000004000, epoch_ms(1700000004000), 90000, 'text/html', '/trap.png', NULL),
          ('fv07', 1700000005000, epoch_ms(1700000005000), 80000, '', '/index.html', NULL)
      `);
      const res = await queryValueSummary({
        engine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        sections: ["io"],
      });
      expect(res.io?.image_request_count).toBe(4);
      expect(res.io?.image_bandwidth_bytes).toBe(160000);
      expect(res.io?.format_distribution).toHaveLength(3);
      expect(res.io?.format_distribution).toEqual(
        expect.arrayContaining([
          { format: "jpeg", count: 2, pct: 50 },
          { format: "png", count: 1, pct: 25 },
          { format: "svg", count: 1, pct: 25 },
        ]),
      );
      expect(res.io?.optimization_opportunities).toEqual([
        { url: "/hero.JPG", request_count: 1, total_bytes: 70000, avg_kb: 68.36 },
      ]);
    } finally {
      await engine.reset();
    }
  });

  it("includes SYNTH and ERROR in origin_requests, clamps bucket_sec >= 60, and trims/limits top_bots and top_waf_signals to 10", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const queries = vi.spyOn(engine, "executeSql");
    try {
      const botRows = Array.from({ length: 12 }, (_, i) =>
        `('v2', ${1700000000000 + i * 1000}, epoch_ms(${1700000000000 + i * 1000}), 'MISS', ' bot-${i} ', ' sig-${i} ')`
      );
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", cache, _bot_name, waf_sig)
        VALUES
          ${botRows.join(",\n          ")},
          ('v2', 1700000012000, epoch_ms(1700000012000), 'SYNTH', NULL, NULL),
          ('v2', 1700000013000, epoch_ms(1700000013000), 'ERROR', NULL, NULL)
      `);
      const res = await queryValueSummary({
        engine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        chart_interval: "1 second",
      });
      expect(res.overview?.origin_requests).toBe(14);
      expect(res.bots?.top_bots).toHaveLength(10);
      expect(res.bots?.top_bots?.[0]?.name).not.toMatch(/^\s|\s$/);
      expect(res.security?.top_waf_signals).toHaveLength(10);
      expect(res.security?.top_waf_signals?.[0]?.signal).not.toMatch(/^\s|\s$/);
      const bucketSql = queries.mock.calls.find(([sql]) => sql.includes("time_bucket("))?.[0] ?? "";
      expect(bucketSql).toContain("INTERVAL '60 seconds'");
    } finally {
      queries.mockRestore();
      await engine.reset();
    }
  });

  it("skips DESCRIBE requests when filtering only on known catalog columns", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const sqls: string[] = [];
    const spyEngine = {
      executeSql: <T extends Record<string, unknown>>(sql: string, params?: unknown[], signal?: AbortSignal) => {
        sqls.push(sql);
        return engine.executeSql<T>(sql, params, signal);
      },
    } as unknown as DuckDBAnalyticalEngine;
    try {
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", status, cache, resp_bytes)
        VALUES ('known-col', 1700000000000, epoch_ms(1700000000000), 200, 'HIT', 100),
          ('known-col', 1700000001000, epoch_ms(1700000001000), 404, 'MISS', 50)
      `);
      const res = await queryValueSummary({
        engine: spyEngine,
        start_time: "2023-11-14T22:00:00Z",
        end_time: "2023-11-14T23:00:00Z",
        sections: ["overview"],
        filters: { status: { mode: "include", values: ["200"] } },
      });
      expect(sqls.filter((sql) => sql.startsWith("DESCRIBE"))).toHaveLength(0);
      expect(res.overview?.total_requests).toBe(1);
    } finally {
      await engine.reset();
    }
  });
});
