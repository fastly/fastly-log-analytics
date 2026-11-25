// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => new Set(["ttl", "resp_header_content_encoding"]),
}));
import {
  computeSummaryFromDims,
  emptyAssetsAggregates,
  handleAssetsRequest,
  queryAssetsAggregates,
} from "./assets";
import { DuckDBAnalyticalEngine } from "./dashboard/test-fixtures";
import { formatPercent } from "@/lib/format";

describe("Feature F22 — Assets & Shielding cloud-engine", () => {
  it("returns native Assets consumer fractions without scaling request counts", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, url, cache, resp_bytes, resp_header_content_encoding)
        SELECT 'native', 1700000000000 + i, '/app.css',
          CASE WHEN i < 8 THEN 'HIT' ELSE 'MISS' END,
          1000, CASE WHEN i < 6 THEN 'gzip' ELSE 'identity' END
        FROM range(12) AS rows(i)`);
      const data = await queryAssetsAggregates({ engine, filters: {} });
      const css = data.asset_type_breakdown[0];
      expect(css).toMatchObject({ asset_type: "JavaScript/CSS", requests: 12, egress_bytes: 12000 });
      expect(css.cache_hit_ratio).toBeCloseTo(8 / 12);
      expect(css.compression_rate).toBe(6 / 12);
      expect(formatPercent(Number(css.cache_hit_ratio))).toBe("66.7%");
      expect(formatPercent(Number(css.compression_rate))).toBe("50.0%");
      expect(data.cache_performance.find((row) => row.cache_status === "HIT")?.requests).toBe(8);
      expect(data.compression_performance.find((row) => row.content_encoding === "gzip")?.requests).toBe(6);
      expect(css.hit_ratio_pct).toBe(66.67);
    } finally {
      await engine.reset();
    }
  });

  it("accepts page filter records and preserves native asset dimensions on the wire", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, url, country, cache, resp_bytes, ttfb, elapsed, resp_header_content_encoding, ttl)
        VALUES ('native', 1700000000000, '/app.css', 'US', 'HIT', 20000, 0.05, 100000, 'gzip', 120),
          ('native', 1700000001000, '/app.css', 'CA', 'MISS', 30000, 0.1, 200000, 'identity', 240)`);
      const all = await queryAssetsAggregates({ engine, filters: {} });
      expect(all.asset_type_breakdown[0]).toMatchObject({
        egress_bytes: 50000, cache_hit_ratio: 0.5, compression_rate: 0.5,
      });
      expect(all.cache_performance.every((row) => row.asset_type === "JavaScript/CSS")).toBe(true);
      expect(all.compression_performance.every((row) => row.asset_type === "JavaScript/CSS")).toBe(true);
      expect(all.summary.avg_ttfb_ms).toBe(75);
      expect(all.large_uncompressed_assets[0]).toMatchObject({ avg_bytes: 30000 });
      expect(all.low_ttl_assets[0]).toMatchObject({ avg_ttl: 240 });
      const filtered = await queryAssetsAggregates({
        engine, filters: { country: { mode: "include", values: ["US"] } },
      });
      expect(filtered.summary.total_requests).toBe(1);
      expect(filtered.asset_type_breakdown[0].egress_bytes).toBe(20000);
    } finally {
      await engine.reset();
    }
  });

  it("classifies extensionless URLs, empty encodings and empty cache values as High-Scale does", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, url, cache, resp_bytes, resp_header_content_encoding, content_type)
        VALUES ('native', 1700000000000, '/login', 'PASS', 100, '', 'text/html'),
          ('native', 1700000001000, '/invite/user', NULL, 100, NULL, NULL),
          ('native', 1700000002000, '/app.js?v=2', 'HIT', 100, 'gzip, br', NULL)`);
      const data = await queryAssetsAggregates({ engine, filters: {} });
      expect(data.asset_type_breakdown).toEqual([
        expect.objectContaining({ asset_type: "API/Dynamic", requests: 3, cache_hit_ratio: expect.closeTo(1 / 3), compression_rate: 0 }),
      ]);
      expect(data.cache_performance.map((row) => row.cache_status).sort()).toEqual(["HIT", "MISS", "PASS"]);
      expect(data.compression_performance.map((row) => [row.content_encoding, row.requests]).sort())
        .toEqual([["br", 1], ["uncompressed", 2]]);
    } finally {
      await engine.reset();
    }
  });

  it("computes summary and breakdowns from dimension rows", () => {
    const summary = computeSummaryFromDims([
      {
        asset_type: "JavaScript",
        cache_status: "HIT",
        content_encoding: "br",
        requests: 900,
        total_bytes: 9_000_000,
        sum_ttfb: 9000,
        sum_origin_ms: 0,
        origin_Count: 0,
      },
      {
        asset_type: "JavaScript",
        cache_status: "MISS",
        content_encoding: "none",
        requests: 100,
        total_bytes: 2_500_000,
        sum_ttfb: 12000,
        sum_origin_ms: 9000,
        origin_Count: 100,
      },
    ]);
    expect(summary.total_requests).toBe(1000);
    expect(summary.overall_hit_ratio).toBe(90);
    expect(summary.asset_type_breakdown).toHaveLength(1);
    expect(summary.compression_performance).toHaveLength(2);
  });

  it("queries assets aggregates, large uncompressed assets and low TTL hotspots by High-Scale's rules", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", url, asset_type, content_type,
        cache_status, content_encoding, resp_body_size, ttfb_ms, origin_time_ms, ttl
      ) VALUES
        ('a1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, '/app.js', 'JavaScript', 'application/javascript', 'HIT', 'br', 15000, 8.0, 0.0, 86400),
        ('a1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, '/vendor-uncompressed.js', 'JavaScript', 'application/javascript', 'MISS', 'none', 45000, 120.0, 95.0, 300),
        ('a1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, '/styles.css', 'CSS', 'text/css', 'MISS', 'identity', 20000, 80.0, 60.0, 120),
        ('a1', 1700000003000, epoch_ms(1700000003000)::TIMESTAMPTZ, '/logo.png', NULL, NULL, 'MISS', NULL, 5000, 10.0, 5.0, 60)`,
    );

    const res = await handleAssetsRequest("/api/assets/aggregates", { engine });
    expect(res.summary.total_requests).toBe(4);
    expect(res.asset_type_breakdown.map((row) => [row.asset_type, row.requests]))
      .toEqual([["JavaScript/CSS", 3], ["Images", 1]]);
    // Compressible URLs served uncompressed (the PNG isn't compressible text).
    expect(res.large_uncompressed_assets.map((row) => row.url).sort()).toEqual(["/styles.css", "/vendor-uncompressed.js"]);
    // Static URLs answered with a MISS.
    expect(res.low_ttl_assets.map((row) => row.url).sort()).toEqual(["/logo.png", "/styles.css", "/vendor-uncompressed.js"]);

    const filtered = await queryAssetsAggregates({
      engine,
      filters: [{ column: "asset_type", operator: "=", value: "Images" }],
    });
    expect(filtered.summary.total_requests).toBe(1);
    expect(emptyAssetsAggregates().summary.total_requests).toBe(0);
    await engine.reset();
  });

  it("enforces >=10KB & strips query suffixes on large uncompressed, <3600s on low TTL, and includes HIT/MISS clusters in ratio", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", url, cache_status, content_encoding, resp_body_size, ttl
      ) VALUES
        ('a2', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, '/tiny.js', 'HIT-CLUSTER', 'none', 4000, 3600),
        ('a2', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, '/vuln.php?url=http://example.com/index1.html', 'MISS-CLUSTER', 'none', 25000, 120),
        ('a2', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, '/big.css?v=1', 'MISS', 'none', 15000, 3600),
        ('a2', 1700000003000, epoch_ms(1700000003000)::TIMESTAMPTZ, '/short.png', 'MISS', 'none', 2000, 300)`,
    );
    const res = await handleAssetsRequest("/api/assets/aggregates", { engine });
    expect(res.large_uncompressed_assets.map((r) => r.url)).toEqual(["/big.css?v=1"]);
    expect(res.low_ttl_assets.map((r) => r.url)).toEqual(["/short.png"]);
    expect(res.summary.overall_hit_ratio).toBeCloseTo(25);
    await engine.reset();
  });
});
