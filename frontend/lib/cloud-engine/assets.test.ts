// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  computeSummaryFromDims,
  emptyAssetsAggregates,
  handleAssetsRequest,
  queryAssetsAggregates,
} from "./assets";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";

describe("Feature F22 — Assets & Shielding cloud-engine", () => {
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

  it("queries assets aggregates, large uncompressed assets (>10KB), and low TTL assets (<3600s)", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", url, asset_type, content_type,
        cache_status, content_encoding, resp_body_size, ttfb_ms, origin_time_ms, ttl
      ) VALUES
        ('a1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, '/app.js', 'JavaScript', 'application/javascript', 'HIT', 'br', 15000, 8.0, 0.0, 86400),
        ('a1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, '/vendor-uncompressed.js', 'JavaScript', 'application/javascript', 'MISS', 'none', 45000, 120.0, 95.0, 300),
        ('a1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, '/styles.css', 'CSS', 'text/css', 'MISS', 'identity', 20000, 80.0, 60.0, 120)`,
    );

    const res = await handleAssetsRequest("/api/assets/aggregates", { engine });
    expect(res.summary.total_requests).toBe(3);
    expect(res.large_uncompressed_assets).toHaveLength(2);
    expect(res.low_ttl_assets).toHaveLength(2);

    const filtered = await queryAssetsAggregates({
      engine,
      filters: [{ column: "asset_type", operator: "=", value: "CSS" }],
    });
    expect(filtered.summary.total_requests).toBe(1);
    expect(emptyAssetsAggregates().summary.total_requests).toBe(0);
    await engine.reset();
  });
});
