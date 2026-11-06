// @vitest-environment node
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";
import {
  emptyValueSummary,
  handleValueRequest,
  queryValueSummary,
} from "./value";

describe("Feature F22 — Fastly Value cloud-engine", () => {
  it("returns 7-section empty value summary when no rows exist", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const res = await queryValueSummary({ engine });
    expect(res).toEqual(emptyValueSummary());
    await engine.reset();
  });

  it("computes offload, cache acceleration factor, and IO savings from DuckDB", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", status, cache_status, resp_body_size,
        ttfb_ms, geo_country, fastly_pop, client_asn, tls_version, protocol,
        content_type, content_encoding, url, fastly_is_shield, bot_category, bot_verified
      ) VALUES
        ('v1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, 200, 'HIT', 100000, 10.0, 'US', 'IAD', 15169, 'TLSv1.3', 'HTTP/3', 'image/webp', 'br', '/hero.webp?ifmt=webp', TRUE, NULL, FALSE),
        ('v1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, 200, 'HIT', 80000, 10.0, 'US', 'IAD', 15169, 'TLSv1.3', 'HTTP/2', 'image/avif', 'br', '/banner.avif', FALSE, 'search_engine', TRUE),
        ('v1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, 200, 'MISS', 120000, 50.0, 'GB', 'LHR', 3356, 'TLSv1.2', 'HTTP/1.1', 'image/jpeg', 'identity', '/photo.jpg', FALSE, NULL, FALSE)`,
    );

    const summary = await handleValueRequest("/api/value/summary", { engine });
    expect(summary.overview.total_requests).toBe(3);
    expect(summary.caching.cache_hits).toBe(2);
    expect(summary.caching.cache_misses).toBe(1);
    expect(summary.caching.origin_offload_pct).toBeCloseTo(66.67, 1);
    expect(summary.caching.cache_acceleration_factor).toBe(5);
    expect(summary.caching.bandwidth_saved_bytes).toBe(180000);
    expect(summary.io.total_images).toBe(3);
    expect(summary.io.modern_format_images).toBe(2);
    expect(summary.io.unoptimized_large_images).toBe(1);
    await engine.reset();
  });
});
