// @vitest-environment node
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";
import {
  computeNetworkHealthScore,
  deduplicateMetroLeaderboard,
  handleNetworkRequest,
  queryNetworkBundle,
  queryNetworkHealth,
} from "./network";

describe("Feature F21 — Network cloud-engine", () => {
  it("returns ISO-aligned ASN timeline buckets with actual native health scores", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, asn, tcp_rtt, status)
        SELECT 'native', 1700000100000 + i, 64500, 10000,
          CASE WHEN i >= 10 THEN 503 ELSE 200 END
        FROM range(12) AS rows(i)`);
      const data = await queryNetworkHealth({
        engine,
        bucket_seconds: 300,
        sections: ["summary", "heatmap", "buckets"],
      });
      expect(data.summary).toMatchObject({ global_health_score: 75, avg_rtt_ms: 10 });
      expect(data.buckets).toEqual(["2023-11-14T22:15:00.000Z"]);
      expect(data.heatmap).toEqual([
        expect.objectContaining({
          asn: 64500,
          label: "AS64500",
          total_reqs: 12,
          buckets: [
            expect.objectContaining({
              bucket: data.buckets[0],
              bucket_idx: 0,
              reqs: 12,
              health_score: 75,
            }),
          ],
        }),
      ]);
    } finally {
      await engine.reset();
    }
  });

  it("reports missing shielding inputs, and joins retained edge/shield requests when present", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, edge, rid, prid, pop, ttfb, country)
        VALUES ('native', 1700000100000, true, 'edge-request', NULL, 'IAD', 0.1, 'US')`);
      const missing = await handleNetworkRequest("/api/network-health", { engine, sections: ["shielding_analysis"] });
      expect(missing).toHaveProperty("shielding_analysis.requires_fields", expect.arrayContaining(["prid"]));
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, edge, rid, prid, pop, ttfb, country)
        VALUES ('native', 1700000100001, false, 'shield-request', 'edge-request', 'LHR', 0.04, 'GB')`);
      const data = await handleNetworkRequest("/api/network-health", {
        engine, sections: ["shielding_analysis"], filters: { country: { mode: "include", values: ["US"] } },
      });
      expect(data).toHaveProperty("shielding_analysis.has_data", true);
      expect(data).toHaveProperty("shielding_analysis.rows", [
        expect.objectContaining({ edge_pop: "IAD", shield_pop: "LHR", requests: 1, p50_ms: 60 }),
      ]);
    } finally {
      await engine.reset();
    }
  });

  it("exposes native populated PoPs and health sections through page wire fields", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, ip, country, city, lat, lon, asn, pop, status, resp_bytes, cache, ttfb, tcp_rtt, proto)
        VALUES ('native', 1700000100000, '192.0.2.1', 'US', 'Denver', 39.74, -104.99, 64500, 'DEN', 200, 20000, 'HIT', 0.05, 10000, 'HTTP/2'),
          ('native', 1700000160000, '192.0.2.2', 'CA', 'Toronto', 43.65, -79.38, 64501, 'YYZ', 503, 10000, 'MISS', 0.1, NULL, 'HTTP/3')`);
      const bundle = await queryNetworkBundle({
        engine, bucket_seconds: 300, filters: { country: { mode: "include", values: ["US"] } },
      });
      expect(bundle.health.leaderboard[0]).toMatchObject({ asn: 64500, total_reqs: 1 });
      expect(bundle.health.summary).toMatchObject({
        worst_asn: { label: "AS64500", score: 100 },
        worst_country: { label: "US", score: 100 },
      });
      expect(bundle.health.cities[0]).toMatchObject({ name: "Denver", lat: 39.74 });
      expect(bundle.health.map_buckets[0]).toMatchObject({ cities: [expect.objectContaining({ reqs: 1 })] });
      expect(bundle.health.heatmap[0]).toMatchObject({ buckets: [expect.objectContaining({ reqs: 1 })] });
      expect(bundle.transport).toMatchObject({ total_requests: 1, tcp_rtt_p50_ms: 10 });
      const pop = await handleNetworkRequest("/api/network/pop-health", { engine });
      expect(pop).toHaveProperty("data", expect.arrayContaining([
        expect.objectContaining({ pop: "DEN", bandwidth_bytes: 20000, cache_hit_rate: 100, p95_ttfb_ms: 50 }),
        expect.objectContaining({ pop: "YYZ", errors: 1, p50_rtt_us: null }),
      ]));
    } finally {
      await engine.reset();
    }
  });

  it("computes composite network health score clamped to [0, 100]", () => {
    expect(computeNetworkHealthScore(10_000, 10_000, 0.0, 0.0)).toBe(100);
    expect(computeNetworkHealthScore(500_000, 10_000, 1.0, 100.0)).toBe(0);
    expect(
      computeNetworkHealthScore(100_000_000, 0, 0, 0, 0),
    ).toBeGreaterThanOrEqual(90);
    expect(
      computeNetworkHealthScore(0, 200_000, 0.2, 100_000, 50),
    ).toBeLessThanOrEqual(10);
  });

  it("deduplicates metro leaderboard by (city, country) with request-weighted averages", () => {
    const deduped = deduplicateMetroLeaderboard([
      {
        city: "New York",
        country: "US",
        reqs: 100,
        health_score: 90,
        rtt_med_us: 20_000,
      },
      {
        city: "New York",
        country: "US",
        reqs: 300,
        health_score: 70,
        rtt_med_us: 40_000,
      },
      {
        city: "London",
        country: "GB",
        reqs: 50,
        health_score: 95,
        rtt_med_us: 15_000,
      },
    ]);
    expect(deduped).toHaveLength(2);
    expect(deduped[0].city).toBe("New York");
    expect(deduped[0].reqs).toBe(400);
    expect(deduped[0].health_score).toBe(75);
    expect(deduped[0].rtt_med_us).toBe(35_000);
  });

  it("queries network bundle, health, quality, and pop health against DuckDB", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO requests (
        scoped_key, ts_ms, "timestamp", status, protocol, geo_country, geo_city,
        client_asn, as_name, fastly_pop, client_rtt_us, client_ploss, client_cwnd,
        client_tcpi_rtt, client_tcpi_rttvar, client_tcpi_delta_retrans, resp_body_size, time_elapsed_usec
      ) VALUES
        ('k1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, 200, 'HTTP/3', 'US', 'New York', 15169, 'Google', 'IAD', 15000, 0.001, 64, 15000, 2000, 0, 50000, 10000),
        ('k1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, 200, 'HTTP/2', 'US', 'New York', 15169, 'Google', 'IAD', 25000, 0.002, 48, 25000, 4000, 1, 60000, 12000),
        ('k1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, 503, 'HTTP/1.1', 'GB', 'London', 3356, 'Level3', 'LHR', 80000, 0.02, 32, 80000, 10000, 2, 10000, 50000)`,
    );

    const bundle = await queryNetworkBundle({ engine });
    expect(bundle.network_health.summary.total_requests).toBe(3);
    expect(bundle.network_health.by_country).toHaveLength(2);
    expect(bundle.network_health.metro_leaderboard).toHaveLength(2);
    expect(
      bundle.network_quality.protocol_distribution.map((p) => p.protocol),
    ).toEqual(["HTTP/3", "HTTP/2", "HTTP/1.1", "HTTP/1.0", "Other"]);
    expect(bundle.pop_health.pops).toHaveLength(2);

    const viaHandler = await handleNetworkRequest("/api/network/bundle", {
      engine,
    });
    expect(viaHandler).toHaveProperty("network_health");
    await engine.reset();
  });
});
