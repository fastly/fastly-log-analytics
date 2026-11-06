// @vitest-environment node
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";
import {
  computeNetworkHealthScore,
  deduplicateMetroLeaderboard,
  handleNetworkRequest,
  queryNetworkBundle,
} from "./network";

describe("Feature F21 — Network cloud-engine", () => {
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
