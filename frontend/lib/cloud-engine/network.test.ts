// @vitest-environment node
import { describe, expect, it, vi } from "vitest";

const settings = vi.hoisted(() => ({ off: new Set<string>() }));
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => ({ has: (id: string) => !settings.off.has(id) }) as unknown as Set<string>,
}));
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  ALL_TABLE_FIELDS, createNodeBackend, DuckDBAnalyticalEngine as ParquetEngine, FIELD_DUCKDB_TYPES, type DuckDBReadContext,
} from "./dashboard/duckdb";
import { DuckDBAnalyticalEngine } from "./dashboard/test-fixtures";
import {
  computeNetworkHealthScore,
  deduplicateMetroLeaderboard,
  handleNetworkRequest,
  NETWORK_SCAN_COLUMNS,
  networkScanColumns,
  normalizeProtocolBucket,
  queryNetworkBundle,
  queryNetworkHealth,
} from "./network";
import { resetAsnNamesForTest, setAsnNamesForTest } from "./asn-names";

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

  it("names shielding inputs the log field settings leave off, and joins retained edge/shield requests", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, edge, rid, prid, pop, ttfb, country)
        VALUES ('native', 1700000100000, true, 'edge-request', NULL, 'IAD', 0.1, 'US'),
          ('native', 1700000100001, false, 'shield-request', 'edge-request', 'LHR', 0.04, 'GB')`);
      settings.off = new Set(["prid"]);
      const missing = await handleNetworkRequest("/api/network-health", { engine, sections: ["shielding_analysis"] });
      expect(missing).toHaveProperty("shielding_analysis.requires_fields", ["prid"]);
      settings.off = new Set();
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

  it("joins edge and shield requests across retained Parquet files without exhausting DuckDB memory", async () => {
    const engine = new ParquetEngine();
    const rows = 10_000;
    const rid = (i: number) => i.toString(16).padStart(8, "0").repeat(4);
    const lease = engine.lease([]);
    try {
      for (let file = 0; file < 4; file++) {
        const ids = Array.from({ length: rows }, (_, row) => file * rows + row + 1);
        await lease.ingestFile(`shield-${file}`, `shield-${file}.parquet`, parquetWriteBuffer({ columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: ids.map((i) => new Date(1700000000000 + i)) },
          { name: "edge", type: "BOOLEAN", data: ids.map((i) => i % 5 !== 0) },
          { name: "pop", type: "STRING", data: ids.map((i) => (i % 5 ? "IAD" : "LHR")) },
          { name: "ttfb", type: "DOUBLE", data: ids.map((i) => (i % 5 ? 0.1 : 0.04)) },
          { name: "rid", type: "STRING", data: ids.map(rid) },
          { name: "prid", type: "STRING", data: ids.map((i) => (i % 5 ? null : rid(i - 1))) },
        ] }), undefined, undefined, "request", { deferView: true });
      }
      await lease.publishView();
      // The per-file view used to exhaust this scaled-down limit, as ~1M real rows exhausted the page's 256MB.
      await engine.executeSql("SET memory_limit = '16MB'");
      const data = await handleNetworkRequest("/api/network-health", {
        engine: lease.readContext(), sections: ["shielding_analysis"],
      });
      expect(data).toHaveProperty("shielding_analysis.rows", [
        expect.objectContaining({ edge_pop: "IAD", shield_pop: "LHR", requests: 8_000, p50_ms: 60 }),
      ]);
    } finally {
      lease.release();
      await engine.reset();
    }
  });

  it("reads only the columns a projected /network load fetches", async () => {
    const backend = await createNodeBackend();
    try {
      // Only the projected columns exist, so any other column reference fails to bind.
      const physical = NETWORK_SCAN_COLUMNS.filter((column) => column !== "timestamp" && column !== "ts_ms");
      await backend.query(`CREATE TABLE requests AS SELECT ts_ms, epoch_ms(ts_ms)::TIMESTAMPTZ AS "timestamp",
        ${physical.map((column) => `NULL::${FIELD_DUCKDB_TYPES[column]} AS "${column}"`).join(", ")}
        FROM (VALUES (1700000100000::BIGINT), (1700000100001::BIGINT)) rows(ts_ms)`);
      await backend.query(`UPDATE requests SET edge = ts_ms = 1700000100000, rid = 'r' || ts_ms::VARCHAR,
        prid = CASE WHEN ts_ms = 1700000100001 THEN 'r1700000100000' END, pop = 'IAD', ttfb = 0.1, country = 'US',
        city = 'Ashburn', asn = 64500, tcp_rtt = 10000, status = 200, proto = 'HTTP/2'`);
      const sqls: string[] = [];
      const read = {
        executeSql: async (sql: string) => {
          sqls.push(sql);
          const result = await backend.query(sql) as unknown as { toArray(): Record<string, unknown>[] };
          return result.toArray().map((row) => Object.fromEntries(Object.entries({ ...row })
            .map(([key, value]) => [key, typeof value === "bigint" ? Number(value) : value])));
        },
        getPresentColumns: async () => new Set(ALL_TABLE_FIELDS),
        getCustomColumnTypes: () => new Map(),
      } as unknown as DuckDBReadContext;
      const filters = { country: { mode: "include" as const, values: ["US"] } };
      const health = await handleNetworkRequest("/api/network-health", { engine: read, filters, map_asn: "64500" });
      expect(health).toHaveProperty("summary.total_requests", 2);
      expect(health).toHaveProperty("shielding_analysis.has_data", true);
      expect(await handleNetworkRequest("/api/network/bundle", { engine: read, filters }))
        .toHaveProperty("transport.has_transport_data", true);
      expect(sqls.length).toBeGreaterThan(8);
    } finally {
      await backend.dispose();
    }
  });

  it("runs only the requested sections, so the page's split requests never repeat the core scans", async () => {
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
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, edge, rid, prid, pop, ttfb, country, asn, tcp_rtt, status, proto)
        VALUES ('native', 1700000100000, true, 'edge-request', NULL, 'IAD', 0.1, 'US', 64500, 10000, 200, 'HTTP/2'),
               ('native', 1700000100001, false, 'shield-request', 'edge-request', 'LHR', 0.04, 'GB', 64500, 12000, 200, 'HTTP/2')`);

      const shielding = await handleNetworkRequest("/api/network-health", { engine: counted, sections: ["shielding_analysis"] });
      expect(shielding).toHaveProperty("shielding_analysis.has_data", true);
      expect(shielding).not.toHaveProperty("summary");
      expect(executed).toHaveLength(1);

      executed.length = 0;
      const transport = await handleNetworkRequest("/api/network/bundle", { engine: counted, sections: ["transport"] });
      expect(transport).toHaveProperty("transport.has_transport_data", true);
      expect(transport).toMatchObject({ health: null, quality: null, pop_health: null });
      expect(transport).toHaveProperty("transport.protocol_distribution", [
        { protocol: "HTTP/2", requests: 2, share_pct: 100 },
      ]);
      expect(executed).toHaveLength(1);

      // Each page section shares one aggregation pass per grouping instead of a scan per table.
      executed.length = 0;
      const core = await handleNetworkRequest("/api/network-health", {
        engine: counted, sections: ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets", "cities", "map_buckets"],
      });
      expect(core).toHaveProperty("summary.total_requests", 2);
      // The page reads POP health from its own pop-health request; the core sections don't repeat that scan.
      expect(core).not.toHaveProperty("by_pop");
      expect(executed).toHaveLength(2);

      executed.length = 0;
      const quality = await handleNetworkRequest("/api/network-quality", { engine: counted });
      expect(quality).toHaveProperty("summary.total_requests", 2);
      expect(executed).toHaveLength(2);
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
        worst_country: { label: "United States", score: 100 },
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

  it("emits p95_rtt_us and p99_rtt_us on leaderboard and excludes AS0 and unknown '?' cities", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(
        `INSERT INTO requests (
          scoped_key, ts_ms, "timestamp", status, protocol, geo_country, geo_city,
          lat, lon, client_asn, as_name, fastly_pop, client_rtt_us, client_ploss, client_cwnd,
          client_tcpi_rtt, client_tcpi_rttvar, client_tcpi_delta_retrans, resp_body_size, time_elapsed_usec
        ) VALUES
          ('k1', 1700000000000, epoch_ms(1700000000000)::TIMESTAMPTZ, 200, 'HTTP/3', 'US', 'New York', 40.71, -74.01, 15169, 'Google', 'IAD', 15000, 0.001, 64, 15000, 2000, 0, 50000, 10000),
          ('k1', 1700000001000, epoch_ms(1700000001000)::TIMESTAMPTZ, 200, 'HTTP/2', 'US', 'New York', 40.71, -74.01, 15169, 'Google', 'IAD', 45000, 0.002, 48, 45000, 4000, 1, 60000, 12000),
          ('k1', 1700000002000, epoch_ms(1700000002000)::TIMESTAMPTZ, 200, 'HTTP/2', 'US', '?', 0, 0, 0, 'AS0', 'IAD', 30000, 0.0, 32, 30000, 3000, 0, 20000, 8000)`,
      );

      const bundle = await queryNetworkBundle({ engine });
      expect(bundle.network_health.leaderboard).toHaveLength(1);
      expect(bundle.network_health.leaderboard[0]).toMatchObject({
        asn: 15169,
        p95_rtt_us: expect.any(Number),
        p99_rtt_us: expect.any(Number),
      });
      expect(bundle.network_health.leaderboard[0].p95_rtt_us).toBeGreaterThan(15000);
      expect(bundle.network_health.leaderboard[0].p99_rtt_us).toBeGreaterThanOrEqual(
        Number(bundle.network_health.leaderboard[0].p95_rtt_us),
      );
      expect(bundle.network_health.by_asn.some((r) => r.asn === 0)).toBe(false);
      expect(bundle.network_health.heatmap.some((r) => r.asn === 0)).toBe(false);
      expect(bundle.network_quality.by_asn.some((r) => r.asn === 0)).toBe(false);
      expect(bundle.network_health.metro_leaderboard.some((r) => r.city === "?")).toBe(false);
      expect(bundle.network_health.cities.some((c) => c.name === "?")).toBe(false);
    } finally {
      await engine.reset();
    }
  });

  it("emits positive metro_code on map_buckets cities and handles absent or non-positive metro columns", async () => {
    expect(networkScanColumns("/api/network-health", { sections: ["buckets", "cities", "map_buckets"] })).toContain("metro");
    expect(networkScanColumns("/api/network-health", { sections: ["summary", "cities", "map_buckets"] })).toContain("metro");

    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, country, city, lat, lon, metro, tcp_rtt, status)
        VALUES
          ('native', 1700000100000, 'US', 'Denver', 39.74, -104.99, 751, 10000, 200),
          ('native', 1700000105000, 'US', 'Ashburn', 39.04, -77.49, 0, 12000, 200),
          ('native', 1700000110000, 'GB', 'London', 51.51, -0.13, NULL, 15000, 200)`);
      const health = await queryNetworkHealth({
        engine,
        bucket_seconds: 300,
        sections: ["buckets", "cities", "map_buckets"],
      });
      const bucket0 = health.map_buckets[0] as { cities: Record<string, unknown>[] };
      const byCity = new Map(
        bucket0.cities.map((c) => [
          health.cities[c.city_idx as number].name,
          c.metro_code,
        ]),
      );
      expect(byCity.get("Denver")).toBe(751);
      expect(byCity.get("Ashburn")).toBeNull();
      expect(byCity.get("London")).toBeNull();
    } finally {
      await engine.reset();
    }

    const parquetEngine = new ParquetEngine();
    const lease = parquetEngine.lease([]);
    try {
      await lease.ingestFile("no-metro", "no-metro.parquet", parquetWriteBuffer({
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: [new Date(1700000100000)] },
          { name: "country", type: "STRING", data: ["US"] },
          { name: "city", type: "STRING", data: ["Seattle"] },
          { name: "lat", type: "DOUBLE", data: [47.61] },
          { name: "lon", type: "DOUBLE", data: [-122.33] },
          { name: "status", type: "INT64", data: [BigInt(200)] },
        ],
      }));
      const health = await queryNetworkHealth({
        engine: lease.readContext(),
        bucket_seconds: 300,
        sections: ["buckets", "cities", "map_buckets"],
      });
      const bucket0 = health.map_buckets[0] as { cities: Record<string, unknown>[] };
      expect(bucket0.cities[0]).toMatchObject({ country: "US", metro_code: null });
    } finally {
      lease.release();
      await parquetEngine.reset();
    }
  });

  it("formats ASN names and full country/city labels while keeping raw filter values", async () => {
    const engine = new DuckDBAnalyticalEngine();
    setAsnNamesForTest([[64500, "Example Fiber, US"]]);
    try {
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, ip, country, city, lat, lon, asn, pop, status, resp_bytes, cache, ttfb, tcp_rtt, proto)
        VALUES ('native', 1700000100000, '192.0.2.1', 'US', 'council bluffs', 41.26, -95.86, 64500, 'OMA', 200, 20000, 'HIT', 0.05, 10000, 'HTTP/2')`);
      const bundle = await queryNetworkBundle({ engine, bucket_seconds: 300 });
      expect(bundle.health.summary).toMatchObject({
        worst_asn: { label: "Example Fiber, US (64500)" },
        worst_country: { label: "United States" },
      });
      expect(bundle.health.leaderboard[0]).toMatchObject({
        asn: 64500,
        as_name: "Example Fiber, US (64500)",
        label: "Example Fiber, US (64500)",
      });
      expect(bundle.health.heatmap[0]).toMatchObject({
        asn: 64500,
        label: "Example Fiber, US (64500)",
      });
      expect(bundle.health.metro_leaderboard[0]).toMatchObject({
        city: "Council Bluffs, United States",
        raw_city: "council bluffs",
        country: "US",
      });
      expect(bundle.quality).toMatchObject({
        by_country: [expect.objectContaining({
          value: "US",
          label: "United States",
        })],
        by_asn: [expect.objectContaining({
          asn: 64500,
          value: "64500",
          label: "Example Fiber, US (64500)",
        })],
      });
    } finally {
      resetAsnNamesForTest();
      await engine.reset();
    }
  });

  it("normalizes short protocol strings, populates retrans_rate_pct, numeric bandwidth_bytes, 1% worst floor, and leaderboard trend", async () => {
    expect([
      normalizeProtocolBucket("1.1"),
      normalizeProtocolBucket("1.0"),
      normalizeProtocolBucket("2"),
      normalizeProtocolBucket("2.0"),
      normalizeProtocolBucket("3"),
      normalizeProtocolBucket("3.0"),
    ]).toEqual(["HTTP/1.1", "HTTP/1.0", "HTTP/2", "HTTP/2", "HTTP/3", "HTTP/3"]);

    const engine = new DuckDBAnalyticalEngine();
    try {
      // 105 rows total over 2 hourly buckets:
      // - 102 in US / AS64500 (fast/healthy, 2 with retrans > 0, HTTP/1.1 + "1.1")
      // - 1 noisy outlier in KR / AS64599 (< 1% of 105 = 1.05, terrible RTT/loss)
      // - 2 in GB / AS64501 (> 1% of 105, moderately worse RTT)
      const inserts: string[] = [];
      for (let i = 0; i < 52; i++) {
        const retrans = i === 0 ? 1 : 0;
        const proto = i % 2 === 0 ? "1.1" : "HTTP/1.1";
        inserts.push(`('native', 1700000100000, 'US', 'Chicago', 41.88, -87.63, 64500, 'ORD', 200, 1000, 'HIT', 0.02, 20000, 0.0, ${retrans}, '${proto}')`);
      }
      for (let i = 0; i < 50; i++) {
        const retrans = i === 0 ? 2 : 0;
        inserts.push(`('native', 1700003700000, 'US', 'Chicago', 41.88, -87.63, 64500, 'ORD', 200, 1000, 'HIT', 0.02, 20000, 0.0, ${retrans}, '2')`);
      }
      for (let i = 0; i < 2; i++) {
        inserts.push(`('native', 1700003700000, 'GB', 'London', 51.5, -0.1, 64501, 'LHR', 200, 500, 'MISS', 0.08, 60000, 0.02, 0, '3.0')`);
      }
      inserts.push(`('native', 1700003700000, 'KR', 'Seoul', 37.5, 126.9, 64599, 'ICN', 503, 500, 'MISS', 0.5, 300000, 0.2, 0, '1.0')`);
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, country, city, lat, lon, asn, pop, status, resp_bytes, cache, ttfb, tcp_rtt, ploss, retrans, proto)
        VALUES ${inserts.join(", ")}`);

      const bundle = await queryNetworkBundle({ engine, bucket_seconds: 3600 });
      const tr = bundle.transport as Record<string, unknown>;
      expect(typeof tr.bandwidth_bytes).toBe("number");
      expect(tr.retrans_rate_pct).toBeCloseTo((2 * 100) / 105, 1);
      const protoMap = Object.fromEntries(
        bundle.transport.protocol_distribution.map((p: { protocol: string; requests: number }) => [p.protocol, p.requests]),
      );
      expect(protoMap["HTTP/1.1"]).toBe(52);
      expect(protoMap["HTTP/2"]).toBe(50);
      expect(protoMap["HTTP/3"]).toBe(2);
      expect(protoMap["HTTP/1.0"]).toBe(1);

      expect(bundle.health.summary).toMatchObject({
        worst_asn: { label: "AS64501" },
        worst_country: { label: "United Kingdom" },
      });
      const topLead = bundle.health.leaderboard.find((r) => r.asn === 64500) as Record<string, unknown>;
      expect(topLead.health_score_1h_ago).toEqual(expect.any(Number));
      expect(topLead.health_score_1w_ago).toEqual(expect.any(Number));
      expect(topLead.trend).toBe("stable");
    } finally {
      await engine.reset();
    }
  });

  it("unifies networkScanColumns across all /api/network* endpoints on NETWORK_SCAN_COLUMNS plus dynamic filters", () => {
    const canonical = [...NETWORK_SCAN_COLUMNS].toSorted();
    expect(networkScanColumns("/api/network-health", { sections: ["summary"] })?.toSorted()).toEqual(canonical);
    expect(networkScanColumns("/api/network-health", { sections: ["buckets", "cities", "map_buckets"] })?.toSorted()).toEqual(canonical);
    expect(networkScanColumns("/api/network-quality", { quality_parts: ["throughput"] })?.toSorted()).toEqual(canonical);
    expect(networkScanColumns("/api/network/pop-health", {})?.toSorted()).toEqual(canonical);
    expect(networkScanColumns("/api/network/bundle", { sections: ["transport"] })?.toSorted()).toEqual(canonical);
    expect(networkScanColumns("/api/network/bundle", {
      sections: ["transport"],
      filters: { status: { mode: "include", values: ["200"] }, host: { mode: "include", values: ["a.example"] } },
    })?.toSorted()).toEqual([...NETWORK_SCAN_COLUMNS, "host"].toSorted());
    expect(networkScanColumns("/api/network/bundle", { filters: "invalid-json" })).toBeNull();
  });
});
