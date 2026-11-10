// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine, getDuckDBEngine } from "./dashboard/test-fixtures";
import { cloudRumAnalytics } from "./rum-wire";
import {
  classifyCwvRating,
  emptyRumAnalytics,
  handleRumRequest,
  queryRumAnalytics,
  queryRumLiveEvents,
  shapeRumEnvironments,
} from "./rum";

describe("Feature F23 — RUM & Web Vitals cloud-engine", () => {
  it("filters RUM-only shared, vital-only and error-only fields against their actual tables", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, browser_name)
        VALUES ('v', 1700000000000, 'LCP', 1000, 'Chrome'),
          ('v', 1700000001000, 'CLS', 0.1, 'Safari')`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, error_type, error_message, browser_name)
        VALUES ('e', 1700000000000, 'TypeError', 'Synthetic failure', 'Chrome')`);
      for (const [field, mode, value, vitals, errors] of [
        ["browser_name", "include", "Chrome", 1, 1],
        ["browser", "include", "Chrome", 1, 1],
        ["metric_name", "include", "LCP", 1, 0],
        ["metric_value", "include", "1000.0", 1, 0],
        ["error_type", "include", "TypeError", 0, 1],
        ["metric_name", "exclude", "LCP", 1, 1],
      ] as const) {
        const params = { engine, filters: { [field]: { mode, values: [value] } } };
        const data = await queryRumAnalytics(params);
        expect(data.summary.total_vitals_events, field).toBe(vitals);
        expect(data.summary.total_errors, field).toBe(errors);
        expect((await queryRumLiveEvents(params)).events, field).toHaveLength(vitals + errors);
      }
      const bounded = { engine, start_time: 1700000000000, end_time: 1700000001000 };
      expect((await queryRumAnalytics(bounded)).summary)
        .toMatchObject({ total_vitals_events: 1, total_errors: 1 });
      expect((await queryRumLiveEvents(bounded)).events).toHaveLength(2);
    } finally {
      await engine.reset();
    }
  });

  it("rejects active fields absent from RUM instead of ignoring them or compiling request columns", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      for (const field of ["unknown_field", "host"]) {
        const params = { engine, filters: { [field]: { mode: "exclude" as const, values: ["offline-value"] } } };
        await expect(queryRumAnalytics(params)).rejects.toThrow("Unsupported RUM filter");
        await expect(queryRumLiveEvents(params)).rejects.toThrow("Unsupported RUM filter");
      }
    } finally {
      await engine.reset();
    }
  });

  it("returns CWV wire distributions as percentages for two LCP beacons", async () => {
    const engine = getDuckDBEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value)
        VALUES ('rum', 1700000000000, 'LCP', 2000),
          ('rum', 1700000001000, 'LCP', 4000)`);
      const data = await cloudRumAnalytics({});
      expect(data.beacon_count).toBe(2);
      expect(data.vitals.lcp).toEqual({
        p75: 3.5,
        distribution: { good: 50, needs_improvement: 50, poor: 0 },
      });
      expect(data.vitals.cls).toEqual({
        p75: null,
        distribution: { good: 0, needs_improvement: 0, poor: 0 },
      });
    } finally {
      await engine.reset();
    }
  });

  it("keeps CWV wire distributions zero and percentiles null with no beacons", async () => {
    const engine = getDuckDBEngine();
    try {
      const data = await cloudRumAnalytics({});
      expect(data.no_data).toBe(true);
      expect(data.beacon_count).toBe(0);
      for (const name of ["lcp", "cls", "inp"] as const) {
        expect(data.vitals[name]).toEqual({
          p75: null,
          distribution: { good: 0, needs_improvement: 0, poor: 0 },
        });
      }
    } finally {
      await engine.reset();
    }
  });

  it("applies JSON page filters to analytics and live events", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, country, page_url)
        VALUES ('rum', 1700000000000, 'LCP', 2000, 'US', '/home'),
          ('rum', 1700000001000, 'LCP', 4000, 'CA', '/other')`);
      const params = {
        engine, filters: JSON.stringify({ country: { mode: "include", values: ["US"] } }),
      };
      expect((await queryRumAnalytics(params)).metrics.LCP.count).toBe(1);
      expect((await queryRumLiveEvents(params)).events).toHaveLength(1);
    } finally {
      await engine.reset();
    }
  });

  it("returns emptyRumAnalytics with no_data: true when tables are empty", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const res = await queryRumAnalytics({ engine });
    expect(res.no_data).toBe(true);
    expect(res).toEqual(emptyRumAnalytics());
    await engine.reset();
  });

  it("classifies CWV boundary ratings accurately", () => {
    expect(classifyCwvRating("LCP", 2500)).toBe("good");
    expect(classifyCwvRating("LCP", 2501)).toBe("needs-improvement");
    expect(classifyCwvRating("LCP", 4001)).toBe("poor");
    expect(classifyCwvRating("CLS", 0.1)).toBe("good");
    expect(classifyCwvRating("CLS", 0.26)).toBe("poor");
    expect(classifyCwvRating("INP", 200)).toBe("good");
    expect(classifyCwvRating("INP", 501)).toBe("poor");
  });

  it("shapes RUM environment distributions with percentage shares", () => {
    const shaped = shapeRumEnvironments({
      browsers: [
        { name: "Chrome", count: 75 },
        { name: "Safari", count: 25 },
      ],
      devices: [["desktop", 100]],
    });
    expect(shaped.browsers[0].pct).toBe(75);
    expect(shaped.browsers[1].pct).toBe(25);
    expect(shaped.devices[0].pct).toBe(100);
  });

  it("aggregates a day of per-period RUM files within the engine memory limit", async () => {
    // Cloud exposes client_vitals as one UNION ALL branch per parquet file, and
    // every branch pays for each count(DISTINCT …) hash table: a few hundred
    // files × the analytics distinct counts exhausted the 256 MB limit on ~10k rows.
    const engine = new DuckDBAnalyticalEngine();
    const base = 1700000000000;
    const files = Array.from({ length: 500 }, (_, index) => {
      const columnData = [
        { name: "ts_ms", type: "INT64" as const, data: Array.from({ length: 20 }, (_, row) => BigInt(base + index * 60000 + row * 1000)) },
        { name: "req_id", type: "STRING" as const, data: Array.from({ length: 20 }, (_, row) => `req-${index}-${row}`) },
        { name: "metric_name", type: "STRING" as const, data: Array.from({ length: 20 }, (_, row) => row % 2 ? "LCP" : "CLS") },
        { name: "metric_value", type: "DOUBLE" as const, data: Array.from({ length: 20 }, (_, row) => row % 2 ? 2000 : 0.05) },
        { name: "pathname", type: "STRING" as const, data: Array.from({ length: 20 }, () => "/home") },
      ];
      const bytes = parquetWriteBuffer({ columnData, codec: "SNAPPY" });
      return { scopedKey: `rum:period-${index}`, key: `period-${index}.parquet`, bytes };
    });
    try {
      await engine.syncRumFiles(files, []);
      const data = await cloudRumAnalytics({ engine });
      expect(data.beacon_count).toBe(10000);
      expect(data.trends.timestamps.length).toBeGreaterThan(1);
    } finally {
      await engine.reset();
    }
  }, 90_000);

  it("queries RUM analytics, live events, and beacon health from DuckDB", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql(
      `INSERT INTO client_vitals (
        scoped_key, "timestamp", ts_ms, metric_name, metric_value, page_url,
        browser_name, os_name, device_type, connection_type, country, session_id
      ) VALUES
        ('r1', epoch_ms(1700000000000)::TIMESTAMPTZ, 1700000000000, 'LCP', 1800.0, '/home', 'Chrome', 'macOS', 'desktop', '4g', 'US', 's1'),
        ('r1', epoch_ms(1700000001000)::TIMESTAMPTZ, 1700000001000, 'LCP', 2200.0, '/home', 'Chrome', 'macOS', 'desktop', '4g', 'US', 's2'),
        ('r1', epoch_ms(1700000002000)::TIMESTAMPTZ, 1700000002000, 'LCP', 4500.0, '/home', 'Safari', 'iOS', 'mobile', '4g', 'US', 's3'),
        ('r1', epoch_ms(1700000003000)::TIMESTAMPTZ, 1700000003000, 'CLS', 0.05, '/only-one', 'Chrome', 'macOS', 'desktop', '4g', 'US', 's1')`,
    );
    await engine.executeSql(
      `INSERT INTO client_errors (
        scoped_key, "timestamp", ts_ms, error_message, filename, page_url, session_id
      ) VALUES
        ('e1', epoch_ms(1700000004000)::TIMESTAMPTZ, 1700000004000, 'TypeError: null', 'app.js', '/home', 's3')`,
    );

    const analytics = await handleRumRequest(
      "/api/services/svc1/rum/analytics",
      { engine },
    );
    expect(analytics).toHaveProperty("no_data", false);
    if ("metrics" in analytics) {
      expect(analytics.metrics.LCP.count).toBe(3);
      expect(analytics.metrics.LCP.good_pct).toBeCloseTo(66.67, 1);
      expect(analytics.metrics.LCP.poor_pct).toBeCloseTo(33.33, 1);
      // /home has 3 samples (>= 3), /only-one has 1 sample (< 3)
      expect(analytics.worst_pages).toHaveLength(1);
      expect(analytics.worst_pages[0].page_url).toBe("/home");
      expect(analytics.top_errors).toHaveLength(1);
    }

    const live = await handleRumRequest("/api/services/svc1/rum/live-events", {
      engine,
    });
    expect(live).toHaveProperty("events");
    await engine.reset();
  });
});
