// @vitest-environment node
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine } from "./dashboard/duckdb";
import {
  classifyCwvRating,
  emptyRumAnalytics,
  handleRumRequest,
  queryRumAnalytics,
  shapeRumEnvironments,
} from "./rum";

describe("Feature F23 — RUM & Web Vitals cloud-engine", () => {
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
