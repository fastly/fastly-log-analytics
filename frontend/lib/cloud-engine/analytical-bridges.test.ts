// @vitest-environment node
import { beforeEach, describe, expect, it } from "vitest";
import { getDuckDBEngine } from "./dashboard/test-fixtures";
import { CLOUD_INSIGHT_DETECTORS, queryCloudCacheCollapse, queryCloudInsights } from "./insight-scans";
import { cloudRumAnalytics, cloudRumLiveEvents } from "./rum-wire";

const engine = getDuckDBEngine();
const end = "2026-01-03T00:00:00.000Z";
const signal = new AbortController().signal;
beforeEach(async () => { await engine.reset(); });

describe("Cloud analytical wire bridges", () => {
  it("uses actual request aggregates and reports unimplemented detectors instead of clean cards", async () => {
    const endMs = Date.parse(end);
    await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, ip, status, cache)
      SELECT 'baseline', epoch_ms(${endMs - 26 * 3600000}), ${endMs - 26 * 3600000},
        '/login', '192.0.2.1', 200, 'HIT' FROM range(100)`);
    await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, ip, status, cache)
      SELECT 'current', epoch_ms(${endMs - 1800000}), ${endMs - 1800000},
        '/login', '192.0.2.1', 401, 'MISS' FROM range(30)`);
    const body = { window_size_hrs: 1, baseline_hours: 48, clamp_end: end };
    const options = { signal, maskIps: false, filters: [] };
    const result = await queryCloudInsights(body, options);
    expect(result.unsupported_detectors.length).toBeGreaterThan(0);
    expect(result.insights.every((card) => CLOUD_INSIGHT_DETECTORS.has(card.id))).toBe(true);
    expect(result.insights.find((card) => card.id === "cache_collapse")).toMatchObject({
      severity: "critical",
      items: [{ label: "/login", current_val: 0, baseline_val: 100 }],
    });
    const detail = await queryCloudCacheCollapse({ ...body, url: "/login" }, options);
    expect(detail).toMatchObject({ window_hit_rate: 0, baseline_hit_rate: 100, has_data: true });
    expect(detail.recent_misses).toHaveLength(30);
  });

  it("returns measured RUM wire fields and limits live events to the requested range", async () => {
    const at = Date.parse(end) - 1000;
    await engine.executeSql(`INSERT INTO client_vitals (
      scoped_key, timestamp, ts_ms, metric_name, metric_value, page_url, browser_name, os_name, session_id, request_event_id
    ) SELECT 'v', epoch_ms(${at}), ${at}, 'LCP', 1800 + range * 200, '/home', 'Chrome', 'Test OS', 's', 'r' || range
      FROM range(3)`);
    await engine.executeSql(`INSERT INTO client_errors (
      scoped_key, timestamp, ts_ms, error_message, filename, page_url, session_id, lineno, colno
    ) VALUES ('e', epoch_ms(${at}), ${at}, 'Failure', 'app.js', '/home', 's', 12, 4),
      ('old', epoch_ms(1000), 1000, 'Outside range', 'old.js', '/old', 's', 1, 1)`);
    const params = { start_time: "2026-01-02T00:00:00Z", end_time: end, signal };
    const result = await cloudRumAnalytics(params);
    expect(result).toMatchObject({
      beacon_count: 4, pageview_count: 3, error_count: 1,
      environments: { browsers: { Chrome: 3 } },
      errors: [{ message: "Failure", file: "app.js", line: 12, col: 4, count: 1 }],
      vitals: { lcp: { p75: 2.1 } },
      worst_pages: [{ path: "/home", avg_load_time: null, lcp_p75: 2.1, error_rate: 25 }],
    });
    expect(result.trends.pageviews).toEqual([3]);
    await engine.executeSql(`INSERT INTO client_vitals (
      scoped_key, timestamp, ts_ms, metric_name, metric_value, page_url, request_event_id
    ) VALUES ('v', epoch_ms(${at}), ${at}, 'pageLoadTime', 3500, '/home', 'r0')`);
    const navigation = await cloudRumAnalytics(params);
    expect(navigation.beacon_count).toBe(4);
    expect(navigation.worst_pages[0].avg_load_time).toBe(3.5);
    const live = await cloudRumLiveEvents(params);
    expect(live).toHaveLength(5);
    expect(live.filter((row) => row.os === "Test OS")).toHaveLength(3);
    expect(live.some((row) => row.path === "/old")).toBe(false);
  });
});
