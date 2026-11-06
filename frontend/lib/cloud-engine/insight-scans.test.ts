// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { CLOUD_INSIGHT_DETECTORS, queryCloudCacheCollapse, queryCloudInsights } from "./insight-scans";
import { handleInsightAvailability, INSIGHT_DEFINITIONS } from "./insights";

const engine = getDuckDBEngine();
const end = "2026-01-03T00:00:00.000Z";
const endMs = Date.parse(end);
const signal = new AbortController().signal;
const body = { service_id: "example", window_size_hrs: 1, baseline_hours: 48, clamp_end: end };
const options = { signal, maskIps: false, filters: [] };

beforeEach(async () => {
  await engine.reset();
  engine.setPresentColumnsOverride(null);
});

async function seedRichHistory() {
  const baseline = endMs - 26 * 3_600_000;
  const current = endMs - 1800_000;
  await engine.executeSql(`INSERT INTO requests
    (scoped_key, timestamp, ts_ms, url, ip, status, cache, elapsed, city, region, country, asn, p_type, pop, ua, referer, method, transport)
    SELECT 'baseline', epoch_ms(${baseline}), ${baseline},
      '/login', '192.0.2.1', 200, 'HIT', 100000, 'Example City', 'CA', 'US', 64500,
      '', 'IAD', 'baseline-browser', '', 'GET', 'quic' FROM range(300)`);
  await engine.executeSql(`INSERT INTO requests
    (scoped_key, timestamp, ts_ms, url, ip, status, cache, elapsed, city, region, country, asn, p_type, pop, ua, referer, method, transport)
    SELECT 'current', epoch_ms(${current}), ${current},
      '/login', '192.0.2.2', CASE WHEN range < 100 THEN 503 ELSE 403 END, 'MISS',
      CASE WHEN range % 5 = 0 THEN 3000000 ELSE 100000 END,
      'New City', 'ON', 'CA', 64501, 'hosting', 'IAD', 'new-agent', 'https://example.test/spam', 'POST', 'tcp'
      FROM range(220)`);
  await engine.executeSql(`INSERT INTO requests
    (scoped_key, timestamp, ts_ms, url, ip, status, cache, elapsed, city, region, country, pop)
    SELECT 'baseline', epoch_ms(${baseline}), ${baseline}, '/pass', '192.0.2.1',
      200, 'HIT', 100000, 'Example City', 'CA', 'US', 'IAD' FROM range(100)`);
  await engine.executeSql(`INSERT INTO requests
    (scoped_key, timestamp, ts_ms, url, ip, status, cache, elapsed, city, region, country, pop)
    SELECT 'current', epoch_ms(${current}), ${current}, '/pass', '192.0.2.3',
      503, 'PASS', 3000000, 'Example City', 'CA', 'US', 'IAD' FROM range(50)`);
  await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, ip, status)
    SELECT 'current', epoch_ms(${current}), ${current}, '/missing/' || range,
      '192.0.2.4', 404 FROM range(30)`);
}

describe("local Parquet insight scans", () => {
  it("executes every advertised Cloud algorithm against measured request data", async () => {
    await seedRichHistory();
    const result = await queryCloudInsights(body, options);
    expect(result.insights.map((card) => card.id).sort()).toEqual([...CLOUD_INSIGHT_DETECTORS].sort());
    for (const card of result.insights) {
      expect(card.items.length, `${card.id} should detect the seeded anomaly`).toBeGreaterThan(0);
      expect(card.severity).not.toBe("clean");
    }
    expect(result.window_total_requests).toBe(300);
    expect(result.insights.find((card) => card.id === "cache_collapse")?.items[0])
      .toMatchObject({ label: "/login", current_val: 0, baseline_val: 100 });
    expect(result.insights.find((card) => card.id === "latency_regression")?.items[0].current_val).toBe(3000);
    expect(result.insights.find((card) => card.id === "credential_enumeration")?.items[0])
      .toMatchObject({ label: "192.0.2.2", current_val: 120 });
    expect(result.unsupported_detectors).toHaveLength(29);
    expect(result.unsupported_detectors.every((d) => d.reason === "cloud_detector_unsupported")).toBe(true);
    const availability = handleInsightAvailability("example", await engine.getPresentColumns());
    expect(availability.insights.filter((d) => d.available).map((d) => d.id).sort())
      .toEqual(result.insights.map((card) => card.id).sort());
  });

  it("scopes every detector by filters and does not treat non-auth 403s as credential attacks", async () => {
    await seedRichHistory();
    await engine.executeSql(`UPDATE requests SET url = '/assets/protected.css'
      WHERE url = '/login' AND status = 403`);
    const result = await queryCloudInsights(body, {
      ...options, filters: [{ field: "country", mode: "include", value: "US" }],
    });
    expect(result.window_total_requests).toBe(50);
    expect(result.insights.find((card) => card.id === "credential_enumeration")?.items).toEqual([]);
    expect(result.insights.find((card) => card.id === "new_country_traffic")?.items).toEqual([]);
    const allCountries = await queryCloudInsights(body, options);
    expect(allCountries.insights.find((card) => card.id === "credential_enumeration")?.severity).toBe("clean");
  });

  it("reports absent schema and insufficient history without inventing clean findings", async () => {
    const empty = await queryCloudInsights(body, options);
    expect(empty.insights).toEqual([]);
    expect(empty.unavailable_detectors).toHaveLength(INSIGHT_DEFINITIONS.length);
    const current = endMs - 1800_000;
    await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, cache)
      VALUES ('current', epoch_ms(${current}), ${current}, '/only-current', 'MISS')`);
    const execute = vi.spyOn(engine, "executeSql");
    const recent = await queryCloudInsights(body, options);
    expect(recent.insights.length).toBeGreaterThan(0);
    expect(recent.insights.every((card) => card.severity === "info" && card.summary.includes("Not evaluated"))).toBe(true);
    expect(execute.mock.calls).toHaveLength(1);
    execute.mockRestore();
  });

  it("masks all IP evidence and queries real cache-collapse detail", async () => {
    await seedRichHistory();
    const masked = await queryCloudInsights(body, { ...options, maskIps: true });
    expect(masked.insights.find((card) => card.id === "credential_enumeration")?.items[0].label).toBe("0.0.0.0");
    expect(JSON.stringify(masked)).not.toContain("192.0.2.");
    const detail = await queryCloudCacheCollapse({ ...body, url: "/login" }, { signal, maskIps: true });
    expect(detail).toMatchObject({ window_hit_rate: 0, baseline_hit_rate: 100, has_data: true });
    expect(detail.recent_misses).toHaveLength(50);
    expect(detail.recent_misses.every((row) => row.ip === "0.0.0.0")).toBe(true);
  });

  it("runs current-window detectors without a baseline and keeps concurrent scopes isolated", async () => {
    await seedRichHistory();
    const [canada, unitedStates] = await Promise.all([
      queryCloudInsights(body, { ...options, filters: [{ field: "country", mode: "include", value: "CA" }] }),
      queryCloudInsights(body, { ...options, filters: [{ field: "country", mode: "include", value: "US" }] }),
    ]);
    expect(canada.window_total_requests).toBe(220);
    expect(unitedStates.window_total_requests).toBe(50);
    expect(canada.insights.find((card) => card.id === "tail_latency")?.items[0].label).toBe("/login");
    expect(canada.insights.find((card) => card.id === "cache_collapse")?.summary).toContain("Not evaluated");
    expect(unitedStates.insights.find((card) => card.id === "cacheability_regression")?.items[0].label).toBe("/pass");
    expect(unitedStates.insights.find((card) => card.id === "tail_latency")?.items).toEqual([]);
  });

  it("honors fractional windows and the selected short baseline", async () => {
    const baseline = endMs - 75 * 60_000;
    const current = endMs - 5 * 60_000;
    await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, cache)
      SELECT 'baseline', epoch_ms(${baseline}), ${baseline}, '/short', 'HIT' FROM range(100)`);
    await engine.executeSql(`INSERT INTO requests (scoped_key, timestamp, ts_ms, url, cache)
      SELECT 'current', epoch_ms(${current}), ${current}, '/short', 'MISS' FROM range(20)`);
    const result = await queryCloudInsights({ ...body, window_size_hrs: 0.25, baseline_hours: 1 }, options);
    expect(result.window_hours).toBe(0.25);
    expect(result.window_start).toBe(new Date(endMs - 15 * 60_000).toISOString());
    expect(result.insights.find((card) => card.id === "cache_collapse")?.severity).toBe("critical");
  });

  it("rejects malformed ranges and surfaces failed local scans", async () => {
    await expect(queryCloudInsights({ ...body, window_size_hrs: Number.NaN }, options))
      .rejects.toThrow("Invalid insight analysis window");
    await seedRichHistory();
    const execute = vi.spyOn(engine, "executeSql").mockRejectedValueOnce(new Error("Local query failed"));
    await expect(queryCloudInsights(body, options)).rejects.toThrow("Local query failed");
    execute.mockRestore();
  });
});
