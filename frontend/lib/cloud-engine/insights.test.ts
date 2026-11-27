import { describe, expect, it } from "vitest";
import {
  COALESCED_SCAN_NAMES,
  CLOUD_INSIGHT_DETECTORS,
  INSIGHTS_ROLLUP_MIN_COVERAGE,
  INSIGHT_CARDS,
  INSIGHT_DEFINITIONS,
  _coalesced_asn_aggregates,
  _coalesced_city_aggregates,
  _coalesced_dim_aggregates,
  _coalesced_fp_aggregates,
  _coalesced_geo_latency_aggregates,
  _coalesced_ip_security_aggregates,
  _coalesced_scalar_aggregates,
  _coalesced_url_aggregates,
  build_insights_envelope,
  canUseInsightsRollupFastpath,
  handleCacheCollapseDetail,
  handleInsightAvailability,
  handleInsights,
  threshold_checks,
  zscore,
} from "./insights";

describe("cloud-engine insights module", () => {
  it("exports all 51 insight detector definitions and 8 coalesced scans", () => {
    expect(INSIGHT_DEFINITIONS).toHaveLength(51);
    expect(INSIGHT_CARDS).toHaveLength(51);
    expect(COALESCED_SCAN_NAMES).toHaveLength(8);
    expect(INSIGHTS_ROLLUP_MIN_COVERAGE).toBe(0.8);

    expect(_coalesced_city_aggregates()).toContain("GROUP BY");
    expect(_coalesced_url_aggregates()).toContain("w_hits");
    expect(_coalesced_ip_security_aggregates()).toContain("ce_w_denied");
    expect(_coalesced_fp_aggregates()).toContain("GROUPING SETS");
    expect(_coalesced_dim_aggregates()).toContain("GROUPING SETS");
    expect(_coalesced_geo_latency_aggregates()).toContain("GROUPING SETS");
    expect(_coalesced_asn_aggregates()).toContain("w_hosting");
    expect(_coalesced_scalar_aggregates()).toContain("w_quic");
  });

  it("enforces 80% (0.8) rollup coverage and empty-filters fastpath gate", () => {
    expect(canUseInsightsRollupFastpath(undefined, 0.8)).toBe(true);
    expect(canUseInsightsRollupFastpath(undefined, 0.95)).toBe(true);
    expect(canUseInsightsRollupFastpath(undefined, 0.79)).toBe(false);
    expect(
      canUseInsightsRollupFastpath({ country: ["US"] }, 1.0),
    ).toBe(false);
  });

  it("computes insight availability based on active service columns", () => {
    const partial = handleInsightAvailability("svc-1", ["status", "url"]);
    const errorSpikes = partial.insights.find((i) => i.id === "error_spikes");
    const botnet = partial.insights.find((i) => i.id === "botnet_grouping");
    expect(errorSpikes?.available).toBe(true);
    expect(errorSpikes?.missing_fields).toBeNull();
    expect(botnet?.available).toBe(false);
    expect(botnet?.missing_fields).toEqual(["ja3", "ja4"]);
    expect(botnet?.reason).toBe("cloud_detector_unsupported");
    const allFields = INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields);
    const complete = handleInsightAvailability("svc-1", allFields);
    expect(complete.insights.filter((d) => d.available).map((d) => d.id).sort())
      .toEqual([...CLOUD_INSIGHT_DETECTORS].sort());
    expect(complete.unavailable).toHaveLength(29);
    expect(complete.unavailable.every((d) => d.reason === "cloud_detector_unsupported")).toBe(true);
    expect(handleInsightAvailability("svc-1").available).toBe(false);
  });

  it("detects cache_collapse and masks IPs in credential_enumeration when maskIps is true", async () => {
    const env = await handleInsights(
      { service_id: "svc-1", window_size_hrs: 24, baseline_hours: 168 },
      {
        role: "analyst-no-pii",
        availableColumns: new Set(["url", "cache", "ip", "status"]),
        availableHistoryHours: 168,
        urlAggregateRows: [
          {
            url: "/assets/app.js",
            w_hits: 10,
            w_miss: 90,
            b_hits: 95,
            b_miss: 5,
          },
        ],
        ipSecurityRows: [
          {
            ip: "198.51.100.77",
            ce_w_denied: 120,
            ce_b_denied: 2,
            ce_w_attempts: 120,
            ce_w_paths: 1,
          },
        ],
        scalarAggregateRows: [{ w_hits: 10, w_cacheable: 100, b_hits: 95, b_cacheable: 100 }],
        windowTotalRequests: 5000,
      },
    );

    expect(env.window_hours).toBe(24);
    expect(env.baseline_hours).toBe(168);
    expect(env.window_total_requests).toBe(5000);

    const ccCard = env.insights.find((c) => c.id === "cache_collapse");
    expect(ccCard?.severity).toBe("critical");
    expect(ccCard?.items[0].label).toBe("/assets/app.js");

    const ceCard = env.insights.find((c) => c.id === "credential_enumeration");
    expect(ceCard?.severity).toBe("critical");
    expect(ceCard?.items[0].label).toBe("198.51.100.xxx");
    expect(JSON.stringify(env)).not.toContain("198.51.100.77");
  });

  it("does not claim clean results when history or detector inputs are missing", async () => {
    const body = { window_size_hrs: 1, baseline_hours: 48 };
    const context = { availableColumns: new Set(["url", "cache"]) };
    const result = await handleInsights(body, context);
    expect(result.insights.map((card) => card.severity)).toEqual(["info", "info", "info"]);
    expect(result.insights.every((card) => card.summary.includes("Not evaluated"))).toBe(true);
    await expect(handleInsights(body, { ...context, availableHistoryHours: 48 }))
      .rejects.toThrow("Missing local aggregate input");
  });

  it("shapes /api/insights/cache-collapse-detail and masks recent_misses IPs", async () => {
    const detail = await handleCacheCollapseDetail(
      { url: "/assets/app.js" },
      {
        maskIps: true,
        rates: {
          w_hits: 20,
          w_miss: 80,
          w_pass: 10,
          w_total: 110,
          b_hits: 90,
          b_miss: 10,
          b_pass: 5,
          b_total: 105,
        },
        timelineRows: [
          {
            bucket: "2026-11-05T00:00:00Z",
            total_requests: 55,
            real_hits: 10,
            misses: 40,
          },
        ],
        missRows: [
          {
            timestamp: "2026-11-05T00:10:00Z",
            cache: "MISS",
            pop: "IAD",
            ip: "203.0.113.88",
            status: 200,
          },
        ],
      },
    );

    expect(detail.window_hit_rate).toBeCloseTo(20);
    expect(detail.baseline_hit_rate).toBeCloseTo(90);
    expect(detail.breakdown).toEqual({
      hits: 20,
      misses: 80,
      passes: 10,
      other: 0,
    });
    expect(detail.timeline[0].expected_hits).toBe(45);
    expect(detail.timeline[0].hit_rate).toBeCloseTo(0.2);
    expect(detail.clusters).toEqual([]);
    expect(detail.recent_misses[0].ip).toBe("203.0.113.xxx");
  });

  it("computes zscore, threshold_checks, and build_insights_envelope", () => {
    expect(zscore(30, 10, 5)).toBe(4);
    expect(zscore(10, 10, 0)).toBe(0);
    expect(
      threshold_checks({ windowValue: 0.5, baselineValue: 0.1, windowCount: 25 }),
    ).toBe(true);
    const env = build_insights_envelope(
      [],
      "2026-11-04T00:00:00Z",
      "2026-11-05T00:00:00Z",
      "2026-10-28T00:00:00Z",
      "2026-11-04T00:00:00Z",
      24,
      168,
    );
    expect(env.insights).toEqual([]);
  });

  it("excludes empty-string URLs from coalesced url and ip_security aggregates", () => {
    const urlSql = _coalesced_url_aggregates();
    expect(urlSql).toMatch(/NULLIF\("url", ''\) IS NOT NULL|"url" IS NOT NULL AND "url" != ''/);

    const ipSql = _coalesced_ip_security_aggregates();
    expect(ipSql).toContain(`COUNT(DISTINCT NULLIF("url", '')) FILTER (WHERE is_w AND is_auth) AS ce_w_paths`);
    expect(ipSql).toContain(`COUNT(DISTINCT NULLIF("url", '')) FILTER (WHERE is_w AND status = 404) AS cd_distinct_404`);
  });

  it("matches High-Scale severity thresholds and unrounded tail_latency ratio and honors end_time", async () => {
    const env = await handleInsights(
      {
        service_id: "svc-1",
        window_size_hrs: 6,
        baseline_hours: 24,
        end_time: "2026-11-24T18:00:00Z",
      },
      {
        now: new Date("2026-11-24T20:00:00Z"),
        availableColumns: new Set(["url", "elapsed", "city", "country", "status", "ua", "asn", "p_type", "method", "referer"]),
        availableHistoryHours: 48,
        windowTotalRequests: 10000,
        baselineTotalRequests: 40000,
        urlAggregateRows: [
          {
            url: "/wp-content/precious.gif",
            w_total: 35,
            b_total: 75,
            w_lat_total: 35,
            b_lat_total: 75,
            w_p99: 242,
            w_p50: 3,
            w_p99_raw: 242.1,
            w_p50_raw: 2.63,
          },
        ],
        ipSecurityRows: [],
        cityAggregateRows: [
          { city: "davis", region: "CA", country: "US", w_cnt: 1888, b_cnt: 3119, w_errors_4xx: 1088, b_errors_4xx: 168 },
          { city: "council bluffs", region: "IA", country: "US", w_cnt: 17180, b_cnt: 0, w_errors_4xx: 0, b_errors_4xx: 0 },
          { city: "smallville", region: "KS", country: "US", w_cnt: 12, b_cnt: 0, w_errors_4xx: 0, b_errors_4xx: 0 },
          { city: "wrexham", region: "WRX", country: "GB", w_cnt: 100, b_cnt: 80, w_errors_4xx: 0, b_errors_4xx: 0 },
        ],
        dimensionAggregateRows: [
          { dimension: "ua", ua: "Bot/1", w_cnt: 6000, b_cnt: 10 },
          { dimension: "ua", ua: "Human/1", w_cnt: 4000, b_cnt: 39990 },
          { dimension: "country", country: "NZ", w_cnt: 25, b_cnt: 0 },
          { dimension: "method", method: "POST", w_cnt: 3000, b_cnt: 100 },
          { dimension: "method", method: "GET", w_cnt: 7000, b_cnt: 39900 },
        ],
        asnAggregateRows: [
          { asn: 64512, w_cnt: 150, b_cnt: 0, w_proxy_total: 0, b_proxy_total: 0, w_hosting: 0, b_hosting: 0 },
          { asn: 64513, w_cnt: 6000, b_cnt: 100, w_proxy_total: 100, b_proxy_total: 100, w_hosting: 80, b_hosting: 5 },
        ],
        geoLatencyRows: [],
        scalarAggregateRows: [],
      },
    );

    expect(env.window_end).toBe("2026-11-24T18:00:00.000Z");

    const cityErr = env.insights.find((c) => c.id === "city_error_spikes");
    expect(cityErr?.severity).toBe("critical");
    expect(cityErr?.items[0].severity).toBe("critical");

    const newCity = env.insights.find((c) => c.id === "new_city_traffic");
    expect(newCity?.severity).toBe("warning");
    expect(newCity?.items.find((i) => i.label.includes("council bluffs"))?.severity).toBe("warning");
    expect(newCity?.items.find((i) => i.label.includes("smallville"))?.severity).toBe("info");

    const tail = env.insights.find((c) => c.id === "tail_latency");
    expect(tail?.severity).toBe("critical");
    expect(tail?.items[0].severity).toBe("critical");
    expect(tail?.items[0].meta.ratio).toBeCloseTo(92.1, 1);

    const citySurges = env.insights.find((c) => c.id === "city_surges");
    expect(citySurges?.items.find((i) => i.label.includes("wrexham"))?.severity).toBe("info");
    expect(citySurges?.items.find((i) => i.label.includes("council bluffs"))?.severity).toBe("warning");

    expect(env.insights.find((c) => c.id === "new_country_traffic")?.severity).toBe("warning");
    expect(env.insights.find((c) => c.id === "new_asn_traffic")?.severity).toBe("warning");
    expect(env.insights.find((c) => c.id === "ua_monoculture")?.severity).toBe("critical");
    expect(env.insights.find((c) => c.id === "method_drift")?.severity).toBe("critical");
    expect(env.insights.find((c) => c.id === "asn_concentration")?.severity).toBe("critical");
    expect(env.insights.find((c) => c.id === "asn_hosting_shift")?.severity).toBe("critical");
  });

  it("matches High-Scale meta keys, 15-item cap, and envelope pending/refreshing fields", async () => {
    const urls = Array.from({ length: 20 }, (_, i) => ({
      url: `/err/${i}`,
      w_total: 20 + i,
      b_total: 100,
      w_5xx: 10 + i,
      b_5xx: 0,
      w_lat_total: 20,
      b_lat_total: 50,
      w_p95: 1000,
      b_p95: 100,
      w_p99: 2000,
      w_p50: 100,
    }));
    const env = await handleInsights(
      { service_id: "svc-meta", window_size_hrs: 1, baseline_hours: 24 },
      {
        availableColumns: new Set(["url", "status", "elapsed", "ip", "city", "country", "ua", "referer", "method", "p_type", "asn", "pop", "cache", "transport"]),
        availableHistoryHours: 24,
        windowTotalRequests: 5000,
        baselineTotalRequests: 50000,
        urlAggregateRows: urls,
        ipSecurityRows: [{ ip: "198.51.100.5", cd_w_404: 50, w_total: 60, cd_distinct_404: 20, cd_b_404: 0 }],
        cityAggregateRows: [{ city: "austin", region: "TX", country: "US", w_cnt: 50, b_cnt: 100, w_errors_4xx: 30, b_errors_4xx: 1, w_lat_total: 20, b_lat_total: 60, w_p95: 2000, b_p95: 100 }],
        dimensionAggregateRows: [
          { dimension: "ua", ua: "Bot/1", w_cnt: 3000, b_cnt: 10 },
          { dimension: "ua", ua: "Human/1", w_cnt: 2000, b_cnt: 49990 },
          { dimension: "method", method: "POST", w_cnt: 1500, b_cnt: 50 },
          { dimension: "method", method: "GET", w_cnt: 3500, b_cnt: 49950 },
          { dimension: "p_type", p_type: "hosting", w_cnt: 1000, b_cnt: 10 },
          { dimension: "p_type", p_type: "", w_cnt: 4000, b_cnt: 49990 },
        ],
        asnAggregateRows: [{ asn: 64513, w_cnt: 3000, b_cnt: 100, w_proxy_total: 100, b_proxy_total: 100, w_hosting: 80, b_hosting: 5 }],
        geoLatencyRows: [{ pop: "IAD", w_lat_total: 50, b_lat_total: 100, w_p95: 500, b_p95: 100 }],
        scalarAggregateRows: [{ w_hits: 10, w_cacheable: 200, b_hits: 300, b_cacheable: 300, w_quic: 10, w_tr_total: 200, b_quic: 150, b_tr_total: 200 }],
      },
    );

    expect(env.pending).toBe(false);
    expect(env.refreshing).toBe(false);
    expect(env.insights.find((c) => c.id === "error_spikes")?.items).toHaveLength(15);
    expect(env.insights.find((c) => c.id === "error_spikes")?.items[0].meta).toMatchObject({
      requests: expect.any(Number),
      errors: expect.any(Number),
    });
    expect(env.insights.find((c) => c.id === "error_spikes")?.items[0].meta).not.toHaveProperty("window_errors");
    expect(env.insights.find((c) => c.id === "latency_regression")?.items[0].meta).toMatchObject({
      regression_ratio: 10,
      window_requests: 20,
    });
    expect(env.insights.find((c) => c.id === "tail_latency")?.items[0].meta).toMatchObject({
      p99_ms: 2000,
      p50_ms: 100,
      ratio: 20,
      requests: 20,
    });
    expect(env.insights.find((c) => c.id === "content_discovery")?.items[0].meta).toMatchObject({
      requests: 60,
      not_found: 50,
      distinct_404_urls: 20,
    });
    expect(env.insights.find((c) => c.id === "city_surges")?.items[0].meta).toHaveProperty("spike_ratio");
    expect(env.insights.find((c) => c.id === "city_error_spikes")?.items[0].meta).toMatchObject({ requests: 50, errors: 30 });
    expect(env.insights.find((c) => c.id === "city_latency_regressions")?.items[0].meta).toMatchObject({ regression_ratio: 20, requests: 20 });
    expect(env.insights.find((c) => c.id === "method_drift")?.items[0].meta).toMatchObject({ requests: 1500, method: "POST" });
    expect(env.insights.find((c) => c.id === "proxy_surge")?.items[0].meta).toMatchObject({ requests: 1000 });
    expect(env.insights.find((c) => c.id === "asn_hosting_shift")?.items[0].meta).toMatchObject({ window_hosting: 80, window_total: 100, asn: 64513 });
    expect(env.insights.find((c) => c.id === "pop_latency_regression")?.items[0].meta).toMatchObject({ regression_ratio: 5, window_requests: 50 });
    expect(env.insights.find((c) => c.id === "cache_hit_cliff")?.items[0].meta).toMatchObject({ window_cacheable: 200, baseline_cacheable: 300 });
    expect(env.insights.find((c) => c.id === "http3_fallback")?.items[0].meta).toMatchObject({ window_quic: 10, window_total: 200, filters: { transport: "tcp" } });
  });
});
