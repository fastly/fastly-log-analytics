import { describe, expect, it } from "vitest";
import {
  COALESCED_SCAN_NAMES,
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
  });

  it("detects cache_collapse and masks IPs in credential_enumeration when maskIps is true", async () => {
    const env = await handleInsights(
      { service_id: "svc-1", window_size_hrs: 24, baseline_hours: 168 },
      {
        role: "analyst-no-pii",
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
          },
        ],
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
    expect(ceCard?.items[0].label).toBe("0.0.0.0");
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
    expect(detail.recent_misses[0].ip).toBe("0.0.0.0");
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
});
