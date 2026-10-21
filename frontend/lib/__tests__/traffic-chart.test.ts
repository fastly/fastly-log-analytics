import { describe, expect, it } from "vitest";
import { buildTrafficData, type BuildTrafficDataParams } from "../traffic-chart";

function makeBaseParams(overrides: Partial<BuildTrafficDataParams> = {}): BuildTrafficDataParams {
  return {
    aggregates: {
      metric: "requests",
      interval: "1 minute",
      time_series: [
        { time: "2026-10-20T10:00:00Z", value: 100 },
        { time: "2026-10-20T10:01:00Z", value: 200 },
        { time: "2026-10-20T10:02:00Z", value: 150 },
        { time: "2026-10-20T10:03:00Z", value: 250 },
        { time: "2026-10-20T10:04:00Z", value: 300 },
        { time: "2026-10-20T10:05:00Z", value: 400 },
      ],
    },
    compareAggregates: null,
    compareMode: false,
    compareStartTime: null,
    startTime: "2026-10-20T10:00:00Z",
    endTime: "2026-10-20T10:05:00Z",
    trend: "off",
    timezone: "UTC",
    metric: "requests",
    effectiveInterval: "1 minute",
    hiddenCategories: new Set(),
    catalog: null,
    ...overrides,
  };
}

describe("buildTrafficData golden characterization", () => {
  it("builds primary bar trace, baseline trace, and sliding-window trend line accurately", () => {
    const params = makeBaseParams({
      trend: "5m",
      compareMode: true,
      compareStartTime: "2026-10-19T10:00:00Z",
      compareAggregates: {
        metric: "requests",
        interval: "1 minute",
        time_series: [
          { time: "2026-10-19T10:00:00Z", value: 90 },
          { time: "2026-10-19T10:01:00Z", value: 180 },
          { time: "2026-10-19T10:02:00Z", value: 140 },
          { time: "2026-10-19T10:03:00Z", value: 220 },
          { time: "2026-10-19T10:04:00Z", value: 280 },
          { time: "2026-10-19T10:05:00Z", value: 350 },
        ],
      },
    });

    const traces = buildTrafficData(params);
    expect(traces).toHaveLength(3);

    const [primary, baseline, trend] = traces;
    expect(primary.type).toBe("bar");
    expect(primary.y).toEqual([100, 200, 150, 250, 300, 400]);

    expect(baseline.name).toBe("Comparison Range");
    expect(baseline.x).toEqual(primary.x);
    expect(baseline.y).toEqual([90, 180, 140, 220, 280, 350]);

    // 5m trend over 1-minute buckets => windowSize = 5:
    // idx 0: 100 / 1 = 100
    // idx 1: (100+200) / 2 = 150
    // idx 2: (100+200+150) / 3 = 150
    // idx 3: (100+200+150+250) / 4 = 175
    // idx 4: (100+200+150+250+300) / 5 = 200
    // idx 5: (200+150+250+300+400) / 5 = 260
    expect(trend.name).toBe("Trend");
    expect(trend.x).toEqual(primary.x);
    expect(trend.y).toEqual([null, null, null, null, 200, 260]);
  });

  it("handles categorized 5xx status breakdown and hiddenCategories", () => {
    const params = makeBaseParams({
      metric: "5xx",
      aggregates: {
        metric: "5xx",
        interval: "1 minute",
        time_series: [
          { time: "2026-10-20T10:00:00Z", category: "500", value: 12 },
          { time: "2026-10-20T10:00:00Z", category: "503", value: 4 },
          { time: "2026-10-20T10:01:00Z", category: "500", value: 15 },
          { time: "2026-10-20T10:01:00Z", category: "503", value: 2 },
        ],
      },
      startTime: "2026-10-20T10:00:00Z",
      endTime: "2026-10-20T10:01:00Z",
      hiddenCategories: new Set(["503"]),
    });

    const traces = buildTrafficData(params);
    expect(traces).toHaveLength(2);
    expect(traces[0].name).toBe("500");
    expect(traces[0].visible).toBe(true);
    expect(traces[1].name).toBe("503");
    expect(traces[1].visible).toBe("legendonly");
  });
});
