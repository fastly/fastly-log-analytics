import { describe, expect, it } from "vitest";
import { parseServerTiming, summarizeCalls } from "./call-timeline";

describe("parseServerTiming", () => {
  it("reads Compute phases and edge cache state in order", () => {
    expect(
      parseServerTiming(
        'app;dur=12.5, auth;dur=1.0, list;dur=8, cdn-cache;desc=HIT, edge;desc="MISS-CLUSTER";dur=40',
      ),
    ).toEqual([
      { name: "app", dur: 12.5 },
      { name: "auth", dur: 1 },
      { name: "list", dur: 8 },
      { name: "cdn-cache", desc: "HIT" },
      { name: "edge", desc: "MISS-CLUSTER", dur: 40 },
    ]);
  });

  it("ignores empty values and malformed durations", () => {
    expect(parseServerTiming(null)).toEqual([]);
    expect(parseServerTiming(" , app;dur=abc")).toEqual([{ name: "app" }]);
  });
});

describe("summarizeCalls", () => {
  const call = (service: string, start: number, ms: number) => ({
    service,
    start_ms: start,
    time_ms: ms,
  });

  it("reports wall-clock span, busy time and the longest sequential chain", () => {
    const summary = summarizeCalls([
      call("Compute API", 1000, 100),
      call("CDN Cache", 1050, 100),
      call("CDN Cache", 1060, 20),
      call("FOS S3", 1150, 200),
      call("Browser Cache", 1500, 10),
    ]);
    expect(summary.summedMs).toBe(430);
    expect(summary.wallMs).toBe(510);
    expect(summary.busyMs).toBe(360);
    expect(summary.criticalPath).toEqual({ calls: 3, ms: 310 });
    expect(summary.bySource).toEqual([
      { source: "Compute API", count: 1, ms: 100 },
      { source: "CDN Cache", count: 2, ms: 120 },
      { source: "FOS S3", count: 1, ms: 200 },
      { source: "Browser Cache", count: 1, ms: 10 },
    ]);
  });

  it("leaves the timeline empty when calls carry no start time", () => {
    const summary = summarizeCalls([{ service: "Compute", time_ms: 5 }]);
    expect(summary).toMatchObject({
      summedMs: 5,
      wallMs: null,
      busyMs: null,
      criticalPath: null,
    });
  });
});
