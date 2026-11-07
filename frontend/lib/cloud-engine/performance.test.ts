// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getDuckDBEngine } from "./dashboard/test-fixtures";
import {
  buildPerfScatterSql,
  buildPerfTopAsnsSql,
  buildPerfTopUrlsSql,
  buildPerfTtlDistSql,
  buildPerfWaterfallSql,
  buildTtlBucketCaseSql,
  executePerformanceAggregates,
  expandPerformanceSections,
  handlePerformanceRequest,
  resolveSortColumn,
  SCATTER_SAMPLE_SIZE,
  setPerformanceEngineClient,
  shapePerformanceAsnRows,
  shapePerfScatterRows,
  shapePerfTopUrlRows,
  shapePerfTtlDistRows,
  shapeWaterfallAvg,
} from "./performance";

describe("performance cloud-engine module (F19)", () => {
  beforeEach(() => {
    setPerformanceEngineClient(null);
  });

  it("does not recapture a fresh empty backend after awaited request parsing", async () => {
    const engine = getDuckDBEngine();
    await engine.reset();
    const request = new Request("http://localhost/api/performance/aggregates", { method: "POST", body: "{}" });
    const clone = request.clone();
    let enter!: () => void, resume!: (body: object) => void;
    const entered = new Promise<void>((resolve) => { enter = resolve; });
    const body = new Promise<object>((resolve) => { resume = resolve; });
    vi.spyOn(clone, "json").mockImplementation(() => { enter(); return body; });
    vi.spyOn(request, "clone").mockReturnValue(clone);
    const pending = handlePerformanceRequest(request);
    const rejected = expect(pending).rejects.toThrow(/generation|expired/i);
    await entered;
    await engine.reset();
    resume({});
    await rejected;
    vi.restoreAllMocks();
  });

  it("returns populated short-field results using the page's table and chart contract", async () => {
    const engine = getDuckDBEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", elapsed, ttfb, cache, url, asn, ttl)
        SELECT 'performance-test', 1700000000000, epoch_ms(1700000000000),
          100000, 0.02, 'HIT', '/a', 15169, 300 FROM range(12)
      `);
      const response = await handlePerformanceRequest(
        new Request("https://example.test/api/performance/aggregates", {
          method: "POST",
          body: JSON.stringify({
            start_time: "2023-11-14T22:00:00Z",
            end_time: "2023-11-14T23:00:00Z",
          }),
        }),
      );
      const data = await response!.json();
      expect(data.top_urls[0]).toMatchObject({
        url: "/a", requests: 12, avg: 100, p50: 100, p95: 100, p99: 100,
      });
      expect(data.top_asns[0]).toMatchObject({ requests: 12, avg: 100 });
      expect(data.waterfall.avg).toMatchObject({ edge_processing: 20, client_download: 80 });
      expect(data.scatter[0]).toEqual({ origin: 20, edge: 80, cache: "HIT" });
    } finally {
      await engine.reset();
    }
  });

  it("expands section union pairs (top_urls+top_asns and waterfall+scatter)", () => {
    expect([...expandPerformanceSections(["top_urls"])].sort()).toEqual(["top_asns", "top_urls"]);
    expect([...expandPerformanceSections(["waterfall"])].sort()).toEqual(["scatter", "waterfall"]);
    expect(expandPerformanceSections().size).toBe(5);
  });

  it("resolves sort_by into SQL order column with p99 default", () => {
    expect(resolveSortColumn("avg")).toBe("avg_ms");
    expect(resolveSortColumn("p50")).toBe("p50_ms");
    expect(resolveSortColumn("p95")).toBe("p95_ms");
    expect(resolveSortColumn("p99")).toBe("p99_ms");
    expect(resolveSortColumn("unknown")).toBe("p99_ms");
    expect(resolveSortColumn()).toBe("p99_ms");
  });

  it("builds SQL for top_urls, top_asns, ttl_dist, waterfall, and scatter", () => {
    const req = {
      start_time: "2026-06-15T00:00:00Z",
      end_time: "2026-06-15T01:00:00Z",
      sort_by: "p95" as const,
    };

    const topUrls = buildPerfTopUrlsSql(req);
    expect(topUrls.sql).toContain("approx_quantile");
    expect(topUrls.sql).toContain("HAVING COUNT(*) > 5");
    expect(topUrls.sql).toContain("ORDER BY p95_ms DESC NULLS LAST");

    const topAsns = buildPerfTopAsnsSql(req);
    expect(topAsns.sql).toContain("HAVING COUNT(*) > 10");
    expect(topAsns.sql).toContain("LIMIT 20");

    const ttlCase = buildTtlBucketCaseSql("ttl_val");
    expect(ttlCase).toContain("< 60s");
    expect(ttlCase).toContain("1m - 1h");
    expect(ttlCase).toContain("1h - 1d");
    expect(ttlCase).toContain("> 1d");

    const ttlDist = buildPerfTtlDistSql(req);
    expect(ttlDist.sql).toContain("< 60s");
    expect(ttlDist.sql).toContain("> 1d");

    const waterfall = buildPerfWaterfallSql(req, new Set(["elapsed", "ttfb", "ottfb", "ottlb"]));
    expect(waterfall.sql).toContain("edge_processing");
    expect(waterfall.sql).toContain("origin_wait");
    expect(waterfall.sql).toContain("origin_download");
    expect(waterfall.sql).toContain("client_download");

    const scatter = buildPerfScatterSql(req, new Set(["elapsed", "ttfb", "cache", "timestamp", "url"]));
    expect(scatter.sql).toContain(`LIMIT ${SCATTER_SAMPLE_SIZE}`);
    expect(scatter.sql).toContain("(elapsed_ms - ttfb_ms) >= 0");
  });

  it("shapes top_urls, top_asns, ttl_dist, waterfall, and scatter rows", () => {
    const urls = shapePerfTopUrlRows([
      { url: "/checkout", req_count: 42, avg_ms: 12.345, p50_ms: 10.1, p95_ms: 40.556, p99_ms: 88.912 },
    ]);
    expect(urls[0]).toEqual({
      url: "/checkout",
      requests: 42,
      avg: 12.35,
      p50: 10.1,
      p95: 40.56,
      p99: 88.91,
    });

    const asns = shapePerformanceAsnRows([
      { asn: 15169, req_count: 120, avg_ms: 8.2, p50_ms: 6.1, p95_ms: 19.4, p99_ms: 35.0 },
    ]);
    expect(asns[0]?.asn).toBe("15169");
    expect(asns[0]?.label).toContain("Google");

    const ttl = shapePerfTtlDistRows([
      { bucket: "< 60s", count: 15 },
      { bucket: "1m - 1h", count: 80 },
      { bucket: "1h - 1d", count: 40 },
      { bucket: "> 1d", count: 5 },
    ]);
    expect(ttl).toHaveLength(4);
    expect(ttl[3]).toEqual({ bucket: "> 1d", count: 5 });

    expect(shapeWaterfallAvg(null)).toEqual({
      edge_processing: 0,
      origin_wait: 0,
      origin_download: 0,
      client_download: 0,
    });

    const wf = shapeWaterfallAvg({
      edge_processing: 1.234,
      origin_wait: 24.567,
      origin_download: 8.9,
      client_download: 3.456,
    });
    expect(wf).toEqual({
      edge_processing: 1.23,
      origin_wait: 24.57,
      origin_download: 8.9,
      client_download: 3.46,
    });

    const scatter = shapePerfScatterRows([{ origin_ms: 12.345, edge_ms: 4.567, cache: "MISS" }]);
    expect(scatter).toEqual([{ origin: 12.35, edge: 4.57, cache: "MISS" }]);
  });

  it("uses perf_top_urls, perf_top_asns, and perf_ttl_dist rollups when unfiltered and dispatches /api/performance/aggregates", async () => {
    const tryRollup = vi.fn(async (name: string) => {
      if (name === "perf_top_urls") {
        return [{ url: "/fast", req_count: 100, avg_ms: 4.2, p50_ms: 3.0, p95_ms: 9.0, p99_ms: 15.0 }];
      }
      if (name === "perf_top_asns") {
        return [{ asn: "13335", req_count: 90, avg_ms: 5.1, p50_ms: 4.0, p95_ms: 10.0, p99_ms: 18.0 }];
      }
      if (name === "perf_ttl_dist") {
        return [{ bucket: "1m - 1h", count: 190 }];
      }
      return null;
    });

    setPerformanceEngineClient({
      getAvailableColumns: vi.fn().mockResolvedValue(new Set(["elapsed", "ttfb", "ottfb", "ottlb", "cache", "ttl", "url", "asn"])),
      tryRollup,
      query: vi.fn().mockResolvedValue([
        {
          edge_processing: 2.0,
          origin_wait: 10.0,
          origin_download: 5.0,
          client_download: 1.5,
          origin_ms: 10.0,
          edge_ms: 2.0,
          cache: "HIT",
        },
      ]),
    });

    const agg = await executePerformanceAggregates({
      start_time: "2026-06-15T00:00:00Z",
      end_time: "2026-06-15T01:00:00Z",
    });
    expect(agg._approx).toBe(true);
    expect(agg.top_urls?.[0]?.url).toBe("/fast");
    expect(agg.top_asns?.[0]?.asn).toBe("13335");
    expect(agg.ttl_dist?.[0]?.bucket).toBe("1m - 1h");
    expect(agg.waterfall?.avg?.origin_wait).toBe(10);

    const req = new Request("https://example.test/api/performance/aggregates", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_time: "2026-06-15T00:00:00Z",
        end_time: "2026-06-15T01:00:00Z",
        sections: ["waterfall"],
      }),
    });
    const res = await handlePerformanceRequest(req);
    expect(res).not.toBeNull();
    const json = await res!.json();
    expect(json).toHaveProperty("waterfall");
    expect(json).toHaveProperty("scatter");

    const other = await handlePerformanceRequest(new Request("https://example.test/api/other"));
    expect(other).toBeNull();
  });
});
