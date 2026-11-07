// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { getDuckDBEngine } from "./dashboard/test-fixtures";
import {
  buildOriginDrilldownSql,
  buildOriginIpHealthSql,
  buildOriginPathBreakdownSql,
  buildOriginPopLatencySql,
  buildOriginShieldingAnalysisSql,
  buildOriginSlowUrlsSql,
  buildOriginStatusCodesSql,
  buildOriginSummarySql,
  buildOriginTimeseriesSql,
  enrichShieldingRouteWithPopDistance,
  handleOriginRequest,
  isPopLatencyElevated,
  originLatencyUsExpr,
  queryOriginAggregates,
  queryOriginDrilldown,
  queryOriginIpHealthWithFallback,
  queryOriginShieldingAnalysis,
  queryOriginSlowUrlsWithFallback,
  setOriginSqlExecutor,
  shapeOriginDrilldownRows,
  shapeOriginIpHealthRows,
  shapeOriginPathBreakdownRows,
  shapeOriginPopLatencyRows,
  shapeOriginSlowUrlsRows,
  shapeOriginStatusCodesRows,
  shapeOriginSummaryRow,
  shapeOriginTimeseriesRows,
  type OriginSqlExecutor,
} from "./origin";

describe("origin cloud-engine module (F18)", () => {
  beforeEach(() => {
    setOriginSqlExecutor(null);
  });

  it.each(["aggregates", "shielding-analysis", "drilldown"])(
    "uses captured custom types for %s while another lease changes the global schema", async (path) => {
      const engine = getDuckDBEngine();
      const selected = engine.lease([{ id: "tenant", type: "string" }]);
      const other = engine.lease([{ id: "tenant", type: "number" }]);
      try {
        await selected.ingestFile("origin-selected", "origin-selected.parquet", parquetWriteBuffer({
          columnData: [
            { name: "timestamp", type: "TIMESTAMP", data: [new Date(1700000000000), new Date(1700000000001)] },
            { name: "tenant", type: "STRING", data: ["selected", "selected"] },
            { name: "cache", type: "STRING", data: ["MISS", "MISS"] },
            { name: "ttfb", type: "DOUBLE", data: [0.02, 0.01] },
            { name: "edge", type: "BOOLEAN", data: [true, false] },
            { name: "rid", type: "STRING", data: ["edge-request", "shield-request"] },
            { name: "prid", type: "STRING", data: ["", "edge-request"] },
            { name: "pop", type: "STRING", data: ["IAD", "CHI"] },
            { name: "host", type: "STRING", data: ["origin.example.test", "origin.example.test"] },
          ],
        }));
        const read = selected.readContext();
        await other.ingestFile("origin-other", "origin-other.parquet", parquetWriteBuffer({
          columnData: [
            { name: "timestamp", type: "TIMESTAMP", data: [new Date(1700000000000)] },
            { name: "tenant", type: "DOUBLE", data: [1] },
          ],
        }));
        const response = await handleOriginRequest(new Request(`https://example.test/api/origin/${path}`, {
          method: "POST",
          body: JSON.stringify({
            sections: ["summary"],
            custom_filters: { tenant: { mode: "include", values: ["selected"] } },
          }),
        }), undefined, undefined, read);
        const data = await response!.json();
        if (path === "aggregates") {
          expect(data.summary).toMatchObject({ has_data: true, total_misses: 2 });
        } else {
          expect(data.has_data).toBe(true);
          expect(data.rows).toHaveLength(1);
          expect(data.rows[0].requests).toBe(path === "shielding-analysis" ? 1 : 2);
        }
      } finally {
        selected.release();
        other.release();
        await engine.reset();
      }
    },
  );

  it("preserves query-string URL filters with the default executor and request signal", async () => {
    const engine = getDuckDBEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", cache, ttfb, url)
        VALUES ('origin-test', 1700000000000, epoch_ms(1700000000000), 'MISS', 0.02, '/a?b=1')
      `);
      const response = await handleOriginRequest(
        new Request("https://example.test/api/origin/aggregates", {
          method: "POST",
          body: JSON.stringify({
            start_time: "2023-11-14T22:00:00Z",
            end_time: "2023-11-14T23:00:00Z",
            sections: ["summary", "slow_urls"],
            filters: { url: { mode: "include", values: ["/a?b=1"] } },
          }),
        }),
      );
      const data = await response!.json();
      expect(data.summary.has_data).toBe(true);
      expect(data.summary.ottfb_p50_ms).toBe(20);
      expect(data.slow_urls.rows[0].url).toBe("/a?b=1");
    } finally {
      await engine.reset();
    }
  });

  it("resolves originLatencyUsExpr across ottfb and ttfb combinations", () => {
    expect(originLatencyUsExpr(new Set(["ottfb", "ttfb"]))).toBe(
      'COALESCE("ottfb", "ttfb" * 1000000.0)',
    );
    expect(originLatencyUsExpr(new Set(["ottfb"]))).toBe('"ottfb"');
    expect(originLatencyUsExpr(new Set(["ttfb"]))).toBe('"ttfb" * 1000000.0');
    expect(originLatencyUsExpr(new Set(), null)).toBeNull();
  });

  it.each([true, false])("buckets epoch milliseconds as core UTC TIMESTAMP with timestamp column present=%s", async (hasTimestamp) => {
    const engine = getDuckDBEngine();
    try {
      await engine.executeSql(`
        INSERT INTO requests (scoped_key, ts_ms, "timestamp", cache, ttfb)
        VALUES ('origin-utc', 1700000000000, epoch_ms(1700000000000), 'MISS', 0.02)
      `);
      const cols = new Set(["ts_ms", "ttfb"]);
      if (hasTimestamp) cols.add("timestamp");
      const sql = buildOriginTimeseriesSql("requests", "TRUE", cols, {
        bucketMinutes: 5, percentile: "p50",
      });
      const rows = await engine.executeSql(`
        SELECT typeof(ts) AS bucket_type, epoch_ms(ts) AS bucket_ms, value FROM (${sql})
      `);
      expect(rows).toEqual([{
        bucket_type: "TIMESTAMP", bucket_ms: Date.parse("2023-11-14T22:10:00Z"), value: 20,
      }]);
    } finally {
      await engine.reset();
    }
  });

  it("builds SQL for summary, timeseries, slow_urls, status_codes, path_breakdown, pop_latency, ip_health, shielding, and drilldown", () => {
    const cols = new Set([
      "timestamp",
      "cache",
      "ottfb",
      "ttfb",
      "ottlb",
      "elapsed",
      "ost",
      "obytes",
      "url",
      "edge",
      "pop",
      "oip",
      "host",
      "rid",
      "prid",
    ]);

    const summarySql = buildOriginSummarySql("requests", "TRUE", cols);
    expect(summarySql).toContain("starts_with(\"cache\", 'MISS')");
    expect(summarySql).toContain("starts_with(\"cache\", 'PASS')");
    expect(summarySql).toContain("APPROX_QUANTILE");

    const tsSql = buildOriginTimeseriesSql("requests", "TRUE", cols, {
      metric: "ttfb",
      percentile: "p95",
      bucketMinutes: 5,
      splitByLeg: true,
    });
    expect(tsSql).toContain("time_bucket");
    expect(tsSql).toContain('"edge"');

    const slowSql = buildOriginSlowUrlsSql("requests", "TRUE", cols, 10, 20);
    expect(slowSql).toContain("HAVING COUNT(*) >= 10");
    expect(slowSql).toContain("SEMI JOIN qualified");

    const statusSql = buildOriginStatusCodesSql("requests", "TRUE", cols);
    expect(statusSql).toContain('WHEN "ost" BETWEEN 100 AND 599');

    const pathSql = buildOriginPathBreakdownSql("requests", "TRUE", cols);
    expect(pathSql).toContain('GROUP BY "edge"');

    const popSql = buildOriginPopLatencySql("requests", "TRUE", cols, 30);
    expect(popSql).toContain('GROUP BY "pop"');

    const ipSql = buildOriginIpHealthSql("requests", "TRUE", cols, 30, 10);
    expect(ipSql).toContain('GROUP BY "oip"');
    expect(ipSql).toContain("HAVING COUNT(*) >= 10");

    const shieldSql = buildOriginShieldingAnalysisSql("requests", "TRUE", "TRUE", 50);
    expect(shieldSql).toContain('INNER JOIN shield_logs s ON s."prid" = e."rid"');

    const drillSql = buildOriginDrilldownSql("requests", "TRUE", cols, "host", 30);
    expect(drillSql).toContain("CAST(host AS VARCHAR)");
  });

  it("shapes summary, timeseries, status_codes, path_breakdown, pop_latency (1.5x median), ip_health, and drilldown rows", () => {
    expect(shapeOriginSummaryRow(null).has_data).toBe(false);
    const summary = shapeOriginSummaryRow({
      total_misses: 120,
      total_passes: 30,
      ottfb_p50_ms: 15.234,
      ottfb_p75_ms: 25.1,
      ottfb_p95_ms: 60.789,
      ottfb_p99_ms: 110.5,
      ottlb_p50_ms: 20.0,
      ottlb_p95_ms: 80.0,
      cdn_overhead_p50_ms: 2.5,
      origin_error_rate: 0.025,
      obytes_p50: 4096,
    });
    expect(summary.has_data).toBe(true);
    expect(summary.ottfb_p50_ms).toBe(15.23);
    expect(summary.ottfb_p95_ms).toBe(60.79);

    const ts = shapeOriginTimeseriesRows(
      [{ ts: "2026-06-15T00:00:00.000Z", miss_count: 40, value: 32.456, edge: true }],
      true,
    );
    expect(ts.has_data).toBe(true);
    expect(ts.series[0]?.value).toBe(32.46);
    expect(ts.series[0]?.edge).toBe(true);

    const status = shapeOriginStatusCodesRows([
      { status: 200, count: 80 },
      { status: 503, count: 20 },
    ]);
    expect(status.rows[1]?.pct).toBe(20);

    const path = shapeOriginPathBreakdownRows([
      { edge: true, requests: 100, p50_ms: 12, p95_ms: 30 },
      { edge: false, requests: 40, p50_ms: 8, p95_ms: 18 },
    ]);
    expect(path.shielding_detected).toBe(true);

    expect(isPopLatencyElevated(30.0, 20.0, 1.5)).toBe(false);
    expect(isPopLatencyElevated(30.1, 20.0, 1.5)).toBe(true);

    const popEmpty = shapeOriginPopLatencyRows([], { requiresGroupC: true });
    expect(popEmpty.requires_group_c).toBe(true);

    const popLat = shapeOriginPopLatencyRows([
      { pop: "SJC", requests: 100, p50_ms: 10, p95_ms: 20 },
      { pop: "IAD", requests: 100, p50_ms: 11, p95_ms: 20 },
      { pop: "SYD", requests: 50, p50_ms: 25, p95_ms: 45 },
    ]);
    expect(popLat.median_p95_ms).toBe(20);
    expect(popLat.rows.find((r) => r.pop === "SYD")?.elevated).toBe(true);
    expect(popLat.rows.find((r) => r.pop === "SJC")?.elevated).toBe(false);

    const ipHealth = shapeOriginIpHealthRows([
      { oip: "203.0.113.10", requests: 55, p50_ms: 14.2, p95_ms: 39.8, error_pct: 3.6 },
    ]);
    expect(ipHealth.rows[0]?.oip).toBe("203.0.113.10");

    const slow = shapeOriginSlowUrlsRows([
      { url: "/api/v1/report", requests: 25, p50_ms: 120.4, p95_ms: 450.8, p99_ms: 890.1 },
    ]);
    expect(slow.rows[0]?.p95_ms).toBe(450.8);

    const dd = shapeOriginDrilldownRows(
      [{ dim_key: "origin.example.com", requests: 90, p50_ms: 12, p95_ms: 35, p99_ms: 60, error_rate: 1.5, bytes: 10240 }],
      "host",
    );
    expect(dd.has_data).toBe(true);
    expect(dd.rows[0]?.key).toBe("origin.example.com");
  });

  it("falls back from min_requests = 10 to 1 for slow_urls and ip_health when primary query returns empty", async () => {
    const queries: string[] = [];
    const executor: OriginSqlExecutor = {
      query: vi.fn(async (sql: string) => {
        queries.push(sql);
        if (sql.includes("HAVING COUNT(*) >= 10")) return [];
        return [{ url: "/rare-slow", oip: "198.51.100.7", requests: 2, p50_ms: 100, p95_ms: 250, p99_ms: 300, error_pct: 50 }];
      }),
    };
    const cols = new Set(["url", "ottfb", "oip", "ost"]);

    const slowRes = await queryOriginSlowUrlsWithFallback(executor, "requests", "TRUE", cols, 10, 20);
    expect(slowRes.has_data).toBe(true);
    expect(slowRes.rows[0]?.url).toBe("/rare-slow");

    const ipRes = await queryOriginIpHealthWithFallback(executor, "requests", "TRUE", cols, 30, 10);
    expect(ipRes.has_data).toBe(true);
    expect(ipRes.rows[0]?.oip).toBe("198.51.100.7");
    expect(queries.length).toBe(4);
  });

  it("enriches shielding routes with great-circle POP distance and anomaly eligibility", () => {
    const route = enrichShieldingRouteWithPopDistance({
      edge_pop: "SJC",
      shield_pop: "IAD",
      requests: 100,
      p50_ms: 180,
      p95_ms: 260,
      p99_ms: 350,
    });
    expect(route.distance_km).not.toBeNull();
    expect(route.distance_km!).toBeGreaterThan(3800);
    expect(route.anomaly_eligible).toBe(true);
    expect(route.anomaly_static).toBe(true);
  });

  it("dispatches /api/origin/* routes via handleOriginRequest", async () => {
    const mockExecutor: OriginSqlExecutor = {
      tableName: "requests",
      getColumns: vi.fn(async () => new Set(["timestamp", "cache", "ottfb", "ttfb", "url", "ost", "edge", "pop", "oip", "host", "rid", "prid"])),
      query: vi.fn(async () => [
        {
          total_misses: 10,
          total_passes: 5,
          ottfb_p50_ms: 12,
          ottfb_p95_ms: 30,
          ts: "2026-06-15T00:00:00Z",
          miss_count: 10,
          value: 25,
          url: "/item",
          requests: 15,
          p50_ms: 12,
          p95_ms: 30,
          p99_ms: 45,
          status: 200,
          count: 15,
          edge: true,
          pop: "IAD",
          oip: "192.0.2.1",
          error_pct: 0,
          edge_pop: "SJC",
          shield_pop: "IAD",
          dim_key: "origin.example.com",
          error_rate: 0,
          bytes: 2048,
        },
      ]),
    };
    setOriginSqlExecutor(mockExecutor);

    const aggRes = await handleOriginRequest(
      new Request("https://example.test/api/origin/aggregates", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sections: ["summary", "pop_latency"] }),
      }),
    );
    expect(aggRes?.status).toBe(200);
    const aggData = await aggRes!.json();
    expect(aggData.summary.has_data).toBe(true);
    expect(aggData.pop_latency.has_data).toBe(true);

    const shieldData = await queryOriginShieldingAnalysis({}, undefined, mockExecutor);
    expect(shieldData.has_data).toBe(true);
    expect(shieldData.rows[0]?.edge_pop).toBe("SJC");

    const drillData = await queryOriginDrilldown({ dimension: "host" }, undefined, mockExecutor);
    expect(drillData.has_data).toBe(true);

    const fullAgg = await queryOriginAggregates({}, undefined, mockExecutor);
    expect(fullAgg.has_data).toBe(true);

    const unrelated = await handleOriginRequest(new Request("https://example.test/api/other"));
    expect(unrelated).toBeNull();
  });
});
