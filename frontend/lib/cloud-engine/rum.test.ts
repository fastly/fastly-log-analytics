// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it, vi } from "vitest";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";
import { fetchLanes } from "./fetch-lanes";
import { queryRumWireFromRollup, type RumRollupSource } from "./rum-rollup";
import { DuckDBAnalyticalEngine, getDuckDBEngine } from "./dashboard/test-fixtures";
import { redactQueryRowsForPii } from "./query";
import { cloudRumAnalytics, cloudRumLiveEvents, cloudRumRequestLog, cloudRumUsage } from "./rum-wire";
import * as rumModule from "./rum";
import {
  classifyCwvRating,
  emptyRumAnalytics,
  handleRumRequest,
  queryRumAnalytics,
  queryRumBeaconHealth,
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

  it("ranks worst pages by error rate and reports real page load time like High-Scale", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      const busy = Array.from({ length: 10 }, (_, i) =>
        `('rum', ${1700000000000 + i * 1000}, 'busy-${i}', 'LCP', 1000, '/busy')`).join(", ");
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, request_event_id, metric_name, metric_value, page_url)
        VALUES ${busy}, ('rum', 1700000020000, 'broken-0', 'LCP', 3000, '/broken'),
          ('rum', 1700000020000, 'broken-0', 'TTFB', 1000, '/broken'),
          ('rum', 1700000020000, 'broken-0', 'page_load', 2000, '/broken'),
          ('rum', 1700000040000, 'rid00001', 'ttlb', 90000, '/busy')`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, request_event_id, error_message, page_url)
        VALUES ('rum', 1700000030000, 'busy-e1', 'x', '/busy'),
          ('rum', 1700000031000, 'busy-e2', 'x', '/busy'),
          ('rum', 1700000032000, 'broken-e1', 'x', '/broken')`);
      const analytics = await cloudRumAnalytics({ engine });
      const pages = analytics.worst_pages;
      expect(pages.map((page) => page.path)).toEqual(["/broken", "/busy"]);
      expect(pages[0]).toMatchObject({ views: 1, avg_load_time: 2, error_rate: 50 });
      // A resource timing measures one request, not a page view: it adds no view, and a page
      // with no load timing reports none instead of an average of unrelated vitals.
      expect(pages[1]).toMatchObject({ views: 10, avg_load_time: null, error_rate: 16.67 });
      expect(analytics.pageview_count).toBe(11);
    } finally {
      await engine.reset();
    }
  });

  it("sets no_data message and null CWV distributions when no beacons exist", async () => {
    const engine = getDuckDBEngine();
    try {
      const data = await cloudRumAnalytics({});
      expect(data.no_data).toBe(true);
      expect(data.beacon_count).toBe(0);
      expect(data).toHaveProperty("message", "Waiting for real-time RUM user events...");
      for (const name of ["lcp", "cls", "inp"] as const) {
        expect(data.vitals[name]).toEqual({
          p75: null,
          distribution: null,
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

  it("analytics from rum_summary equal raw on the fixture", async () => {
    const T0 = 1_800_000_000_000;
    const T1 = T0 + 300_000;
    const vitals = [
      ...Array.from({ length: 10 }, (_, i) => ({
        ts: T0 + (i + 1) * 1000, metric: "LCP", val: 1500 + i * 350, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: `s${i % 3 + 1}`,
      })),
      { ts: T0 + 14_000, metric: "LCP", val: 2500, path: "/home", browser: "Safari", os: "iOS", device: "mobile", cid: "s3" },
      { ts: T0 + 15_000, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_100, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_200, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_300, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_400, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_500, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      { ts: T0 + 15_600, metric: "CLS", val: 0, path: "/home", browser: "Chrome", os: "macOS", device: "desktop", cid: "s1" },
      ...Array.from({ length: 14 }, (_, i) => ({
        ts: T0 + 16_000 + i * 100, metric: "CLS", val: Number((0.02 * (i + 1)).toFixed(2)), path: i === 0 ? "/only-one" : "/slow", browser: "Firefox", os: "Linux", device: "desktop", cid: "s4",
      })),
      { ts: T0 + 19_000, metric: "INP", val: 150, path: "/slow", browser: "Firefox", os: "Linux", device: "desktop", cid: "s4" },
      { ts: T0 + 20_000, metric: "FCP", val: 1200, path: "/slow", browser: "Firefox", os: "Linux", device: "desktop", cid: "s4" },
      { ts: T0 + 21_000, metric: "TTFB", val: 400, path: "/slow", browser: "Firefox", os: "Linux", device: "desktop", cid: null },
    ];
    const errors = [
      { ts: T0 + 10_000, msg: "TypeError: null", file: "app.js", line: 20, col: 5, cid: "s2" },
      { ts: T0 + 12_000, msg: "TypeError: null", file: "app.js", line: 10, col: 3, cid: "s3" },
      { ts: T0 + 15_000, msg: "RangeError", file: "lib.js", line: 1, col: 1, cid: "s2" },
    ];

    const engine = new DuckDBAnalyticalEngine();
    try {
      const vitalValues = vitals.map((v) =>
        `('r', epoch_ms(${v.ts})::TIMESTAMPTZ, ${v.ts}, '${v.metric}', ${v.val}, '${v.path}', '${v.browser}', '${v.os}', '${v.device}', ${v.cid ? `'${v.cid}'` : "NULL"})`
      ).join(", ");
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, "timestamp", ts_ms, metric_name, metric_value, page_url, browser_name, os_name, device_type, session_id)
        VALUES ${vitalValues}`);
      const errorValues = errors.map((e) =>
        `('e', epoch_ms(${e.ts})::TIMESTAMPTZ, ${e.ts}, '${e.msg}', '${e.file}', ${e.line}, ${e.col}, '${e.cid}')`
      ).join(", ");
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, "timestamp", ts_ms, error_message, filename, lineno, colno, session_id)
        VALUES ${errorValues}`);

      const raw = await queryRumAnalytics({ engine, start_time: T0, end_time: T1 });

      // Build the matching rum_summary Parquet buffer
      const addHist = (h: Histogram, v: number) => {
        h.add(Math.round(v));
      };

      const rows: {
        kind: string; bucket: bigint | null; key: string | null; count: bigint | null;
        good: bigint | null; poor: bigint | null; value_hist: Uint8Array | null;
        lcp_hist: Uint8Array | null; cls_hist: Uint8Array | null; inp_hist: Uint8Array | null;
        cid_hll: Uint8Array | null; msg_hll: Uint8Array | null;
        min_line: bigint | null; min_col: bigint | null; last_ts: bigint | null; truncated: boolean | null;
      }[] = [];

      const byMetric = new Map<string, { count: number; good: number; poor: number; hist: Histogram }>();
      const byPage = new Map<string, { count: number; lcp: Histogram; cls: Histogram; inp: Histogram }>();
      const byEnv = new Map<string, number>();
      const vHll = new Hll();
      const thresholds: Record<string, [number, number]> = {
        LCP: [2500, 4000], CLS: [0.1, 0.25], INP: [200, 500], FCP: [1800, 3000], TTFB: [800, 1800],
      };

      for (const v of vitals) {
        if (v.cid) vHll.add(v.cid);
        const m = byMetric.get(v.metric) ?? { count: 0, good: 0, poor: 0, hist: new Histogram() };
        m.count++;
        const [g, p] = thresholds[v.metric];
        if (v.val <= g) m.good++;
        if (v.val > p) m.poor++;
        addHist(m.hist, v.val * (v.metric === "CLS" ? 1_000_000 : 1_000));
        byMetric.set(v.metric, m);

        const pg = byPage.get(v.path) ?? { count: 0, lcp: new Histogram(), cls: new Histogram(), inp: new Histogram() };
        pg.count++;
        if (v.metric === "LCP") addHist(pg.lcp, v.val * 1_000);
        if (v.metric === "CLS") addHist(pg.cls, v.val * 1_000_000);
        if (v.metric === "INP") addHist(pg.inp, v.val * 1_000);
        byPage.set(v.path, pg);

        for (const [dim, val] of [["browser", v.browser], ["os", v.os], ["device", v.device]] as const) {
          const k = JSON.stringify([dim, val]);
          byEnv.set(k, (byEnv.get(k) ?? 0) + 1);
        }
      }

      for (const [name, m] of byMetric) {
        rows.push({
          kind: "metric", bucket: BigInt(T0), key: name, count: BigInt(m.count),
          good: BigInt(m.good), poor: BigInt(m.poor), value_hist: m.hist.toBytes(),
          lcp_hist: null, cls_hist: null, inp_hist: null, cid_hll: null, msg_hll: null,
          min_line: null, min_col: null, last_ts: null, truncated: null,
        });
      }
      rows.push({
        kind: "vitals_total", bucket: BigInt(T0), key: null, count: BigInt(vitals.length),
        good: null, poor: null, value_hist: null, lcp_hist: null, cls_hist: null, inp_hist: null,
        cid_hll: vHll.toBytes(), msg_hll: null, min_line: null, min_col: null, last_ts: null, truncated: null,
      });
      for (const [path, pg] of byPage) {
        rows.push({
          kind: "page", bucket: null, key: path, count: BigInt(pg.count),
          good: null, poor: null, value_hist: null,
          lcp_hist: pg.lcp.toBytes(), cls_hist: pg.cls.toBytes(), inp_hist: pg.inp.toBytes(),
          cid_hll: null, msg_hll: null, min_line: null, min_col: null, last_ts: null, truncated: false,
        });
      }
      for (const [k, cnt] of byEnv) {
        rows.push({
          kind: "env", bucket: null, key: k, count: BigInt(cnt),
          good: null, poor: null, value_hist: null, lcp_hist: null, cls_hist: null, inp_hist: null,
          cid_hll: null, msg_hll: null, min_line: null, min_col: null, last_ts: null, truncated: false,
        });
      }

      const eCidHll = new Hll();
      const eMsgHll = new Hll();
      const byErr = new Map<string, { count: number; minLine: number; minCol: number; lastTs: number; hll: Hll }>();
      for (const e of errors) {
        eCidHll.add(e.cid);
        eMsgHll.add(e.msg);
        const k = JSON.stringify([e.msg, e.file]);
        const cur = byErr.get(k) ?? { count: 0, minLine: e.line, minCol: e.col, lastTs: e.ts, hll: new Hll() };
        cur.count++;
        cur.minLine = Math.min(cur.minLine, e.line);
        cur.minCol = Math.min(cur.minCol, e.col);
        cur.lastTs = Math.max(cur.lastTs, e.ts);
        cur.hll.add(e.cid);
        byErr.set(k, cur);
      }
      rows.push({
        kind: "errors_total", bucket: BigInt(T0), key: null, count: BigInt(errors.length),
        good: null, poor: null, value_hist: null, lcp_hist: null, cls_hist: null, inp_hist: null,
        cid_hll: eCidHll.toBytes(), msg_hll: eMsgHll.toBytes(), min_line: null, min_col: null, last_ts: null, truncated: null,
      });
      for (const [k, er] of byErr) {
        rows.push({
          kind: "error", bucket: null, key: k, count: BigInt(er.count),
          good: null, poor: null, value_hist: null, lcp_hist: null, cls_hist: null, inp_hist: null,
          cid_hll: er.hll.toBytes(), msg_hll: null, min_line: BigInt(er.minLine), min_col: BigInt(er.minCol),
          last_ts: BigInt(er.lastTs), truncated: false,
        });
      }

      const summaryBytes = parquetWriteBuffer({
        columnData: [
          { name: "kind", type: "STRING", data: rows.map((r) => r.kind) },
          { name: "bucket", type: "INT64", data: rows.map((r) => r.bucket) },
          { name: "key", type: "STRING", data: rows.map((r) => r.key) },
          { name: "count", type: "INT64", data: rows.map((r) => r.count) },
          { name: "good", type: "INT64", data: rows.map((r) => r.good) },
          { name: "poor", type: "INT64", data: rows.map((r) => r.poor) },
          { name: "value_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.value_hist) },
          { name: "lcp_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.lcp_hist) },
          { name: "cls_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.cls_hist) },
          { name: "inp_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.inp_hist) },
          { name: "cid_hll", type: "BYTE_ARRAY", data: rows.map((r) => r.cid_hll) },
          { name: "msg_hll", type: "BYTE_ARRAY", data: rows.map((r) => r.msg_hll) },
          { name: "min_line", type: "INT64", data: rows.map((r) => r.min_line) },
          { name: "min_col", type: "INT64", data: rows.map((r) => r.min_col) },
          { name: "last_ts", type: "INT64", data: rows.map((r) => r.last_ts) },
          { name: "truncated", type: "BOOLEAN", data: rows.map((r) => r.truncated) },
        ],
        kvMetadata: [{ key: "flp_rum_summary", value: "1" }],
      });

      const rawKey = `processed/rum/client_vitals/year=2027/month=01/day=15/hour=08/window=${T0 / 1000}-${T1 / 1000}-p1s0-n9.parquet`;
      const sumKey = `processed/rum/summary/year=2027/month=01/day=15/hour=08/summary=${T0 / 1000}-${T1 / 1000}-p1s0-n12.parquet`;
      const rollupSource: RumRollupSource = {
        async fetchJson(url) {
          const view = new URL(url, "http://localhost").searchParams.get("view");
          if (view === "summary") {
            return {
              files: [{ key: sumKey, start: new Date(T0).toISOString(), end: new Date(T1).toISOString(), size: summaryBytes.byteLength, rows: rows.length }],
              next_cursor: null,
              complete: true,
            };
          }
          return {
            files: [{ key: rawKey, start: new Date(T0).toISOString(), end: new Date(T1).toISOString(), size: 100, rows: vitals.length }],
            next_cursor: null,
            complete: true,
          };
        },
        async fetchFile(key) {
          expect(key).toBe(sumKey);
          return { bytes: summaryBytes, cached: false };
        },
      };

      const sortEnv = (e: typeof raw.environments) => ({
        browsers: [...e.browsers].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
        os: [...e.os].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
        devices: [...e.devices].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
        connections: [...e.connections].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
      });

      const rolled = await queryRumAnalytics({ start_time: T0, end_time: T1, rollupSource });
      expect(rolled.no_data).toBe(false);
      expect(rolled.summary).toEqual(raw.summary);
      expect(sortEnv(rolled.environments)).toEqual(sortEnv(raw.environments));
      expect(rolled.error_summary).toEqual(raw.error_summary);
      expect(rolled.top_errors).toEqual(raw.top_errors);

      for (const name of ["LCP", "CLS", "INP", "FCP", "TTFB"] as const) {
        expect(rolled.metrics[name].count).toBe(raw.metrics[name].count);
        expect(rolled.metrics[name].good_pct).toBe(raw.metrics[name].good_pct);
        expect(rolled.metrics[name].needs_improvement_pct).toBe(raw.metrics[name].needs_improvement_pct);
        expect(rolled.metrics[name].poor_pct).toBe(raw.metrics[name].poor_pct);
        for (const q of ["p50", "p75", "p95"] as const) {
          const want = raw.metrics[name][q];
          const got = rolled.metrics[name][q];
          expect(Math.abs(got - want)).toBeLessThanOrEqual(Math.max(want * 0.08, name === "CLS" ? 0.02 : 2));
        }
      }

      expect(rolled.worst_pages.map((p) => [p.page_url, p.sample_count]))
        .toEqual(raw.worst_pages.map((p) => [p.page_url, p.sample_count]));

      // Filtered queries bypass rum_summary and hit raw DuckDB (service still has RUM data, so no_data stays false)
      const filtered = await queryRumAnalytics({ engine, start_time: T0, end_time: T1, rollupSource, country: "US" });
      expect(filtered.summary.total_vitals_events).toBe(0);
      expect(filtered.no_data).toBe(false);

      // A listed summary whose object is gone leaves the read to raw DuckDB
      const gone: RumRollupSource = {
        fetchJson: rollupSource.fetchJson,
        async fetchFile(key) {
          throw new MissingCloudFileError(key);
        },
      };
      const fallback = await queryRumAnalytics({ engine, start_time: T0, end_time: T1, rollupSource: gone });
      expect(fallback.summary).toEqual(raw.summary);
      expect(fallback.top_errors).toEqual(raw.top_errors);
    } finally {
      await engine.reset();
    }
  });

  it("excludes empty-string session_id and error_message from DISTINCT counts", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, session_id)
        VALUES ('v', 1700000000000, 'LCP', 1500, 's1'),
               ('v', 1700000001000, 'LCP', 1600, ''),
               ('v', 1700000002000, 'LCP', 1700, NULL)`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, error_message, message, filename, session_id)
        VALUES ('e', 1700000000000, 'TypeError', NULL, 'app.js', 's1'),
               ('e', 1700000001000, 'TypeError', NULL, 'app.js', ''),
               ('e', 1700000002000, '', '', 'app.js', '')`);

      const data = await queryRumAnalytics({ engine });
      expect(data.summary.unique_sessions).toBe(1);
      expect(data.error_summary.affected_sessions).toBe(1);
      expect(data.error_summary.unique_error_messages).toBe(1);
      const typeErr = data.top_errors.find((e) => e.message === "TypeError");
      expect(typeErr?.affected_sessions).toBe(1);
    } finally {
      await engine.reset();
    }
  });

  it("applies case-insensitive RUM filters, returns no_data=false when data exists outside filter, and includes live event context fields", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, device_type, browser_name, os_name, country, region, city, pop, tls, ttfb)
        VALUES ('v', 1700000000000, 'LCP', 1800, 'Desktop', 'Chrome', 'macOS', 'US', 'VA', 'Ashburn', 'IAD', 'TLSv1.3', 0.042)`);
      const matched = await queryRumAnalytics({
        engine,
        filters: { device: { mode: "include", values: ["desktop"] } },
      });
      expect(matched.summary.total_vitals_events).toBe(1);

      const filteredEmpty = await queryRumAnalytics({
        engine,
        filters: { device: { mode: "include", values: ["tablet"] } },
      });
      expect(filteredEmpty.summary.total_vitals_events).toBe(0);
      expect(filteredEmpty.no_data).toBe(false);

      const live = await queryRumLiveEvents({
        engine,
        filters: { browser: { mode: "include", values: ["chrome"] } },
      });
      expect(live.events).toHaveLength(1);
      expect(live.events[0]).toMatchObject({
        region: "VA",
        city: "Ashburn",
        pop: "IAD",
        tls: "TLSv1.3",
        ttfb: 0.042,
      });
    } finally {
      await engine.reset();
    }
  });

  it("scales Cloud RUM counts by sample_rate, populates fcp/ttfb vitals, and pads >48h trends daily", async () => {
    const engine = getDuckDBEngine();
    try {
      const t0 = Date.parse("2026-09-01T00:10:00Z");
      const t3 = Date.parse("2026-09-03T12:10:00Z");
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, client_id, metric_name, metric_value, page_url, browser_name, sample_rate)
        VALUES
          ('rum', ${t0}, 'sampled', 'LCP', 1000, '/home', 'Firefox', 0.5),
          ('rum', ${t0}, 'sampled', 'CLS', 0.1, '/home', 'Firefox', 0.5),
          ('rum', ${t0}, 'sampled', 'FCP', 1500, '/home', 'Firefox', 0.5),
          ('rum', ${t0}, 'sampled', 'TTFB', 500, '/home', 'Firefox', 0.5),
          ('rum', ${t0}, 'sampled', 'event_click', 1, '/home', 'Firefox', 0.5),
          ('rum', ${t3}, 'unsampled', 'LCP', 3000, '/home', 'Chrome', 1.0),
          ('rum', ${t3}, 'unsampled', 'ttlb', 250, '/home', 'Chrome', 1.0)`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, client_id, error_message, page_url, sample_rate)
        VALUES
          ('rum', ${t0}, 'sampled', 'Boom', '/home', 0.25),
          ('rum', ${t0}, 'sampled', 'Boom', '/home', 0.25)`);

      const data = await cloudRumAnalytics({
        start_time: "2026-09-01T00:00:00Z",
        end_time: "2026-09-04T00:00:00Z",
      });
      expect(data.estimated).toBe(true);
      expect(data.pageview_count).toBe(3);
      expect(data.interaction_count).toBe(2);
      expect(data.error_count).toBe(4);
      expect(data.beacon_count).toBe(5);
      expect(data.vitals.fcp).toEqual({
        p75: 1.5,
        distribution: { good: 100, needs_improvement: 0, poor: 0 },
      });
      expect(data.vitals.ttfb).toEqual({
        p75: 0.5,
        distribution: { good: 100, needs_improvement: 0, poor: 0 },
      });
      expect(data.environments.browsers).toEqual({ Firefox: 2, Chrome: 1 });
      expect(data.errors[0].count).toBe(8);
      // >48h range uses 3 dense UTC daily buckets (Sep 1, Sep 2 empty, Sep 3)
      expect(data.trends.timestamps).toEqual([
        "2026-09-01T00:00:00.000Z",
        "2026-09-02T00:00:00.000Z",
        "2026-09-03T00:00:00.000Z",
      ]);
      expect(data.trends.pageviews).toEqual([2, 0, 1]);
      expect(data.trends.errors).toEqual([4, 0, 0]);
      expect(data.trends.fcp).toEqual([1.5, null, null]);
      expect(data.trends.ttfb).toEqual([0.5, null, null]);
      expect(data.trends.error_rate).toEqual([66.67, null, 0]);
    } finally {
      await engine.reset();
    }
  });

  it("deduplicates hour_bundle over window summaries, covers compacted raw files, uses fetchLanes, preserves FCP/TTFB in vitals and trends, and skips raw DuckDB", async () => {
    const T_H8 = Date.UTC(2027, 0, 15, 8, 0, 0);
    const T_H9 = Date.UTC(2027, 0, 15, 9, 0, 0);
    const T_OPEN_END = T_H9 + 3 * 60_000;
    const T_END = T_H9 + 5 * 60_000;

    function buildRumSummaryBuf(opts: {
      bucketMs: number;
      lcpMs: number;
      clsVal: number;
      inpMs: number;
      fcpMs: number;
      ttfbMs: number;
      cid: string;
      sampled?: boolean;
    }): ArrayBuffer {
      const hist = (scaled: number) => {
        const h = new Histogram();
        h.add(scaled);
        return h.toBytes();
      };
      const cidHll = new Hll();
      cidHll.add(opts.cid);
      const msgHll = new Hll();
      msgHll.add("TypeError: boom");

      type Row = {
        kind: string;
        bucket: bigint | null;
        key: string | null;
        count: bigint | null;
        good: bigint | null;
        poor: bigint | null;
        value_hist: Uint8Array | null;
        lcp_hist: Uint8Array | null;
        cls_hist: Uint8Array | null;
        inp_hist: Uint8Array | null;
        cid_hll: Uint8Array | null;
        msg_hll: Uint8Array | null;
        min_line: bigint | null;
        min_col: bigint | null;
        last_ts: bigint | null;
        truncated: boolean | null;
      };
      const blank = {
        bucket: null, key: null, count: null, good: null, poor: null,
        value_hist: null, lcp_hist: null, cls_hist: null, inp_hist: null,
        cid_hll: null, msg_hll: null, min_line: null, min_col: null, last_ts: null, truncated: null,
      };
      const rows: Row[] = [
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "LCP", count: BigInt(1), good: BigInt(1), poor: BigInt(0), value_hist: hist(opts.lcpMs * 1000) },
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "CLS", count: BigInt(1), good: BigInt(1), poor: BigInt(0), value_hist: hist(opts.clsVal * 1_000_000) },
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "INP", count: BigInt(1), good: BigInt(1), poor: BigInt(0), value_hist: hist(opts.inpMs * 1000) },
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "FCP", count: BigInt(1), good: BigInt(1), poor: BigInt(0), value_hist: hist(opts.fcpMs * 1000) },
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "TTFB", count: BigInt(1), good: BigInt(1), poor: BigInt(0), value_hist: hist(opts.ttfbMs * 1000) },
        { ...blank, kind: "metric", bucket: BigInt(opts.bucketMs), key: "EVENT_CLICK", count: BigInt(2), good: BigInt(0), poor: BigInt(0), value_hist: hist(1000) },
        { ...blank, kind: "vitals_total", bucket: BigInt(opts.bucketMs), count: BigInt(7), cid_hll: cidHll.toBytes() },
        { ...blank, kind: "page", key: "/checkout", count: BigInt(1), lcp_hist: hist(opts.lcpMs * 1000), cls_hist: hist(opts.clsVal * 1_000_000), inp_hist: hist(opts.inpMs * 1000), truncated: false },
        { ...blank, kind: "env", key: JSON.stringify(["browser", "Chrome"]), count: BigInt(1), truncated: false },
        { ...blank, kind: "env", key: JSON.stringify(["os", "macOS"]), count: BigInt(1), truncated: false },
        { ...blank, kind: "env", key: JSON.stringify(["device", "desktop"]), count: BigInt(1), truncated: false },
        { ...blank, kind: "errors_total", bucket: BigInt(opts.bucketMs), count: BigInt(1), cid_hll: cidHll.toBytes(), msg_hll: msgHll.toBytes() },
        { ...blank, kind: "error", key: JSON.stringify(["TypeError: boom", "app.js"]), count: BigInt(1), cid_hll: cidHll.toBytes(), min_line: BigInt(12), min_col: BigInt(4), last_ts: BigInt(opts.bucketMs), truncated: false },
      ];
      if (opts.sampled) {
        rows.push({ ...blank, kind: "sampled", count: BigInt(1) });
      }
      return parquetWriteBuffer({
        columnData: [
          { name: "kind", type: "STRING", data: rows.map((r) => r.kind) },
          { name: "bucket", type: "INT64", data: rows.map((r) => r.bucket) },
          { name: "key", type: "STRING", data: rows.map((r) => r.key) },
          { name: "count", type: "INT64", data: rows.map((r) => r.count) },
          { name: "good", type: "INT64", data: rows.map((r) => r.good) },
          { name: "poor", type: "INT64", data: rows.map((r) => r.poor) },
          { name: "value_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.value_hist) },
          { name: "lcp_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.lcp_hist) },
          { name: "cls_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.cls_hist) },
          { name: "inp_hist", type: "BYTE_ARRAY", data: rows.map((r) => r.inp_hist) },
          { name: "cid_hll", type: "BYTE_ARRAY", data: rows.map((r) => r.cid_hll) },
          { name: "msg_hll", type: "BYTE_ARRAY", data: rows.map((r) => r.msg_hll) },
          { name: "min_line", type: "INT64", data: rows.map((r) => r.min_line) },
          { name: "min_col", type: "INT64", data: rows.map((r) => r.min_col) },
          { name: "last_ts", type: "INT64", data: rows.map((r) => r.last_ts) },
          { name: "truncated", type: "BOOLEAN", data: rows.map((r) => r.truncated) },
        ],
        kvMetadata: [{ key: "flp_rum_summary", value: "1" }],
      });
    }

    const hourKey = "processed/summary_bundle/rum/year=2027/month=01/day=15/hour_bundle=08-c60-n600-h0123456789abcdef.parquet";
    const redundantWinKey = `processed/rum/summary/year=2027/month=01/day=15/hour=08/summary=${T_H8 / 1000}-${(T_H8 + 60_000) / 1000}-p0s0-n10.parquet`;
    const openKey = `processed/summary_bundle/rum/year=2027/month=01/day=15/open_bundle=09-${T_OPEN_END}-c3-n30-h0123456789abcdef.parquet`;
    const trailingWinKey = `processed/rum/summary/year=2027/month=01/day=15/hour=09/summary=${T_OPEN_END / 1000}-${T_END / 1000}-p4s0-n10.parquet`;
    const compactRawKey = `processed/rum/client_vitals/year=2027/month=01/day=15/hour=08/compact=${T_H8 / 1000}-${T_H9 / 1000}-c1-n600.parquet`;
    const openRawKey = `processed/rum/client_vitals/year=2027/month=01/day=15/hour=09/window=${T_H9 / 1000}-${T_END / 1000}-p1s0-n40.parquet`;

    const buffers: Record<string, ArrayBuffer> = {
      [hourKey]: buildRumSummaryBuf({ bucketMs: T_H8, lcpMs: 2000, clsVal: 0.05, inpMs: 120, fcpMs: 1100, ttfbMs: 400, cid: "c1" }),
      [openKey]: buildRumSummaryBuf({ bucketMs: T_H9, lcpMs: 2400, clsVal: 0.08, inpMs: 150, fcpMs: 1400, ttfbMs: 600, cid: "c2" }),
      [trailingWinKey]: buildRumSummaryBuf({ bucketMs: T_H9, lcpMs: 2400, clsVal: 0.08, inpMs: 150, fcpMs: 1400, ttfbMs: 600, cid: "c3" }),
    };

    const fetchedKeys: string[] = [];
    const rollupSource: RumRollupSource = {
      async fetchJson(url) {
        const view = new URL(url, "http://localhost").searchParams.get("view");
        if (view === "summary") {
          return {
            files: [
              { key: hourKey, kind: "hour", start: new Date(T_H8).toISOString(), end: new Date(T_H9).toISOString(), size: 100, rows: 10 },
              { key: redundantWinKey, start: new Date(T_H8).toISOString(), end: new Date(T_H8 + 60_000).toISOString(), size: 50, rows: 5 },
              { key: openKey, kind: "hour", start: new Date(T_H9).toISOString(), end: new Date(T_OPEN_END).toISOString(), size: 80, rows: 8 },
              { key: trailingWinKey, start: new Date(T_OPEN_END).toISOString(), end: new Date(T_END).toISOString(), size: 40, rows: 4 },
            ],
            next_cursor: null,
            complete: true,
          };
        }
        return {
          files: [
            { key: compactRawKey, start: new Date(T_H8).toISOString(), end: new Date(T_H9).toISOString(), size: 1000, rows: 600 },
            { key: openRawKey, start: new Date(T_H9).toISOString(), end: new Date(T_END).toISOString(), size: 200, rows: 40 },
          ],
          next_cursor: null,
          complete: true,
        };
      },
      async fetchFile(key) {
        fetchedKeys.push(key);
        return { bytes: buffers[key], cached: false };
      },
    };

    const laneSpy = vi.spyOn(fetchLanes, "run");
    const explodingEngine = {
      executeSql: vi.fn(() => {
        throw new Error("raw DuckDB must not be queried when RUM rollup covers range");
      }),
    } as unknown as DuckDBAnalyticalEngine;

    try {
      const wire = await cloudRumAnalytics({
        start_time: T_H8,
        end_time: T_END,
        engine: explodingEngine,
        rollupSource,
      });
      // Redundant window summary inside hour 08 is NOT fetched.
      expect(fetchedKeys.sort()).toEqual([hourKey, openKey, trailingWinKey].sort());
      expect(laneSpy).toHaveBeenCalledWith("summary", expect.any(Function));
      expect(explodingEngine.executeSql).not.toHaveBeenCalled();

      expect(wire.no_data).toBe(false);
      expect(wire.estimated).toBe(false);
      expect(wire.vitals.fcp.p75).toBeGreaterThan(1.0);
      expect(wire.vitals.ttfb.p75).toBeGreaterThan(0.3);
      expect(wire.vitals.fid.fcp).toBe(wire.vitals.fcp.p75);
      expect(wire.vitals.fid.ttfb).toBe(wire.vitals.ttfb.p75);
      expect(wire.trends.timestamps).toEqual([
        "2027-01-15T08:00:00.000Z",
        "2027-01-15T09:00:00.000Z",
      ]);
      expect(wire.trends.fcp[0]).toBeGreaterThan(1.0);
      expect(wire.trends.fcp[1]).toBeGreaterThan(1.3);
      expect(wire.trends.ttfb[0]).toBeGreaterThan(0.3);
      expect(wire.trends.ttfb[1]).toBeGreaterThan(0.5);
      expect(wire.worst_pages[0].path).toBe("/checkout");
      expect(wire.errors[0].message).toBe("TypeError: boom");
      expect(wire.environments.browsers).toEqual({ Chrome: 3 });

      // A sampled summary returns null from queryRumWireFromRollup so raw DuckDB handles 1/sample_rate weighting.
      const sampledBuf = buildRumSummaryBuf({
        bucketMs: T_H8, lcpMs: 2000, clsVal: 0.05, inpMs: 120, fcpMs: 1100, ttfbMs: 400, cid: "c1", sampled: true,
      });
      const sampledSource: RumRollupSource = {
        ...rollupSource,
        async fetchFile(key) {
          return { bytes: key === hourKey ? sampledBuf : buffers[key], cached: false };
        },
      };
      const sampledWire = await queryRumWireFromRollup(sampledSource, { start_time: T_H8, end_time: T_END });
      expect(sampledWire).toBeNull();
    } finally {
      laneSpy.mockRestore();
    }
  });

  it("second-aligns listing bounds and skips duplicate rum/raw fetch when rawManifest is pre-supplied", async () => {
    const startMs = Date.parse("2027-01-15T08:00:00.250Z");
    const endMs = Date.parse("2027-01-15T09:00:00.250Z");
    const seenUrls: string[] = [];
    const source: RumRollupSource = {
      async fetchJson(url) {
        seenUrls.push(url);
        return { files: [], next_cursor: null, complete: true };
      },
      async fetchFile() {
        throw new Error("should not fetch files for empty manifest");
      },
    };

    await queryRumWireFromRollup(
      source,
      { start_time: startMs, end_time: endMs },
      { rawManifest: { files: [], complete: true } },
    );

    expect(seenUrls).toHaveLength(1);
    const params = new URL(seenUrls[0], "http://localhost").searchParams;
    expect(params.get("view")).toBe("summary");
    expect(params.get("from")).toBe("2027-01-15T08:00:00.000Z");
    expect(params.get("to")).toBe("2027-01-15T09:00:01.000Z");
  });

  it("joins client ttlb resource timings to requests, coerces Utf8 asn strings, and masks request-log PII", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      const vitalsBytes = parquetWriteBuffer({
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: [new Date(1700000005000), new Date(1700000010000), new Date(1700000020000)] },
          { name: "metric_name", type: "STRING", data: ["LCP", "ttlb", "ttlb"] },
          { name: "metric_value", type: "DOUBLE", data: [2100, 1200, 800] },
          { name: "metric_rating", type: "STRING", data: ["good", "poor", "needs_improvement"] },
          { name: "pathname", type: "STRING", data: ["/checkout", "/checkout", "/checkout"] },
          { name: "resource_url", type: "STRING", data: ["", "/api/pay", "/api/pay"] },
          { name: "req_id", type: "STRING", data: ["rid-page-1", "rid-join-1", "rid-join-2"] },
          { name: "asn", type: "STRING", data: ["15169", "AS15169", ""] },
          { name: "sample_rate", type: "DOUBLE", data: [0.5, 0.5, 0.5] },
          { name: "browser", type: "STRING", data: ["Chrome", "Chrome", "Firefox"] },
          { name: "os", type: "STRING", data: ["macOS", "macOS", "Linux"] },
          { name: "device", type: "STRING", data: ["Desktop", "Desktop", "Desktop"] },
          { name: "cid", type: "STRING", data: ["c1", "c1", "c2"] },
          { name: "geo_country_code", type: "STRING", data: ["US", "US", "US"] },
          { name: "geo_city", type: "STRING", data: ["Austin", "Austin", "Austin"] },
          { name: "geo_region", type: "STRING", data: ["TX", "TX", "TX"] },
          { name: "server_pop", type: "STRING", data: ["DFW", "DFW", "DFW"] },
          { name: "tls_version", type: "STRING", data: ["TLSv1.3", "TLSv1.3", "TLSv1.3"] },
        ],
        codec: "SNAPPY",
      });
      await engine.syncRumFiles(
        [{ scopedKey: "r:1", key: "r1.parquet", bytes: vitalsBytes }],
        [],
      );
      await engine.executeSql(`INSERT INTO requests
        (scoped_key, ts_ms, rid, method, url, status, pop, ip, asn, elapsed)
        VALUES ('req-ttlb', 1700000010000, 'rid-join-1', 'GET', 'https://shop.test/users/203.0.113.9/pay', 200, 'DFW', '203.0.113.9', 15169, 320500)`);

      const analytics = await cloudRumAnalytics({ engine });
      expect(analytics.estimated).toBe(true);
      expect(analytics.slowest_urls).toHaveLength(1);
      expect(analytics.slowest_urls?.[0]).toMatchObject({
        url: "/api/pay",
        requests: 4,
        matched_requests: 2,
        edge_avg: 320.5,
      });
      expect(analytics.slowest_networks?.[0]).toMatchObject({
        asn: "AS15169",
        requests: 2,
        matched_requests: 2,
      });

      const live = await cloudRumLiveEvents({ engine });
      const resourceEvent = live.find((e) => e.type === "resource" && e.req_id === "rid-join-1");
      expect(resourceEvent).toBeDefined();
      expect(resourceEvent?.req_id).toBe("rid-join-1");
      expect(resourceEvent?.resource_url).toBe("/api/pay");

      const unmasked = await cloudRumRequestLog({ engine, req_id: "rid-join-1" });
      expect(unmasked.found).toBe(true);
      expect(unmasked.request?.client_ip).toBe("203.0.113.9");

      const maskedRow = redactQueryRowsForPii([unmasked.request!], true)[0];
      expect(maskedRow.client_ip).toBe("0.0.0.0");
      expect(JSON.stringify(maskedRow)).not.toContain("203.0.113.9");
    } finally {
      await engine.reset();
    }
  });

  it("enforces RUM parity: live-events maskIps/ttfb/pageview/limit, beacon-health contract, removed stubs, errors[].file fallback, and worst_pages sort", async () => {
    expect("queryRumStatus" in rumModule).toBe(false);
    expect("queryRumVersions" in rumModule).toBe(false);

    const engine = new DuckDBAnalyticalEngine();
    try {
      const nowMs = Date.now();
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, page_url, client_id, session_id, ttfb)
        VALUES
          ('v1', ${nowMs - 10_000}, 'LCP', 2100, '/checkout?ip=203.0.113.9', 'cid-secret-1', 'sess-secret-1', 0.123),
          ('v2', ${nowMs - 20_000}, 'page_load', 1200, '/fast', 'cid-2', 'sess-2', 45),
          ('v3', ${nowMs - 30_000}, 'page_load', 3400, '/slow', 'cid-3', 'sess-3', 80)`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, error_message, filename, page_url, session_id)
        VALUES ('e1', ${nowMs - 5_000}, 'ReferenceError', '', '/checkout?ip=203.0.113.9', 'sess-secret-1')`);

      const maskedLive = await cloudRumLiveEvents({ engine, maskIps: true });
      expect(maskedLive.length).toBeGreaterThan(0);
      expect(maskedLive.every((ev) => ev.type !== "vital")).toBe(true);
      const vitalEv = maskedLive.find((ev) => ev.metric_name === "LCP");
      expect(vitalEv?.type).toBe("pageview");
      expect(vitalEv?.raw_log.ttfb).toBe(123);
      expect(JSON.stringify(maskedLive)).not.toContain("203.0.113.9");
      expect(JSON.stringify(maskedLive)).not.toContain("sess-secret-1");
      expect(JSON.stringify(maskedLive)).not.toContain("cid-secret-1");

      const analytics = await cloudRumAnalytics({ engine });
      expect(analytics.errors[0].file).toBe("unknown.js");
      expect(analytics.worst_pages.map((p) => p.path)).toEqual([
        "/checkout?ip=203.0.113.9",
        "/slow",
        "/fast",
      ]);

      const health = await queryRumBeaconHealth({ engine });
      expect(health).toMatchObject({
        enabled: true,
        beacon_fire_rate: 4,
        recent_beacons: 4,
        setup_complete: true,
      });
      expect(typeof health.last_beacon_time).toBe("string");
      expect(health.message).toContain("4 beacon");
      expect("status_2xx" in health).toBe(false);
    } finally {
      await engine.reset();
    }
  });

  it("counts physical unweighted distinct beacons for RUM Class A usage billing and rounds cost and daily average", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, metric_name, metric_value, page_url, client_id, request_event_id, sample_rate)
        VALUES
          ('v1', 1767229200000, 'LCP', 1200, '/a', 'beacon-1', 'req-1', 0.25),
          ('v2', 1767232800000, 'CLS', 0.05, '/a', 'beacon-1', 'req-1', 0.25),
          ('v3', 1767236400000, 'LCP', 900, '/b', 'beacon-2', 'req-2', 0.25)`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, error_message, page_url, client_id, request_event_id, sample_rate)
        VALUES ('e1', 1767240000000, 'Boom', '/b', 'beacon-2', 'req-2', 0.25)`);

      const usage = await cloudRumUsage(
        {
          engine,
          start_time: "2026-01-01T00:00:00Z",
          end_time: "2026-01-04T00:00:00Z",
        },
        0.0025,
      );
      expect(usage.data).toEqual([
        {
          date: "2026-01-01",
          beacon_count: 2,
          estimated_class_a_ops: 2,
          estimated_cost_usd: 0.000005,
        },
      ]);
      expect(usage.total_beacons).toBe(2);
      expect(usage.total_estimated_class_a).toBe(2);
      expect(usage.total_estimated_cost_usd).toBe(0.000005);
      expect(usage.average_beacons_per_day).toBe(0.67);
    } finally {
      await engine.reset();
    }
  });
});
