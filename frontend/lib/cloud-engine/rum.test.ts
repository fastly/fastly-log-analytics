// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
import { Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";
import type { RumRollupSource } from "./rum-rollup";
import { DuckDBAnalyticalEngine, getDuckDBEngine } from "./dashboard/test-fixtures";
import { cloudRumAnalytics } from "./rum-wire";
import {
  classifyCwvRating,
  emptyRumAnalytics,
  handleRumRequest,
  queryRumAnalytics,
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

  it("ranks worst pages by error rate and averages web-vital load times like High-Scale", async () => {
    const engine = new DuckDBAnalyticalEngine();
    try {
      const busy = Array.from({ length: 10 }, (_, i) =>
        `('rum', ${1700000000000 + i * 1000}, 'busy-${i}', 'LCP', 1000, '/busy')`).join(", ");
      await engine.executeSql(`INSERT INTO client_vitals
        (scoped_key, ts_ms, request_event_id, metric_name, metric_value, page_url)
        VALUES ${busy}, ('rum', 1700000020000, 'broken-0', 'LCP', 3000, '/broken'),
          ('rum', 1700000020000, 'broken-0', 'TTFB', 1000, '/broken')`);
      await engine.executeSql(`INSERT INTO client_errors
        (scoped_key, ts_ms, request_event_id, error_message, page_url)
        VALUES ('rum', 1700000030000, 'busy-e1', 'x', '/busy'),
          ('rum', 1700000031000, 'busy-e2', 'x', '/busy'),
          ('rum', 1700000032000, 'broken-e1', 'x', '/broken')`);
      const pages = (await cloudRumAnalytics({ engine })).worst_pages;
      expect(pages.map((page) => page.path)).toEqual(["/broken", "/busy"]);
      expect(pages[0]).toMatchObject({ views: 1, avg_load_time: 2, error_rate: 50 });
      expect(pages[1]).toMatchObject({ views: 10, avg_load_time: 1, error_rate: 16.67 });
    } finally {
      await engine.reset();
    }
  });

  it("keeps CWV wire distributions zero and percentiles null with no beacons", async () => {
    const engine = getDuckDBEngine();
    try {
      const data = await cloudRumAnalytics({});
      expect(data.no_data).toBe(true);
      expect(data.beacon_count).toBe(0);
      for (const name of ["lcp", "cls", "inp"] as const) {
        expect(data.vitals[name]).toEqual({
          p75: null,
          distribution: { good: 0, needs_improvement: 0, poor: 0 },
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
        const clamped = Math.max(-(2 ** 52 - 1), Math.min(2 ** 52 - 1, Math.round(v)));
        h.min = Math.min(h.min, clamped);
        h.max = Math.max(h.max, clamped);
        if (clamped < 100) h.under++;
        else if (clamped >= 60_000_000) h.over++;
        else {
          let lo = 0, hi = 128;
          while (lo < hi) {
            const mid = (lo + hi) >>> 1;
            const bound = Math.round(100 * 600000 ** (mid / 128));
            if (bound <= clamped) lo = mid + 1;
            else hi = mid;
          }
          h.buckets[lo - 1]++;
        }
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

      // Filtered queries bypass rum_summary and hit raw DuckDB
      const filtered = await queryRumAnalytics({ engine, start_time: T0, end_time: T1, rollupSource, country: "US" });
      expect(filtered.no_data).toBe(true);
    } finally {
      await engine.reset();
    }
  });
});
