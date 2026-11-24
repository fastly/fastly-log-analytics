// @vitest-environment node
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it, vi } from "vitest";

// Builders that ask which log fields are enabled get every field the checks below need.
vi.mock("../active-log-fields", async () => {
  const { INSIGHT_DEFINITIONS } = await import("../insights");
  return {
    cloudActiveLogFieldIds: async () => new Set([
      ...INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields), "edge", "rid", "prid", "pop", "ttfb", "ottfb", "edge_score",
    ]),
  };
});
import {
  DUCKDB_WASM_VERSION,
  DuckDBAnalyticalEngine,
  duckdbAssetUrls,
  interpolateSqlParams,
} from "./duckdb";
import { Aggregator } from "./engine";
import { fixtureEngine } from "./test-fixtures";

describe("DuckDB browser assets", () => {
  it("load from an immutable versioned path", () => {
    expect(duckdbAssetUrls("https://flp.example", "1.33.1-dev57.0")).toEqual({
      worker:
        "https://flp.example/duckdb/1.33.1-dev57.0/duckdb-browser-eh.worker.js",
      wasm: "https://flp.example/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
      extensions: "https://flp.example/duckdb/1.33.1-dev57.0/extensions",
    });
  });

  it("pin the exact installed duckdb-wasm version", () => {
    const read = (path: string) =>
      JSON.parse(readFileSync(join(__dirname, "../../..", path), "utf8"));
    const installed = read(
      "node_modules/@duckdb/duckdb-wasm/package.json",
    ).version;
    expect(read("package.json").dependencies["@duckdb/duckdb-wasm"]).toBe(
      installed,
    );
    expect(DUCKDB_WASM_VERSION).toBe(installed);
  });

  it("vendor the parquet extension built for the bundled DuckDB engine", () => {
    const vendorDir = join(__dirname, "../../../vendor/duckdb-extensions");
    const manifest = JSON.parse(
      readFileSync(join(vendorDir, "manifest.json"), "utf8"),
    ) as { duckdb_version: string; files: Record<string, string> };
    expect(Object.keys(manifest.files)).toEqual([
      `${manifest.duckdb_version}/wasm_eh/parquet.duckdb_extension.wasm`,
    ]);
    for (const [file, sha256] of Object.entries(manifest.files)) {
      const digest = createHash("sha256")
        .update(readFileSync(join(vendorDir, file)))
        .digest("hex");
      expect(digest, file).toBe(sha256);
    }
    const duckdb = createRequire(import.meta.url)(
      "@duckdb/duckdb-wasm/dist/duckdb-node-blocking",
    ) as {
      createDuckDB: (
        bundles: unknown,
        logger: unknown,
        runtime: unknown,
      ) => Promise<{
        instantiate: () => Promise<void>;
        connect: () => {
          query: (sql: string) => { toArray: () => { v: string }[] };
        };
      }>;
      VoidLogger: new () => unknown;
      NODE_RUNTIME: unknown;
    };
    const dist = join(
      __dirname,
      "../../../node_modules/@duckdb/duckdb-wasm/dist",
    );
    return duckdb
      .createDuckDB(
        {
          mvp: { mainModule: join(dist, "duckdb-mvp.wasm"), mainWorker: "" },
          eh: { mainModule: join(dist, "duckdb-eh.wasm"), mainWorker: "" },
        },
        new duckdb.VoidLogger(),
        duckdb.NODE_RUNTIME,
      )
      .then(async (db) => {
        await db.instantiate();
        const [row] = db.connect().query("SELECT version() AS v").toArray();
        expect(row.v).toBe(manifest.duckdb_version);
      });
  });

  it("creates requests, logs view, client_vitals, and client_errors with executeSql and getPresentColumns", async () => {
    const engine = fixtureEngine(new DuckDBAnalyticalEngine());
    await engine.executeSql(
      `INSERT INTO requests (scoped_key, ts_ms, "timestamp", status, client_rtt_us, cmcd_sid, asset_type)
       VALUES ($1, $2, epoch_ms($2)::TIMESTAMPTZ, $3, $4, $5, $6)`,
      ["test-key", 1700000000000, 200, 12000, "sid-1", "JavaScript"],
    );
    const rows = await engine.executeSql<{
      status: number;
      client_rtt_us: number;
      cmcd_sid: string;
      asset_type: string;
    }>(`SELECT status, client_rtt_us, cmcd_sid, asset_type FROM logs`);
    expect(rows).toEqual([
      {
        status: 200,
        client_rtt_us: 12000,
        cmcd_sid: "sid-1",
        asset_type: "JavaScript",
      },
    ]);
    const present = await engine.getPresentColumns();
    expect(present.has("client_rtt_us")).toBe(true);
    expect(present.has("cmcd_sid")).toBe(true);
    expect(present.has("asset_type")).toBe(true);

    await engine.executeSql(
      `INSERT INTO client_vitals (scoped_key, "timestamp", ts_ms, metric_name, metric_value, page_url)
       VALUES ('v-1', epoch_ms(1700000000000)::TIMESTAMPTZ, 1700000000000, 'LCP', 2100.0, 'https://example.com/')`,
    );
    const vitals = await engine.executeSql<{
      metric_name: string;
      metric_value: number;
    }>(`SELECT metric_name, metric_value FROM client_vitals`);
    expect(vitals).toEqual([{ metric_name: "LCP", metric_value: 2100 }]);
    await engine.reset();
  });

  it("quotes all 30 DuckDB reserved-keyword custom field names in views, card stats, bounded qualify, and selective Parquet scans (SEC-T5-ITER23-03)", async () => {
    const reservedCustomIds = [
      "desc",
      "asc",
      "default",
      "window",
      "check",
      "unique",
      "primary",
      "foreign",
      "references",
      "returning",
      "except",
      "intersect",
      "fetch",
      "only",
      "any",
      "some",
      "both",
      "leading",
      "trailing",
      "placing",
      "to",
      "for",
      "symmetric",
      "asymmetric",
      "variadic",
      "array",
      "collate",
      "analyze",
      "lateral",
      "qualify",
    ] as const;
    const customFields = reservedCustomIds.map((id, idx) => ({
      id,
      type: idx % 3 === 1 ? ("number" as const) : idx % 3 === 2 ? ("boolean" as const) : ("string" as const),
    }));
    const ts0 = 1_700_000_000_000;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(ts0), new Date(ts0 + 1000)] },
        { name: "status", type: "INT64", data: [BigInt(200), BigInt(200)] },
        { name: "cache", type: "STRING", data: ["HIT", "MISS"] },
        { name: "resp_bytes", type: "INT64", data: [BigInt(512), BigInt(1024)] },
        { name: "edge", type: "BOOLEAN", data: [true, true] },
        ...customFields.map((cf, idx) =>
          cf.type === "number"
            ? { name: cf.id, type: "DOUBLE" as const, data: [idx + 1.5, -0.0] }
            : cf.type === "boolean"
              ? { name: cf.id, type: "BOOLEAN" as const, data: [true, false] }
              : { name: cf.id, type: "STRING" as const, data: [`val_${cf.id}`, ""] },
        ),
      ],
    });

    const engine = new DuckDBAnalyticalEngine();
    const lease = engine.lease(customFields);
    try {
      await lease.ingestFile("svc-kw:1", "svc-kw/1.parquet", bytes, undefined, undefined, "request", {
        deferView: true,
      });
      await lease.publishView();
      const ctx = lease.readContext();

      // 1. Resident view + queryIntoAggregator (unbounded + filter + fieldSearch):
      const aggResident = new Aggregator(ts0 - 1000, ts0 + 5000, [...reservedCustomIds]);
      await engine.queryIntoAggregator(
        ["svc-kw:1"],
        ts0 - 1000,
        ts0 + 5000,
        [{ field: "window", mode: "include", value: "val_window" }],
        aggResident,
        { context: ctx, fieldSearch: { field: "desc", query: "val_" } },
      );
      expect(aggResident.totalRequests).toBe(1);
      expect([...(aggResident.counts.get("window")?.entries() ?? [])]).toEqual([["val_window", 1]]);
      expect([...(aggResident.counts.get("default")?.entries() ?? [])]).toEqual([[true, 1]]);
      expect([...(aggResident.counts.get("desc")?.entries() ?? [])]).toEqual([["val_desc", 1]]);
    } finally {
      lease.release();
    }

    // 2. Selective Parquet scan (bounded mode -> exercises parquetRelation + fieldStatsSql + boundedQualify + fieldValuesSql):
    const aggSelective = new Aggregator(ts0 - 1000, ts0 + 5000, [...reservedCustomIds]);
    await engine.queryParquetIntoAggregator(
      { kind: "buffer", bytes },
      ts0 - 1000,
      ts0 + 5000,
      [{ field: "window", mode: "exclude", value: "never_match" }],
      aggSelective,
      {},
      customFields,
    );
    expect(aggSelective.totalRequests).toBe(2);
    for (const cf of customFields) {
      expect(aggSelective.nonNull.get(cf.id), cf.id).toBe(2);
    }

    // 3. Per-file Parquet scan (exercises queryRelationPerFile + fieldStatsSql(file=true, maxLength=true) + boundedQualify):
    const aggPerFile = new Aggregator(ts0 - 1000, ts0 + 5000, [...reservedCustomIds]);
    await engine.queryParquetPerFile(
      [{ access: { kind: "buffer", bytes }, target: aggPerFile, fromMs: ts0 - 1000, toMs: ts0 + 5000 }],
      [],
      {},
      customFields,
    );
    expect(aggPerFile.totalRequests).toBe(2);
    await engine.reset();
  });

  it("SEC-T5-ITER24-03: interpolateSqlParams skips literals/identifiers/comments and DuckDB safely handles NUL bytes", async () => {
    // 1. Single-pass token-aware interpolateSqlParams:
    const raw = `SELECT "col?$1" FROM requests -- comment ? $1\n/* block ? $2 */ WHERE ts_ms >= ? AND ts_ms < ? AND url = '/search?q=alpha&p=$1' AND tag = 'prefix_$1_suffix' AND extra = $3`;
    const interpolated = interpolateSqlParams(raw, [
      100,
      200,
      "safe' || (SELECT ip FROM requests LIMIT 1) || '",
    ]);
    expect(interpolated).toContain(`"col?$1"`);
    expect(interpolated).toContain(`WHERE ts_ms >= 100 AND ts_ms < 200`);
    expect(interpolated).toContain(`url = '/search?q=alpha&p=$1'`);
    expect(interpolated).toContain(`tag = 'prefix_$1_suffix'`);
    expect(interpolated).toContain(
      `extra = 'safe'' || (SELECT ip FROM requests LIMIT 1) || '''`,
    );

    // 2. Real DuckDB-WASM NUL byte handling in string filters and identifier rejection without poisoning engine:
    const ts0 = 1_700_000_000_000;
    const goodBytes = parquetWriteBuffer({
      codec: "SNAPPY",
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(ts0)] },
        { name: "status", type: "INT64", data: [BigInt(200)] },
        { name: "url", type: "STRING", data: ["/search?q=alpha"] },
        { name: "cache", type: "STRING", data: ["HIT"] },
        { name: "resp_bytes", type: "INT64", data: [BigInt(128)] },
        { name: "edge", type: "BOOLEAN", data: [true] },
      ],
    });
    const engine = new DuckDBAnalyticalEngine();
    const lease = engine.lease([]);
    try {
      await lease.ingestFile("svc-nul:good", "svc-nul/good.parquet", goodBytes);
      await lease.publishView();
      const ctx = lease.readContext();

      // NUL byte inside filter value must execute cleanly without Parser Error: unterminated quoted string!
      const aggNul = new Aggregator(ts0 - 1000, ts0 + 5000, ["url"]);
      await engine.queryIntoAggregator(
        ["svc-nul:good"],
        ts0 - 1000,
        ts0 + 5000,
        [{ field: "url", mode: "exclude", value: "abc\0def" }],
        aggNul,
        { context: ctx },
      );
      expect(aggNul.totalRequests).toBe(1);

      // NUL byte in parameter via executeSql must preserve exact NUL string length (7 chars):
      const nulRows = await ctx.executeSql<{ len: number }>(
        "SELECT length(?) AS len",
        ["abc\0def"],
      );
      expect(Number(nulRows[0]?.len)).toBe(7);

      // URL filter containing '?' alongside positional '?' params (the exact 1s /api/timeseries shape) must match accurately:
      const qmarkRows = await ctx.executeSql<{ cnt: number }>(
        "SELECT count(*) AS cnt FROM requests WHERE ts_ms >= ? AND ts_ms < ? AND url = '/search?q=alpha'",
        [ts0 - 1000, ts0 + 5000],
      );
      expect(Number(qmarkRows[0]?.cnt)).toBe(1);
    } finally {
      lease.release();
    }

    // Custom column containing NUL byte must fail cleanly BEFORE poisoning ParquetBindingStore,
    // so subsequent clean queries on the same engine continue working!
    const badLease = engine.lease([{ id: "bad\0col", type: "string" }]);
    try {
      await expect(
        badLease.ingestFile("svc-nul:bad", "svc-nul/bad.parquet", goodBytes),
      ).rejects.toThrow(/NUL/i);
    } finally {
      badLease.release();
    }
    const checkLease = engine.lease([]);
    try {
      await checkLease.ingestFile(
        "svc-nul:good",
        "svc-nul/good.parquet",
        goodBytes,
      );
      await checkLease.publishView();
      const okRows = await checkLease
        .readContext()
        .executeSql<{ cnt: number }>("SELECT count(*) AS cnt FROM requests");
      expect(Number(okRows[0]?.cnt)).toBe(1);
    } finally {
      checkLease.release();
    }
    await engine.reset();
  });

  it("BUG-T5-ITER25-02 & BUG-T5-ITER25-04: all Cloud-Engine DuckDB builders handle NUL bytes and interpolateSqlParams handles E'...', a$1, and $π$ in real DuckDB-WASM", async () => {
    const { buildSecurityWellKnownBotPairsSql } = await import("../security");
    const { buildSessionsEnrichSql } = await import("../sessions-window");
    const { handleSessionsDetail } = await import("../sessions");
    const { bindCloudReadContext } = await import("../range-loader");
    const { queryCloudScoring } = await import("../scoring-reads");
    const { queryCloudCacheCollapse } = await import("../insight-scans");
    const { queryCmcdAggregates } = await import("../cmcd");
    const { rangeServiceSql } = await import("../query-params");
    const { queryValueSummary } = await import("../value");
    const { queryAssetsAggregates } = await import("../assets");
    const { queryNetworkQuality } = await import("../network");
    const { queryRumAnalytics } = await import("../rum");

    // 1. BUG-T5-ITER25-04: E'...' escapes, $-identifiers (a$1, cost$2), and Unicode dollar tags ($π$...$π$)
    const eLitSql = interpolateSqlParams(
      `SELECT E'foo\\'bar?baz$1' AS esc_s, a$1, cost$2, $π$hello ? $1$π$ AS uni_s, $1 AS p1 FROM (SELECT 10 AS a$1, 20 AS cost$2) WHERE 1 = 1`,
      [999, 888],
    );
    expect(eLitSql).toContain(`E'foo\\'bar?baz$1'`);
    expect(eLitSql).toContain(`SELECT E'foo\\'bar?baz$1' AS esc_s, a$1, cost$2, $π$hello ? $1$π$ AS uni_s, 999 AS p1`);

    const ts0 = Date.parse("2026-11-20T00:05:00.000Z");
    const nulUa = "Mozilla/5.0 (compatible; Googlebot/2.1)\0poison";
    const nulBytes = parquetWriteBuffer({
      codec: "SNAPPY",
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(ts0)] },
        { name: "status", type: "INT64", data: [BigInt(200)] },
        { name: "url", type: "STRING", data: ["/watch\0poison"] },
        { name: "cache", type: "STRING", data: ["MISS"] },
        { name: "resp_bytes", type: "INT64", data: [BigInt(512)] },
        { name: "edge", type: "BOOLEAN", data: [true] },
        { name: "ip", type: "STRING", data: ["203.0.113.10\0p"] },
        { name: "ja4", type: "STRING", data: ["t13d1516h2\0j"] },
        { name: "ua", type: "STRING", data: [nulUa] },
        { name: "edge_sid", type: "STRING", data: ["sid\0nul"] },
        { name: "edge_score", type: "INT64", data: [BigInt(42)] },
        { name: "cmcd_sid", type: "STRING", data: ["csid1"] },
        { name: "cmcd_cid", type: "STRING", data: ["vid\0nul"] },
        { name: "cmcd_sf", type: "STRING", data: ["d\0nul"] },
        { name: "cmcd_ot", type: "STRING", data: ["v\0nul"] },
        { name: "cmcd_st", type: "STRING", data: ["v\0nul"] },
        { name: "cmcd_br", type: "INT64", data: [BigInt(3200)] },
        { name: "country", type: "STRING", data: ["US"] },
        { name: "pop", type: "STRING", data: ["DEN"] },
        { name: "service_id", type: "STRING", data: ["svc\0one"] },
      ],
    });

    const engine = new DuckDBAnalyticalEngine();
    const lease = engine.lease([]);
    try {
      await lease.ingestFile("svc-nul2:part", "svc-nul2/part.parquet", nulBytes);
      await lease.publishView();
      const ctx = lease.readContext();

      // Verify BUG-T5-ITER25-04 query actually executes in real @duckdb/duckdb-wasm!
      const lexRows = await ctx.executeSql<{ esc_s: string; "a$1": number; "cost$2": number; uni_s: string; p1: number }>(
        `SELECT E'foo\\'bar?baz$1' AS esc_s, a$1, cost$2, $π$hello ? $1$π$ AS uni_s, $1 AS p1 FROM (SELECT 10 AS a$1, 20 AS cost$2)`,
        [999, 888],
      );
      expect(lexRows[0]?.esc_s).toBe("foo'bar?baz$1");
      expect(Number(lexRows[0]?.["a$1"])).toBe(10);
      expect(Number(lexRows[0]?.["cost$2"])).toBe(20);
      expect(lexRows[0]?.uni_s).toBe("hello ? $1");
      expect(Number(lexRows[0]?.p1)).toBe(999);

      // 2. BUG-T5-ITER25-02: Stored & direct NUL bytes across ALL Cloud-Engine DuckDB SQL builders:
      const startIso = "2026-11-20T00:00:00.000Z";
      const endIso = "2026-11-20T00:15:00.000Z";
      const ac = new AbortController();

      // 2a. security.ts buildSecurityWellKnownBotPairsSql (stored Parquet ua with \0):
      const botPair = buildSecurityWellKnownBotPairsSql(
        { start_time: startIso, end_time: endIso },
        [nulUa],
        new Set(["ua", "ip", "ts_ms"]),
      );
      const botRows = await ctx.executeSql<{ ua: string; cnt: number }>(
        botPair.sql.replace(/\bFROM logs\b/, "FROM requests"),
        botPair.params,
      );
      expect(Number(botRows[0]?.cnt)).toBe(1);

      // 2b. sessions-window.ts buildSessionsEnrichSql (stored Parquet ip/ja4 with \0):
      const enrichSql = buildSessionsEnrichSql({
        keys: [{ ip: "203.0.113.10\0p", ja4: "t13d1516h2\0j", startMs: ts0 - 1000, endMs: ts0 + 1000 }],
        hasJa4: true,
        msSql: "ts_ms",
        filterSql: "TRUE",
        ua: true,
        urls: true,
      });
      const enrichRows = await ctx.executeSql<{ unique_urls: number }>(enrichSql);
      expect(Number(enrichRows[0]?.unique_urls)).toBe(1);

      // 2c. sessions.ts handleSessionsDetail:
      const sessDetail = await handleSessionsDetail(
        {
          ip: "203.0.113.10\0p",
          ja4: "t13d1516h2\0j",
          start_time: startIso,
          end_time: endIso,
        },
        {
          runSql: (sql: string) => ctx.executeSql(sql),
          availableColumns: new Set(["ts_ms", "timestamp", "ip", "ja4", "url", "status", "ua"]),
        },
      );
      expect(sessDetail.status).toBe(200);
      expect((sessDetail.body as { data: unknown[] }).data.length).toBe(1);

      // 2d. scoring-reads.ts queryCloudScoring (%00 in session route + \0 in label edge_sid):
      const evReq = bindCloudReadContext(
        new Request(
          `http://localhost/api/services/svc-1/scoring/sessions/sid%00nul/events?start_time=${encodeURIComponent(startIso)}&end_time=${encodeURIComponent(endIso)}`,
        ),
        ctx,
      );
      const scoringEvents = await queryCloudScoring(evReq, "sessions/sid%00nul/events");
      expect((scoringEvents as { event_count: number }).event_count).toBe(1);

      const origFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        Response.json({
          labels: [
            ...Array.from({ length: 5 }, (_, idx) => ({ edge_sid: `good\0${idx}`, label: "good" })),
            ...Array.from({ length: 5 }, (_, idx) => ({
              edge_sid: idx === 0 ? "sid\0nul" : `bad\0${idx}`,
              label: "bad",
            })),
          ],
        })) as typeof fetch;
      try {
        const evalReq = bindCloudReadContext(
          new Request(
            `http://localhost/api/services/svc-1/scoring/evaluation?start_time=${encodeURIComponent(startIso)}&end_time=${encodeURIComponent(endIso)}`,
          ),
          ctx,
        );
        const scoringEval = await queryCloudScoring(evalReq, "evaluation");
        expect((scoringEval as { has_min_samples: boolean }).has_min_samples).toBe(true);
      } finally {
        globalThis.fetch = origFetch;
      }

      // 2e. insight-scans.ts queryCloudCacheCollapse:
      const collapse = await queryCloudCacheCollapse(
        { url: "/watch\0poison", service_id: "svc\0one", window_size_hrs: 1, baseline_hours: 2 },
        { signal: ac.signal, maskIps: false, engine: ctx },
      );
      expect(collapse.url).toBe("/watch\0poison");

      // 2f. query-params.ts rangeServiceSql + cmcd.ts + value.ts + assets.ts + network.ts + rum.ts:
      const rSql = rangeServiceSql({ start_time: startIso, end_time: endIso, service_id: "svc\0one" });
      const rRows = await ctx.executeSql<{ cnt: number }>(`SELECT count(*) AS cnt FROM requests WHERE ${rSql}`);
      expect(Number(rRows[0]?.cnt)).toBe(1);

      const cmcdRes = await queryCmcdAggregates({
        start_time: startIso,
        end_time: endIso,
        service_id: "svc\0one",
        filter_cid: "vid\0nul",
        filter_sf: "d\0nul",
        filter_ot: "v\0nul",
        filter_st: "v\0nul",
        engine: ctx,
      });
      expect(cmcdRes.top_content[0]?.requests).toBe(1);

      const valRes = await queryValueSummary({
        start_time: startIso,
        end_time: endIso,
        service_id: "svc\0one",
        engine: ctx,
      });
      expect(valRes).toBeDefined();

      const assetRes = await queryAssetsAggregates({
        start_time: startIso,
        end_time: endIso,
        service_id: "svc\0one",
        filters: [{ column: "url", operator: "CONTAINS", value: "watch\0poison" }],
        engine: ctx,
      });
      expect(assetRes).toBeDefined();

      const netRes = await queryNetworkQuality({
        start_time: startIso,
        end_time: endIso,
        service_id: "svc\0one",
        filter_country: "US\0x",
        filter_pop: "DEN\0x",
        region_country: "US\0x",
        engine: ctx,
      });
      expect(netRes.summary).toBeDefined();

      // rum.ts buildWhereClause via queryRumAnalytics with a stubbed client_vitals view in DuckDB:
      await expect(
        queryRumAnalytics({
          start_time: startIso,
          end_time: endIso,
          service_id: "svc\0one",
          device_type: "desktop\0x",
          country: "US\0x",
          page_url: "/watch\0poison",
          engine: {
            getPresentColumns: (scopedKeys) => ctx.getPresentColumns(scopedKeys),
            executeSql: async <T = Record<string, unknown>>(sql: string, params?: unknown[]) => {
              const adapted = sql
                .replace(/\bFROM client_vitals\b/g, "FROM (SELECT ts_ms, service_id, 's1' AS session_id, country, url AS page_url, url, url AS path, 'desktop' AS device_type, 'LCP' AS metric_name, 100.0 AS metric_value, 'good' AS metric_rating, 'c1' AS client_id, 'Chrome' AS browser_name, 'Mac' AS os_name, '4g' AS connection_type FROM requests)")
                .replace(/\bFROM client_errors\b/g, "FROM (SELECT ts_ms, service_id, 's1' AS session_id, country, url AS page_url, url, url AS path, 'desktop' AS device_type, 'err' AS error_message, 'err' AS message, 'f.js' AS filename, 'f.js' AS source, 1 AS lineno, 1 AS colno, 'c1' AS client_id FROM requests WHERE FALSE)");
              return ctx.executeSql<T>(adapted, params);
            },
          },
        }),
      ).resolves.toBeDefined();
    } finally {
      lease.release();
    }
    await engine.reset();
  });
});
