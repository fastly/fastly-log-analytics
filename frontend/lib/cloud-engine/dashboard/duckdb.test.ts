// @vitest-environment node
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
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
});
