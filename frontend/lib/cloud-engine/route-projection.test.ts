// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ALL_TABLE_FIELDS, createNodeBackend, FIELD_DUCKDB_TYPES, type DuckDBReadContext } from "./dashboard/duckdb";
import { assetsScanColumns, handleAssetsRequest } from "./assets";
import { handleOriginRequest, originScanColumns } from "./origin";
import { handlePerformanceRequest, performanceScanColumns } from "./performance";
import { handleValueRequest, valueScanColumns } from "./value";
import { handleSessionsList, sessionsScanColumns } from "./sessions";

type Backend = Awaited<ReturnType<typeof createNodeBackend>>;
type Variant = { name: string; columns: string[] | null; run: (read: DuckDBReadContext) => Promise<unknown> };

const START_MS = 1_700_000_000_000;
const ROWS = 240;
const RANGE = { start_time: new Date(START_MS).toISOString(), end_time: new Date(START_MS + ROWS * 60_000).toISOString() };
const cycle = (values: readonly (string | number | null)[]) =>
  `(${JSON.stringify(values).replaceAll('"', "'").replaceAll("null", "NULL")})[1 + i % ${values.length}]`;
// Varied values in every view column, so a scan that read one the projection drops would differ or fail.
const DATA: Record<string, string> = {
  cache: cycle(["HIT", "MISS", "PASS", "HIT-CLUSTER", "MISS"]),
  cache_status: cycle([null, "", "hit", null]),
  status: cycle([200, 304, 404, 503, 200]),
  ost: cycle([200, 500, null, 404, 503, 200]),
  oip: "'10.0.0.' || (i % 5)::VARCHAR",
  url: "'/assets/' || (i % 6)::VARCHAR || CASE WHEN i % 2 = 0 THEN '.js' ELSE '.png' END",
  path: cycle([null, "/p.css", "/p.woff2"]),
  content_type: cycle(["image/png", "text/css", "application/javascript", "text/html", "image/jpeg", null]),
  resp_header_content_type: cycle([null, "image/gif", "application/json"]),
  content_encoding: cycle([null, "gzip", "", "br"]),
  resp_header_content_encoding: cycle(["identity", null, "zstd"]),
  proto: cycle(["HTTP/2", "HTTP/1.1", "HTTP/3", null]),
  protocol: cycle([null, null, "h3"]),
  pop: cycle(["IAD", "LHR", "SJC", "FRA"]),
  server_region: cycle(["US-East", "EU-West", null]),
  host: cycle(["a.example", "b.example"]),
  edge: "i % 5 <> 0",
  rid: "'r' || i::VARCHAR",
  prid: "CASE WHEN i % 5 = 0 THEN 'r' || (i - 1)::VARCHAR END",
  service_id: cycle(["svc", null, "other"]),
  waf_action: cycle(["block", "log", "allow", null]),
  ngwaf_decision: cycle([null, "BLOCKED", "PASS"]),
  waf_sig: cycle(["SQLI,XSS", null, "CMDEXE"]),
  _bot_name: cycle([null, "Googlebot", ""]),
  ngwaf_bot_name: cycle(["", "Bingbot", null]),
  bot_category: cycle([null, null, "crawler"]),
  io_output_format: cycle(["webp", null, "avif", ""]),
  ofmt: cycle([null, "jpeg", null]),
  io_input_format: cycle(["jpeg", "", null]),
  ifmt: cycle([null, "png"]),
  tls_version: cycle([null, "TLSv1.3", ""]),
  tls: cycle(["TLSv1.2", null]),
  asset_type: cycle([null, "", "Font", null, null]),
};

function columnValue(column: string): string {
  const type = FIELD_DUCKDB_TYPES[column];
  const custom = DATA[column];
  if (custom) return `${custom}::${type}`;
  if (type === "VARCHAR") return `CASE WHEN i % 11 = 3 THEN NULL ELSE '${column}-' || (i % 7)::VARCHAR END`;
  if (type === "BOOLEAN") return `CASE WHEN i % 13 = 5 THEN NULL ELSE (i + ${column.length}) % 3 = 0 END`;
  if (type === "DOUBLE") return `CASE WHEN i % 17 = 2 THEN NULL ELSE ((i * ${column.length + 7}) % 997)::DOUBLE * 37.5 + 0.25 END`;
  return `CASE WHEN i % 19 = 4 THEN NULL ELSE ((i * ${column.length + 11}) % 1009) * 1103 + 600 END`;
}

describe("Cloud route column projections", () => {
  let backend: Backend;
  const sqls: string[] = [];
  const read = {
    assertCurrent: () => undefined,
    async executeSql(sql: string) {
      sqls.push(sql);
      // Like the Cloud view, the schema always lists every column, projected or not.
      const statement = /^\s*DESCRIBE\b/i.test(sql) ? "DESCRIBE source" : sql;
      const result = await backend.query(statement) as unknown as { toArray(): Record<string, unknown>[] };
      return result.toArray().map((row) => Object.fromEntries(Object.entries({ ...row })
        .map(([key, value]) => [key, typeof value === "bigint" ? Number(value) : value])));
    },
    getPresentColumns: async () => new Set(ALL_TABLE_FIELDS),
    getCustomColumnTypes: () => new Map(),
  } as unknown as DuckDBReadContext;
  const view = (columns: readonly string[]) =>
    backend.query(`CREATE OR REPLACE VIEW requests AS SELECT ${columns.map((c) => `"${c}"`).join(", ")} FROM source`);
  const all = ["ts_ms", "timestamp", ...ALL_TABLE_FIELDS];

  beforeAll(async () => {
    backend = await createNodeBackend();
    await backend.query(`CREATE TABLE source AS SELECT ${START_MS}::BIGINT + i * 60000 AS ts_ms,
      epoch_ms(${START_MS}::BIGINT + i * 60000)::TIMESTAMPTZ AS "timestamp",
      ${ALL_TABLE_FIELDS.map((column) => `${columnValue(column)} AS "${column}"`).join(", ")}
      FROM range(${ROWS}) rows(i)`);
    await view(all);
  });
  afterAll(async () => { await backend?.dispose(); });

  // Each variant's own projection gives the full-table result; every projected column is read by some variant.
  async function guard(variants: Variant[]) {
    for (const variant of variants) {
      expect(variant.columns, variant.name).not.toBeNull();
      expect(variant.columns!.filter((column) => !all.includes(column)), variant.name).toEqual([]);
      await view(all);
      const expected = await variant.run(read);
      await view(variant.columns!);
      expect(await variant.run(read), variant.name).toEqual(expected);
    }
    // The loader derives ts_ms from the physical timestamp, which no scan names.
    const used = new Set(variants.flatMap((variant) => variant.columns!).filter((column) => column !== "timestamp"));
    for (const column of used) {
      let failed = false;
      for (const variant of variants.filter((item) => item.columns!.includes(column))) {
        await view(variant.columns!.filter((item) => item !== column));
        if (await variant.run(read).then(() => false, () => true)) { failed = true; break; }
      }
      expect(failed, `no scan read ${column}`).toBe(true);
    }
    await view(all);
  }

  const origin = (path: string, body: Record<string, unknown>): Variant => ({
    name: `${path} ${JSON.stringify(body)}`,
    columns: originScanColumns(path, body),
    run: async (context) => {
      const response = await handleOriginRequest(new Request(`http://localhost${path}`, {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ...RANGE, ...body }),
      }), path, undefined, context);
      return response!.json();
    },
  });

  it("loads /api/origin scans as exact projections", async () => {
    const page = {
      filters: {}, bucket_minutes: 60, split_by_leg: false, timeseries_metric: "ttfb", timeseries_percentile: "p95",
      slow_urls_limit: 20, slow_urls_min_requests: 50, pop_latency_limit: 30, ip_health_limit: 30,
      sections: ["summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"],
    };
    const result = await origin("/api/origin/aggregates", page).run(read);
    expect(result).toMatchObject({ has_data: true, summary: { has_data: true }, ip_health: { has_data: true } });
    await guard([
      origin("/api/origin/aggregates", page),
      origin("/api/origin/aggregates", { split_by_leg: true, timeseries_metric: "ttlb" }),
      origin("/api/origin/aggregates", { ...page, filters: { country_1: { mode: "include", values: ["country-1"] } } }),
      origin("/api/origin/timeseries", { metric: "ttlb", split_by_leg: true }),
      origin("/api/origin/shielding-analysis", {}),
      ...["host", "pop", "datacenter", "protocol", "oip"].map((dimension) =>
        origin("/api/origin/drilldown", { dimension })),
    ]);
    expect(originScanColumns("/api/origin/aggregates", {
      filters: { host_2: { mode: "include", values: ["a"] } }, custom_filters: { team: { mode: "exclude", values: ["x"] } },
    })).toEqual(expect.arrayContaining(["host", "team"]));
    expect(originScanColumns("/api/origin/aggregates", { filters: [{ field: "host" }] })).toBeNull();
    expect(originScanColumns("/api/origin/aggregates", { custom_filters: "host" })).toBeNull();
    expect(originScanColumns("/api/origin/unknown", {})).toBeNull();
  });

  const value = (body: Record<string, unknown>): Variant => ({
    name: `/api/value/summary ${JSON.stringify(body)}`,
    columns: valueScanColumns(body),
    run: (context) => handleValueRequest("/api/value/summary", { ...RANGE, ...body, engine: context }),
  });

  it("loads /api/value/summary scans as exact projections", async () => {
    const page = { filters: {}, chart_interval: "1 day", sections: ["overview", "caching", "network"], service_id: "svc" };
    expect(await value(page).run(read)).toMatchObject({ overview: { total_requests: expect.any(Number) } });
    await guard([
      value(page),
      value({ service_id: "svc", chart_interval: "1 hour" }),
      value({ filters: { ua_1: { mode: "exclude", values: ["ua-2"] } }, custom_filters: { method: { mode: "include", values: ["method-1"] } } }),
    ]);
    expect(valueScanColumns({ filters: "host" })).toBeNull();
    expect(valueScanColumns({ custom_filters: [] })).toBeNull();
  });

  const assets = (body: Record<string, unknown>): Variant => ({
    name: `/api/assets/aggregates ${JSON.stringify(body)}`,
    columns: assetsScanColumns(body.filters),
    run: (context) => handleAssetsRequest("/api/assets/aggregates", { ...RANGE, ...body, engine: context }),
  });

  it("loads /api/assets/aggregates scans as exact projections", async () => {
    expect(await assets({ filters: {}, service_id: "svc" }).run(read))
      .toMatchObject({ summary: { total_requests: expect.any(Number) }, low_ttl_assets: expect.any(Array) });
    await guard([
      assets({ filters: {}, service_id: "svc" }),
      assets({ filters: { method: { mode: "include", values: ["method-1"] } } }),
      assets({ filters: [{ column: "geo_country", operator: "!=", value: "x" }, { column: "asset_type", operator: "=", value: "Image" }] }),
    ]);
    expect(assetsScanColumns([{ column: "ua", operator: "=", value: "x" }])).not.toContain("ua");
    expect(assetsScanColumns("host")).toBeNull();
    expect(assetsScanColumns([null])).toBeNull();
  });

  const performance = (body: Record<string, unknown>): Variant => ({
    name: `/api/performance/aggregates ${JSON.stringify(body)}`,
    columns: performanceScanColumns(body.filters),
    run: async (context) => (await handlePerformanceRequest(new Request("http://localhost/api/performance/aggregates", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ...RANGE, ...body }),
    // The Cloud read context's querySql describes the view, so the scan filters on ts_ms.
    }), undefined, undefined, { ...context, querySql: context.executeSql } as DuckDBReadContext))!.json(),
  });

  it("loads /api/performance/aggregates scans as exact projections", async () => {
    expect(await performance({ filters: {}, sort_by: "p99" }).run(read)).toMatchObject({
      top_urls: expect.arrayContaining([expect.objectContaining({ requests: expect.any(Number) })]),
      waterfall: { avg: expect.any(Object) }, scatter: expect.any(Array),
    });
    await guard([
      performance({ filters: {}, sort_by: "p99" }),
      performance({ filters: { status_1: { mode: "include", values: ["200"] } }, sort_by: "avg" }),
      performance({ filters: { host: ["a.example"], pop: { mode: "exclude", values: ["IAD"] } } }),
    ]);
    expect(performanceScanColumns({ status_1: { mode: "include", values: ["200"] } })).toContain("status");
    expect(performanceScanColumns("status")).toBeNull();
    expect(performanceScanColumns([{ field: "status" }])).toBeNull();
  });

  const sessions = (body: Record<string, unknown>): Variant => ({
    name: `/api/sessions ${JSON.stringify(body)}`,
    columns: sessionsScanColumns(body.filters),
    run: async (context) => {
      const response = await handleSessionsList({ ...RANGE, ...body }, {
        // The Cloud view's present columns include the loader-derived ts_ms.
        availableColumns: new Set(["ts_ms", ...await context.getPresentColumns()]),
        runSql: (sql) => context.executeSql(sql),
      });
      if (!("sessions" in response.body)) throw new Error("Session scan failed");
      return response.body.sessions;
    },
  });

  it("loads /api/sessions scans as exact projections", async () => {
    expect(await sessions({}).run(read)).toEqual(expect.arrayContaining([
      expect.objectContaining({ req_count: expect.any(Number), unique_urls: expect.any(Number) }),
    ]));
    await guard([
      sessions({}),
      sessions({ sort_by: "median_rtt_ms", flagged_only: true, min_reqs_flag: 2 }),
      sessions({ filters: { host: { mode: "include", values: ["a.example"] } }, sort_by: "unique_urls" }),
    ]);
    expect(sessionsScanColumns({ host: ["a.example"] })).toContain("host");
    expect(sessionsScanColumns("host")).toBeNull();
    expect(sessionsScanColumns([{ field: "host" }])).toBeNull();
  });
});
