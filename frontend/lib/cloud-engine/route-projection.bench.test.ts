// @vitest-environment node
// Opt-in measurement of the bytes /origin, /fastly-value, /assets-shield and /network load per
// cold scan, whole files against their column projections, over 720k rows of Cloud-shaped Parquet
// (40 ZSTD files over 24h, under the 64 MiB whole-file budget), and of the projected results' exactness. Run with FLP_ROUTE_BENCH=1.
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import {
  assembleProjectedParquet, FOOTER_PROBE_BYTES, fetchProjectedRanges, planParquetProjection, readParquetFooter,
} from "./dashboard/parquet-projection";
import { assetsScanColumns, handleAssetsRequest } from "./assets";
import { handleNetworkRequest, networkScanColumns } from "./network";
import { handleOriginRequest, originScanColumns } from "./origin";
import { handleValueRequest, valueScanColumns } from "./value";

const FILES = Number(process.env.FLP_ROUTE_BENCH_FILES ?? 40);
const ROWS_PER_FILE = Number(process.env.FLP_ROUTE_BENCH_ROWS ?? 18_000);
const END_MS = Date.parse("2026-11-07T00:00:00.000Z");
const START_MS = END_MS - 24 * 3_600_000;
const RANGE = { start_time: new Date(START_MS).toISOString(), end_time: new Date(END_MS).toISOString() };

type BlockingDuckDB = {
  instantiate(): Promise<void>;
  connect(): { query(sql: string): unknown; close(): void };
  reset(): void | Promise<void>;
};

async function generateFiles(): Promise<ArrayBuffer[]> {
  const getBuiltin = (process as unknown as { getBuiltinModule: (id: string) => unknown }).getBuiltinModule;
  const req = (getBuiltin("module") as { createRequire: (url: string) => (id: string) => unknown })
    .createRequire(import.meta.url);
  const path = getBuiltin("path") as { resolve: (...p: string[]) => string; join: (...p: string[]) => string };
  const duckdb = req("@duckdb/duckdb-wasm/dist/duckdb-node-blocking") as {
    createDuckDB(bundles: unknown, logger: unknown, runtime: unknown): Promise<BlockingDuckDB>;
    VoidLogger: new () => unknown;
    NODE_RUNTIME: unknown;
  };
  const dist = path.resolve(process.cwd(), "node_modules/@duckdb/duckdb-wasm/dist");
  const db = await duckdb.createDuckDB({ eh: {
    mainModule: path.join(dist, "duckdb-eh.wasm"), mainWorker: path.join(dist, "duckdb-node-eh.worker.cjs"),
  } }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  const conn = db.connect();
  const fs = getBuiltin("fs") as {
    readFileSync(path: string): Uint8Array; rmSync(path: string, options?: { recursive?: boolean; force?: boolean }): void;
    mkdirSync(path: string, options?: { recursive?: boolean }): void;
  };
  // The Node binding's COPY writes to the real filesystem.
  const scratch = path.resolve(process.cwd(), ".route-bench");
  fs.rmSync(scratch, { recursive: true, force: true });
  fs.mkdirSync(scratch, { recursive: true });
  const fileMs = (END_MS - START_MS) / FILES;
  const files: ArrayBuffer[] = [];
  try {
    for (let f = 0; f < FILES; f++) {
      const name = path.join(scratch, `${f}.parquet`);
      // u(k): a deterministic uniform [0,1) per row and stream k.
      const u = (k: number) => `((hash(i, ${k}) % 1000003)::DOUBLE / 1000003)`;
      conn.query(`COPY (
        WITH base AS (
          SELECT ${f * ROWS_PER_FILE} + r AS i FROM range(${ROWS_PER_FILE}) t(r)
        ), shaped AS (
          SELECT i,
            floor(1500 * pow(${u(1)}, 2.5))::INT AS city_id,
            floor(2000 * pow(${u(2)}, 3))::INT AS asn_id,
            floor(90 * pow(${u(3)}, 2))::INT AS pop_id,
            floor(5000 * pow(${u(8)}, 2))::INT AS url_id,
            ${u(4)} AS u4, ${u(5)} AS u5, ${u(6)} AS u6, ${u(7)} AS u7, ${u(8)} AS u8, ${u(9)} AS u9
          FROM base
        )
        SELECT
          make_timestamp(((${START_MS} + ${fileMs} * ${f} + (i - ${f * ROWS_PER_FILE}) * ${fileMs / ROWS_PER_FILE}) * 1000)::BIGINT) AS "timestamp",
          '10.0.' || (asn_id % 250)::VARCHAR || '.' || (hash(i, 10) % 200)::VARCHAR AS ip,
          CASE WHEN u4 < 0.90 THEN 200 WHEN u4 < 0.95 THEN 304 WHEN u4 < 0.985 THEN 404 WHEN u4 < 0.995 THEN 503 ELSE 500 END::BIGINT AS status,
          (2000 + 100 * floor(4000 * pow(u5, 3)))::BIGINT AS elapsed,
          CASE WHEN u6 < 0.70 THEN 'HIT' WHEN u6 < 0.95 THEN 'MISS' ELSE 'PASS' END AS cache,
          (500 + 100 * floor(20000 * pow(u7, 4)))::BIGINT AS resp_bytes,
          (300 + floor(400 * u5))::BIGINT AS resp_header_bytes,
          (200 + 100 * floor(20000 * pow(u7, 4)))::BIGINT AS resp_body_bytes,
          (400 + floor(1200 * u9))::BIGINT AS req_bytes,
          'www' || (i % 3)::VARCHAR || '.example.com' AS host,
          '/assets/' || url_id::VARCHAR || CASE url_id % 4 WHEN 0 THEN '.js' WHEN 1 THEN '.css' WHEN 2 THEN '.png' ELSE '.html' END
            || CASE WHEN u9 < 0.3 THEN '?v=' || (hash(i, 11) % 100000)::VARCHAR ELSE '' END AS url,
          CASE url_id % 4 WHEN 0 THEN 'application/javascript' WHEN 1 THEN 'text/css' WHEN 2 THEN 'image/png' ELSE 'text/html' END AS content_type,
          CASE WHEN u5 < 0.6 THEN 'br' WHEN u5 < 0.8 THEN 'gzip' ELSE NULL END AS resp_header_content_encoding,
          CASE WHEN u9 < 0.95 THEN 'GET' ELSE 'POST' END AS method,
          CASE WHEN u5 < 0.6 THEN 'HTTP/2' WHEN u5 < 0.85 THEN 'HTTP/1.1' ELSE 'HTTP/3' END AS proto,
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 agent ' || floor(200 * pow(u9, 2))::INT::VARCHAR AS ua,
          CASE WHEN u4 < 0.5 THEN NULL ELSE 'https://ref' || (asn_id % 50)::VARCHAR || '.example/' END AS referer,
          (60 * floor(1 + 1440 * pow(u6, 2)))::BIGINT AS ttl,
          floor(3600 * u7)::BIGINT AS age,
          floor(10 * pow(u8, 3))::BIGINT AS hits,
          md5('d' || url_id::VARCHAR) AS digest,
          'POP' || pop_id::VARCHAR AS pop,
          'F_origin_' || (pop_id % 4)::VARCHAR AS backend,
          i % 5 <> 0 AS edge,
          'region' || (pop_id % 12)::VARCHAR AS server_region,
          u8 < 0.2 AS is_ipv6,
          (1 + floor(20 * u6))::BIGINT AS conn_requests,
          CASE WHEN u7 < 0.9 THEN 'TLSv1.3' ELSE 'TLSv1.2' END AS tls,
          md5('c' || (asn_id % 40)::VARCHAR) AS tls_ciphers_sha,
          u9 < 0.02 AS waf,
          round(0.001 + 0.4 * pow(u5, 3), 4)::DOUBLE AS ttfb,
          CASE WHEN u6 >= 0.70 THEN (1000 + 10 * floor(30000 * pow(u7, 3)))::DOUBLE END AS ottfb,
          CASE WHEN u6 >= 0.70 THEN (2000 + 10 * floor(40000 * pow(u7, 3)))::DOUBLE END AS ottlb,
          CASE WHEN u6 >= 0.70 THEN CASE WHEN u4 < 0.97 THEN 200 ELSE 503 END END::BIGINT AS ost,
          CASE WHEN u6 >= 0.70 THEN '192.0.2.' || (pop_id % 8)::VARCHAR END AS oip,
          CASE WHEN u6 >= 0.70 THEN (500 + 100 * floor(20000 * pow(u7, 4))) END::BIGINT AS obytes,
          CASE WHEN url_id % 4 = 2 AND u5 < 0.4 THEN 'webp' END AS io_output_format,
          CASE WHEN url_id % 4 = 2 AND u5 < 0.4 THEN 'png' END AS io_input_format,
          'C' || (city_id % 60)::VARCHAR AS country,
          'City ' || city_id::VARCHAR AS city,
          'Region ' || (city_id % 300)::VARCHAR AS region,
          (-60 + (city_id * 37 % 120))::DOUBLE + 0.25 AS lat,
          (-170 + (city_id * 53 % 340))::DOUBLE + 0.5 AS lon,
          (500 + city_id % 400)::BIGINT AS metro,
          (64500 + asn_id)::BIGINT AS asn,
          CASE WHEN u9 < 0.03 THEN NULL ELSE (3000 + 10 * floor(25000 * pow(u4 * 0.5 + u6 * 0.5, 3)))::BIGINT END AS tcp_rtt,
          CASE WHEN u5 < 0.85 THEN 'tcp' ELSE 'quic' END AS transport,
          CASE WHEN u8 < 0.9 THEN 0.0 ELSE round(0.05 * u7, 4) END::DOUBLE AS ploss,
          (1000 + 10 * floor(2000 * u6))::BIGINT AS rtt_min,
          (500 + 10 * floor(3000 * pow(u8, 2)))::BIGINT AS rtt_var,
          CASE WHEN u9 < 0.95 THEN 0 ELSE floor(10 * u4) END::BIGINT AS retrans,
          (100000 + 1000 * floor(5000 * u7))::BIGINT AS delivery_rate,
          md5(i::VARCHAR) AS rid,
          CASE WHEN i % 5 = 0 THEN md5((i - 1)::VARCHAR) ELSE NULL END AS prid
        FROM shaped
      ) TO '${name}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536)`);
      const bytes = fs.readFileSync(name);
      fs.rmSync(name);
      files.push(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer);
    }
  } finally {
    conn.close();
    await db.reset();
    fs.rmSync(scratch, { recursive: true, force: true });
  }
  return files;
}

type Route = { name: string; columns: string[] | null; run: (read: DuckDBReadContext) => Promise<unknown> };

const origin = async (read: DuckDBReadContext) => (await handleOriginRequest(new Request("http://localhost/api/origin/aggregates", {
  method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ...RANGE, ...ORIGIN_PAGE }),
}), "/api/origin/aggregates", undefined, read))!.json();
const ORIGIN_PAGE = {
  filters: {}, bucket_minutes: 60, split_by_leg: false, timeseries_metric: "ttfb", timeseries_percentile: "p95",
  slow_urls_limit: 20, slow_urls_min_requests: 50, pop_latency_limit: 30, ip_health_limit: 30,
  sections: ["summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"],
};
const VALUE_PAGE = { filters: {}, chart_interval: "1 day" };
const NETWORK_PAGE: [string, Record<string, unknown>][] = [
  ["/api/network-health", { bucket_seconds: 3600, top_n: 30, map_asn: "all",
    sections: ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets", "cities", "map_buckets"] }],
  ["/api/network-health", { bucket_seconds: 3600, top_n: 30, map_asn: "all", sections: ["shielding_analysis"] }],
  ["/api/network-quality", { region_country: "US" }],
  ["/api/network/pop-health", {}],
  ["/api/network/bundle", { sections: ["transport"], bucket_seconds: 300, top_n: 30, map_asn: "all", region_country: "US" }],
];

// Each route's page-load requests, and the columns its projected load fetches.
const ROUTES: Route[] = [
  { name: "/origin", columns: originScanColumns("/api/origin/aggregates", ORIGIN_PAGE), run: origin },
  {
    name: "/fastly-value", columns: valueScanColumns(VALUE_PAGE),
    run: async (read) => Promise.all([["overview", "caching", "network"], ["performance"], ["security"], ["bots"], ["io"]]
      .map((sections) => handleValueRequest("/api/value/summary", { ...RANGE, ...VALUE_PAGE, sections, engine: read } as never))),
  },
  {
    name: "/assets-shield", columns: assetsScanColumns({}),
    run: (read) => handleAssetsRequest("/api/assets/aggregates", { ...RANGE, filters: {}, engine: read }),
  },
  {
    name: "/network", columns: networkScanColumns({}),
    run: async (read) => {
      const out = [];
      for (const [path, params] of NETWORK_PAGE) out.push(await handleNetworkRequest(path, { ...RANGE, ...params, engine: read } as never));
      return out;
    },
  },
];

async function results(files: ArrayBuffer[], presentColumns: string[] | undefined, run: Route["run"]) {
  const engine = new DuckDBAnalyticalEngine();
  const lease = engine.lease([]);
  try {
    for (const [index, bytes] of files.entries()) {
      await lease.ingestFile(`bench:${index}`, `bench/${index}.parquet`, bytes, undefined, undefined, "request",
        presentColumns ? { deferView: true, presentColumns } : { deferView: true });
    }
    await lease.publishView();
    const started = performance.now();
    const out = await run(lease.readContext());
    return { json: JSON.stringify(out), ms: performance.now() - started };
  } finally {
    lease.release();
    await engine.reset();
  }
}

const mib = (bytes: number) => `${(bytes / 1048576).toFixed(1)} MiB`;

describe.skipIf(!process.env.FLP_ROUTE_BENCH)("Cloud route projection benchmark", () => {
  it("measures whole against projected bytes per route, and the projected results' exactness", async () => {
    const files = await generateFiles();
    const whole = files.reduce((sum, file) => sum + file.byteLength, 0);
    console.log(`generated ${FILES} files, ${FILES * ROWS_PER_FILE} rows, ${mib(whole)}`);
    for (const route of ROUTES) {
      expect(route.columns, route.name).not.toBeNull();
      let downloaded = 0;
      const projected: ArrayBuffer[] = [];
      let presentColumns: string[] = [];
      for (const file of files) {
        const fetchRange = async (start: number, end: number) => { downloaded += end - start; return file.slice(start, end); };
        const plan = planParquetProjection(await readParquetFooter(file.byteLength, fetchRange), new Set(route.columns));
        presentColumns = plan.presentColumns;
        projected.push(assembleProjectedParquet(plan, await fetchProjectedRanges(plan, fetchRange)));
      }
      const full = await results(files, undefined, route.run);
      const part = await results(projected, presentColumns, route.run);
      console.log(`${route.name}: whole ${mib(whole)} -> projected ${mib(downloaded)} (${(100 * downloaded / whole).toFixed(1)}%, ` +
        `footer probes ${mib(Math.min(FOOTER_PROBE_BYTES, files[0].byteLength) * files.length)}); ` +
        `query ${Math.round(full.ms)} ms -> ${Math.round(part.ms)} ms; results ${part.json === full.json ? "identical" : "DIFFER"} ` +
        `(${full.json.length} chars)`);
      expect(part.json, route.name).toBe(full.json);
    }
  }, 900_000);
});
