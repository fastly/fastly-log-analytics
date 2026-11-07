// @vitest-environment node
// Opt-in benchmark of the Cloud /network page's scans over ~1M rows of
// Cloud-shaped Parquet (40 files over 24h, ZSTD like the Cloud writer), on the same DuckDB-WASM Node
// binding the tests use. Run with FLP_NETWORK_BENCH=1; set
// FLP_NETWORK_BENCH_OUT=<file> to write the page's results for an exactness diff.
import { describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import { handleNetworkRequest, type NetworkQueryParams } from "./network";

const FILES = Number(process.env.FLP_NETWORK_BENCH_FILES ?? 40);
const ROWS_PER_FILE = Number(process.env.FLP_NETWORK_BENCH_ROWS ?? 25_000);
const END_MS = Date.parse("2026-11-07T00:00:00.000Z");
const START_MS = END_MS - 24 * 3_600_000;

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
  const scratch = path.resolve(process.cwd(), ".network-bench");
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
          'www' || (i % 3)::VARCHAR || '.example.com' AS host,
          '/assets/' || floor(5000 * pow(u8, 2))::INT::VARCHAR || '.js' AS url,
          CASE WHEN u9 < 0.95 THEN 'GET' ELSE 'POST' END AS method,
          CASE WHEN u5 < 0.6 THEN 'HTTP/2' WHEN u5 < 0.85 THEN 'HTTP/1.1' ELSE 'HTTP/3' END AS proto,
          'Mozilla/5.0 agent ' || floor(200 * pow(u9, 2))::INT::VARCHAR AS ua,
          CASE WHEN u4 < 0.5 THEN NULL ELSE 'https://ref' || (asn_id % 50)::VARCHAR || '.example/' END AS referer,
          'POP' || pop_id::VARCHAR AS pop,
          'F_origin_' || (pop_id % 4)::VARCHAR AS backend,
          i % 5 <> 0 AS edge,
          round(0.001 + 0.4 * pow(u5, 3), 4)::DOUBLE AS ttfb,
          (1000 + 10 * floor(30000 * pow(u7, 3)))::DOUBLE AS ottfb,
          'region' || (pop_id % 12)::VARCHAR AS server_region,
          u8 < 0.2 AS is_ipv6,
          (1 + floor(20 * u6))::BIGINT AS conn_requests,
          CASE WHEN u7 < 0.9 THEN 'TLSv1.3' ELSE 'TLSv1.2' END AS tls,
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
      if (!process.env.FLP_NETWORK_BENCH_KEEP) fs.rmSync(name);
      files.push(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer);
    }
  } finally {
    conn.close();
    await db.reset();
    if (!process.env.FLP_NETWORK_BENCH_KEEP) fs.rmSync(scratch, { recursive: true, force: true });
  }
  return files;
}

type Timing = { label: string; ms: number };

function timed(read: DuckDBReadContext, sink: Timing[]): NonNullable<NetworkQueryParams["engine"]> {
  return {
    getPresentColumns: () => read.getPresentColumns(),
    getCustomColumnTypes: () => read.getCustomColumnTypes(),
    async executeSql<T>(sql: string, params?: readonly unknown[], signal?: AbortSignal) {
      const started = performance.now();
      const rows = await read.executeSql<T>(sql, params, signal);
      const label = sql.replace(/\s+/g, " ").trim().slice(0, 90);
      sink.push({ label, ms: performance.now() - started });
      return rows;
    },
  } as NonNullable<NetworkQueryParams["engine"]>;
}

// The requests /network issues on load, in the order the serial scan queue runs them.
const PAGE_REQUESTS: [string, string, NetworkQueryParams][] = [
  ["core", "/api/network-health", { bucket_seconds: 3600, top_n: 30, map_asn: "all",
    sections: ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets", "cities", "map_buckets"] }],
  ["shielding", "/api/network-health", { bucket_seconds: 3600, top_n: 30, map_asn: "all", sections: ["shielding_analysis"] }],
  ["quality", "/api/network-quality", { region_country: "US" }],
  ["pop-health", "/api/network/pop-health", {}],
  ["transport", "/api/network/bundle", { sections: ["transport"], bucket_seconds: 300, top_n: 30, map_asn: "all", region_country: "US" }],
];

describe.skipIf(!process.env.FLP_NETWORK_BENCH)("Cloud /network benchmark", () => {
  it("times every page request and query over Cloud-shaped Parquet", async () => {
    const generationStarted = performance.now();
    const files = await generateFiles();
    const totalBytes = files.reduce((sum, file) => sum + file.byteLength, 0);
    console.log(`generated ${FILES} files, ${FILES * ROWS_PER_FILE} rows, ${(totalBytes / 1048576).toFixed(1)} MiB in ${
      Math.round(performance.now() - generationStarted)} ms`);
    const engine = new DuckDBAnalyticalEngine();
    const lease = engine.lease([]);
    try {
      for (const [index, bytes] of files.entries()) {
        await lease.ingestFile(`bench:${index}`, `bench/${index}.parquet`, bytes, undefined, undefined, "request", { deferView: true });
      }
      await lease.publishView();
      const read = lease.readContext();
      const results: Record<string, unknown> = {};
      const rounds = Number(process.env.FLP_NETWORK_BENCH_ROUNDS ?? 2);
      for (let round = 0; round < rounds; round++) {
        let pageMs = 0;
        for (const [name, path, params] of PAGE_REQUESTS) {
          const queries: Timing[] = [];
          const started = performance.now();
          let failure = "";
          try {
            results[name] = await handleNetworkRequest(path, {
              ...params, start_time: START_MS, end_time: END_MS, engine: timed(read, queries),
            });
          } catch (error) {
            failure = ` FAILED: ${String(error).split("\n")[0]}`;
            results[name] = { error: String(error).split("\n")[0] };
          }
          const ms = performance.now() - started;
          pageMs += ms;
          console.log(`[round ${round}] ${name}: ${Math.round(ms)} ms over ${queries.length} queries${failure}`);
          for (const query of queries) console.log(`    ${String(Math.round(query.ms)).padStart(6)} ms  ${query.label}`);
        }
        console.log(`[round ${round}] page total: ${Math.round(pageMs)} ms`);
      }
      expect(results.core).toHaveProperty("summary.total_requests", FILES * ROWS_PER_FILE);
      const out = process.env.FLP_NETWORK_BENCH_OUT;
      if (out) {
        const fs = (process as unknown as { getBuiltinModule: (id: string) => unknown })
          .getBuiltinModule("fs") as { writeFileSync(path: string, data: string): void };
        fs.writeFileSync(out, JSON.stringify(results, null, 1));
      }
    } finally {
      lease.release();
      await engine.reset();
    }
  }, 900_000);
});
