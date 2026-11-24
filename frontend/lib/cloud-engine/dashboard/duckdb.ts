import { sqlEscape } from "@/lib/sqlBuilder";
import type {
  Aggregator,
  FieldAggregate,
  FileSource,
  MinuteBucket,
  PartialAggregates,
  Scalar,
} from "./engine";
import { numericFilterValue, type DashboardFilter } from "./filters";
import { fetchManifest, filesOverlapping, type ManifestFile } from "./manifest";
import type { ParquetQuerySchema } from "./parquet-projection";
import { CARD_FIELDS, type CustomField } from "./summaries";
import {
  boundedDuckDBCleanup,
  DuckDBBackendInitializationError,
  isFatalDuckDBError,
  ParquetBindingStore,
} from "./duckdb-bindings";

// wasm32 caps the heap at 4 GiB; DuckDB's default (~3 GiB) would starve the tab.
const MEMORY_LIMIT = "256MB";
const MINUTE_MS = 60_000;

export type DuckDBColumnType = "BIGINT" | "DOUBLE" | "BOOLEAN" | "VARCHAR";

export const FIELD_DUCKDB_TYPES: Record<string, DuckDBColumnType> = {
  resp_bytes: "BIGINT",
  ip: "VARCHAR",
  asn: "BIGINT",
  host: "VARCHAR",
  url: "VARCHAR",
  method: "VARCHAR",
  status: "BIGINT",
  cache: "VARCHAR",
  proto: "VARCHAR",
  ua: "VARCHAR",
  referer: "VARCHAR",
  resp_header_content_encoding: "VARCHAR",
  ttl: "DOUBLE",
  age: "DOUBLE",
  hits: "BIGINT",
  digest: "VARCHAR",
  city: "VARCHAR",
  region: "VARCHAR",
  country: "VARCHAR",
  metro: "BIGINT",
  transport: "VARCHAR",
  c_speed: "VARCHAR",
  c_type: "VARCHAR",
  pop: "VARCHAR",
  backend: "VARCHAR",
  edge: "BOOLEAN",
  server_region: "VARCHAR",
  tls: "VARCHAR",
  is_ipv6: "BOOLEAN",
  conn_requests: "BIGINT",
  waf: "BOOLEAN",
  waf_resp: "BIGINT",
  waf_ms: "BIGINT",
  p_type: "VARCHAR",
  p_desc: "VARCHAR",
  ja3: "VARCHAR",
  ja4: "VARCHAR",
  tls_ciphers_sha: "VARCHAR",
  io_input_format: "VARCHAR",
  io_output_format: "VARCHAR",
  // Additional catalog columns required by Pages 8–15 (F18–F26):
  elapsed: "BIGINT",
  ttfb: "DOUBLE",
  ottfb: "DOUBLE",
  ottlb: "DOUBLE",
  ost: "BIGINT",
  oip: "VARCHAR",
  obytes: "BIGINT",
  ohbytes: "BIGINT",
  rid: "VARCHAR",
  prid: "VARCHAR",
  req_bytes: "BIGINT",
  req_header_bytes: "BIGINT",
  req_body_bytes: "BIGINT",
  resp_header_bytes: "BIGINT",
  resp_body_bytes: "BIGINT",
  tcpi_min_rtt: "BIGINT",
  tcpi_rtt: "BIGINT",
  tcpi_rtt_var: "BIGINT",
  tcpi_delivery_rate: "BIGINT",
  tcpi_total_retrans: "BIGINT",
  tcpi_snd_cwnd: "BIGINT",
  tcp_rtt: "BIGINT",
  rtt_min: "BIGINT",
  rtt_var: "BIGINT",
  retrans: "BIGINT",
  ploss: "DOUBLE",
  delivery_rate: "BIGINT",
  cwnd: "BIGINT",
  lat: "DOUBLE",
  lon: "DOUBLE",
  protocol: "VARCHAR",
  is_ssl: "BOOLEAN",
  ipv6: "BOOLEAN",
  is_shield: "BOOLEAN",
  h2fp: "VARCHAR",
  ohfp: "VARCHAR",
  waf_sig: "VARCHAR",
  waf_action: "VARCHAR",
  waf_req_id: "VARCHAR",
  waf_logged_rules: "VARCHAR",
  waf_blocked_rules: "VARCHAR",
  ngwaf_bot_name: "VARCHAR",
  is_bot: "BOOLEAN",
  verified_bot: "BOOLEAN",
  _bot_name: "VARCHAR",
  content_type: "VARCHAR",
  resp_header_content_type: "VARCHAR",
  io_input_bytes: "BIGINT",
  io_output_bytes: "BIGINT",
  io_transform: "VARCHAR",
  ifmt: "VARCHAR",
  ofmt: "VARCHAR",
  cmcd_sid: "VARCHAR",
  cmcd_cid: "VARCHAR",
  cmcd_br: "BIGINT",
  cmcd_bl: "BIGINT",
  cmcd_bs: "BOOLEAN",
  cmcd_d: "BIGINT",
  cmcd_dl: "BIGINT",
  cmcd_mtp: "BIGINT",
  cmcd_nor: "VARCHAR",
  cmcd_nrr: "VARCHAR",
  cmcd_ot: "VARCHAR",
  cmcd_pr: "DOUBLE",
  cmcd_rtp: "BIGINT",
  cmcd_sf: "VARCHAR",
  cmcd_st: "VARCHAR",
  cmcd_su: "BOOLEAN",
  cmcd_tb: "BIGINT",
  cmcd_v: "BIGINT",
  edge_sid: "VARCHAR",
  edge_score: "BIGINT",
  edge_score_reason: "VARCHAR",
  cookie_session: "VARCHAR",
  service_id: "VARCHAR",
  fastly_req_id: "VARCHAR",
  // Long-form alias columns for Pages 8–15 compatibility:
  path: "VARCHAR",
  query_string: "VARCHAR",
  cache_status: "VARCHAR",
  resp_body_size: "BIGINT",
  req_body_size: "BIGINT",
  time_elapsed_usec: "BIGINT",
  time_to_first_byte_ns: "BIGINT",
  ttfb_ms: "DOUBLE",
  origin_time_ms: "DOUBLE",
  origin_Whole_ms: "DOUBLE",
  origin_ttfb_ms: "DOUBLE",
  origin_status: "BIGINT",
  origin_host: "VARCHAR",
  origin_ip: "VARCHAR",
  fastly_pop: "VARCHAR",
  fastly_is_shield: "BOOLEAN",
  is_edge: "BOOLEAN",
  geo_country: "VARCHAR",
  geo_city: "VARCHAR",
  geo_region: "VARCHAR",
  client_ip: "VARCHAR",
  client_asn: "BIGINT",
  as_name: "VARCHAR",
  user_agent: "VARCHAR",
  tls_version: "VARCHAR",
  tls_cipher: "VARCHAR",
  ja3_md5: "VARCHAR",
  h2_fingerprint: "VARCHAR",
  oh_fingerprint: "VARCHAR",
  client_rtt_us: "DOUBLE",
  client_ploss: "DOUBLE",
  client_cwnd: "BIGINT",
  client_tcpi_rtt: "DOUBLE",
  client_tcpi_rttvar: "DOUBLE",
  client_tcpi_rcv_space: "BIGINT",
  client_tcpi_snd_mss: "BIGINT",
  client_tcpi_delta_retrans: "BIGINT",
  content_encoding: "VARCHAR",
  asset_type: "VARCHAR",
  cache_control: "VARCHAR",
  surrogate_control: "VARCHAR",
  waf_rule_id: "VARCHAR",
  waf_message: "VARCHAR",
  waf_severity: "VARCHAR",
  ngwaf_signals: "VARCHAR",
  ngwaf_decision: "VARCHAR",
  bot_score: "DOUBLE",
  bot_category: "VARCHAR",
  bot_verified: "BOOLEAN",
  ddos_action: "VARCHAR",
};

const FILTERABLE_FIELDS = new Set<string>([
  ...CARD_FIELDS,
  ...Object.keys(FIELD_DUCKDB_TYPES),
]);

export const ALL_TABLE_FIELDS = Object.keys(FIELD_DUCKDB_TYPES);

const CUSTOM_DUCKDB_TYPES: Record<CustomField["type"], DuckDBColumnType> = {
  string: "VARCHAR",
  number: "DOUBLE",
  boolean: "BOOLEAN",
};

/** Column types of the custom fields the `requests` table holds. */
type CustomTypes = ReadonlyMap<string, DuckDBColumnType>;

export type IngestedFileMeta = {
  scopedKey: string;
  key: string;
  rawRows: number;
  byteLength: number;
  hasBytes: boolean;
  hasEdge: boolean;
  minTs: number;
  maxTs: number;
  /** Custom columns filled at ingest; others read NULL until re-ingested. */
  customColumns?: readonly string[];
  /** Physical columns present in the ingested Parquet file. */
  presentColumns?: readonly string[];
};

type ArrowColumnLike = {
  toJSON(): unknown[];
};

type ArrowSchemaField = {
  name: string;
};

export type ArrowTableLike = {
  numRows: number;
  schema?: { fields?: ArrowSchemaField[] };
  getChild(name: string): ArrowColumnLike | null;
};

export type DuckDBBackend = {
  registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>;
  registerParquet?(name: string, file: ParquetAccess): Promise<void>;
  dropFile(name: string): Promise<void>;
  /** Rejects with the abort reason only if the statement did not complete. */
  query(sql: string, signal?: AbortSignal): Promise<ArrowTableLike>;
  dispose(): Promise<void>;
};

export type ParquetAccess = (
  | { kind: "http"; url: string }
  | {
      kind: "opfs";
      handle: FileSystemFileHandle;
      fallbackUrl?: string;
      onPersistenceError?: (error: unknown) => Promise<void>;
    }
  | {
      kind: "buffer";
      bytes: ArrayBuffer;
      /** The file's schema from its footer; without one the read describes the file. */
      schema?: ParquetQuerySchema | null;
    }
) & {
  cached?: boolean;
  release?: () => void;
};

function concatBatches(
  batches: ArrowTableLike[],
  schema?: { fields?: ArrowSchemaField[] },
): ArrowTableLike {
  return {
    numRows: batches.reduce((sum, batch) => sum + batch.numRows, 0),
    schema: schema ?? batches[0]?.schema,
    getChild(name: string) {
      const columns = batches.map((batch) => batch.getChild(name));
      if (columns.length === 0 || columns.some((c) => !c)) return null;
      return {
        toJSON: () => columns.flatMap((column) => column!.toJSON()),
      };
    },
  };
}

export function duckSqlString(value: string): string {
  if (!value.includes("\0")) return `'${sqlEscape(value)}'`;
  return `(${value
    .split("\0")
    .map((part) => `'${sqlEscape(part)}'`)
    .join(" || chr(0) || ")})`;
}

const sqlString = duckSqlString;

const quoteDuckIdent = (s: string): string => {
  if (s.includes("\0")) {
    throw new Error("Invalid DuckDB identifier: NUL byte is not allowed");
  }
  return `"${s.replace(/"/g, '""')}"`;
};
const sqlIdent = quoteDuckIdent;

function escapeRegex(value: string): string {
  return value.replace(/[.+?^${}()|[\]\\]/g, "\\$&");
}

/** SQL twin of `filterText`: DOUBLE 3600 renders as "3600", not "3600.0". */
function filterTextSql(colSql: string, type: DuckDBColumnType): string {
  if (type !== "DOUBLE") return `CAST(${colSql} AS VARCHAR)`;
  return `CASE WHEN isfinite(${colSql}) AND ${colSql} = trunc(${colSql}) AND abs(${colSql}) < 1e21 THEN CAST(CAST(${colSql} AS HUGEINT) AS VARCHAR) ELSE CAST(${colSql} AS VARCHAR) END`;
}

function compileMatcherSql(
  field: string,
  pattern: string,
  customTypes: CustomTypes,
): string {
  const colSql = sqlIdent(field);
  const type = FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field) ?? "VARCHAR";
  const textExpr = filterTextSql(colSql, type);
  if (!pattern.includes("*")) {
    if (field === "conn_requests" && (type === "BIGINT" || type === "DOUBLE")) {
      if (pattern === "2\u20135" || pattern === "2-5") return `${colSql} BETWEEN 2 AND 5`;
      if (pattern === "6\u201320" || pattern === "6-20") return `${colSql} BETWEEN 6 AND 20`;
      if (pattern === "21+") return `${colSql} >= 21`;
    }
    if (type === "BIGINT" || type === "DOUBLE") {
      const numeric = numericFilterValue(pattern);
      return numeric === null
        ? "FALSE"
        : `TRY_CAST(${sqlString(String(numeric))} AS DOUBLE) = ${colSql}`;
    }
    return `${textExpr} = ${sqlString(pattern)}`;
  }
  const regexSource =
    "^" + pattern.split("*").map(escapeRegex).join(".*") + "$";
  return `regexp_matches(${textExpr}, ${sqlString(regexSource)}, 's')`;
}

export function compileFiltersToSql(
  filters: readonly DashboardFilter[],
  edgeOnly = false,
  noEdgeScopedKeys: readonly string[] = [],
  customTypes: CustomTypes = getDuckDBEngine().getCustomColumnTypes(),
): string {
  const valid = filters.filter(
    (f) =>
      (FILTERABLE_FIELDS.has(f.field) || customTypes.has(f.field)) &&
      f.value.length > 0,
  );

  const include = new Map<string, string[]>();
  const exclude = new Map<string, string[]>();
  for (const f of valid) {
    const expr = compileMatcherSql(f.field, f.value, customTypes);
    const map = f.mode === "include" ? include : exclude;
    const list = map.get(f.field) ?? [];
    list.push(expr);
    map.set(f.field, list);
  }

  const clauses: string[] = [];
  if (edgeOnly) {
    if (noEdgeScopedKeys.length > 0) {
      const noEdgeList = noEdgeScopedKeys.map(sqlString).join(", ");
      clauses.push(`("edge" = TRUE OR scoped_key IN (${noEdgeList}))`);
    } else {
      clauses.push(`("edge" = TRUE)`);
    }
  }
  for (const [field, exprs] of include) {
    const col = sqlIdent(field);
    clauses.push(`(${col} IS NOT NULL AND (${exprs.join(" OR ")}))`);
  }
  for (const [field, exprs] of exclude) {
    const col = sqlIdent(field);
    clauses.push(`(${col} IS NULL OR NOT (${exprs.join(" OR ")}))`);
  }
  return clauses.length > 0 ? clauses.join(" AND ") : "TRUE";
}

export async function createNodeBackend(): Promise<DuckDBBackend> {
  const getBuiltin = (
    process as unknown as {
      getBuiltinModule?: (id: string) => unknown;
    }
  ).getBuiltinModule;
  if (!getBuiltin) {
    throw new Error("Node builtin module loader unavailable");
  }
  const moduleBuiltin = getBuiltin("module") as {
    createRequire: (url: string) => (id: string) => unknown;
  };
  const pathBuiltin = getBuiltin("path") as {
    resolve: (...parts: string[]) => string;
    join: (...parts: string[]) => string;
  };
  const req = moduleBuiltin.createRequire(import.meta.url);
  const duckdb = req("@duckdb/duckdb-wasm/dist/duckdb-node-blocking") as {
    createDuckDB: (
      bundles: unknown,
      logger: unknown,
      runtime: unknown,
    ) => Promise<{
      instantiate: () => Promise<void>;
      registerFileBuffer: (name: string, buf: Uint8Array) => void;
      dropFile: (name: string) => void;
      reset: () => void | Promise<void>;
      connect: () => {
        query: (sql: string) => ArrowTableLike;
        close: () => void;
      };
    }>;
    VoidLogger: new () => unknown;
    NODE_RUNTIME: unknown;
  };
  const fsBuiltin = getBuiltin("fs") as {
    readFileSync: (filePath: string) => BufferSource;
  };
  const distDir = pathBuiltin.resolve(
    process.cwd(),
    "node_modules/@duckdb/duckdb-wasm/dist",
  );
  const wasmPath = pathBuiltin.join(distDir, "duckdb-eh.wasm");
  const db = await duckdb.createDuckDB(
    {
      eh: {
        mainModule: wasmPath,
        mainWorker: pathBuiltin.join(distDir, "duckdb-node-eh.worker.cjs"),
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  const nodeDb = db as typeof db & {
    _runtime?: Record<string, unknown>;
    instantiateWasm?: (
      imports: WebAssembly.Imports,
      cb: (inst: WebAssembly.Instance, mod: WebAssembly.Module) => void,
    ) => unknown;
  };
  if (typeof nodeDb.instantiateWasm === "function" && nodeDb._runtime) {
    const cacheSym = Symbol.for("flp.duckdb.nodeWasmModuleCache");
    const globalHolder = globalThis as unknown as Record<
      symbol,
      Map<string, Promise<WebAssembly.Module>> | undefined
    >;
    let modCache = globalHolder[cacheSym];
    if (!modCache) {
      modCache = new Map<string, Promise<WebAssembly.Module>>();
      globalHolder[cacheSym] = modCache;
    }
    const runtimeObj = nodeDb._runtime;
    nodeDb.instantiateWasm = (imports, cb) => {
      const rtGlobal = globalThis as unknown as {
        DUCKDB_RUNTIME?: Record<string, unknown>;
      };
      rtGlobal.DUCKDB_RUNTIME = {};
      for (const prop of Object.getOwnPropertyNames(runtimeObj)) {
        if (prop !== "constructor") {
          rtGlobal.DUCKDB_RUNTIME[prop] = Object.getOwnPropertyDescriptor(
            runtimeObj,
            prop,
          )?.value;
        }
      }
      let modPromise = modCache.get(wasmPath);
      if (!modPromise) {
        modPromise = WebAssembly.compile(fsBuiltin.readFileSync(wasmPath));
        modCache.set(wasmPath, modPromise);
      }
      void modPromise.then(async (compiledMod) => {
        const instance = await WebAssembly.instantiate(compiledMod, imports);
        cb(instance, compiledMod);
      });
      return [];
    };
  }
  let conn: ReturnType<typeof db.connect>;
  try {
    await db.instantiate();
    conn = db.connect();
  } catch (error) {
    try {
      await boundedDuckDBCleanup(async () => { await db.reset(); });
    } catch (cleanupError) {
      throw new DuckDBBackendInitializationError(error, false, cleanupError);
    }
    throw new DuckDBBackendInitializationError(error, true);
  }
  return {
    async registerFileBuffer(name: string, buffer: Uint8Array) {
      db.registerFileBuffer(name, buffer);
    },
    async dropFile(name: string) {
      db.dropFile(name);
    },
    async query(sql: string, signal?: AbortSignal) {
      // The Node build is blocking, so a statement cannot be interrupted.
      signal?.throwIfAborted();
      return conn.query(sql);
    },
    async dispose() {
      conn.close();
      await db.reset();
    },
  };
}

/** Matches the `/duckdb/<version>/` layout written by scripts/copy-duckdb-assets.mjs. */
export function duckdbAssetUrls(origin: string, version: string) {
  const base = `${origin}/duckdb/${version}`;
  return {
    worker: `${base}/duckdb-browser-eh.worker.js`,
    wasm: `${base}/duckdb-eh.wasm`,
    extensions: `${base}/extensions`,
  };
}

/** Pinned in package.json; lets the worker start before the SDK module loads. */
export const DUCKDB_WASM_VERSION = "1.33.1-dev57.0";

export async function createBrowserBackend(): Promise<DuckDBBackend> {
  const origin =
    typeof self !== "undefined" && self.location ? self.location.origin : "";
  const assets = duckdbAssetUrls(origin, DUCKDB_WASM_VERSION);
  const worker = new Worker(assets.worker);
  let duckdb: typeof import("@duckdb/duckdb-wasm");
  try {
    duckdb = await import("@duckdb/duckdb-wasm");
  } catch (error) {
    worker.terminate();
    throw error;
  }
  const logger = new duckdb.VoidLogger();
  let db: InstanceType<typeof duckdb.AsyncDuckDB>;
  try {
    db = new duckdb.AsyncDuckDB(logger, worker);
  } catch (error) {
    try { worker.terminate(); }
    catch (cleanupError) { throw new DuckDBBackendInitializationError(error, false, cleanupError); }
    throw new DuckDBBackendInitializationError(error, true);
  }
  let conn: Awaited<ReturnType<typeof db.connect>>;
  // AsyncDuckDB drops its pending requests on a worker error without settling
  // them, so initialization would otherwise wait forever.
  let workerFailed!: (error: Error) => void;
  const workerFailure = new Promise<never>((_, reject) => { workerFailed = reject; });
  const onWorkerError = (event: Event) => {
    workerFailed(new Error(`DuckDB worker failed: ${(event as ErrorEvent).message || "script error"}`));
  };
  worker.addEventListener("error", onWorkerError);
  try {
    conn = await Promise.race([(async () => {
      await db.instantiate(assets.wasm);
      await db.open({
        // Disable open-time full-file fallback after the source's 206 identity
        // probe. The SDK can still accept 200 responses on subsequent range reads.
        filesystem: {
          reliableHeadRequests: true,
          allowFullHTTPReads: false,
          forceFullHTTPReads: false,
        },
        opfs: { fileHandling: "manual" },
      });
      const connection = await db.connect();
      await connection.query(
        `SET custom_extension_repository = ${sqlString(assets.extensions)}`,
      );
      return connection;
    })(), workerFailure]);
  } catch (error) {
    try { await boundedDuckDBCleanup(() => db.terminate()); }
    catch (cleanupError) { throw new DuckDBBackendInitializationError(error, false, cleanupError); }
    throw new DuckDBBackendInitializationError(error, true);
  } finally {
    worker.removeEventListener("error", onWorkerError);
  }
  // One pending query per connection: serialize so cancelSent() only ever
  // cancels the statement that belongs to the aborted signal.
  let queue: Promise<unknown> = Promise.resolve();
  return {
    async dispose() {
      await db.terminate();
    },
    async registerFileBuffer(name: string, buffer: Uint8Array) {
      // Transfers buffer.buffer to the DuckDB worker; the caller must own it.
      await db.registerFileBuffer(name, buffer);
    },
    async registerParquet(name, file) {
      if (file.kind === "http") {
        // Direct I/O bypasses DuckDB's page cache and amplifies tiny range reads.
        await db.registerFileURL(
          name,
          file.url,
          duckdb.DuckDBDataProtocol.HTTP,
          false,
        );
      } else if (file.kind === "opfs") {
        try {
          await db.registerFileHandle(
            name,
            file.handle,
            duckdb.DuckDBDataProtocol.BROWSER_FSACCESS,
            true,
          );
        } catch (error) {
          if (!file.fallbackUrl) throw error;
          await file.onPersistenceError?.(error);
          file.cached = false;
          await db.registerFileURL(
            name,
            file.fallbackUrl,
            duckdb.DuckDBDataProtocol.HTTP,
            false,
          );
        }
      } else {
        await db.registerFileBuffer(name, new Uint8Array(file.bytes));
      }
    },
    async dropFile(name: string) {
      await db.dropFile(name);
    },
    query(sql: string, signal?: AbortSignal) {
      const run = queue.then(async () => {
        signal?.throwIfAborted();
        const onAbort = () => {
          void conn.cancelSent().catch(() => {});
        };
        signal?.addEventListener("abort", onAbort, { once: true });
        try {
          const reader = await conn.send(sql);
          const batches: ArrowTableLike[] = [];
          let rows = 0;
          let bytes = 0;
          for await (const batch of reader) {
            rows += batch.numRows;
            bytes += batch.data.byteLength;
            if (rows > 200_000 || bytes > 64 * 1024 * 1024) {
              await conn.cancelSent();
              throw new Error(
                "The selective query exceeds the browser aggregate budget; choose a shorter range or add a filter.",
              );
            }
            batches.push(batch as unknown as ArrowTableLike);
          }
          return concatBatches(
            batches,
            (reader as { schema?: { fields?: ArrowSchemaField[] } }).schema,
          );
        } catch (error) {
          if (isFatalDuckDBError(error)) throw error;
          signal?.throwIfAborted();
          throw error;
        } finally {
          signal?.removeEventListener("abort", onAbort);
        }
      });
      queue = run.catch(() => {});
      return run;
    },
  };
}

export type DuckDBLease = {
  /**
   * Returns resident metadata and pins the file until `release`; undefined
   * when it isn't resident with every custom column the lease needs.
   */
  getFileMeta(scopedKey: string, kind?: "request" | "vitals" | "errors"): IngestedFileMeta | undefined;
  /** Ingests (or reuses) a file and pins it until `release`. */
  ingestFile(
    scopedKey: string,
    key: string,
    bytes: ArrayBuffer,
    knownStats?: { rawRows: number; minTs: number; maxTs: number },
    signal?: AbortSignal,
    kind?: "request" | "vitals" | "errors",
    /** `presentColumns`: the source file's columns, for a column-projected file. */
    options?: { deferView?: boolean; presentColumns?: readonly string[] },
  ): Promise<IngestedFileMeta>;
  /**
   * Rebuilds the views once over every held file. Call after ingesting with
   * `deferView`; on failure the deferred files are retired.
   */
  publishView(signal?: AbortSignal): Promise<void>;
  release(): void;
  readContext(): DuckDBReadContext;
};

export type DuckDBReadContext = {
  release?(): void;
  assertCurrent(): void;
  executeSql<T = Record<string, unknown>>(sql: string, params?: readonly unknown[], signal?: AbortSignal): Promise<T[]>;
  querySql<T = Record<string, unknown>>(sql: string, params?: readonly unknown[], signal?: AbortSignal): Promise<T[]>;
  getPresentColumns(scopedKeys?: readonly string[]): Promise<Set<string>>;
  getCustomColumnTypes(): CustomTypes;
  getFileMeta(scopedKey: string): IngestedFileMeta | undefined;
};

export type AggregateQueryOptions = {
  context?: DuckDBReadContext;
  edgeOnly?: boolean;
  /**
   * Return only the top-K values (plus ties) per field. Applied only while the
   * aggregator is still empty; otherwise full counts are needed to merge exactly.
   */
  topK?: number | null;
  /** Fields that always need every value (e.g. the country map). */
  allValuesFields?: readonly string[];
  /** Fields that keep more top values than `topK` (e.g. user agents for bots). */
  rankedFields?: Readonly<Record<string, number>>;
  signal?: AbortSignal;
  /** Field search must match before ranking and cannot use approximate candidates. */
  fieldSearch?: { field: string; query: string };
};

function toScalarValue(value: unknown): Scalar | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isNaN(value) ? null : value;
  return String(value);
}

export function interpolateSqlParams(
  sql: string,
  params: readonly unknown[] = [],
): string {
  if (!params || params.length === 0) return sql;
  const encodeParam = (val: unknown): string => {
    if (val === null || val === undefined) return "NULL";
    if (typeof val === "number") {
      return Number.isFinite(val) ? String(val) : "NULL";
    }
    if (typeof val === "bigint") return String(val);
    if (typeof val === "boolean") return val ? "TRUE" : "FALSE";
    if (val instanceof Date) return sqlString(val.toISOString());
    if (Array.isArray(val)) {
      return `[${val.map(encodeParam).join(", ")}]`;
    }
    return sqlString(String(val));
  };
  let positionalIdx = 0;
  let out = "";
  let i = 0;
  const len = sql.length;
  while (i < len) {
    const ch = sql[i];
    if (
      (ch === "E" || ch === "e") &&
      i + 1 < len &&
      sql[i + 1] === "'" &&
      (i === 0 || !/[A-Za-z0-9_$\u0080-\uffff]/.test(sql[i - 1]))
    ) {
      let j = i + 2;
      while (j < len) {
        if (sql[j] === "\\" && j + 1 < len) {
          j += 2;
          continue;
        }
        if (sql[j] === "'") {
          if (j + 1 < len && sql[j + 1] === "'") {
            j += 2;
            continue;
          }
          j += 1;
          break;
        }
        j += 1;
      }
      out += sql.slice(i, j);
      i = j;
      continue;
    }
    if (ch === "'") {
      let j = i + 1;
      while (j < len) {
        if (sql[j] === "'") {
          if (j + 1 < len && sql[j + 1] === "'") {
            j += 2;
            continue;
          }
          j += 1;
          break;
        }
        j += 1;
      }
      out += sql.slice(i, j);
      i = j;
      continue;
    }
    if (ch === '"') {
      let j = i + 1;
      while (j < len) {
        if (sql[j] === '"') {
          if (j + 1 < len && sql[j + 1] === '"') {
            j += 2;
            continue;
          }
          j += 1;
          break;
        }
        j += 1;
      }
      out += sql.slice(i, j);
      i = j;
      continue;
    }
    if (ch === "-" && i + 1 < len && sql[i + 1] === "-") {
      let j = i + 2;
      while (j < len && sql[j] !== "\n") j += 1;
      if (j < len) j += 1;
      out += sql.slice(i, j);
      i = j;
      continue;
    }
    if (ch === "/" && i + 1 < len && sql[i + 1] === "*") {
      let j = i + 2;
      let depth = 1;
      while (j < len && depth > 0) {
        if (sql[j] === "/" && j + 1 < len && sql[j + 1] === "*") {
          depth += 1;
          j += 2;
          continue;
        }
        if (sql[j] === "*" && j + 1 < len && sql[j + 1] === "/") {
          depth -= 1;
          j += 2;
          continue;
        }
        j += 1;
      }
      out += sql.slice(i, j);
      i = j;
      continue;
    }
    if (/[A-Za-z_\u0080-\uffff]/.test(ch)) {
      const identMatch = sql
        .slice(i)
        .match(/^[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*/);
      if (identMatch) {
        out += identMatch[0];
        i += identMatch[0].length;
        continue;
      }
    }
    if (ch === "$") {
      const rest = sql.slice(i);
      const numMatch = rest.match(/^\$([0-9]+)/);
      if (numMatch) {
        const idx = Number(numMatch[1]) - 1;
        out +=
          idx >= 0 && idx < params.length ? encodeParam(params[idx]) : "NULL";
        i += numMatch[0].length;
        continue;
      }
      const tagMatch = rest.match(
        /^\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/,
      );
      if (tagMatch) {
        const delim = tagMatch[0];
        const endIdx = sql.indexOf(delim, i + delim.length);
        const j = endIdx === -1 ? len : endIdx + delim.length;
        out += sql.slice(i, j);
        i = j;
        continue;
      }
    }
    if (ch === "?") {
      const idx = positionalIdx++;
      out += idx < params.length ? encodeParam(params[idx]) : "NULL";
      i += 1;
      continue;
    }
    out += ch;
    i += 1;
  }
  return out;
}

function arrowTableToRows<T = Record<string, unknown>>(
  table: ArrowTableLike,
): T[] {
  const numRows = table.numRows;
  if (numRows === 0) return [];
  const fieldNames = table.schema?.fields?.map((f) => f.name) ?? [];
  const columns = new Map<string, unknown[]>();
  for (const name of fieldNames) {
    columns.set(name, table.getChild(name)?.toJSON() ?? []);
  }
  const rows: T[] = [];
  for (let r = 0; r < numRows; r++) {
    const obj: Record<string, unknown> = {};
    for (const [name, col] of columns) {
      const raw = col[r];
      if (typeof raw === "bigint") {
        obj[name] = Number(raw);
      } else if (typeof raw === "number" && Number.isNaN(raw)) {
        obj[name] = null;
      } else {
        obj[name] = raw ?? null;
      }
    }
    rows.push(obj as T);
  }
  return rows;
}

export const RUM_VITALS_COLUMNS: Readonly<Record<string, DuckDBColumnType>> = {
  request_event_id: "VARCHAR",
  client_id: "VARCHAR",
  metric_name: "VARCHAR",
  metric_value: "DOUBLE",
  rating: "VARCHAR",
  url: "VARCHAR",
  page_url: "VARCHAR",
  path: "VARCHAR",
  browser_name: "VARCHAR",
  browser_version: "VARCHAR",
  os_name: "VARCHAR",
  os_version: "VARCHAR",
  device_type: "VARCHAR",
  connection_type: "VARCHAR",
  country: "VARCHAR",
  city: "VARCHAR",
  asn: "BIGINT",
  as_name: "VARCHAR",
  session_id: "VARCHAR",
  nav_type: "VARCHAR",
  element: "VARCHAR",
  attribution: "VARCHAR",
  service_id: "VARCHAR",
};

export const RUM_ERRORS_COLUMNS: Readonly<Record<string, DuckDBColumnType>> = {
  request_event_id: "VARCHAR",
  client_id: "VARCHAR",
  message: "VARCHAR",
  error_message: "VARCHAR",
  error_type: "VARCHAR",
  filename: "VARCHAR",
  source: "VARCHAR",
  lineno: "BIGINT",
  colno: "BIGINT",
  stack: "VARCHAR",
  url: "VARCHAR",
  page_url: "VARCHAR",
  path: "VARCHAR",
  browser_name: "VARCHAR",
  os_name: "VARCHAR",
  device_type: "VARCHAR",
  country: "VARCHAR",
  session_id: "VARCHAR",
  service_id: "VARCHAR",
};

export class DuckDBAnalyticalEngine extends ParquetBindingStore {
  private nextVirtualFileId = 1;
  /** Most value rows and key bytes one per-file values pass may return. */
  private readonly valuesBudget: { rows: number; bytes: number };

  constructor(options: {
    backendFactory?: () => Promise<DuckDBBackend>;
    valuesBudget?: { rows: number; bytes: number };
  } = {}) {
    super({
      factory: options.backendFactory ?? (() => typeof process !== "undefined" && process.versions?.node
        ? createNodeBackend() : createBrowserBackend()),
      schemas: { requests: FIELD_DUCKDB_TYPES, client_vitals: RUM_VITALS_COLUMNS, client_errors: RUM_ERRORS_COLUMNS },
      memoryLimit: MEMORY_LIMIT,
      rows: arrowTableToRows,
      interpolate: interpolateSqlParams,
    });
    this.valuesBudget = options.valuesBudget ?? {
      rows: 150_000,
      bytes: 32 * 1024 * 1024,
    };
  }

  async memoryLimit(): Promise<string> {
    return this.getMemoryLimit();
  }

  /**
   * Aggregates the scoped files in SQL and merges only the aggregates
   * (minute buckets, per-field counts) into `aggregator`.
   */
  async queryIntoAggregator(
    scopedKeys: readonly string[],
    fromMs: number,
    toMs: number,
    filters: readonly DashboardFilter[],
    aggregator: Aggregator,
    options: AggregateQueryOptions = {},
  ): Promise<void> {
    const { signal } = options;
    signal?.throwIfAborted();
    const context = options.context ?? this.readContext(scopedKeys);
    context.assertCurrent();
    if (scopedKeys.length === 0) return;
    const customTypes = context.getCustomColumnTypes();
    const noEdgeKeys: string[] = [];
    let hasBytes = false;
    for (const sk of scopedKeys) {
      const meta = context.getFileMeta(sk);
      if (meta?.hasBytes) hasBytes = true;
      if (meta && !meta.hasEdge) noEdgeKeys.push(sk);
    }
    const where = [
      `scoped_key IN (${scopedKeys.map(sqlString).join(", ")})`,
      "ts_ms IS NOT NULL",
      ...(Number.isFinite(fromMs) ? [`ts_ms >= ${Math.floor(fromMs)}`] : []),
      ...(Number.isFinite(toMs) ? [`ts_ms < ${Math.ceil(toMs)}`] : []),
      `(${compileFiltersToSql(filters, Boolean(options.edgeOnly), noEdgeKeys, customTypes)})`,
    ].join(" AND ");

    await this.runRead(context, signal, (backend) => this.queryRelation(
      backend, "requests", where, hasBytes, customTypes, aggregator, options,
    ));
  }

  async queryParquetIntoAggregator(
    files: ParquetAccess | readonly ParquetAccess[],
    fromMs: number,
    toMs: number,
    filters: readonly DashboardFilter[],
    aggregator: Aggregator,
    options: AggregateQueryOptions = {},
    customFields: readonly CustomField[] = [],
  ): Promise<void> {
    const accesses: readonly ParquetAccess[] = isParquetAccessList(files)
      ? files
      : [files];
    const customTypes = customTypesOf(customFields);
    await this.withParquetGroups(accesses, options, async (backend, groups) => {
      // Small files pay a fixed cost per query, so files whose Parquet
      // schemas match exactly share one pass: the union reads like each file.
      for (const group of groups) {
        const relation = parquetRelation(
          group,
          [{ names: group.names, fromMs, toMs }],
          [...aggregator.fields, ...filters.map((filter) => filter.field)],
          customTypes,
        );
        await this.queryRelation(
          backend,
          relation,
          selectiveWhere(group, filters, options, customTypes),
          group.columns.has("resp_bytes"),
          customTypes,
          aggregator,
          options,
          !options.fieldSearch,
        );
      }
    });
  }

  /**
   * Like `queryParquetIntoAggregator`, but each file gets its own time bounds
   * and its own result in `target`, while files with one schema still share a
   * single pass. Values are bounded per file, as one-file queries would be.
   */
  async queryParquetPerFile(
    files: readonly PerFileQuery[],
    filters: readonly DashboardFilter[],
    options: AggregateQueryOptions = {},
    customFields: readonly CustomField[] = [],
  ): Promise<void> {
    if (options.fieldSearch) {
      throw new Error("Per-file queries do not support field search");
    }
    const customTypes = customTypesOf(customFields);
    const fields = [
      ...new Set(files.flatMap((file) => file.target.fields)),
    ];
    await this.withParquetGroups(
      files.map((file) => file.access),
      options,
      async (backend, groups, registered) => {
        for (const group of groups) {
          const parts = group.names.map((name) => {
            const file = registered.indexOf(name);
            return {
              names: [name],
              fromMs: files[file].fromMs,
              toMs: files[file].toMs,
              file,
            };
          });
          const relation = parquetRelation(
            group,
            parts,
            [...fields, ...filters.map((filter) => filter.field)],
            customTypes,
          );
          const targets = new Map(
            parts.map((part) => [part.file, files[part.file].target]),
          );
          await this.queryRelationPerFile(
            backend,
            relation,
            selectiveWhere(group, filters, options, customTypes),
            group.columns.has("resp_bytes"),
            customTypes,
            targets,
            options,
          );
        }
      },
    );
  }

  /** Registers the files, describes each schema group, and always drops them. */
  private withParquetGroups(
    accesses: readonly ParquetAccess[],
    options: AggregateQueryOptions,
    run: (
      backend: DuckDBBackend,
      groups: ParquetGroup[],
      registered: readonly string[],
    ) => Promise<void>,
  ): Promise<void> {
    const { signal } = options;
    return this.runRead(options.context, signal, async (backend) => {
      signal?.throwIfAborted();
      const registered: string[] = [];
      let failure: unknown;
      try {
        for (const file of accesses) {
          const name = `selective_${this.nextVirtualFileId++}.parquet`;
          if (file.kind === "buffer") {
            await backend.registerFileBuffer(name, new Uint8Array(file.bytes.slice(0)));
          } else if (backend.registerParquet) {
            await backend.registerParquet(name, file);
          } else {
            throw new Error(
              "Remote Parquet reads require the browser DuckDB runtime",
            );
          }
          registered.push(name);
          signal?.throwIfAborted();
        }
        await this.cacheParquetFooters(backend, signal);
        const footerGroups = footerSchemaGroups(accesses, registered);
        const groups: ParquetGroup[] = footerGroups ?? [];
        for (const names of footerGroups ? [] : await parquetSchemaGroups(backend, registered, signal)) {
          const schema = await backend.query(
            `DESCRIBE SELECT * FROM read_parquet([${names.map(sqlString).join(", ")}])`,
            signal,
          );
          const columnNames =
            (schema.getChild("column_name")?.toJSON() as string[]) ?? [];
          const types =
            (schema.getChild("column_type")?.toJSON() as string[]) ?? [];
          const tsType = types[columnNames.indexOf("timestamp")]?.toUpperCase();
          if (!tsType) throw new Error("Parquet file has no timestamp column");
          groups.push({ names, columns: new Set(columnNames), tsType });
        }
        await run(backend, groups, registered);
      } catch (error) {
        failure = error;
        throw error;
      } finally {
        let cleanupError: unknown;
        let cleanupFailed = false;
        for (const name of registered) {
          try {
            await backend.dropFile(name);
          } catch (error) {
            if (!cleanupFailed) cleanupError = error;
            cleanupFailed = true;
          }
        }
        if (cleanupFailed) {
          if (failure !== undefined && failure !== cleanupError) {
            throw new AggregateError([failure, cleanupError],
              `${failure instanceof Error ? failure.message : "Cloud query failed"}; ${cleanupError instanceof Error ? cleanupError.message : "DuckDB file cleanup failed"}`);
          }
          throw cleanupError;
        }
      }
    });
  }

  /**
   * `queryRelation` in bounded mode, grouped by the relation's file tag: each
   * target receives exactly what a one-file query would have merged.
   */
  private async queryRelationPerFile(
    backend: DuckDBBackend,
    relation: string,
    where: string,
    hasBytes: boolean,
    customTypes: CustomTypes,
    targets: ReadonlyMap<number, Aggregator>,
    options: AggregateQueryOptions,
  ): Promise<void> {
    const { signal } = options;
    const fields = [
      ...new Set([...targets.values()].flatMap((target) => target.fields)),
    ].filter((f) => f in FIELD_DUCKDB_TYPES || customTypes.has(f));
    const project = (columns: readonly string[]) =>
      columns
        .map((field) =>
          (FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field)) === "DOUBLE"
            ? `(${sqlIdent(field)} + 0.0) AS ${sqlIdent(field)}`
            : sqlIdent(field),
        )
        .join(", ");
    const bucketTable = await backend.query(
      `WITH base AS NOT MATERIALIZED (SELECT ${FILE_COLUMN}, ts_ms, ${project(["status", "cache", "resp_bytes"])} FROM ${relation} WHERE ${where}) SELECT ${FILE_COLUMN} AS file, (floor(ts_ms / ${MINUTE_MS}) * ${MINUTE_MS})::BIGINT AS t, COUNT(*)::BIGINT AS requests, COALESCE(SUM("resp_bytes"), 0)::DOUBLE AS bytes, (COUNT(*) FILTER (WHERE "status" >= 500 AND "status" < 600))::BIGINT AS s5xx, (COUNT(*) FILTER (WHERE "status" >= 400 AND NOT ("status" >= 500 AND "status" < 600)))::BIGINT AS s4xx, COUNT("cache")::BIGINT AS cache_rows, (COUNT(*) FILTER (WHERE starts_with(upper("cache"), 'HIT')))::BIGINT AS hits, MIN(ts_ms)::BIGINT AS min_ts, MAX(ts_ms)::BIGINT AS max_ts FROM base GROUP BY 1, 2 ORDER BY 1, 2`,
      signal,
    );
    signal?.throwIfAborted();
    if (hasBytes) for (const target of targets.values()) target.hasBytes = true;
    if (bucketTable.numRows === 0) return;

    const column = (table: ArrowTableLike, name: string) =>
      table.getChild(name)?.toJSON() ?? [];
    const parts = new Map<number, PartialAggregates>();
    const partOf = (file: number) => {
      let part = parts.get(file);
      if (!part) {
        part = { totalRequests: 0, minTs: Infinity, maxTs: -Infinity, buckets: [], fields: {} };
        parts.set(file, part);
      }
      return part;
    };
    const bf = column(bucketTable, "file");
    const bt = column(bucketTable, "t");
    const requests = column(bucketTable, "requests");
    const bytes = column(bucketTable, "bytes");
    const s4xx = column(bucketTable, "s4xx");
    const s5xx = column(bucketTable, "s5xx");
    const cacheRows = column(bucketTable, "cache_rows");
    const hits = column(bucketTable, "hits");
    const minCol = column(bucketTable, "min_ts");
    const maxCol = column(bucketTable, "max_ts");
    for (let i = 0; i < bucketTable.numRows; i++) {
      const part = partOf(Number(bf[i]));
      const bucket: MinuteBucket = {
        t: Number(bt[i]),
        requests: Number(requests[i]),
        bytes: Number(bytes[i]),
        s4xx: Number(s4xx[i]),
        s5xx: Number(s5xx[i]),
        hits: Number(hits[i]),
        cacheRows: Number(cacheRows[i]),
      };
      part.buckets.push(bucket);
      part.totalRequests += bucket.requests;
      part.minTs = Math.min(part.minTs, Number(minCol[i]));
      part.maxTs = Math.max(part.maxTs, Number(maxCol[i]));
    }

    if (fields.length > 0) {
      const typeOf: FieldTypeOf = (field) =>
        FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field)!;
      // The same per-field bounds as a one-file `queryRelation`.
      const candidateLimit = Math.min(4096, Math.floor(150_000 / fields.length));
      const fieldByteCap = Math.floor((32 * 1024 * 1024) / fields.length);
      // An inlined base rescans the Parquet once per card branch, and one
      // statement over every card held each branch's per-file hash tables and
      // windows at once, which ran out of memory on a batch of small files. A
      // few cards share each statement, over their own columns of the
      // filtered rows, materialized once per statement while they are few
      // enough to hold.
      const fieldGroups = fieldGroupsOf(fields.length);
      let rows = 0;
      for (const part of parts.values()) rows += part.totalRequests;
      const materialize =
        rows <= MATERIALIZE_ROWS ? "MATERIALIZED" : "NOT MATERIALIZED";
      const groupBase = (group: readonly number[]) =>
        `WITH base AS ${materialize} (SELECT ${FILE_COLUMN}, ${project(group.map((i) => fields[i]))} FROM ${relation} WHERE ${where})`;

      const sizes = new Map<number, { rows: number; bytes: number }>();
      // Statistics rows carry `nn` (a COUNT, never null); value rows `cnt`.
      const readStats = (table: ArrowTableLike) => {
        const sf = column(table, "file");
        const si = column(table, "field_idx");
        const snn = column(table, "nn");
        const sd = column(table, "d");
        const sml = column(table, "ml");
        const cnt = column(table, "cnt");
        for (let r = 0; r < table.numRows; r++) {
          if (cnt[r] != null) continue;
          const file = Number(sf[r]);
          const i = Number(si[r]);
          const distinct = Number(sd[r] ?? 0);
          const keyBytes = 48 + 4 * Number(sml[r] ?? 0);
          const kept = Math.min(distinct, candidateLimit);
          let size = sizes.get(file);
          if (!size) sizes.set(file, (size = { rows: 0, bytes: 0 }));
          size.rows += kept;
          size.bytes += Math.min(kept * keyBytes, fieldByteCap + keyBytes);
          partOf(file).fields[fields[i]] = {
            nonNull: Number(snn[r] ?? 0),
            distinct,
            entries: [],
            complete: false,
          };
        }
      };
      const readValues = (table: ArrowTableLike) => {
        const vf = column(table, "file");
        const idxCol = column(table, "field_idx");
        const cntCol = column(table, "cnt");
        const valueCols = fields.map((field) => column(table, valueColumn(typeOf(field))));
        for (let r = 0; r < table.numRows; r++) {
          if (cntCol[r] == null) continue;
          const i = Number(idxCol[r]);
          const value = toScalarValue(valueCols[i][r]);
          if (value === null) continue;
          parts.get(Number(vf[r]))?.fields[fields[i]]?.entries.push([value, Number(cntCol[r])]);
        }
      };
      const qualify = boundedQualify(fields, candidateLimit, fieldByteCap);

      if (parts.size === 1) {
        // One file's values are always a single pass, so they share the
        // statistics statement and its columns are decoded once.
        for (const group of fieldGroups) {
          const table = await backend.query(
            `${groupBase(group)} SELECT * FROM (${fieldStatsSql(fields, true, true, group)}) UNION ALL BY NAME SELECT * FROM (${fieldValuesSql(fields, typeOf, true, "", qualify, group)})`,
            signal,
          );
          signal?.throwIfAborted();
          readStats(table);
          readValues(table);
        }
      } else {
        for (const group of fieldGroups) {
          readStats(await backend.query(
            `${groupBase(group)} ${fieldStatsSql(fields, true, true, group)}`,
            signal,
          ));
          signal?.throwIfAborted();
        }

        // Each file may return its own bounded values, so files share a values
        // pass only while their worst case fits the browser's result budget.
        const chunks: number[][] = [];
        let chunkRows = 0;
        let chunkBytes = 0;
        for (const [file, { rows, bytes }] of [...sizes].sort(([a], [b]) => a - b)) {
          if (
            chunks.length === 0 ||
            chunkRows + rows > this.valuesBudget.rows ||
            chunkBytes + bytes > this.valuesBudget.bytes
          ) {
            chunks.push([]);
            chunkRows = 0;
            chunkBytes = 0;
          }
          chunks[chunks.length - 1].push(file);
          chunkRows += rows;
          chunkBytes += bytes;
        }

        for (const chunk of chunks) {
          const only = chunks.length > 1 ? ` AND ${FILE_COLUMN} IN (${chunk.join(", ")})` : "";
          for (const group of fieldGroups) {
            readValues(await backend.query(
              `${groupBase(group)} ${fieldValuesSql(fields, typeOf, true, only, qualify, group)}`,
              signal,
            ));
            signal?.throwIfAborted();
          }
        }
      }
      for (const part of parts.values()) {
        for (const aggregate of Object.values(part.fields)) {
          aggregate.complete = keptDistinct(aggregate) === aggregate.distinct;
          aggregate.approximate = !aggregate.complete;
        }
      }
    }

    for (const [file, part] of parts) {
      const target = targets.get(file);
      if (!target) continue;
      target.mergeAggregates(part);
      target.boundRankings();
    }
  }

  private async queryRelation(
    backend: DuckDBBackend,
    relation: string,
    where: string,
    hasBytes: boolean,
    customTypes: CustomTypes,
    aggregator: Aggregator,
    options: AggregateQueryOptions,
    bounded = false,
  ): Promise<void> {
    const { signal } = options;
    const fields = aggregator.fields.filter(
      (f) => f in FIELD_DUCKDB_TYPES || customTypes.has(f),
    );
    const baseCols = new Set(["status", "cache", "resp_bytes", ...fields]);
    const projection = [...baseCols]
      .map((field) =>
        (FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field)) === "DOUBLE"
          ? // Folds -0 into 0, as JavaScript Map keys do.
            `(${sqlIdent(field)} + 0.0) AS ${sqlIdent(field)}`
          : sqlIdent(field),
      )
      .join(", ");
    const search = options.fieldSearch;
    const searchType = search &&
      (FIELD_DUCKDB_TYPES[search.field] ?? customTypes.get(search.field));
    // Numeric casts and Unicode case folding differ from JavaScript's search text.
    const searchWhere = search &&
      (searchType === "VARCHAR" || searchType === "BOOLEAN") &&
      /^[ -~]*$/.test(search.query)
      ? ` AND contains(lower(CAST(${sqlIdent(search.field)} AS VARCHAR)), ${sqlString(search.query.trim().toLowerCase())})`
      : "";
    const base = `WITH base AS NOT MATERIALIZED (SELECT ts_ms, ${projection} FROM ${relation} WHERE ${where}${searchWhere})`;
    const groupBase = (group: readonly number[], materialize: boolean) =>
      `WITH base AS ${materialize ? "" : "NOT "}MATERIALIZED (SELECT ${group
        .map((i) => fields[i])
        .map((field) =>
          (FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field)) === "DOUBLE"
            ? `(${sqlIdent(field)} + 0.0) AS ${sqlIdent(field)}`
            : sqlIdent(field),
        )
        .join(", ")} FROM ${relation} WHERE ${where}${searchWhere})`;

    const bucketTable = await backend.query(
      `${base} SELECT (floor(ts_ms / ${MINUTE_MS}) * ${MINUTE_MS})::BIGINT AS t, COUNT(*)::BIGINT AS requests, COALESCE(SUM("resp_bytes"), 0)::DOUBLE AS bytes, (COUNT(*) FILTER (WHERE "status" >= 500 AND "status" < 600))::BIGINT AS s5xx, (COUNT(*) FILTER (WHERE "status" >= 400 AND NOT ("status" >= 500 AND "status" < 600)))::BIGINT AS s4xx, COUNT("cache")::BIGINT AS cache_rows, (COUNT(*) FILTER (WHERE starts_with(upper("cache"), 'HIT')))::BIGINT AS hits, MIN(ts_ms)::BIGINT AS min_ts, MAX(ts_ms)::BIGINT AS max_ts FROM base GROUP BY 1 ORDER BY 1`,
      signal,
    );
    signal?.throwIfAborted();
    if (hasBytes) aggregator.hasBytes = true;
    if (bucketTable.numRows === 0) return;

    const column = (table: ArrowTableLike, name: string) =>
      table.getChild(name)?.toJSON() ?? [];
    const buckets: MinuteBucket[] = [];
    const bt = column(bucketTable, "t");
    const requests = column(bucketTable, "requests");
    const bytes = column(bucketTable, "bytes");
    const s4xx = column(bucketTable, "s4xx");
    const s5xx = column(bucketTable, "s5xx");
    const cacheRows = column(bucketTable, "cache_rows");
    const hits = column(bucketTable, "hits");
    const minCol = column(bucketTable, "min_ts");
    const maxCol = column(bucketTable, "max_ts");
    let totalRequests = 0;
    let minTs = Infinity;
    let maxTs = -Infinity;
    for (let i = 0; i < bucketTable.numRows; i++) {
      const bucket: MinuteBucket = {
        t: Number(bt[i]),
        requests: Number(requests[i]),
        bytes: Number(bytes[i]),
        s4xx: Number(s4xx[i]),
        s5xx: Number(s5xx[i]),
        hits: Number(hits[i]),
        cacheRows: Number(cacheRows[i]),
      };
      buckets.push(bucket);
      totalRequests += bucket.requests;
      minTs = Math.min(minTs, Number(minCol[i]));
      maxTs = Math.max(maxTs, Number(maxCol[i]));
    }

    const fieldAggregates: Record<string, FieldAggregate> = {};
    if (fields.length > 0) {
      const typeOf: FieldTypeOf = (field) =>
        FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field)!;
      const topK =
        options.topK != null && aggregator.isPristine() ? options.topK : null;
      const allValues = new Set(options.allValuesFields ?? []);
      const fullSet = new Set(
        fields.flatMap((field, i) =>
          topK === null || allValues.has(field) ? [i] : [],
        ),
      );
      // RANK keeps every value tied at the K-th count, so the final order can
      // use Intl.Collator in JavaScript, which DuckDB collations can't match.
      const rankLimit = (k: number) => Math.max(0, Math.floor(k));
      const topLimit = (field: string) => {
        const k = options.rankedFields?.[field];
        return rankLimit(k !== undefined && k > (topK ?? 0) ? k : (topK ?? 0));
      };
      const qualify = bounded
        ? boundedQualify(
            fields,
            Math.min(4096, Math.floor(150_000 / fields.length)),
            Math.floor((32 * 1024 * 1024) / fields.length),
          )
        : (i: number, window: string) =>
            fullSet.has(i) ? "" : `rank() OVER (${window}) <= ${topLimit(fields[i])}`;
      fields.forEach((field, i) => {
        fieldAggregates[field] = {
          nonNull: 0,
          distinct: 0,
          entries: [],
          complete: fullSet.has(i),
        };
      });
      // As in `queryRelationPerFile`: a few cards per statement, each over
      // its own columns of the filtered rows. Statistics and values share the
      // statement, so the group's columns are decoded from Parquet once.
      const materialize = totalRequests <= MATERIALIZE_ROWS;
      for (const group of fieldGroupsOf(fields.length)) {
        const table = await backend.query(
          `${groupBase(group, materialize)} SELECT * FROM (${fieldStatsSql(fields, false, false, group)}) UNION ALL BY NAME SELECT * FROM (${fieldValuesSql(fields, typeOf, false, "", qualify, group)})`,
          signal,
        );
        signal?.throwIfAborted();
        const idxCol = column(table, "field_idx");
        const snn = column(table, "nn");
        const sd = column(table, "d");
        const cntCol = column(table, "cnt");
        const valueCols = fields.map((field) => column(table, valueColumn(typeOf(field))));
        for (let r = 0; r < table.numRows; r++) {
          const i = Number(idxCol[r]);
          const aggregate = fieldAggregates[fields[i]];
          // Statistics rows carry `nn` (a COUNT, never null); value rows `cnt`.
          if (cntCol[r] == null) {
            aggregate.nonNull = Number(snn[r] ?? 0);
            aggregate.distinct = Number(sd[r] ?? 0);
            continue;
          }
          const value = toScalarValue(valueCols[i][r]);
          if (value === null) continue;
          aggregate.entries.push([value, Number(cntCol[r])]);
        }
      }
      if (bounded) {
        for (const aggregate of Object.values(fieldAggregates)) {
          aggregate.complete = keptDistinct(aggregate) === aggregate.distinct;
          aggregate.approximate = !aggregate.complete;
        }
      }
    }

    aggregator.mergeAggregates({
      totalRequests,
      minTs,
      maxTs,
      buckets,
      fields: fieldAggregates,
    } satisfies PartialAggregates);
    if (bounded) aggregator.boundRankings();
  }
}

/** One file of a `queryParquetPerFile` pass and the aggregator its rows land in. */
export type PerFileQuery = {
  access: ParquetAccess;
  target: Aggregator;
  fromMs: number;
  toMs: number;
};

type ParquetGroup = {
  names: string[];
  columns: ReadonlySet<string>;
  tsType: string;
};

const FILE_COLUMN = `"__flp_file"`;
const FILENAME_COLUMN = "__flp_filename";
const FIELD_GROUP = 6;
/** Filtered rows above which a statement rescans rather than materializes. */
const MATERIALIZE_ROWS = 500_000;

function fieldGroupsOf(count: number): number[][] {
  const groups: number[][] = [];
  for (let i = 0; i < count; i += FIELD_GROUP) {
    groups.push(
      Array.from({ length: Math.min(FIELD_GROUP, count - i) }, (_, j) => i + j),
    );
  }
  return groups;
}

function customTypesOf(customFields: readonly CustomField[]): CustomTypes {
  return new Map(
    customFields.map((field) => [field.id, CUSTOM_DUCKDB_TYPES[field.type]]),
  );
}

/*
 * Card statistics and values are computed one field per UNION ALL branch over
 * an unmaterialized `base`, so DuckDB holds one narrow hash table at a time.
 * A single statement with a COUNT(DISTINCT) or grouping set per card held
 * every card's table at once: ~128 MB before any data, and past the browser's
 * 256 MB when one hot hour of near-unique ttl/age values arrived.
 */
type FieldTypeOf = (field: string) => DuckDBColumnType;

const valueColumn = (type: DuckDBColumnType) => `v_${type.toLowerCase()}`;

function valueTypes(fields: readonly string[], typeOf: FieldTypeOf) {
  return [...new Set(fields.map(typeOf))];
}

/** One row per field (and file): `field_idx`, `nn`, `d`, and `ml` when asked. */
function fieldStatsSql(
  fields: readonly string[],
  file: boolean,
  maxLength: boolean,
  include: readonly number[] = fields.map((_, i) => i),
): string {
  return include
    .map((i) => {
      const f = sqlIdent(fields[i]);
      const text = `CASE WHEN CAST(${f} AS VARCHAR) != '' THEN ${f} END`;
      return `SELECT ${file ? `${FILE_COLUMN} AS file, ` : ""}${i} AS field_idx, COUNT(${f})::BIGINT AS nn, COUNT(DISTINCT ${text})::BIGINT AS d${maxLength ? `, MAX(length(CAST(${text} AS VARCHAR)))::BIGINT AS ml` : ""} FROM base${file ? ` GROUP BY ${FILE_COLUMN}` : ""}`;
    })
    .join(" UNION ALL ");
}

/**
 * Non-null value counts per field (and file), with the value in the column of
 * its type class. `qualify(i, window)` bounds field `i` given its window spec.
 */
function fieldValuesSql(
  fields: readonly string[],
  typeOf: FieldTypeOf,
  file: boolean,
  where: string,
  qualify: (i: number, window: string) => string,
  include: readonly number[] = fields.map((_, i) => i),
): string {
  const types = valueTypes(fields, typeOf);
  const window = `${file ? `PARTITION BY ${FILE_COLUMN} ` : ""}ORDER BY COUNT(*) DESC`;
  return include
    .map((i) => {
      const field = fields[i];
      const f = sqlIdent(field);
      const values = types
        .map(
          (type) =>
            `${type === typeOf(field) ? `CAST(${f} AS ${type})` : `NULL::${type}`} AS ${valueColumn(type)}`,
        )
        .join(", ");
      const q = qualify(i, window);
      return `SELECT ${file ? `${FILE_COLUMN} AS file, ` : ""}${i} AS field_idx, ${values}, COUNT(*)::BIGINT AS cnt FROM base WHERE ${f} IS NOT NULL${where} GROUP BY ${file ? `${FILE_COLUMN}, ` : ""}${f}${q ? ` QUALIFY ${q}` : ""}`;
    })
    .join(" UNION ALL ");
}

/** Entries that count as distinct values: `d` leaves out the empty string. */
function keptDistinct(aggregate: FieldAggregate): number {
  let kept = 0;
  for (const [value] of aggregate.entries) if (value !== "") kept++;
  return kept;
}

/** The bounded-mode cap: the top candidates whose keys fit the field's byte share. */
function boundedQualify(
  fields: readonly string[],
  candidateLimit: number,
  fieldByteCap: number,
) {
  return (i: number, window: string) =>
    `row_number() OVER (${window}) <= ${candidateLimit} AND sum(48 + 4 * length(CAST(${sqlIdent(fields[i])} AS VARCHAR))) OVER (${window} ROWS UNBOUNDED PRECEDING) <= ${fieldByteCap}`;
}

/**
 * The typed columns a selective query reads from one schema group. Each part
 * keeps its own time bounds; `file` tags its rows for per-file grouping.
 */
function parquetRelation(
  group: ParquetGroup,
  parts: readonly { names: readonly string[]; fromMs: number; toMs: number; file?: number }[],
  fields: readonly string[],
  customTypes: CustomTypes,
): string {
  const { tsType } = group;
  const tsExpr = tsType.startsWith("TIMESTAMP")
    ? `epoch_ms("timestamp")::BIGINT`
    : tsType.includes("CHAR")
      ? `epoch_ms(TRY_CAST("timestamp" AS TIMESTAMP))::BIGINT`
      : `CAST("timestamp" AS BIGINT)`;
  const needed = new Set(["status", "cache", "resp_bytes", "edge", ...fields]);
  const columns = [...needed].flatMap((field) => {
    const type = FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field);
    if (!type) return [];
    const col = sqlIdent(field);
    const cast = `CAST(${col} AS ${type})`;
    return [
      !group.columns.has(field)
        ? `NULL::${type} AS ${col}`
        : type === "DOUBLE"
          ? `CASE WHEN isnan(${cast}) THEN NULL ELSE ${cast} END AS ${col}`
          : `${cast} AS ${col}`,
    ];
  });
  const native = tsType.startsWith("TIMESTAMP") && !tsType.includes("TIME ZONE");
  // The native timestamp predicate can prune row groups; the millisecond
  // predicate remains authoritative for all timestamp encodings.
  const nativeBounds = (fromMs: number, toMs: number) =>
    native
      ? [
          ...(Number.isFinite(fromMs)
            ? [`"timestamp" >= epoch_ms(${Math.floor(fromMs) - 1}::BIGINT)`]
            : []),
          ...(Number.isFinite(toMs)
            ? [`"timestamp" < epoch_ms(${Math.ceil(toMs) + 1}::BIGINT)`]
            : []),
        ]
      : [];
  const msBounds = (fromMs: number, toMs: number) => [
    ...(Number.isFinite(fromMs) ? [`ts_ms >= ${Math.floor(fromMs)}`] : []),
    ...(Number.isFinite(toMs) ? [`ts_ms < ${Math.ceil(toMs)}`] : []),
  ];
  const scan = (names: readonly string[], tag: string, where: string[]) =>
    `SELECT ${tag}${tsExpr} AS ts_ms, ${columns.join(", ")} FROM read_parquet([${names.map(sqlString).join(", ")}]${tag ? `, filename = ${sqlString(FILENAME_COLUMN)}` : ""})${where.length ? ` WHERE ${where.join(" AND ")}` : ""}`;
  const tagged = parts.filter((part) => part.file !== undefined);
  if (tagged.length === 0) {
    const selects = parts.map(({ names, fromMs, toMs }) => {
      const select = scan(names, "", nativeBounds(fromMs, toMs));
      const bounds = msBounds(fromMs, toMs);
      return bounds.length
        ? `SELECT * FROM (${select}) WHERE ${bounds.join(" AND ")}`
        : select;
    });
    return `(${selects.join(" UNION ALL ")})`;
  }
  if (tagged.length !== parts.length) {
    throw new Error("Per-file relations must tag every part");
  }
  // One scan tags each row with its file. A scan per file is inlined into
  // every card's branch of the unmaterialized base, so files × cards parquet
  // readers each held their own buffers.
  const fileOf = `CASE ${sqlIdent(FILENAME_COLUMN)} ${tagged
    .flatMap(({ names, file }) => names.map((name) => `WHEN ${sqlString(name)} THEN ${file}`))
    .join(" ")} END`;
  const lowest = Math.min(...tagged.map((part) => part.fromMs));
  const highest = Math.max(...tagged.map((part) => part.toMs));
  const perFile = tagged
    .map(({ fromMs, toMs, file }) => {
      const bounds = msBounds(fromMs, toMs);
      return bounds.length
        ? `(${FILE_COLUMN} = ${file} AND ${bounds.join(" AND ")})`
        : `${FILE_COLUMN} = ${file}`;
    });
  const select = scan(
    tagged.flatMap((part) => part.names),
    `${fileOf} AS ${FILE_COLUMN}, `,
    nativeBounds(lowest, highest),
  );
  return perFile.some((clause) => clause.includes(" AND "))
    ? `(SELECT * FROM (${select}) WHERE ${perFile.join(" OR ")})`
    : `(${select})`;
}

function selectiveWhere(
  group: ParquetGroup,
  filters: readonly DashboardFilter[],
  options: AggregateQueryOptions,
  customTypes: CustomTypes,
): string {
  return [
    "ts_ms IS NOT NULL",
    `(${compileFiltersToSql(filters, Boolean(options.edgeOnly && group.columns.has("edge")), [], customTypes)})`,
  ].join(" AND ");
}

function isParquetAccessList(
  files: ParquetAccess | readonly ParquetAccess[],
): files is readonly ParquetAccess[] {
  return Array.isArray(files);
}

/** Groups registered Parquet files whose schemas match exactly, in input order. */
/**
 * Groups the files by the schemas their footers settled, or null when any
 * file's schema is unknown and the read must describe them.
 */
function footerSchemaGroups(accesses: readonly ParquetAccess[], names: readonly string[]): ParquetGroup[] | null {
  const groups = new Map<string, ParquetGroup>();
  for (const [i, access] of accesses.entries()) {
    const schema = access.kind === "buffer" ? access.schema : null;
    if (!schema) return null;
    const group = groups.get(schema.signature);
    if (group) group.names.push(names[i]);
    else groups.set(schema.signature, { names: [names[i]], columns: new Set(schema.columns), tsType: schema.timestampType });
  }
  return [...groups.values()];
}

async function parquetSchemaGroups(
  backend: DuckDBBackend,
  names: readonly string[],
  signal?: AbortSignal,
): Promise<string[][]> {
  if (names.length <= 1) return names.length ? [[...names]] : [];
  const table = await backend.query(
    `SELECT file_name, concat_ws('|', name, CAST(type AS VARCHAR), CAST(repetition_type AS VARCHAR), CAST(converted_type AS VARCHAR), CAST(logical_type AS VARCHAR), CAST(num_children AS VARCHAR)) AS part FROM parquet_schema([${names.map(sqlString).join(", ")}])`,
    signal,
  );
  signal?.throwIfAborted();
  const files = (table.getChild("file_name")?.toJSON() as string[]) ?? [];
  const parts = (table.getChild("part")?.toJSON() as string[]) ?? [];
  const signatures = new Map<string, string[]>(names.map((name) => [name, []]));
  files.forEach((file, i) => signatures.get(file)?.push(parts[i]));
  const groups = new Map<string, string[]>();
  for (const name of names) {
    const signature = JSON.stringify(signatures.get(name));
    const group = groups.get(signature);
    if (group) group.push(name);
    else groups.set(signature, [name]);
  }
  return [...groups.values()];
}

let singletonEngine: DuckDBAnalyticalEngine | null = null;

export function getDuckDBEngine(): DuckDBAnalyticalEngine {
  if (!singletonEngine) {
    singletonEngine = new DuckDBAnalyticalEngine();
  }
  return singletonEngine;
}

export { DuckDBAnalyticalEngine as DashboardDuckDBEngine };
