import { quoteIdent, sqlEscape } from "@/lib/sqlBuilder";
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
  | { kind: "buffer"; bytes: ArrayBuffer }
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

function sqlString(value: string): string {
  return `'${sqlEscape(value)}'`;
}

const sqlIdent = quoteIdent;

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
  const distDir = pathBuiltin.resolve(
    process.cwd(),
    "node_modules/@duckdb/duckdb-wasm/dist",
  );
  const db = await duckdb.createDuckDB(
    {
      eh: {
        mainModule: pathBuiltin.join(distDir, "duckdb-eh.wasm"),
        mainWorker: pathBuiltin.join(distDir, "duckdb-node-eh.worker.cjs"),
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
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

export async function createBrowserBackend(): Promise<DuckDBBackend> {
  const duckdb = await import("@duckdb/duckdb-wasm");
  const origin =
    typeof self !== "undefined" && self.location ? self.location.origin : "";
  const assets = duckdbAssetUrls(origin, duckdb.PACKAGE_VERSION);
  const worker = new Worker(assets.worker);
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
  try {
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
    conn = await db.connect();
    await conn.query(
      `SET custom_extension_repository = ${sqlString(assets.extensions)}`,
    );
  } catch (error) {
    try { await boundedDuckDBCleanup(() => db.terminate()); }
    catch (cleanupError) { throw new DuckDBBackendInitializationError(error, false, cleanupError); }
    throw new DuckDBBackendInitializationError(error, true);
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
  ): Promise<IngestedFileMeta>;
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

function firstNumber(table: ArrowTableLike, column: string): number | null {
  const value = table.getChild(column)?.toJSON()?.[0];
  return value === null || value === undefined ? null : Number(value);
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
  return sql.replace(/\$([0-9]+)|\?/g, (_match, numStr) => {
    if (numStr !== undefined) {
      const idx = Number(numStr) - 1;
      return idx >= 0 && idx < params.length ? encodeParam(params[idx]) : "NULL";
    }
    const idx = positionalIdx++;
    return idx < params.length ? encodeParam(params[idx]) : "NULL";
  });
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

  constructor(options: { backendFactory?: () => Promise<DuckDBBackend> } = {}) {
    super({
      factory: options.backendFactory ?? (() => typeof process !== "undefined" && process.versions?.node
        ? createNodeBackend() : createBrowserBackend()),
      schemas: { requests: FIELD_DUCKDB_TYPES, client_vitals: RUM_VITALS_COLUMNS, client_errors: RUM_ERRORS_COLUMNS },
      memoryLimit: MEMORY_LIMIT,
      rows: arrowTableToRows,
      interpolate: interpolateSqlParams,
    });
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
    file: ParquetAccess,
    fromMs: number,
    toMs: number,
    filters: readonly DashboardFilter[],
    aggregator: Aggregator,
    options: AggregateQueryOptions = {},
    customFields: readonly CustomField[] = [],
  ): Promise<void> {
    const { signal } = options;
    return this.runRead(options.context, signal, async (backend) => {
      signal?.throwIfAborted();
      const name = `selective_${this.nextVirtualFileId++}.parquet`;
      let registered = false;
      let failure: unknown;
      const customTypes = new Map(
        customFields.map((field) => [field.id, CUSTOM_DUCKDB_TYPES[field.type]]),
      );
      try {
        if (file.kind === "buffer") {
          await backend.registerFileBuffer(name, new Uint8Array(file.bytes.slice(0)));
        } else if (backend.registerParquet) {
          await backend.registerParquet(name, file);
        } else {
          throw new Error(
            "Remote Parquet reads require the browser DuckDB runtime",
          );
        }
        registered = true;
        signal?.throwIfAborted();
        const parquet = `read_parquet(${sqlString(name)})`;
        const schema = await backend.query(
          `DESCRIBE SELECT * FROM ${parquet}`,
          signal,
        );
        const names =
          (schema.getChild("column_name")?.toJSON() as string[]) ?? [];
        const types =
          (schema.getChild("column_type")?.toJSON() as string[]) ?? [];
        const typeByCol = new Map(
          names.map((field, i) => [field, types[i].toUpperCase()]),
        );
        const tsType = typeByCol.get("timestamp");
        if (!tsType) throw new Error("Parquet file has no timestamp column");
        const tsExpr = tsType.startsWith("TIMESTAMP")
          ? `epoch_ms("timestamp")::BIGINT`
          : tsType.includes("CHAR")
            ? `epoch_ms(TRY_CAST("timestamp" AS TIMESTAMP))::BIGINT`
            : `CAST("timestamp" AS BIGINT)`;
        const needed = new Set([
          "status",
          "cache",
          "resp_bytes",
          "edge",
          ...aggregator.fields,
          ...filters.map((filter) => filter.field),
        ]);
        const columns = [...needed].flatMap((field) => {
          const type = FIELD_DUCKDB_TYPES[field] ?? customTypes.get(field);
          if (!type) return [];
          const col = sqlIdent(field);
          const cast = `CAST(${col} AS ${type})`;
          return [
            !typeByCol.has(field)
              ? `NULL::${type} AS ${col}`
              : type === "DOUBLE"
                ? `CASE WHEN isnan(${cast}) THEN NULL ELSE ${cast} END AS ${col}`
                : `${cast} AS ${col}`,
          ];
        });
        // The native timestamp predicate can prune row groups; the millisecond
        // predicate below remains authoritative for all timestamp encodings.
        const nativeBounds =
          tsType.startsWith("TIMESTAMP") && !tsType.includes("TIME ZONE")
            ? [
                ...(Number.isFinite(fromMs)
                  ? [`"timestamp" >= epoch_ms(${Math.floor(fromMs) - 1}::BIGINT)`]
                  : []),
                ...(Number.isFinite(toMs)
                  ? [`"timestamp" < epoch_ms(${Math.ceil(toMs) + 1}::BIGINT)`]
                  : []),
              ]
            : [];
        const relation = `(SELECT ${tsExpr} AS ts_ms, ${columns.join(", ")} FROM ${parquet}${nativeBounds.length ? ` WHERE ${nativeBounds.join(" AND ")}` : ""})`;
        const where = [
          "ts_ms IS NOT NULL",
          ...(Number.isFinite(fromMs) ? [`ts_ms >= ${Math.floor(fromMs)}`] : []),
          ...(Number.isFinite(toMs) ? [`ts_ms < ${Math.ceil(toMs)}`] : []),
          `(${compileFiltersToSql(filters, Boolean(options.edgeOnly && typeByCol.has("edge")), [], customTypes)})`,
        ].join(" AND ");
        await this.queryRelation(
          backend,
          relation,
          where,
          typeByCol.has("resp_bytes"),
          customTypes,
          aggregator,
          options,
          !options.fieldSearch,
        );
      } catch (error) {
        failure = error;
        throw error;
      } finally {
        if (registered) {
          try {
            await backend.dropFile(name);
          } catch (cleanupError) {
            if (failure !== undefined && failure !== cleanupError) {
              throw new AggregateError([failure, cleanupError],
                `${failure instanceof Error ? failure.message : "Cloud query failed"}; ${cleanupError instanceof Error ? cleanupError.message : "DuckDB file cleanup failed"}`);
            }
            throw cleanupError;
          }
        }
      }
    });
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
    const base = `WITH base AS (SELECT ts_ms, ${projection} FROM ${relation} WHERE ${where}${searchWhere})`;

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
      const statsTable = await backend.query(
        `${base} SELECT ${fields
          .map(
            (field, i) =>
              `COUNT(${sqlIdent(field)})::BIGINT AS nn_${i}, COUNT(DISTINCT ${sqlIdent(field)})::BIGINT AS d_${i}`,
          )
          .join(", ")} FROM base`,
        signal,
      );
      signal?.throwIfAborted();

      const topK =
        options.topK != null && aggregator.isPristine() ? options.topK : null;
      const allValues = new Set(options.allValuesFields ?? []);
      const fullIdx = fields.flatMap((field, i) =>
        topK === null || allValues.has(field) ? [i] : [],
      );
      const cols = fields.map(sqlIdent);
      const fieldIdx = `CASE ${cols.map((c, i) => `WHEN GROUPING(${c}) = 0 THEN ${i}`).join(" ")} END`;
      const nonNull = `CASE field_idx ${cols.map((c, i) => `WHEN ${i} THEN ${c} IS NOT NULL`).join(" ")} END`;
      // RANK keeps every value tied at the K-th count, so the final order can
      // use Intl.Collator in JavaScript, which DuckDB collations can't match.
      const candidateLimit = Math.min(
        4096,
        Math.floor(150_000 / fields.length),
      );
      const keyBytes = `48 + CASE field_idx ${cols.map((col, i) => `WHEN ${i} THEN 4 * length(CAST(${col} AS VARCHAR))`).join(" ")} ELSE 0 END`;
      const qualify = bounded
        ? ` QUALIFY row_number() OVER (PARTITION BY field_idx ORDER BY cnt DESC) <= ${candidateLimit} AND sum(${keyBytes}) OVER (PARTITION BY field_idx ORDER BY cnt DESC ROWS UNBOUNDED PRECEDING) <= ${Math.floor((32 * 1024 * 1024) / fields.length)}`
        : fullIdx.length === fields.length
          ? ""
          : ` QUALIFY ${fullIdx.length > 0 ? `field_idx IN (${fullIdx.join(", ")}) OR ` : ""}rank() OVER (PARTITION BY field_idx ORDER BY cnt DESC) <= ${Math.max(0, Math.floor(topK ?? 0))}`;
      const valuesTable = await backend.query(
        `${base} SELECT field_idx, cnt, ${cols.join(", ")} FROM (SELECT ${fieldIdx} AS field_idx, ${cols.join(", ")}, COUNT(*)::BIGINT AS cnt FROM base GROUP BY GROUPING SETS (${cols.map((c) => `(${c})`).join(", ")})) WHERE ${nonNull}${qualify}`,
        signal,
      );
      signal?.throwIfAborted();

      const fullSet = new Set(fullIdx);
      fields.forEach((field, i) => {
        fieldAggregates[field] = {
          nonNull: firstNumber(statsTable, `nn_${i}`) ?? 0,
          distinct: firstNumber(statsTable, `d_${i}`) ?? 0,
          entries: [],
          complete: fullSet.has(i),
        };
      });
      const idxCol = column(valuesTable, "field_idx");
      const cntCol = column(valuesTable, "cnt");
      const valueCols = fields.map((field) => column(valuesTable, field));
      for (let r = 0; r < valuesTable.numRows; r++) {
        const i = Number(idxCol[r]);
        const value = toScalarValue(valueCols[i][r]);
        if (value === null) continue;
        fieldAggregates[fields[i]].entries.push([value, Number(cntCol[r])]);
      }
      if (bounded) {
        for (const aggregate of Object.values(fieldAggregates)) {
          aggregate.complete = aggregate.entries.length === aggregate.distinct;
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

let singletonEngine: DuckDBAnalyticalEngine | null = null;

export function getDuckDBEngine(): DuckDBAnalyticalEngine {
  if (!singletonEngine) {
    singletonEngine = new DuckDBAnalyticalEngine();
  }
  return singletonEngine;
}

export const runDuckDbDashboard = getDuckDBEngine;
export { DuckDBAnalyticalEngine as DashboardDuckDBEngine };
