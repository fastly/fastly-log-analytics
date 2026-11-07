import { quoteIdent, sqlEscape } from "@/lib/sqlBuilder";
import type {
  ArrowTableLike, DuckDBBackend, DuckDBColumnType, DuckDBLease,
  DuckDBReadContext, IngestedFileMeta,
} from "./duckdb";
import type { CustomField } from "./summaries";

type Table = "requests" | "client_vitals" | "client_errors";
type Schema = Readonly<Record<string, DuckDBColumnType>>;
type Binding = {
  name: string;
  table: Table;
  types: Map<string, string>;
  ts: string;
  meta: IngestedFileMeta;
  pins: number;
  retired: boolean;
  dropped: boolean;
};
type Snapshot = {
  generation: number;
  bindings: readonly Binding[];
  custom: Map<string, DuckDBColumnType>;
  present: Set<string> | null;
  valid?: () => boolean;
};
type Config = {
  factory: () => Promise<DuckDBBackend>;
  schemas: Record<Table, Schema>;
  memoryLimit: string;
  rows<T>(table: ArrowTableLike): T[];
  interpolate(sql: string, params: readonly unknown[]): string;
};
const literal = (value: string) => `'${sqlEscape(value)}'`;
const MAX_COMPRESSED_BYTES = 64 * 1024 * 1024;
const CUSTOM_TYPES = { string: "VARCHAR", number: "DOUBLE", boolean: "BOOLEAN" } as const;

export class ExpiredDuckDBReadError extends Error {
  constructor() { super("Cloud analytical read generation expired"); this.name = "ExpiredDuckDBReadError"; }
}

export class DuckDBBackendInitializationError extends Error {
  constructor(error: unknown, readonly cleanupConfirmed: boolean, cleanupError?: unknown) {
    const message = error instanceof Error ? error.message : "DuckDB backend initialization failed";
    super(cleanupConfirmed ? message : `${message}; DuckDB initialization cleanup was not confirmed`, {
      cause: cleanupError === undefined ? error : new AggregateError([error, cleanupError]),
    });
    this.name = "DuckDBBackendInitializationError";
  }
}

export function isFatalDuckDBError(error: unknown): boolean {
  return error instanceof Error &&
    /^(?:(?:Error|RuntimeError):\s*)?(?:Out of Memory Error\b|FATAL Error\b|(?:Failed:\s*)?database has been invalidated\b)/i.test(error.message.trimStart());
}

export async function boundedDuckDBCleanup(dispose: () => Promise<void>): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      dispose(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("DuckDB backend cleanup deadline exceeded")), 2000);
      }),
    ]);
  } catch (error) {
    throw new Error("DuckDB backend cleanup failed; replacement was not started", { cause: error });
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export class ParquetBindingStore {
  private backendPromise: Promise<DuckDBBackend> | null = null;
  private initialized = false;
  private queue: Promise<unknown> = Promise.resolve();
  private recovery: Promise<void> | null = null;
  private generation = 1;
  private poisoned: Error | null = null;
  private creationFailure: DuckDBBackendInitializationError | null = null;
  private nextFile = 1;
  private readonly bindings = new Map<string, Binding>();
  private readonly retired = new Set<Binding>();
  private readonly ownedNames = new Set<string>();
  protected readonly customTypes = new Map<string, DuckDBColumnType>();
  private presentOverride: Set<string> | null = null;
  private activeView = "";
  private readonly contexts = new WeakMap<DuckDBReadContext, Snapshot>();

  constructor(private readonly config: Config) {}

  private key(table: Table, scopedKey: string) { return `${table}:${scopedKey}`; }

  private check(generation: number, signal?: AbortSignal): void {
    signal?.throwIfAborted();
    if (generation !== this.generation) throw new ExpiredDuckDBReadError();
    if (this.poisoned) throw this.poisoned;
  }

  private observeFailure(generation: number, error: unknown): void {
    if (generation === this.generation && isFatalDuckDBError(error)) this.poisoned = error as Error;
  }

  private async dropRegisteredFile(backend: DuckDBBackend, name: string, generation: number): Promise<void> {
    this.check(generation);
    try {
      await backend.dropFile(name);
      this.ownedNames.delete(name);
      this.check(generation);
    } catch (error) {
      if (generation !== this.generation) throw error;
      this.poisoned = new Error(`${error instanceof Error ? error.message : "DuckDB file removal failed"}; DuckDB file cleanup failed`, {
        cause: error,
      });
      throw this.poisoned;
    }
  }

  private operation<T>(generation: number, work: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.queue.then(async () => {
      this.check(generation, signal);
      try {
        const value = await work();
        this.check(generation, signal);
        return value;
      } catch (error) {
        this.observeFailure(generation, error);
        throw error;
      }
    });
    // Settle queue bookkeeping without changing the caller's rejected result.
    this.queue = result.then(() => undefined, () => undefined);
    return result;
  }

  protected async ensureBackend(generation = this.generation): Promise<DuckDBBackend> {
    this.check(generation);
    this.backendPromise ??= this.config.factory();
    let backend: DuckDBBackend;
    try {
      backend = await this.backendPromise;
    } catch (error) {
      if (generation === this.generation) {
        this.creationFailure = error instanceof DuckDBBackendInitializationError ? error : null;
        this.poisoned = error instanceof Error ? error : new Error("DuckDB backend initialization failed", { cause: error });
      }
      throw error;
    }
    this.check(generation);
    if (!this.initialized) {
      await backend.query(`SET memory_limit = ${literal(this.config.memoryLimit)}`);
      this.check(generation);
      await this.publish(backend, this.snapshot([]));
      this.check(generation);
      this.initialized = true;
    }
    return backend;
  }

  async beginRequest(): Promise<DuckDBReadContext> {
    if (!this.poisoned) return this.readContext([]);
    if (this.recovery) {
      const generation = this.generation;
      await this.recovery;
      this.check(generation);
      return this.readContext([]);
    }
    const generation = ++this.generation;
    const recovery = this.queue.then(async () => {
      if (this.backendPromise) {
        let backend: DuckDBBackend | undefined;
        try {
          backend = await this.backendPromise;
        } catch (error) {
          if (!this.creationFailure?.cleanupConfirmed)
            throw new Error("DuckDB backend cleanup failed; replacement was not started", { cause: error });
        }
        if (backend) await boundedDuckDBCleanup(() => backend!.dispose());
      }
      if (generation !== this.generation) throw new ExpiredDuckDBReadError();
      this.backendPromise = null;
      this.initialized = false;
      this.bindings.clear();
      this.retired.clear();
      this.ownedNames.clear();
      this.customTypes.clear();
      this.presentOverride = null;
      this.activeView = "";
      this.poisoned = null;
      this.creationFailure = null;
    });
    this.recovery = recovery;
    this.queue = recovery.then(() => undefined, () => undefined);
    try {
      await recovery;
      this.check(generation);
      return this.readContext([]);
    }
    finally { if (this.recovery === recovery) this.recovery = null; }
  }

  async prewarm(): Promise<void> {
    const generation = this.generation;
    await this.operation(generation, async () => { await this.ensureBackend(generation); });
  }

  getMemoryLimit(): Promise<string> {
    return this.executeSql<{ memory_limit: string }>("SELECT current_setting('memory_limit') AS memory_limit")
      .then((rows) => String(rows[0]?.memory_limit ?? ""));
  }

  private snapshot(bindings = [...this.bindings.values()], custom = this.customTypes): Snapshot {
    this.check(this.generation);
    return { generation: this.generation, bindings: [...bindings], custom: new Map(custom),
      present: this.presentOverride ? new Set(this.presentOverride) : null };
  }

  readContext(scopedKeys?: readonly string[]): DuckDBReadContext {
    const keys = scopedKeys ? new Set(scopedKeys) : null;
    const selected = [...this.bindings.values()].filter((binding) => !keys || keys.has(binding.meta.scopedKey));
    if (keys && [...keys].some((key) => !selected.some((binding) => binding.meta.scopedKey === key)))
      throw new ExpiredDuckDBReadError();
    return this.context(this.snapshot(selected));
  }

  private context(snapshot: Snapshot): DuckDBReadContext {
    const executeSql = <T = Record<string, unknown>>(sql: string, params: readonly unknown[] = [], signal?: AbortSignal) =>
      this.runRead(context, signal, async (backend) =>
        this.config.rows<T>(await backend.query(this.config.interpolate(sql, params), signal)));
    const context: DuckDBReadContext = {
      assertCurrent: () => this.checkSnapshot(snapshot),
      executeSql,
      querySql: executeSql,
      getPresentColumns: async (scopedKeys?: readonly string[]) => {
        this.checkSnapshot(snapshot);
        if (snapshot.present) return new Set(snapshot.present);
        const selected = scopedKeys ? new Set(scopedKeys) : null;
        const columns = new Set<string>();
        for (const binding of snapshot.bindings) {
          if (binding.table !== "requests" || selected && !selected.has(binding.meta.scopedKey)) continue;
          for (const column of binding.meta.presentColumns ?? []) columns.add(column);
          columns.add("timestamp");
          columns.add("ts_ms");
        }
        return columns;
      },
      getCustomColumnTypes: () => { this.checkSnapshot(snapshot); return new Map(snapshot.custom); },
      getFileMeta: (key) => {
        this.checkSnapshot(snapshot);
        return snapshot.bindings.find((binding) => binding.table === "requests" && binding.meta.scopedKey === key)?.meta;
      },
    };
    this.contexts.set(context, snapshot);
    return context;
  }

  private checkSnapshot(snapshot: Snapshot): void {
    this.check(snapshot.generation);
    if (snapshot.valid && !snapshot.valid()) throw new ExpiredDuckDBReadError();
    if (snapshot.bindings.some((binding) => binding.dropped || binding.retired && binding.pins === 0))
      throw new ExpiredDuckDBReadError();
  }

  protected runRead<T>(
    context: DuckDBReadContext | undefined, signal: AbortSignal | undefined,
    work: (backend: DuckDBBackend) => Promise<T>,
  ): Promise<T> {
    const snapshot = this.contexts.get(context ?? this.readContext());
    if (!snapshot) return Promise.reject(new ExpiredDuckDBReadError());
    try { this.checkSnapshot(snapshot); signal?.throwIfAborted(); }
    catch (error) { return Promise.reject(error); }
    for (const binding of snapshot.bindings) binding.pins++;
    return this.operation(snapshot.generation, async () => {
      this.checkSnapshot(snapshot);
      const backend = await this.ensureBackend(snapshot.generation);
      this.checkSnapshot(snapshot);
      this.check(snapshot.generation, signal);
      await this.publish(backend, snapshot);
      this.check(snapshot.generation, signal);
      const guarded: DuckDBBackend = {
        ...backend,
        query: async (sql, statementSignal) => {
          this.checkSnapshot(snapshot);
          this.check(snapshot.generation, statementSignal ?? signal);
          try {
            const result = await backend.query(sql, statementSignal ?? signal);
            this.checkSnapshot(snapshot);
            this.check(snapshot.generation, statementSignal ?? signal);
            return result;
          } catch (error) {
            this.observeFailure(snapshot.generation, error);
            throw error;
          }
        },
        registerFileBuffer: async (name, buffer) => {
          this.check(snapshot.generation, signal);
          await backend.registerFileBuffer(name, buffer);
          this.ownedNames.add(name);
          this.check(snapshot.generation, signal);
        },
        ...(backend.registerParquet ? { registerParquet: async (name, file) => {
          this.check(snapshot.generation, signal);
          await backend.registerParquet!(name, file);
          this.ownedNames.add(name);
          this.check(snapshot.generation, signal);
        } } : {}),
        dropFile: (name) => this.dropRegisteredFile(backend, name, snapshot.generation),
      };
      return work(guarded);
    }, signal).finally(() => {
      if (snapshot.generation === this.generation) {
        for (const binding of snapshot.bindings) binding.pins--;
      }
    });
  }

  executeSql<T = Record<string, unknown>>(sql: string, params: readonly unknown[] = [], signal?: AbortSignal): Promise<T[]> {
    try { return this.readContext().executeSql<T>(sql, params, signal); }
    catch (error) { return Promise.reject(error); }
  }

  querySql<T = Record<string, unknown>>(sql: string, params: readonly unknown[] = [], signal?: AbortSignal): Promise<T[]> {
    return this.executeSql<T>(sql, params, signal);
  }

  getPresentColumns(scopedKeys?: readonly string[]): Promise<Set<string>> {
    try { return this.readContext().getPresentColumns(scopedKeys); }
    catch (error) { return Promise.reject(error); }
  }

  getCustomColumnTypes(): ReadonlyMap<string, DuckDBColumnType> {
    this.check(this.generation);
    return new Map(this.customTypes);
  }

  setPresentColumnsOverride(columns: Iterable<string> | null): void {
    this.check(this.generation);
    this.presentOverride = columns ? new Set(columns) : null;
  }

  hasFile(scopedKey: string): boolean { return Boolean(this.getFileMeta(scopedKey)); }

  getFileMeta(scopedKey: string): IngestedFileMeta | undefined {
    this.check(this.generation);
    return this.bindings.get(this.key("requests", scopedKey))?.meta;
  }

  getRumFileByteLength(scopedKey: string, kind: "vitals" | "errors"): number | undefined {
    this.check(this.generation);
    return this.bindings.get(this.key(kind === "vitals" ? "client_vitals" : "client_errors", scopedKey))?.meta.byteLength;
  }

  lease(fields: readonly CustomField[] = []): DuckDBLease {
    const generation = this.generation;
    this.check(generation);
    const required = fields.map((field) => [field.id, CUSTOM_TYPES[field.type]] as const);
    for (const [id, type] of required) this.customTypes.set(id, type);
    const held = new Map<string, Binding>();
    let released = false;
    const hold = (binding: Binding) => {
      this.check(generation);
      if (released) throw new ExpiredDuckDBReadError();
      const key = this.key(binding.table, binding.meta.scopedKey);
      if (held.get(key) === binding) return;
      const prior = held.get(key);
      if (prior) prior.pins--;
      held.set(key, binding);
      binding.pins++;
    };
    const release = () => {
      if (released) return;
      released = true;
      if (generation === this.generation) for (const binding of held.values()) binding.pins--;
      held.clear();
    };
    const tableFor = (kind: "request" | "vitals" | "errors"): Table =>
      kind === "request" ? "requests" : kind === "vitals" ? "client_vitals" : "client_errors";
    return {
      getFileMeta: (key, kind = "request") => {
        this.check(generation);
        const binding = this.bindings.get(this.key(tableFor(kind), key));
        if (!binding || kind === "request" && required.some(([id, type]) => this.customTypes.get(id) !== type)) return undefined;
        hold(binding);
        return binding.meta;
      },
      ingestFile: async (scopedKey, key, bytes, stats, signal, kind = "request") => {
        this.check(generation, signal);
        const meta = await this.register(tableFor(kind), scopedKey, key, bytes, stats, signal,
          kind === "request" ? required : [], generation, hold);
        this.check(generation, signal);
        return meta;
      },
      readContext: () => {
        this.check(generation);
        if (released) throw new ExpiredDuckDBReadError();
        const context = this.context({ ...this.snapshot([...held.values()], new Map(required)), valid: () => !released });
        context.release = release;
        return context;
      },
      release,
    };
  }

  private projection(binding: Binding | undefined, schema: Schema, custom: Schema): string {
    const columns = [
      `${binding ? literal(binding.meta.scopedKey) : "NULL::VARCHAR"} AS scoped_key`,
      `${binding ? `(${binding.ts})::BIGINT` : "NULL::BIGINT"} AS ts_ms`,
      `${binding ? `epoch_ms((${binding.ts})::BIGINT)::TIMESTAMPTZ` : "NULL::TIMESTAMPTZ"} AS "timestamp"`,
      ...Object.entries({ ...schema, ...custom }).map(([name, type]) => {
        const column = quoteIdent(name);
        const aliases: Record<string, string> = binding?.table === "requests" ? {} : {
          request_event_id: "req_id", client_id: "cid", session_id: "cid",
          rating: "metric_rating", page_url: "pathname", path: "pathname",
          browser_name: "browser", os_name: "os", device_type: "device",
          filename: "error_file", lineno: "error_line", colno: "error_col",
        };
        const source = binding?.types.has(name) ? name : aliases[name];
        const cast = source && binding?.types.has(source) ? `CAST(${quoteIdent(source)} AS ${type})` : `NULL::${type}`;
        return `${type === "DOUBLE" ? `CASE WHEN isnan(${cast}) THEN NULL ELSE ${cast} END` : cast} AS ${column}`;
      }),
    ];
    return `SELECT ${columns.join(", ")}${binding
      ? ` FROM read_parquet(${literal(binding.name)})` : " WHERE false"}`;
  }

  private async publish(backend: DuckDBBackend, snapshot: Snapshot): Promise<void> {
    this.checkSnapshot(snapshot);
    const signature = JSON.stringify([snapshot.bindings.map((binding) => binding.name), [...snapshot.custom]]);
    if (this.activeView === signature) return;
    const commands: string[] = [];
    for (const table of ["requests", "client_vitals", "client_errors"] as const) {
      const bindings = snapshot.bindings.filter((binding) => binding.table === table);
      const custom = table === "requests" ? Object.fromEntries(snapshot.custom) : {};
      const parts = bindings.length ? bindings.map((binding) => this.projection(binding, this.config.schemas[table], custom))
        : [this.projection(undefined, this.config.schemas[table], custom)];
      commands.push(`CREATE OR REPLACE VIEW ${table} AS ${parts.join(" UNION ALL ")}`);
    }
    commands.push("CREATE OR REPLACE VIEW logs AS SELECT * FROM requests");
    this.activeView = "";
    await backend.query(commands.join("; "));
    this.checkSnapshot(snapshot);
    this.activeView = signature;
  }

  private async dropRetired(backend: DuckDBBackend, generation: number): Promise<void> {
    this.check(generation);
    const victims = [...this.retired].filter((binding) => binding.pins === 0);
    if (!victims.length) return;
    await this.publish(backend, this.snapshot());
    this.check(generation);
    for (const binding of victims) {
      await this.dropRegisteredFile(backend, binding.name, generation);
      binding.dropped = true;
      this.retired.delete(binding);
    }
  }

  private async register(
    table: Table, scopedKey: string, key: string, bytes: ArrayBuffer,
    stats: { rawRows: number; minTs: number; maxTs: number } | undefined,
    signal: AbortSignal | undefined, required: readonly (readonly [string, DuckDBColumnType])[],
    generation: number,
    hold?: (binding: Binding) => void,
  ): Promise<IngestedFileMeta> {
    return this.operation(generation, async () => {
      const backend = await this.ensureBackend(generation);
      this.check(generation, signal);
      await this.dropRetired(backend, generation);
      this.check(generation, signal);
      const existing = this.bindings.get(this.key(table, scopedKey));
      if (existing) {
        if (required.some(([id, type]) => this.customTypes.get(id) !== type)) {
          const custom = new Map([...this.customTypes, ...required]);
          await this.publish(backend, this.snapshot(undefined, custom));
          this.check(generation, signal);
          for (const [id, type] of required) this.customTypes.set(id, type);
          existing.meta = { ...existing.meta, customColumns: [...custom.keys()] };
        }
        hold?.(existing);
        return existing.meta;
      }
      let registeredBytes = [...this.bindings.values(), ...this.retired].reduce((sum, binding) => sum + binding.meta.byteLength, 0);
      if (registeredBytes + bytes.byteLength > MAX_COMPRESSED_BYTES) {
        const victims: Binding[] = [];
        for (const binding of this.bindings.values()) {
          if (registeredBytes + bytes.byteLength <= MAX_COMPRESSED_BYTES) break;
          if (binding.pins > 0) continue;
          victims.push(binding);
          registeredBytes -= binding.meta.byteLength;
        }
        if (registeredBytes + bytes.byteLength <= MAX_COMPRESSED_BYTES && victims.length) {
          const next = [...this.bindings.values()].filter((binding) => !victims.includes(binding));
          await this.publish(backend, this.snapshot(next));
          this.check(generation, signal);
          if (victims.some((binding) => binding.pins > 0))
            throw new Error("Cloud query registered compressed files exceed the browser byte budget");
          for (const binding of victims) {
            this.bindings.delete(this.key(binding.table, binding.meta.scopedKey));
            binding.retired = true;
            this.retired.add(binding);
          }
          await this.dropRetired(backend, generation);
          this.check(generation, signal);
        } else {
          throw new Error("Cloud query registered compressed files exceed the browser byte budget");
        }
      }
      if (registeredBytes + bytes.byteLength > MAX_COMPRESSED_BYTES)
        throw new Error("Cloud query registered compressed files exceed the browser byte budget");
      const name = `bound_g${generation}_${this.nextFile++}.parquet`;
      let registered = false, committed = false;
      let failure: unknown;
      try {
        await backend.registerFileBuffer(name, new Uint8Array(bytes.slice(0)));
        registered = true;
        this.ownedNames.add(name);
        this.check(generation, signal);
        const schema = await backend.query(`DESCRIBE SELECT * FROM read_parquet(${literal(name)})`, signal);
        this.check(generation, signal);
        const names = (schema.getChild("column_name")?.toJSON() ?? []) as string[];
        const types = (schema.getChild("column_type")?.toJSON() ?? []) as string[];
        const typeMap = new Map(names.map((name, index) => [name, types[index].toUpperCase()]));
        const tsColumn = typeMap.has("timestamp") ? "timestamp" : typeMap.has("ts_ms") ? "ts_ms" : null;
        if (!tsColumn && table === "requests") throw new Error("Cloud Parquet file has no timestamp column");
        const tsType = tsColumn ? typeMap.get(tsColumn)! : "";
        const column = tsColumn ? quoteIdent(tsColumn) : "";
        const ts = !tsColumn ? "0::BIGINT" : tsType.startsWith("TIMESTAMP") ? `epoch_ms(${column})`
          : tsType.includes("CHAR") ? `epoch_ms(TRY_CAST(${column} AS TIMESTAMP))` : `CAST(${column} AS BIGINT)`;
        let known = stats;
        if (!known) {
          const rows = await backend.query(`SELECT count(*) AS raw_rows, min(${ts}) AS min_ts, max(${ts}) AS max_ts FROM read_parquet(${literal(name)})`, signal);
          this.check(generation, signal);
          const number = (name: string, fallback: number) => {
            const value = rows.getChild(name)?.toJSON()?.[0];
            return value === null || value === undefined ? fallback : Number(value);
          };
          known = { rawRows: number("raw_rows", 0), minTs: number("min_ts", Infinity), maxTs: number("max_ts", -Infinity) };
        }
        const custom = new Map([...this.customTypes, ...required]);
        const meta: IngestedFileMeta = {
          scopedKey, key, ...known, byteLength: bytes.byteLength,
          hasBytes: typeMap.has("resp_bytes") || typeMap.has("resp_body_size"),
          hasEdge: typeMap.has("edge") || typeMap.has("is_edge"),
          customColumns: [...custom.keys()], presentColumns: names,
        };
        const binding: Binding = { name, table, types: typeMap, ts, meta, pins: 0, retired: false, dropped: false };
        const next = [...this.bindings.values()];
        next.push(binding);
        await this.publish(backend, { ...this.snapshot(next, custom) });
        this.check(generation, signal);
        for (const [id, type] of required) this.customTypes.set(id, type);
        this.bindings.set(this.key(table, scopedKey), binding);
        committed = true;
        hold?.(binding);
        await this.dropRetired(backend, generation);
        this.check(generation, signal);
        return meta;
      } catch (error) {
        failure = error;
        this.observeFailure(generation, error);
        throw error;
      } finally {
        if (registered && !committed && generation === this.generation && !this.poisoned) {
          try {
            await this.publish(backend, this.snapshot());
            this.check(generation);
            await this.dropRegisteredFile(backend, name, generation);
          } catch (error) {
            if (generation !== this.generation) throw failure ?? error;
            this.poisoned = new Error(`${failure instanceof Error ? failure.message : "Cloud Parquet registration failed"}; DuckDB file cleanup failed; the backend must be recreated`, {
              cause: new AggregateError([failure, error]),
            });
            throw this.poisoned;
          }
        }
      }
    }, signal);
  }

  ingestFile(
    scopedKey: string, key: string, bytes: ArrayBuffer,
    stats?: { rawRows: number; minTs: number; maxTs: number }, signal?: AbortSignal,
  ): Promise<IngestedFileMeta> {
    return this.register("requests", scopedKey, key, bytes, stats, signal, [], this.generation);
  }

  async syncWindow(files: readonly { scopedKey?: string; key: string; bytes: ArrayBuffer;
    knownStats?: { rawRows: number; minTs: number; maxTs: number } }[], signal?: AbortSignal): Promise<IngestedFileMeta[]> {
    const generation = this.generation, result: IngestedFileMeta[] = [];
    for (const file of files) {
      this.check(generation, signal);
      result.push(await this.register("requests", file.scopedKey ?? file.key, file.key, file.bytes, file.knownStats, signal, [], generation));
      this.check(generation, signal);
    }
    return result;
  }

  async syncRumFiles(
    vitals: readonly { scopedKey?: string; key: string; bytes: ArrayBuffer }[],
    errors: readonly { scopedKey?: string; key: string; bytes: ArrayBuffer }[] = [], signal?: AbortSignal,
  ): Promise<void> {
    const generation = this.generation;
    for (const [table, files] of [["client_vitals", vitals], ["client_errors", errors]] as const) {
      for (const file of files) {
        this.check(generation, signal);
        await this.register(table, file.scopedKey ?? file.key, file.key, file.bytes, undefined, signal, [], generation);
        this.check(generation, signal);
      }
    }
  }

  async retainFiles(files: { request: readonly string[]; vitals: readonly string[]; errors: readonly string[] }, signal?: AbortSignal): Promise<void> {
    const generation = this.generation;
    await this.operation(generation, async () => {
      const keep = { requests: new Set(files.request), client_vitals: new Set(files.vitals), client_errors: new Set(files.errors) };
      const next = [...this.bindings.values()].filter((binding) => keep[binding.table].has(binding.meta.scopedKey));
      if (next.length === this.bindings.size) return;
      const backend = await this.ensureBackend(generation);
      this.check(generation, signal);
      await this.publish(backend, this.snapshot(next));
      this.check(generation, signal);
      for (const [key, binding] of this.bindings) {
        if (!keep[binding.table].has(binding.meta.scopedKey)) {
          this.bindings.delete(key);
          binding.retired = true;
          this.retired.add(binding);
        }
      }
      await this.dropRetired(backend, generation);
    }, signal);
  }

  async reset(): Promise<void> {
    if (this.poisoned) { await this.beginRequest(); return; }
    const generation = ++this.generation;
    const reset = this.queue.then(async () => {
      const all = [...this.bindings.values(), ...this.retired];
      const names = [...this.ownedNames];
      this.bindings.clear();
      this.retired.clear();
      this.customTypes.clear();
      this.presentOverride = null;
      this.activeView = "";
      if (!this.backendPromise) return;
      const backend = await this.ensureBackend(generation);
      this.check(generation);
      await this.publish(backend, this.snapshot([]));
      this.check(generation);
      for (const name of names) {
        await this.dropRegisteredFile(backend, name, generation);
      }
      for (const binding of all) binding.dropped = true;
    });
    this.queue = reset.then(() => undefined, () => undefined);
    try {
      await reset;
    } catch (error) {
      if (generation === this.generation) this.poisoned = new Error("DuckDB reset cleanup failed", { cause: error });
      throw error;
    }
  }
}
