import { parquetWriteBuffer } from "hyparquet-writer";
export { parquetFixture, type FixtureRow } from "./parquet-fixture";
import type { FileRollup, FileSource } from "./engine";
import type { ManifestFile } from "./manifest";
import {
  createNodeBackend, getDuckDBEngine as analyticalEngine, interpolateSqlParams,
  DuckDBAnalyticalEngine as AnalyticalEngine, type DuckDBBackend,
} from "./duckdb";

let fixtureBackend: Promise<DuckDBBackend> | undefined;
let nextSqlFixture = 1;
const fixtureProxies = new WeakMap<AnalyticalEngine, AnalyticalEngine>();
const fixtureColumns = new WeakMap<AnalyticalEngine, Map<string, string>>();

export function fixtureEngine(engine: AnalyticalEngine): AnalyticalEngine {
  const existing = fixtureProxies.get(engine);
  if (existing) return existing;
  const proxy = new Proxy(engine, {
    get(target, property) {
      if (property === "executeSql") return async (sql: string, params: readonly unknown[] = [], signal?: AbortSignal) => {
        const insert = /^\s*INSERT INTO (requests|client_vitals|client_errors)\s*\(([^)]+)\)/i.exec(sql);
        const alter = /^\s*ALTER TABLE requests ADD(?: COLUMN)?(?: IF NOT EXISTS)?\s+"?(\w+)"?\s+(\w+)/i.exec(sql);
        if (alter) {
          const columns = fixtureColumns.get(engine) ?? new Map<string, string>();
          columns.set(alter[1], alter[2]);
          fixtureColumns.set(engine, columns);
          return [];
        }
        if (!insert) return target.executeSql(sql, params, signal);
        signal?.throwIfAborted();
        const backend = await (fixtureBackend ??= createNodeBackend());
        const schema = await target.executeSql<{ column_name: string; column_type: string }>(
          `DESCRIBE SELECT * FROM ${insert[1]}`,
        );
        const types = new Map(schema.map((column) => [column.column_name, column.column_type]));
        for (const [name, type] of fixtureColumns.get(engine) ?? []) types.set(name, type);
        await backend.query(`CREATE OR REPLACE TABLE fixture_rows (${[...types]
          .map(([name, type]) => `"${name}" ${type}`).join(", ")})`);
        await backend.query(interpolateSqlParams(sql.replace(insert[1], "fixture_rows"), params));
        const rows = await backend.query("SELECT * FROM fixture_rows");
        const columns = insert[2].split(",").map((name) => name.trim().replaceAll('"', ""));
        const data = new Map(columns.map((name) => [name, rows.getChild(name)?.toJSON() ?? []]));
        const keys = data.get("scoped_key") ?? Array.from({ length: rows.numRows }, () => "fixture");
        for (const key of new Set(keys)) {
          const indices = keys.flatMap((value, index) => value === key ? [index] : []);
          const columnData = columns.filter((name) => name !== "scoped_key").map((name) => {
            const sqlType = types.get(name)!;
            const type = sqlType.startsWith("TIMESTAMP") ? "TIMESTAMP" as const
              : /CHAR|TEXT/i.test(sqlType) ? "STRING" as const
                : /BOOL/i.test(sqlType) ? "BOOLEAN" as const
                  : /FLOAT|DOUBLE|REAL|DECIMAL/i.test(sqlType) ? "DOUBLE" as const : "INT64" as const;
            return { name, type, data: indices.map((index) => {
              const value = data.get(name)?.[index];
              if (value === null || value === undefined) return null;
              if (type === "TIMESTAMP") return new Date(Number(value));
              if (type === "INT64") return BigInt(value as number);
              return value as string | number | boolean;
            }) };
          }).filter(({ name, data }) => name === "timestamp" || name === "ts_ms" || data.some((value) => value !== null));
          const bytes = parquetWriteBuffer({ columnData, codec: "SNAPPY" });
          const scopedKey = `${String(key)}:fixture-${nextSqlFixture++}`;
          if (insert[1] === "requests") {
            const custom = columns.filter((name) => !schema.some((column) => column.column_name === name))
              .map((id) => ({ id, type: /BOOL/i.test(types.get(id)!) ? "boolean" as const
                : /CHAR|TEXT/i.test(types.get(id)!) ? "string" as const : "number" as const }));
            const lease = target.lease(custom);
            try { await lease.ingestFile(scopedKey, `${scopedKey}.parquet`, bytes); }
            finally { lease.release(); }
          } else {
            const file = { scopedKey, key: `${scopedKey}.parquet`, bytes };
            await target.syncRumFiles(insert[1] === "client_vitals" ? [file] : [],
              insert[1] === "client_errors" ? [file] : []);
          }
        }
        return [];
      };
      const value = Reflect.get(target, property);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  fixtureProxies.set(engine, proxy);
  return proxy;
}

export function getDuckDBEngine(): AnalyticalEngine {
  return fixtureEngine(analyticalEngine());
}

export class DuckDBAnalyticalEngine extends AnalyticalEngine {
  constructor(...args: ConstructorParameters<typeof AnalyticalEngine>) {
    super(...args);
    return fixtureEngine(this);
  }
}

let nextFixtureScopeId = 1;

export function manifestEntry(
  key: string,
  start: string,
  end: string,
  rows: number,
): ManifestFile {
  return { key, start, end, size: 1, rows };
}

export function fixtureSource(
  pages: {
    files: ManifestFile[];
    next_cursor: string | null;
    complete: boolean;
  }[],
  buffers: Record<string, ArrayBuffer>,
  log: { json: string[]; files: string[]; summary?: string[] } = {
    json: [],
    files: [],
  },
  summaryPages: typeof pages = [],
): FileSource & { log: typeof log } {
  const seen = new Set<string>();
  const rollups = new Map<string, FileRollup>();
  const cacheScope = `fixture-${nextFixtureScopeId++}`;
  return {
    log,
    cacheScope,
    async fetchJson(url) {
      const params = new URL(url, "http://x").searchParams;
      const index = Number(params.get("cursor") ?? 0);
      if (params.get("view") === "summary") {
        (log.summary ??= []).push(url);
        return (
          summaryPages[index] ?? {
            files: [],
            next_cursor: null,
            complete: true,
          }
        );
      }
      log.json.push(url);
      const page = pages[index];
      if (
        params.get("summaries") === "1" &&
        index === 0 &&
        page &&
        !page.next_cursor &&
        page.complete
      ) {
        return {
          ...page,
          summary_files: summaryPages[0]?.files ?? [],
        };
      }
      return page;
    },
    async fetchFile(key) {
      log.files.push(key);
      const cached = seen.has(key);
      seen.add(key);
      return { bytes: buffers[key], cached };
    },
    async hasFile(key) {
      return seen.has(key);
    },
    async readRollup(key, variant = "") {
      return rollups.get(`${variant}|${key}`) ?? null;
    },
    async writeRollup(key, rollup, variant = "") {
      rollups.set(`${variant}|${key}`, rollup);
    },
  };
}
