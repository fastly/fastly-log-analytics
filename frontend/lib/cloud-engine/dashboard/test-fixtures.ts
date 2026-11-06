import { parquetWriteBuffer } from "hyparquet-writer";
import type { FileRollup, FileSource } from "./engine";
import type { ManifestFile } from "./manifest";

export type FixtureRow = {
  timestamp: number;
  status: number | null;
  cache: string | null;
  resp_bytes: number | null;
  host?: string | null;
  country?: string | null;
  edge?: boolean | null;
  ttl?: number | null;
  age?: number | null;
  /** A custom string field, written only when some row sets it. */
  tenant?: string | null;
};

const OPTIONAL_DOUBLE_COLUMNS = ["ttl", "age"] as const;

let nextFixtureScopeId = 1;

export function parquetFixture(rows: FixtureRow[], omit: string[] = []) {
  const columns = [
    {
      name: "timestamp",
      type: "TIMESTAMP" as const,
      data: rows.map((r) => new Date(r.timestamp)),
    },
    { name: "status", type: "INT32" as const, data: rows.map((r) => r.status) },
    { name: "cache", type: "STRING" as const, data: rows.map((r) => r.cache) },
    {
      name: "resp_bytes",
      type: "INT64" as const,
      data: rows.map((r) =>
        r.resp_bytes === null ? null : BigInt(r.resp_bytes),
      ),
    },
    {
      name: "host",
      type: "STRING" as const,
      data: rows.map((r) => r.host ?? null),
    },
    {
      name: "country",
      type: "STRING" as const,
      data: rows.map((r) => r.country ?? null),
    },
    {
      name: "edge",
      type: "BOOLEAN" as const,
      data: rows.map((r) => r.edge ?? null),
    },
    ...OPTIONAL_DOUBLE_COLUMNS.filter((name) =>
      rows.some((r) => r[name] !== undefined),
    ).map((name) => ({
      name,
      type: "DOUBLE" as const,
      data: rows.map((r) => r[name] ?? null),
    })),
    ...(rows.some((r) => r.tenant !== undefined)
      ? [
          {
            name: "tenant",
            type: "STRING" as const,
            data: rows.map((r) => r.tenant ?? null),
          },
        ]
      : []),
  ].filter((column) => !omit.includes(column.name));
  return parquetWriteBuffer({ columnData: columns, codec: "SNAPPY" });
}

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
