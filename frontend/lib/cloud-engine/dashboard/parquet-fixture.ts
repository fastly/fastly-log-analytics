import { parquetWriteBuffer } from "hyparquet-writer";

// Kept free of the DuckDB engine so Playwright specs, compiled as CommonJS, can import it.
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
