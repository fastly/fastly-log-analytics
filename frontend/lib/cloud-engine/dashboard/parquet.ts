import { parquetMetadata, parquetRead, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

export type DecodedColumns = {
  rows: number;
  columns: Map<string, ArrayLike<unknown>>;
};

export async function readColumns(
  buffer: ArrayBuffer,
  wanted: readonly string[],
  /** False keeps byte-array columns as bytes rather than UTF-8 text. */
  utf8 = true,
): Promise<DecodedColumns> {
  const metadata = parquetMetadata(buffer);
  const present = new Set(
    metadata.schema.slice(1).map((element) => element.name),
  );
  const columns = wanted.filter((name) => present.has(name));
  const chunks = new Map<
    string,
    { rowStart: number; data: ArrayLike<unknown> }[]
  >();
  if (columns.length) {
    await parquetRead({
      file: buffer,
      columns,
      compressors,
      utf8,
      onChunk: ({ columnName, columnData, rowStart }) => {
        const list = chunks.get(columnName) ?? [];
        list.push({ rowStart, data: columnData });
        chunks.set(columnName, list);
      },
    });
  }
  const decoded = new Map<string, ArrayLike<unknown>>();
  for (const [name, list] of chunks) {
    list.sort((a, b) => a.rowStart - b.rowStart);
    decoded.set(
      name,
      list.length === 1
        ? list[0].data
        : list.flatMap((chunk) => Array.from(chunk.data)),
    );
  }
  return { rows: Number(metadata.num_rows), columns: decoded };
}

function normalize(value: unknown): unknown {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  return value;
}

export async function readParquetRows(
  buffer: ArrayBuffer,
  columns?: string[],
): Promise<Record<string, unknown>[]> {
  const rows = (await parquetReadObjects({
    file: buffer,
    compressors,
    ...(columns ? { columns } : {}),
  })) as Record<string, unknown>[];
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([name, value]) => [name, normalize(value)]),
    ),
  );
}

export const readRows = readParquetRows;
