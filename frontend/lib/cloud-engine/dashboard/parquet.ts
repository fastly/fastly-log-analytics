import { parquetMetadata, parquetRead, parquetReadObjects, type FileMetaData } from "hyparquet";
import { compressors } from "hyparquet-compressors";

export type DecodedColumns = {
  rows: number;
  columns: Map<string, ArrayLike<unknown>>;
};

export type ReadColumnsOptions = {
  metadata?: FileMetaData;
  /** Zero-based row group indices to decode; rows are returned in ascending row-group order. */
  rowGroups?: readonly number[];
};

function rowGroupSpans(
  metadata: FileMetaData,
  rowGroups: readonly number[],
): { spans: [number, number][]; totalRows: number } {
  const wanted = new Set(rowGroups);
  const spans: [number, number][] = [];
  let offset = 0;
  let totalRows = 0;
  for (let i = 0; i < metadata.row_groups.length; i++) {
    const count = Number(metadata.row_groups[i].num_rows);
    const end = offset + count;
    if (wanted.has(i) && count > 0) {
      totalRows += count;
      const last = spans[spans.length - 1];
      if (last && last[1] === offset) last[1] = end;
      else spans.push([offset, end]);
    }
    offset = end;
  }
  return { spans, totalRows };
}

function concatChunks(
  list: { rowStart: number; data: ArrayLike<unknown> }[],
): ArrayLike<unknown> {
  if (list.length === 1) return list[0].data;
  list.sort((a, b) => a.rowStart - b.rowStart);
  let total = 0;
  for (let i = 0; i < list.length; i++) total += list[i].data.length;
  const out = new Array<unknown>(total);
  let offset = 0;
  for (let i = 0; i < list.length; i++) {
    const data = list[i].data;
    for (let j = 0; j < data.length; j++) out[offset++] = data[j];
  }
  return out;
}

export async function readColumns(
  buffer: ArrayBuffer,
  wanted: readonly string[],
  /** False keeps byte-array columns as bytes rather than UTF-8 text. */
  utf8 = true,
  options?: ReadColumnsOptions,
): Promise<DecodedColumns> {
  const metadata = options?.metadata ?? parquetMetadata(buffer);
  const present = new Set(
    metadata.schema.slice(1).map((element) => element.name),
  );
  const columns = wanted.filter((name) => present.has(name));
  const selected = options?.rowGroups
    ? rowGroupSpans(metadata, options.rowGroups)
    : null;
  const totalRows = selected ? selected.totalRows : Number(metadata.num_rows);
  if (totalRows === 0 || !columns.length) {
    return { rows: totalRows, columns: new Map() };
  }
  const chunks = new Map<
    string,
    { rowStart: number; data: ArrayLike<unknown> }[]
  >();
  const onChunk = ({
    columnName,
    columnData,
    rowStart,
  }: {
    columnName: string;
    columnData: ArrayLike<unknown>;
    rowStart: number;
  }) => {
    let list = chunks.get(columnName);
    if (!list) {
      list = [];
      chunks.set(columnName, list);
    }
    list.push({ rowStart, data: columnData });
  };
  if (selected) {
    for (const [rowStart, rowEnd] of selected.spans) {
      await parquetRead({
        file: buffer,
        metadata,
        columns,
        compressors,
        utf8,
        rowStart,
        rowEnd,
        onChunk,
      });
    }
  } else {
    await parquetRead({
      file: buffer,
      metadata,
      columns,
      compressors,
      utf8,
      onChunk,
    });
  }
  const decoded = new Map<string, ArrayLike<unknown>>();
  for (const [name, list] of chunks) {
    decoded.set(name, concatChunks(list));
  }
  return { rows: totalRows, columns: decoded };
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
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    for (const key in row) {
      const value = row[key];
      if (value instanceof Date) row[key] = value.getTime();
      else if (typeof value === "bigint") row[key] = Number(value);
    }
  }
  return rows;
}

export const readRows = readParquetRows;
