import { parquetMetadata, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { readParquetRows } from "./dashboard/parquet";

export { readParquetRows };

export type CatalogFieldInput = {
  id: string;
  group?: string | null;
  duckdb_type?: string | null;
  label?: string;
  description?: string;
};

export type CatalogInput = {
  groups?: unknown[];
  fields: readonly CatalogFieldInput[];
};

export type SchemaColumn = {
  name: string;
  type: string;
  catalogType: string;
  label?: string;
  description?: string;
};

export type ColumnStats = {
  populated: number;
  unique: number;
  min: string | null;
  max: string | null;
};

export type SampleStats = { rows: number; stats: Map<string, ColumnStats> };

export const SAMPLE_MAX_ROWS = 10_000;

// Mirrors `ingest::convert::columns()`: every catalog field becomes a nullable
// column whose physical type is one of five Arrow types.
function fileType(catalogType: string): string {
  switch (catalogType) {
    case "VARCHAR":
      return "VARCHAR";
    case "BOOLEAN":
      return "BOOLEAN";
    case "TIMESTAMP":
      return "TIMESTAMP WITH TIME ZONE";
    case "FLOAT":
    case "DOUBLE":
      return "DOUBLE";
    default:
      return "BIGINT";
  }
}

export const EXTRA_JSON_COLUMN = "_extra";

export function parquetColumns(
  catalog:
    | CatalogInput
    | readonly {
        id?: string;
        name?: string;
        group?: string | null;
        duckdb_type?: string | null;
        arrow_type?: string | null;
        label?: string;
        description?: string;
      }[],
): SchemaColumn[] {
  const fields = Array.isArray(catalog)
    ? catalog
    : (catalog as CatalogInput).fields;
  return fields.flatMap((field) => {
    const colType =
      (field as CatalogFieldInput).duckdb_type ??
      (field as { arrow_type?: string | null }).arrow_type;
    const colName =
      (field as CatalogFieldInput).id ??
      (field as { name?: string }).name ??
      EXTRA_JSON_COLUMN;
    return colType
      ? [
          {
            name: colName,
            type: fileType(colType),
            catalogType: colType,
            label: field.label,
            description: field.description,
          },
        ]
      : [];
  });
}

type Comparable = number | string | boolean;

function comparable(value: unknown): Comparable | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (
    typeof value === "number" ||
    typeof value === "string" ||
    typeof value === "boolean"
  )
    return value;
  return JSON.stringify(value);
}

function display(value: Comparable, type: string): string {
  if (type.startsWith("TIMESTAMP") && typeof value === "number")
    return new Date(value).toISOString();
  return String(value);
}

export async function sampleStats(
  buffer: ArrayBuffer,
  columns: SchemaColumn[],
  maxRows = SAMPLE_MAX_ROWS,
): Promise<SampleStats> {
  const metadata = parquetMetadata(buffer);
  const present = new Set(
    metadata.schema.slice(1).map((element) => element.name),
  );
  const wanted = columns.filter((column) => present.has(column.name));
  const rowEnd = Math.min(Number(metadata.num_rows), maxRows);
  const rows = wanted.length
    ? await parquetReadObjects({
        file: buffer,
        metadata,
        columns: wanted.map((column) => column.name),
        rowEnd,
        compressors,
      })
    : [];
  const stats = new Map<string, ColumnStats>();
  for (const column of wanted) {
    const distinct = new Set<Comparable>();
    let populated = 0;
    let min: Comparable | null = null;
    let max: Comparable | null = null;
    for (const row of rows) {
      const value = comparable(row[column.name]);
      if (value === null) continue;
      populated++;
      distinct.add(value);
      if (min === null || value < min) min = value;
      if (max === null || value > max) max = value;
    }
    stats.set(column.name, {
      populated: rows.length ? (populated / rows.length) * 100 : 0,
      unique: distinct.size,
      min: min === null ? null : display(min, column.type),
      max: max === null ? null : display(max, column.type),
    });
  }
  return { rows: rows.length, stats };
}
