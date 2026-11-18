import { parquetMetadata, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { parquetWriteBuffer } from "hyparquet-writer";

/** The bundle layout the writer is to adopt (design layout B): one row group per entry. */
export const LAYOUT_B: readonly (readonly string[])[] = [
  ["overview", "time_series"],
  ["field_totals", "all_fields", "all_fields_ip"],
  ["sessions"],
  ["dims"],
  ["series5"],
  ["bins", "assets", "transport", "origin"],
  ["urls"],
  ["quality", "sample"],
  ["header_ip", "bot_ua", "tls_fp", "proxy"],
  ["value"],
  ["ins_url"],
  ["ins_ip"],
  ["asset_urls"],
  ["ins_lat"],
  ["ins_asn"],
  ["shield_routes"],
  ["filtered_rollups"],
];

/**
 * Rewrites v3 summaries as the bundle writer will with `flp_summary_layout`:
 * their rows (all of `sources`, in order; several make a file larger than the
 * footer probe), schema and key-value metadata, moved into one row group per
 * layout entry (in entry order, file order within one), with each
 * `dim_totals` row in the row group of the group it names. Entries with no
 * rows are dropped, as the writer would. `layout: null` writes the same rows
 * as today's single row group, without the key.
 */
export async function withSummaryLayout(
  sources: ArrayBuffer | readonly ArrayBuffer[],
  layout: readonly (readonly string[])[] | null = LAYOUT_B,
): Promise<ArrayBuffer> {
  const files = Array.isArray(sources) ? sources : [sources as ArrayBuffer];
  const metadata = parquetMetadata(files[0]);
  const fields = metadata.schema.slice(1);
  const binary = new Set(
    fields.filter((f) => f.type === "BYTE_ARRAY" && !f.converted_type && !f.logical_type).map((f) => f.name),
  );
  const names = fields.map((f) => f.name);
  const text: Record<string, unknown>[] = [];
  const raw: Record<string, unknown>[] = [];
  for (const file of files) {
    const schema = parquetMetadata(file).schema.slice(1).map((f) => f.name);
    if (schema.join() !== names.join()) throw new Error("Summaries to combine differ in schema");
    text.push(...await parquetReadObjects({ file, compressors }));
    raw.push(...await parquetReadObjects({ file, columns: [...binary], compressors, utf8: false }));
  }
  if (!layout) {
    return parquetWriteBuffer({
      statisticsTruncateLength: 64,
      schema: metadata.schema,
      rowGroupSize: text.length,
      kvMetadata: (metadata.key_value_metadata ?? []).map(({ key, value }) => ({ key, value: value ?? "" })),
      columnData: names.map((name) => ({ name, data: text.map((row, i) => (binary.has(name) ? raw[i][name] : row[name]) ?? null) })),
    });
  }
  const entryOf = (row: Record<string, unknown>) => {
    const group = String(row.bundle === "dim_totals" ? row.value : row.bundle);
    const entry = layout.findIndex((groups) => groups.includes(group));
    if (entry < 0) throw new Error(`No layout entry holds ${group}`);
    return entry;
  };
  const byEntry = layout.map(() => [] as number[]);
  text.forEach((row, i) => byEntry[entryOf(row)].push(i));
  const kept = layout.map((groups, i) => ({ groups, rows: byEntry[i] })).filter((entry) => entry.rows.length);
  const order = kept.flatMap((entry) => entry.rows);
  const kv = (metadata.key_value_metadata ?? []).filter((entry) => entry.key !== "flp_summary_layout");
  return parquetWriteBuffer({
    statisticsTruncateLength: 64,
    schema: metadata.schema,
    rowGroupSize: kept.map((entry) => entry.rows.length),
    kvMetadata: [
      ...kv.map(({ key, value }) => ({ key, value: value ?? "" })),
      { key: "flp_summary_layout", value: JSON.stringify({ v: 1, rg: kept.map((entry) => entry.groups) }) },
    ],
    columnData: names.map((name) => ({
      name,
      data: order.map((i) => (binary.has(name) ? raw[i][name] : text[i][name]) ?? null),
    })),
  });
}

/**
 * Rewrites one v3 summary with `edit` applied to its rows (binary columns as
 * bytes), keeping its schema and key-value metadata, as one row group.
 */
export async function editSummaryRows(
  source: ArrayBuffer,
  edit: (rows: Record<string, unknown>[]) => Record<string, unknown>[],
): Promise<ArrayBuffer> {
  const metadata = parquetMetadata(source);
  const fields = metadata.schema.slice(1);
  const binary = fields.filter((f) => f.type === "BYTE_ARRAY" && !f.converted_type && !f.logical_type).map((f) => f.name);
  const text = await parquetReadObjects({ file: source, compressors });
  const raw = await parquetReadObjects({ file: source, columns: binary, compressors, utf8: false });
  const rows = edit(text.map((row, i) => ({ ...row, ...raw[i] })));
  return parquetWriteBuffer({
    statisticsTruncateLength: 64,
    schema: metadata.schema,
    rowGroupSize: rows.length,
    kvMetadata: (metadata.key_value_metadata ?? []).map(({ key, value }) => ({ key, value: value ?? "" })),
    columnData: fields.map(({ name }) => ({ name, data: rows.map((row) => row[name] ?? null) })),
  });
}
