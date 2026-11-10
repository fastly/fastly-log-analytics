import { parquetMetadata, type FileMetaData, type SchemaElement } from "hyparquet";
import { ByteWriter } from "hyparquet-writer";
import { writeMetadata } from "hyparquet-writer/src/metadata.js";

/** One tail read usually covers the whole footer. */
export const FOOTER_PROBE_BYTES = 64 * 1024;
/**
 * Needed chunks closer than this always share one HTTP range. Compacted files
 * hold many row groups of ~100 columns, so most chunks are a few KiB and a
 * wider gap pulls in the unneeded columns between them.
 */
export const RANGE_MERGE_GAP = 8 * 1024;
/**
 * What one more range request costs, in bytes of transfer. The gap widens from
 * [`RANGE_MERGE_GAP`] while that lowers `bytes + ranges × cost`: a 6 h window of
 * compacts otherwise needs 100+ requests per file, which under load takes
 * longer than reading the unneeded columns would.
 */
export const RANGE_REQUEST_COST_BYTES = 32 * 1024;

export type FetchRange = (start: number, end: number) => Promise<ArrayBuffer>;

export type ParquetFooter = {
  metadata: FileMetaData;
  footerLength: number;
  size: number;
  /** The file's last bytes as read for the footer, from `start` to its end. */
  tail?: { start: number; bytes: ArrayBuffer };
};

type Chunk = { rowGroup: number; column: number; start: number; length: number };

export type ParquetProjection = {
  footer: ParquetFooter;
  columns: ReadonlySet<string>;
  /** Top-level column names of the original file. */
  presentColumns: string[];
  chunks: Chunk[];
  /** Coalesced byte ranges that cover every needed chunk (end exclusive). */
  ranges: { start: number; end: number }[];
  /** Compressed bytes of the needed column chunks, measured from the footer. */
  neededBytes: number;
  /** Upper bound of the rebuilt file: needed chunks plus the original footer and framing. */
  projectedBytes: number;
};

const MAGIC = 0x31524150; // "PAR1" little-endian

export async function readParquetFooter(size: number, fetchRange: FetchRange): Promise<ParquetFooter> {
  if (!Number.isSafeInteger(size) || size < 12) throw new Error("Parquet file is too small");
  const probeStart = Math.max(0, size - FOOTER_PROBE_BYTES);
  let tail = await fetchRange(probeStart, size);
  const view = new DataView(tail);
  if (view.getUint32(tail.byteLength - 4, true) !== MAGIC) throw new Error("Parquet footer magic is missing");
  const footerLength = view.getUint32(tail.byteLength - 8, true);
  if (footerLength + 12 > size) throw new Error("Parquet footer length exceeds the file");
  let tailStart = probeStart;
  if (footerLength + 8 > tail.byteLength) {
    tailStart = size - footerLength - 8;
    const head = new Uint8Array(await fetchRange(tailStart, probeStart));
    const whole = new Uint8Array(size - tailStart);
    whole.set(head);
    whole.set(new Uint8Array(tail), head.byteLength);
    tail = whole.buffer;
  }
  const footer = tail.slice(tail.byteLength - footerLength - 8);
  return {
    metadata: parquetMetadata(footer, { geoparquet: false }), footerLength, size,
    tail: { start: tailStart, bytes: tail },
  };
}

function topLevel(schema: readonly SchemaElement[]): { name: string; leaves: number }[] {
  const out: { name: string; leaves: number }[] = [];
  let index = 1;
  const walk = (): number => {
    const element = schema[index++];
    if (!element) throw new Error("Parquet schema is truncated");
    if (!element.num_children) return 1;
    let leaves = 0;
    for (let i = 0; i < element.num_children; i++) leaves += walk();
    return leaves;
  };
  for (let i = 0; i < (schema[0]?.num_children ?? 0); i++) {
    const name = schema[index]?.name;
    out.push({ name, leaves: walk() });
  }
  return out;
}

const chunkStart = (meta: { data_page_offset: bigint; dictionary_page_offset?: bigint }) => {
  const data = Number(meta.data_page_offset);
  const dictionary = meta.dictionary_page_offset === undefined ? NaN : Number(meta.dictionary_page_offset);
  return dictionary >= 4 && dictionary < data ? dictionary : data;
};

export function planParquetProjection(footer: ParquetFooter, columns: ReadonlySet<string>): ParquetProjection {
  const { metadata, footerLength, size } = footer;
  const dataEnd = size - footerLength - 8;
  const chunks: Chunk[] = [];
  metadata.row_groups.forEach((group, rowGroup) => group.columns.forEach((chunk, column) => {
    const meta = chunk.meta_data;
    if (!meta || chunk.file_path) throw new Error("Parquet column chunk metadata is not inline");
    if (!columns.has(meta.path_in_schema[0])) return;
    const start = chunkStart(meta);
    const length = Number(meta.total_compressed_size);
    if (!(start >= 4 && length > 0 && start + length <= dataEnd)) throw new Error("Parquet column chunk is outside the file");
    chunks.push({ rowGroup, column, start, length });
  }));
  const ranges = coalesceRanges([...chunks].sort((a, b) => a.start - b.start));
  const neededBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  return {
    footer, columns, chunks, ranges, neededBytes,
    presentColumns: topLevel(metadata.schema).map((field) => field.name),
    projectedBytes: neededBytes + footerLength + 12,
  };
}

/** The cheapest coalescing of the chunks (sorted by start) by bytes plus request cost. */
function coalesceRanges(chunks: readonly Chunk[]): { start: number; end: number }[] {
  let best: { start: number; end: number }[] = [];
  let bestCost = Infinity;
  for (let gap = RANGE_MERGE_GAP; ; gap *= 2) {
    const ranges: { start: number; end: number }[] = [];
    for (const chunk of chunks) {
      const last = ranges[ranges.length - 1];
      if (last && chunk.start - last.end <= gap) last.end = Math.max(last.end, chunk.start + chunk.length);
      else ranges.push({ start: chunk.start, end: chunk.start + chunk.length });
    }
    const bytes = ranges.reduce((sum, range) => sum + range.end - range.start, 0);
    const cost = bytes + ranges.length * RANGE_REQUEST_COST_BYTES;
    if (cost < bestCost) {
      best = ranges;
      bestCost = cost;
    }
    if (ranges.length <= 1) return best;
  }
}

/** The planned ranges' bytes, fetching only what the footer read didn't already cover. */
export async function fetchProjectedRanges(plan: ParquetProjection, fetchRange: FetchRange): Promise<ArrayBuffer[]> {
  const tail = plan.footer.tail;
  return Promise.all(plan.ranges.map(async ({ start, end }) => {
    if (!tail || end <= tail.start) return fetchRange(start, end);
    if (start >= tail.start) return tail.bytes.slice(start - tail.start, end - tail.start);
    const head = new Uint8Array(await fetchRange(start, tail.start));
    const out = new Uint8Array(end - start);
    out.set(head);
    out.set(new Uint8Array(tail.bytes, 0, end - tail.start), head.byteLength);
    return out.buffer;
  }));
}

/**
 * A standalone Parquet file holding only the planned column chunks, byte for
 * byte, under a footer rewritten for their new offsets. Statistics, page
 * indexes and bloom filters are dropped because they point into the original.
 */
export function assembleProjectedParquet(plan: ParquetProjection, rangeBytes: readonly ArrayBuffer[]): ArrayBuffer {
  if (rangeBytes.length !== plan.ranges.length) throw new Error("Projected Parquet ranges are incomplete");
  const { metadata } = plan.footer;
  const fields = topLevel(metadata.schema);
  const schema: SchemaElement[] = [{ ...metadata.schema[0], num_children: 0 }];
  let index = 1;
  for (const field of fields) {
    const subtree = [] as SchemaElement[];
    const take = (): void => {
      const element = metadata.schema[index++];
      subtree.push(element);
      for (let i = 0; i < (element.num_children ?? 0); i++) take();
    };
    take();
    if (!plan.columns.has(field.name)) continue;
    schema.push(...subtree);
    schema[0].num_children!++;
  }
  const writer = new ByteWriter();
  writer.appendUint32(MAGIC);
  const byGroup = new Map<number, Chunk[]>();
  for (const chunk of plan.chunks) byGroup.set(chunk.rowGroup, [...byGroup.get(chunk.rowGroup) ?? [], chunk]);
  const rowGroups = metadata.row_groups.map((group, rowGroup) => {
    const kept = byGroup.get(rowGroup) ?? [];
    const groupOffset = writer.offset;
    const columns = kept.map((chunk) => {
      const range = plan.ranges.findIndex((r) => r.start <= chunk.start && chunk.start + chunk.length <= r.end);
      if (range < 0) throw new Error("Projected Parquet chunk has no fetched range");
      const source = rangeBytes[range];
      if (source.byteLength !== plan.ranges[range].end - plan.ranges[range].start)
        throw new Error("Projected Parquet range has the wrong length");
      const at = chunk.start - plan.ranges[range].start;
      const delta = BigInt(writer.offset - chunk.start);
      const original = group.columns[chunk.column];
      const meta = original.meta_data!;
      const shiftedDictionary = meta.dictionary_page_offset !== undefined && Number(meta.dictionary_page_offset) >= 4
        ? meta.dictionary_page_offset + delta : undefined;
      const out = {
        file_offset: BigInt(writer.offset),
        meta_data: {
          type: meta.type,
          encodings: meta.encodings,
          path_in_schema: meta.path_in_schema,
          codec: meta.codec,
          num_values: meta.num_values,
          total_uncompressed_size: meta.total_uncompressed_size,
          total_compressed_size: meta.total_compressed_size,
          data_page_offset: meta.data_page_offset + delta,
          ...(shiftedDictionary !== undefined ? { dictionary_page_offset: shiftedDictionary } : {}),
          ...(meta.encoding_stats ? { encoding_stats: meta.encoding_stats } : {}),
        },
      };
      writer.appendBytes(new Uint8Array(source, at, chunk.length));
      return out;
    });
    return {
      columns,
      num_rows: group.num_rows,
      total_byte_size: columns.reduce((sum, chunk) => sum + chunk.meta_data.total_uncompressed_size, BigInt(0)),
      total_compressed_size: columns.reduce((sum, chunk) => sum + chunk.meta_data.total_compressed_size, BigInt(0)),
      file_offset: BigInt(groupOffset),
    };
  });
  writeMetadata(writer, {
    version: metadata.version,
    schema,
    num_rows: metadata.num_rows,
    row_groups: rowGroups,
    ...(metadata.created_by ? { created_by: metadata.created_by } : {}),
    metadata_length: 0,
  });
  writer.appendUint32(MAGIC);
  writer.finish();
  return writer.getBuffer();
}
