import type {
  FileRollup,
  FileRollupData,
  MinuteBucket,
  Scalar,
} from "./engine";

/**
 * Binary per-file rollup encoding. Each field stores its values as a
 * dictionary (type tags, numbers, UTF-8 strings) beside a `Uint32Array` of
 * counts, column by column, then the whole payload is gzipped when
 * `CompressionStream` exists. Decoding rebuilds the exact `FileRollup`.
 */
const MAGIC = [0x46, 0x4c, 0x52] as const; // "FLR"
const FORMAT = 2;
const FLAG_GZIP = 1;
const HEADER_BYTES = 5;

const TAG_STRING = 0;
const TAG_NUMBER = 1;
const TAG_FALSE = 2;
const TAG_TRUE = 3;

const BUCKET_FIELDS = [
  "t",
  "requests",
  "bytes",
  "s4xx",
  "s5xx",
  "hits",
  "cacheRows",
] as const satisfies readonly (keyof MinuteBucket)[];

class Writer {
  private chunks: Uint8Array[] = [];
  private scratch = new DataView(new ArrayBuffer(8));
  private readonly encoder = new TextEncoder();
  length = 0;

  bytes(chunk: Uint8Array): void {
    this.chunks.push(chunk);
    this.length += chunk.byteLength;
  }

  u8(value: number): void {
    this.bytes(Uint8Array.of(value));
  }

  u32(value: number): void {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new RangeError(`Rollup value ${value} does not fit in u32`);
    }
    this.scratch.setUint32(0, value, true);
    this.bytes(new Uint8Array(this.scratch.buffer.slice(0, 4)));
  }

  f64(value: number): void {
    this.scratch.setFloat64(0, value, true);
    this.bytes(new Uint8Array(this.scratch.buffer.slice(0, 8)));
  }

  str(value: string): void {
    const encoded = this.encoder.encode(value);
    this.u32(encoded.byteLength);
    this.bytes(encoded);
  }

  u32s(values: ArrayLike<number>): void {
    const out = new Uint32Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const value = values[i];
      if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
        throw new RangeError(`Rollup count ${value} does not fit in u32`);
      }
      out[i] = value;
    }
    this.bytes(littleEndian(new Uint8Array(out.buffer), 4));
  }

  f64s(values: number[]): void {
    this.bytes(
      littleEndian(new Uint8Array(Float64Array.from(values).buffer), 8),
    );
  }

  finish(): Uint8Array {
    const out = new Uint8Array(this.length);
    let at = 0;
    for (const chunk of this.chunks) {
      out.set(chunk, at);
      at += chunk.byteLength;
    }
    return out;
  }
}

const LITTLE_ENDIAN = new Uint8Array(Uint16Array.of(1).buffer)[0] === 1;

/** Typed arrays use host byte order; the format is little-endian. */
function littleEndian(bytes: Uint8Array, width: number): Uint8Array {
  if (LITTLE_ENDIAN) return bytes;
  for (let i = 0; i < bytes.length; i += width) {
    bytes.subarray(i, i + width).reverse();
  }
  return bytes;
}

class Reader {
  private at = 0;
  private readonly view: DataView;
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });

  constructor(private readonly data: Uint8Array) {
    this.view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  take(length: number): Uint8Array {
    if (length < 0 || this.at + length > this.data.byteLength) {
      throw new RangeError("Truncated rollup");
    }
    const out = this.data.subarray(this.at, this.at + length);
    this.at += length;
    return out;
  }

  u8(): number {
    return this.take(1)[0];
  }

  u32(): number {
    const at = this.at;
    this.take(4);
    return this.view.getUint32(at, true);
  }

  f64(): number {
    const at = this.at;
    this.take(8);
    return this.view.getFloat64(at, true);
  }

  str(): string {
    return this.decoder.decode(this.take(this.u32()));
  }

  u32s(count: number): number[] {
    const at = this.at;
    this.take(count * 4);
    const out = new Array<number>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getUint32(at + i * 4, true);
    }
    return out;
  }

  f64s(count: number): number[] {
    const at = this.at;
    this.take(count * 8);
    const out = new Array<number>(count);
    for (let i = 0; i < count; i++) {
      out[i] = this.view.getFloat64(at + i * 8, true);
    }
    return out;
  }

  done(): boolean {
    return this.at === this.data.byteLength;
  }
}

function writeData(w: Writer, data: FileRollupData): void {
  w.f64(data.minTs);
  w.f64(data.maxTs);
  w.f64(data.totalRequests);
  w.u8(data.hasBytes ? 1 : 0);
  w.u32(data.buckets.length);
  for (const field of BUCKET_FIELDS) {
    w.f64s(data.buckets.map((bucket) => bucket[field]));
  }
  const nonNull = Object.entries(data.nonNull);
  w.u32(nonNull.length);
  for (const [field, count] of nonNull) {
    w.str(field);
    w.f64(count);
  }
  const fields = Object.entries(data.counts);
  w.u32(fields.length);
  for (const [field, entries] of fields) {
    w.str(field);
    w.u32(entries.length);
    const tags = new Uint8Array(entries.length);
    const counts = new Array<number>(entries.length);
    const numbers: number[] = [];
    const strings: Uint8Array[] = [];
    const lengths: number[] = [];
    const encoder = new TextEncoder();
    entries.forEach(([value, count], i) => {
      counts[i] = count;
      if (typeof value === "string") {
        tags[i] = TAG_STRING;
        const encoded = encoder.encode(value);
        strings.push(encoded);
        lengths.push(encoded.byteLength);
      } else if (typeof value === "number") {
        tags[i] = TAG_NUMBER;
        numbers.push(value);
      } else {
        tags[i] = value ? TAG_TRUE : TAG_FALSE;
      }
    });
    w.bytes(tags);
    w.u32s(counts);
    w.f64s(numbers);
    w.u32s(lengths);
    for (const encoded of strings) w.bytes(encoded);
  }
}

function readData(r: Reader): FileRollupData {
  const minTs = r.f64();
  const maxTs = r.f64();
  const totalRequests = r.f64();
  const hasBytes = r.u8() === 1;
  const bucketCount = r.u32();
  const columns = BUCKET_FIELDS.map(() => r.f64s(bucketCount));
  const buckets: MinuteBucket[] = [];
  for (let i = 0; i < bucketCount; i++) {
    const bucket = {} as MinuteBucket;
    BUCKET_FIELDS.forEach((field, column) => {
      bucket[field] = columns[column][i];
    });
    buckets.push(bucket);
  }
  const nonNull: Record<string, number> = {};
  for (let n = r.u32(); n > 0; n--) {
    const field = r.str();
    nonNull[field] = r.f64();
  }
  const counts: Record<string, [Scalar, number][]> = {};
  const decoder = new TextDecoder("utf-8", { fatal: true });
  for (let n = r.u32(); n > 0; n--) {
    const field = r.str();
    const size = r.u32();
    const tags = Array.from({ length: size }, () => r.u8());
    let numberCount = 0;
    let stringCount = 0;
    for (const tag of tags) {
      if (tag === TAG_NUMBER) numberCount++;
      else if (tag === TAG_STRING) stringCount++;
      else if (tag !== TAG_TRUE && tag !== TAG_FALSE) {
        throw new RangeError(`Unknown rollup value tag ${tag}`);
      }
    }
    const fieldCounts = r.u32s(size);
    const numbers = r.f64s(numberCount);
    const lengths = r.u32s(stringCount);
    const strings = lengths.map((length) => decoder.decode(r.take(length)));
    let ni = 0;
    let si = 0;
    counts[field] = tags.map((tag, i) => {
      const value: Scalar =
        tag === TAG_STRING
          ? strings[si++]
          : tag === TAG_NUMBER
            ? numbers[ni++]
            : tag === TAG_TRUE;
      return [value, fieldCounts[i]];
    });
  }
  return { minTs, maxTs, totalRequests, hasBytes, buckets, counts, nonNull };
}

async function pipe(
  bytes: Uint8Array,
  stream: CompressionStream | DecompressionStream,
): Promise<Uint8Array> {
  const piped = new Blob([bytes as BlobPart]).stream().pipeThrough(stream);
  return new Uint8Array(await new Response(piped).arrayBuffer());
}

export async function encodeRollup(rollup: FileRollup): Promise<Uint8Array> {
  const w = new Writer();
  w.str(rollup.key);
  w.f64(rollup.rawRows);
  w.f64(rollup.byteLength);
  writeData(w, rollup);
  w.u8(rollup.edgeRollup ? 1 : 0);
  if (rollup.edgeRollup) writeData(w, rollup.edgeRollup);
  let payload = w.finish();
  let flags = 0;
  if (typeof CompressionStream !== "undefined") {
    payload = await pipe(payload, new CompressionStream("gzip"));
    flags |= FLAG_GZIP;
  }
  const out = new Uint8Array(HEADER_BYTES + payload.byteLength);
  out.set([...MAGIC, FORMAT, flags]);
  out.set(payload, HEADER_BYTES);
  return out;
}

/** Returns null for bytes in any other format or a damaged payload. */
export async function decodeRollup(
  bytes: Uint8Array,
): Promise<FileRollup | null> {
  if (
    bytes.byteLength < HEADER_BYTES ||
    MAGIC.some((b, i) => bytes[i] !== b) ||
    bytes[3] !== FORMAT
  ) {
    return null;
  }
  try {
    let payload = bytes.subarray(HEADER_BYTES);
    if (bytes[4] & FLAG_GZIP) {
      if (typeof DecompressionStream === "undefined") return null;
      payload = await pipe(payload, new DecompressionStream("gzip"));
    }
    const r = new Reader(payload);
    const key = r.str();
    const rawRows = r.f64();
    const byteLength = r.f64();
    const data = readData(r);
    const edgeRollup = r.u8() === 1 ? readData(r) : undefined;
    if (!r.done()) return null;
    return {
      version: 1,
      key,
      rawRows,
      byteLength,
      ...data,
      ...(edgeRollup ? { edgeRollup } : {}),
    };
  } catch {
    return null;
  }
}

// Rough V8 sizes: a two-slot tuple array, a heap number, a string header plus
// two bytes per code unit (overestimates one-byte strings, which is safe).
const TUPLE_BYTES = 56;
const NUMBER_BYTES = 16;
const STRING_BYTES = 24;
const BUCKET_BYTES = 96;
const FIELD_BYTES = 96;
const BASE_BYTES = 512;

function estimateDataBytes(data: FileRollupData): number {
  let bytes = BASE_BYTES + data.buckets.length * BUCKET_BYTES;
  bytes += Object.keys(data.nonNull).length * FIELD_BYTES;
  for (const entries of Object.values(data.counts)) {
    bytes += FIELD_BYTES + entries.length * TUPLE_BYTES;
    for (const [value] of entries) {
      if (typeof value === "string") bytes += STRING_BYTES + value.length * 2;
      else if (typeof value === "number") bytes += NUMBER_BYTES;
    }
  }
  return bytes;
}

/** Estimated heap bytes a decoded rollup holds. */
export function estimateRollupBytes(rollup: FileRollup): number {
  return (
    estimateDataBytes(rollup) +
    rollup.key.length * 2 +
    (rollup.edgeRollup ? estimateDataBytes(rollup.edgeRollup) : 0)
  );
}

export const encodeRollupBinary = encodeRollup;
export const decodeRollupBinary = decodeRollup;
