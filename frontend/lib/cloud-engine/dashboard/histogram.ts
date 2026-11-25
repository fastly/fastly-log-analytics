// Byte-compatible with compute/cloud/src/ingest/hist.rs `Hist`: 128 log-spaced buckets over
// [100 µs, 60 s) plus underflow, overflow and the exact min and max, in integer microseconds.
// Blob: `[1, varint under, varint over, zigzag min, zigzag max, (u8 bucket, varint count)...]`.
const VERSION = 1;
const BUCKETS = 128;
const LIMIT = 2 ** 52 - 1;

/** Bucket `i` holds `[BOUNDS[i], BOUNDS[i + 1])` µs. */
export const BOUNDS: readonly number[] = [
  100, 111, 123, 137, 152, 168, 187, 207, 230, 255, 283, 314, 348, 386, 429,
  475, 528, 585, 649, 721, 800, 887, 984, 1092, 1212, 1344, 1492, 1655, 1836,
  2038, 2261, 2508, 2783, 3088, 3426, 3802, 4218, 4680, 5193, 5761, 6393, 7093,
  7870, 8732, 9688, 10749, 11927, 13233, 14683, 16291, 18076, 20055, 22252,
  24690, 27394, 30395, 33724, 37418, 41517, 46065, 51110, 56709, 62920, 69813,
  77460, 85944, 95358, 105804, 117393, 130252, 144520, 160350, 177914, 197402,
  219025, 243016, 269636, 299171, 331941, 368301, 408643, 453405, 503070,
  558174, 619315, 687153, 762422, 845935, 938597, 1041408, 1155480, 1282048,
  1422480, 1578294, 1751176, 1942995, 2155825, 2391967, 2653976, 2944685,
  3267237, 3625121, 4022206, 4462786, 4951627, 5494013, 6095811, 6763528,
  7504385, 8326393, 9238442, 10250393, 11373191, 12618977, 14001223, 15534875,
  17236519, 19124557, 21219404, 23543715, 26122624, 28984019, 32158843,
  35681427, 39589865, 43926422, 48737993, 54076608, 60000000,
];

function readVarint(blob: Uint8Array, at: { i: number }): number | null {
  let value = 0;
  for (let scale = 1; at.i < blob.length; scale *= 128) {
    const b = blob[at.i++];
    value += (b & 0x7f) * scale;
    // Past 2^53 a number loses precision; Rust keeps zigzag min/max below it.
    if (value > Number.MAX_SAFE_INTEGER) return null;
    if ((b & 0x80) === 0) return value;
  }
  return null;
}

function writeVarint(out: number[], value: number) {
  while (value >= 0x80) {
    out.push((value % 128) | 0x80);
    value = Math.floor(value / 128);
  }
  out.push(value);
}

const unzigzag = (v: number) => (v % 2 === 1 ? -(v + 1) / 2 : v / 2);
const zigzag = (v: number) => (v < 0 ? -2 * v - 1 : 2 * v);

export class Histogram {
  under = 0;
  over = 0;
  min = Infinity;
  max = -Infinity;
  /** Count per bucket index. */
  readonly buckets = new Float64Array(BUCKETS);

  /** A well-formed, non-empty blob, or null. */
  static fromBytes(blob: Uint8Array): Histogram | null {
    if (blob[0] !== VERSION) return null;
    const at = { i: 1 };
    const under = readVarint(blob, at);
    if (under === null) return null;
    const over = readVarint(blob, at);
    if (over === null) return null;
    const min = readVarint(blob, at);
    if (min === null) return null;
    const max = readVarint(blob, at);
    if (max === null) return null;
    const h = new Histogram();
    h.under = under;
    h.over = over;
    h.min = unzigzag(min);
    h.max = unzigzag(max);
    let last = -1;
    while (at.i < blob.length) {
      const s = blob[at.i++];
      const c = readVarint(blob, at);
      if (c === null || c === 0 || s >= BUCKETS || s <= last) return null;
      h.buckets[s] = c;
      last = s;
    }
    return (last < 0 && under === 0 && over === 0) || h.min > h.max ? null : h;
  }

  /** Adds one value as hist.rs `Hist::add`: its slot from the value, min/max clamped to ±(2^52 - 1). */
  add(v: number) {
    if (v < BOUNDS[0]) this.under++;
    else if (v >= BOUNDS[BUCKETS]) this.over++;
    else {
      let lo = 0;
      let hi = BUCKETS;
      while (hi - lo > 1) {
        const mid = (lo + hi) >> 1;
        if (BOUNDS[mid] <= v) lo = mid;
        else hi = mid;
      }
      this.buckets[lo]++;
    }
    const clamped = Math.max(-LIMIT, Math.min(LIMIT, v));
    this.min = Math.min(this.min, clamped);
    this.max = Math.max(this.max, clamped);
  }

  isEmpty(): boolean {
    if (this.under > 0 || this.over > 0) return false;
    for (let i = 0; i < BUCKETS; i++) if (this.buckets[i] !== 0) return false;
    return true;
  }

  count(): number {
    let sum = this.under + this.over;
    for (let i = 0; i < BUCKETS; i++) sum += this.buckets[i];
    return sum;
  }

  merge(other: Histogram) {
    if (other.isEmpty()) return;
    this.under += other.under;
    this.over += other.over;
    this.min = Math.min(this.min, other.min);
    this.max = Math.max(this.max, other.max);
    for (let i = 0; i < BUCKETS; i++) this.buckets[i] += other.buckets[i];
  }

  /** Null when empty: an empty histogram is written as null. */
  toBytes(): Uint8Array | null {
    if (this.isEmpty()) return null;
    const out = [VERSION];
    writeVarint(out, this.under);
    writeVarint(out, this.over);
    writeVarint(out, zigzag(this.min));
    writeVarint(out, zigzag(this.max));
    this.buckets.forEach((c, s) => {
      if (c > 0) {
        out.push(s);
        writeVarint(out, c);
      }
    });
    return Uint8Array.from(out);
  }

  /**
   * The `q` quantile (nearest rank), read at its bucket's geometric middle and clamped to
   * [min, max]; underflow reads as min and overflow as max.
   */
  quantile(q: number): number | null {
    const n = this.count();
    if (n === 0) return null;
    const rank = Math.min(
      n,
      Math.max(1, Math.ceil(Math.min(1, Math.max(0, q)) * n)),
    );
    let seen = this.under;
    if (rank <= seen) return this.min;
    for (let s = 0; s < BUCKETS; s++) {
      seen += this.buckets[s];
      if (this.buckets[s] > 0 && rank <= seen) {
        const mid = Math.sqrt(BOUNDS[s] * BOUNDS[s + 1]);
        return Math.min(this.max, Math.max(this.min, mid));
      }
    }
    return this.max;
  }
}
