// Byte-compatible with compute/cloud/src/ingest/hll.rs `Hll`: HyperLogLog++ at p=12, sparse
// (precision 25) up to 1024 entries and dense above, md5 hash as in fla `hll.py`.
// Blob: `[2, 12, 0, varint n, varint entry deltas...]` or `[2, 12, 1, 4096 registers]`.
const VERSION = 2;
const P = 12;
const M = 1 << P;
const SP = 25;
const SPARSE_MAX = 1024;
const LOW = 64 - SP;
const KIND_SPARSE = 0;
const KIND_DENSE = 1;

const S = [
  7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5,
  9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11,
  16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15,
  21,
];
const K = Array.from(
  { length: 64 },
  (_, i) => Math.floor(Math.abs(Math.sin(i + 1)) * 2 ** 32) >>> 0,
);

export function md5(input: Uint8Array): Uint8Array {
  const bitLength = input.length * 8;
  const padded = new Uint8Array((((input.length + 8) >> 6) + 1) * 64);
  padded.set(input);
  padded[input.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, bitLength >>> 0, true);
  view.setUint32(padded.length - 4, Math.floor(bitLength / 2 ** 32), true);
  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;
  const words = new Uint32Array(16);
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++)
      words[i] = view.getUint32(offset + i * 4, true);
    let a = a0;
    let b = b0;
    let c = c0;
    let d = d0;
    for (let i = 0; i < 64; i++) {
      let f: number;
      let g: number;
      if (i < 16) {
        f = (b & c) | (~b & d);
        g = i;
      } else if (i < 32) {
        f = (d & b) | (~d & c);
        g = (5 * i + 1) & 15;
      } else if (i < 48) {
        f = b ^ c ^ d;
        g = (3 * i + 5) & 15;
      } else {
        f = c ^ (b | ~d);
        g = (7 * i) & 15;
      }
      const sum = (a + f + K[i] + words[g]) >>> 0;
      a = d;
      d = c;
      c = b;
      b = (b + ((sum << S[i]) | (sum >>> (32 - S[i])))) >>> 0;
    }
    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }
  const out = new Uint8Array(16);
  const outView = new DataView(out.buffer);
  [a0, b0, c0, d0].forEach((word, i) => outView.setUint32(i * 4, word, true));
  return out;
}

const encoder = new TextEncoder();

function hashWords(item: string): [number, number] {
  const view = new DataView(md5(encoder.encode(item)).buffer);
  return [view.getUint32(4, true), view.getUint32(0, true)];
}

export function hllHash(item: string): bigint {
  const [hi, lo] = hashWords(item);
  return (BigInt(hi) << BigInt(32)) | BigInt(lo);
}

/** The sparse entry `idx25 << 6 | rank` of an item; always below 2^31. */
function entryOf(item: string): number {
  const [hi, lo] = hashWords(item);
  const idx = hi >>> (LOW - 32);
  const high = hi & ((1 << (LOW - 32)) - 1);
  const bits = high !== 0 ? 64 - Math.clz32(high) : 32 - Math.clz32(lo);
  const rank = bits <= 1 ? LOW : LOW + 1 - bits;
  return (idx << 6) | rank;
}

const idx25 = (e: number) => e >>> 6;

function denseRegister(e: number): number {
  return e >>> (6 + SP - P);
}

function denseRank(e: number): number {
  const w = idx25(e) & ((1 << (SP - P)) - 1);
  return w !== 0 ? SP - P + 1 - (32 - Math.clz32(w)) : SP - P + (e & 63);
}

function lowerBound(entries: number[], idx: number): number {
  let lo = 0;
  let hi = entries.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (idx25(entries[mid]) < idx) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function readVarint(blob: Uint8Array, at: { i: number }): number | null {
  let value = 0;
  for (let scale = 1; at.i < blob.length; scale *= 128) {
    const b = blob[at.i++];
    value += (b & 0x7f) * scale;
    if (value > 0xffffffff) return null;
    if ((b & 0x80) === 0) return value;
  }
  return null;
}

function writeVarint(out: number[], value: number) {
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80);
    value = Math.floor(value / 128);
  }
  out.push(value);
}

export class Hll {
  /** Sorted entries, one per idx25 keeping the highest; null once dense. */
  private sparse: number[] | null;
  private registers: Uint8Array | null;

  constructor() {
    this.sparse = [];
    this.registers = null;
  }

  static fromBytes(blob: Uint8Array): Hll | null {
    if (blob.length < 4 || blob[0] !== VERSION || blob[1] !== P) return null;
    const hll = new Hll();
    if (blob[2] === KIND_DENSE) {
      if (blob.length !== 3 + M) return null;
      hll.sparse = null;
      hll.registers = blob.slice(3);
      return hll;
    }
    if (blob[2] !== KIND_SPARSE) return null;
    const at = { i: 3 };
    const n = readVarint(blob, at);
    if (n === null || n > SPARSE_MAX) return null;
    const entries: number[] = [];
    let prev = 0;
    for (let i = 0; i < n; i++) {
      const delta = readVarint(blob, at);
      if (delta === null || (i > 0 && delta === 0)) return null;
      prev += delta;
      if (prev >= 2 ** (SP + 6)) return null;
      const rank = prev & 63;
      if (rank === 0 || rank > LOW) return null;
      if (i > 0 && idx25(entries[i - 1]) === idx25(prev)) return null;
      entries.push(prev);
    }
    if (at.i !== blob.length) return null;
    hll.sparse = entries;
    return hll;
  }

  clone(): Hll {
    const copy = new Hll();
    copy.sparse = this.sparse ? this.sparse.slice() : null;
    copy.registers = this.registers ? this.registers.slice() : null;
    return copy;
  }

  add(item: string) {
    this.addEntry(entryOf(item));
  }

  private addEntry(e: number) {
    if (this.registers) {
      const r = denseRegister(e);
      this.registers[r] = Math.max(this.registers[r], denseRank(e));
      return;
    }
    const entries = this.sparse!;
    const at = lowerBound(entries, idx25(e));
    if (at < entries.length && idx25(entries[at]) === idx25(e)) {
      entries[at] = Math.max(entries[at], e);
    } else {
      entries.splice(at, 0, e);
      this.densifyIfFull();
    }
  }

  private densifyIfFull() {
    if (this.sparse && this.sparse.length > SPARSE_MAX) {
      const registers = new Uint8Array(M);
      for (const e of this.sparse) {
        const r = denseRegister(e);
        registers[r] = Math.max(registers[r], denseRank(e));
      }
      this.sparse = null;
      this.registers = registers;
    }
  }

  merge(other: Hll) {
    if (other.registers) {
      if (this.sparse) {
        const mine = this.sparse;
        this.sparse = null;
        this.registers = other.registers.slice();
        for (const e of mine) this.addEntry(e);
        return;
      }
      const registers = this.registers!;
      for (let i = 0; i < M; i++) {
        if (other.registers[i] > registers[i])
          registers[i] = other.registers[i];
      }
      return;
    }
    const theirs = other.sparse!;
    if (this.registers) {
      for (const e of theirs) this.addEntry(e);
      return;
    }
    const mine = this.sparse!;
    const out: number[] = [];
    let a = 0;
    let b = 0;
    while (a < mine.length || b < theirs.length) {
      const x = mine[a];
      const y = theirs[b];
      if (b === theirs.length || (a < mine.length && idx25(x) < idx25(y))) {
        out.push(x);
        a++;
      } else if (a === mine.length || idx25(y) < idx25(x)) {
        out.push(y);
        b++;
      } else {
        out.push(Math.max(x, y));
        a++;
        b++;
      }
    }
    this.sparse = out;
    this.densifyIfFull();
  }

  toBytes(): Uint8Array {
    if (this.registers) {
      const out = new Uint8Array(3 + M);
      out.set([VERSION, P, KIND_DENSE]);
      out.set(this.registers, 3);
      return out;
    }
    const entries = this.sparse!;
    const out = [VERSION, P, KIND_SPARSE];
    writeVarint(out, entries.length);
    let prev = 0;
    for (const e of entries) {
      writeVarint(out, e - prev);
      prev = e;
    }
    return Uint8Array.from(out);
  }

  count(): number {
    if (this.sparse) {
      const m = 2 ** SP;
      return roundHalfEven(m * Math.log(m / (m - this.sparse.length)));
    }
    let sum = 0;
    let zeros = 0;
    for (const r of this.registers!) {
      sum += 2 ** -r;
      if (r === 0) zeros++;
    }
    let estimate = ((0.7213 / (1 + 1.079 / M)) * M * M) / sum;
    if (estimate <= 2.5 * M && zeros > 0) estimate = M * Math.log(M / zeros);
    return roundHalfEven(estimate);
  }
}

function roundHalfEven(value: number): number {
  const floor = Math.floor(value);
  const diff = value - floor;
  if (diff > 0.5) return floor + 1;
  if (diff < 0.5) return floor;
  return floor % 2 === 0 ? floor : floor + 1;
}
