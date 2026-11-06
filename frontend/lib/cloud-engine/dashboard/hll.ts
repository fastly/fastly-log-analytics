// Byte-compatible with app/src/ingest/summary.rs `Hll` (fla `hll.py`):
// p=8, md5 hash, blob `[version, p, registers...]`.
const VERSION = 1;
const P = 8;
const M = 1 << P;
const WIDTH = 64 - P;

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

export class Hll {
  private readonly registers: Uint8Array;

  constructor(registers?: Uint8Array) {
    this.registers = registers ?? new Uint8Array(M);
  }

  static fromBytes(blob: Uint8Array): Hll | null {
    if (blob.length !== 2 + M || blob[0] !== VERSION || blob[1] !== P) {
      return null;
    }
    return new Hll(blob.slice(2));
  }

  clone(): Hll {
    return new Hll(this.registers.slice());
  }

  add(item: string) {
    const [hi, lo] = hashWords(item);
    const bucket = hi >>> 24;
    const high = hi & 0xffffff;
    const bits = high !== 0 ? 64 - Math.clz32(high) : 32 - Math.clz32(lo);
    const rank = bits <= 1 ? WIDTH : WIDTH + 1 - bits;
    if (rank > this.registers[bucket]) this.registers[bucket] = rank;
  }

  merge(other: Hll) {
    for (let i = 0; i < M; i++) {
      if (other.registers[i] > this.registers[i]) {
        this.registers[i] = other.registers[i];
      }
    }
  }

  toBytes(): Uint8Array {
    const out = new Uint8Array(2 + M);
    out[0] = VERSION;
    out[1] = P;
    out.set(this.registers, 2);
    return out;
  }

  count(): number {
    let sum = 0;
    let zeros = 0;
    for (const r of this.registers) {
      sum += 2 ** -r;
      if (r === 0) zeros++;
    }
    let estimate = ((0.7213 / (1 + 1.079 / M)) * M * M) / sum;
    if (estimate <= 2.5 * M && zeros > 0) estimate = M * Math.log(M / zeros);
    const two64 = 2 ** 64;
    if (estimate > two64 / 30) {
      estimate = -two64 * Math.log(1 - estimate / two64);
    }
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
