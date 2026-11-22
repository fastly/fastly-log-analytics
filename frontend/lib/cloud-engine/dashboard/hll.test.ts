import { describe, expect, it } from "vitest";
import { Hll, md5 } from "./hll";

const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

function sketch(items: string[]): Hll {
  const hll = new Hll();
  for (const item of items) hll.add(item);
  return hll;
}

const range = (n: number, f: (i: number) => string) =>
  Array.from({ length: n }, (_, i) => f(i));

describe("Hll", () => {
  // Shared with compute/cloud/tests/summary.rs `hll_matches_the_reference_vectors`.
  it("matches the Rust and Python reference vectors", () => {
    const one = sketch(["192.0.2.1"]);
    expect(hex(one.toBytes())).toBe("020c0001c5f6e48903");
    expect(one.count()).toBe(1);

    const few = sketch(range(20, (i) => `10.0.0.${i}`));
    expect(few.toBytes().length).toBe(85);
    expect(hex(md5(few.toBytes()))).toBe("39c35cb9e9b2f005a5373f90038ca841");
    expect(few.count()).toBe(20);

    const many = sketch(
      range(2000, (i) => `10.0.${Math.floor(i / 256)}.${i % 256}`),
    );
    const bytes = many.toBytes();
    expect(bytes.length).toBe(3 + 4096);
    expect(hex(bytes.slice(0, 12))).toBe("020c01030000010101000200");
    expect(hex(md5(bytes))).toBe("1c52cca58c4d6b0afa94b13423e57317");
    expect(many.count()).toBe(1977);
  });

  it("merges sparse and dense sketches in any order to the same bytes", () => {
    const parts = [10, 900, 300, 2000, 1].map((n, p) =>
      range(n, (i) => `${p}-${i}`),
    );
    const want = sketch(parts.flat()).toBytes();
    for (const order of [
      [0, 1, 2, 3, 4],
      [4, 3, 2, 1, 0],
      [3, 0, 4, 2, 1],
      [2, 1, 0, 4, 3],
    ]) {
      const got = new Hll();
      for (const i of order) {
        got.merge(Hll.fromBytes(sketch(parts[i]).toBytes())!);
      }
      expect(got.toBytes()).toEqual(want);
    }
    const crossed = sketch(parts[1]);
    crossed.merge(sketch(parts[2]));
    expect(crossed.toBytes()[2]).toBe(1);
    expect(crossed.toBytes()).toEqual(
      sketch([...parts[1], ...parts[2]]).toBytes(),
    );
  });

  it("counts small sets exactly and large ones within the error", () => {
    for (const n of [0, 1, 14, 177, 1000]) {
      expect(sketch(range(n, (i) => `7-${i}`)).count()).toBe(n);
    }
    const big = sketch(range(20_000, (i) => `9-${i}`)).count();
    expect(Math.abs(big - 20_000) / 20_000).toBeLessThan(0.05);
  });

  it("clones independently and rejects foreign or malformed blobs", () => {
    const a = sketch(["x", "y"]);
    const b = a.clone();
    b.add("z");
    expect(a.count()).toBe(2);
    expect(b.count()).toBe(3);

    const v1 = new Uint8Array(2 + 256);
    v1[0] = 1;
    v1[1] = 8;
    expect(Hll.fromBytes(v1)).toBeNull();
    const bytes = a.toBytes();
    expect(Hll.fromBytes(bytes.slice(0, -1))).toBeNull();
    expect(Hll.fromBytes(new Uint8Array([2, 12, 1, 0]))).toBeNull();
    // One entry past idx25's 25 bits: u32::MAX - 62 as a varint.
    expect(
      Hll.fromBytes(
        new Uint8Array([2, 12, 0, 1, 0xc1, 0xff, 0xff, 0xff, 0x0f]),
      ),
    ).toBeNull();
    expect(Hll.fromBytes(bytes)?.toBytes()).toEqual(bytes);
  });
});
