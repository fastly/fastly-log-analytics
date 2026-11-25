import { describe, expect, it } from "vitest";
import { Histogram } from "./histogram";

const fromHex = (hex: string) =>
  Uint8Array.from(hex.match(/../g)!.map((b) => parseInt(b, 16)));
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

// Shared with compute/cloud/src/ingest/hist.rs `matches_the_reference_vector`:
// 50, 100, 150, 1000, 1000, 70_000_000 µs.
const VECTOR = "0201016480f6e042000114017402";
const LEGACY_V1_VECTOR = "0101016480f6e042000103011602";

describe("Histogram", () => {
  it("reads the Rust reference vector and legacy v1 blobs", () => {
    const h = Histogram.fromBytes(fromHex(VECTOR))!;
    expect(h.count()).toBe(6);
    expect(h.min).toBe(50);
    expect(h.max).toBe(70_000_000);
    expect(h.quantile(0.5)).toBe(Math.sqrt(149 * 152));
    expect(h.quantile(0)).toBe(50);
    expect(h.quantile(1)).toBe(70_000_000);
    expect(hex(h.toBytes()!)).toBe(VECTOR);
    const v1 = Histogram.fromBytes(fromHex(LEGACY_V1_VECTOR))!;
    expect(v1.count()).toBe(6);
    expect(v1.min).toBe(50);
    expect(v1.max).toBe(70_000_000);
    // Rust clamps min and max to ±(2^52 - 1), so their zigzag fits a JS number.
    const wild = "020101fdffffffffffff0ffeffffffffffff0f";
    const w = Histogram.fromBytes(fromHex(wild))!;
    expect([w.min, w.max]).toEqual([-(2 ** 52 - 1), 2 ** 52 - 1]);
    expect(hex(w.toBytes()!)).toBe(wild);
    // One past it would lose precision, so it is rejected.
    expect(Histogram.fromBytes(fromHex("020101008080808080808010"))).toBeNull();
  });

  it("merges by adding counts and widening the range", () => {
    const a = Histogram.fromBytes(fromHex(VECTOR))!;
    const b = Histogram.fromBytes(fromHex(VECTOR))!;
    a.merge(b);
    expect(a.count()).toBe(12);
    expect(hex(a.toBytes()!)).toBe("0202026480f6e042000214027404");
    const empty = new Histogram();
    expect(empty.quantile(0.5)).toBeNull();
    expect(empty.toBytes()).toBeNull();
    empty.merge(b);
    expect(hex(empty.toBytes()!)).toBe(VECTOR);
  });

  it("rejects malformed blobs", () => {
    const bytes = fromHex(VECTOR);
    expect(Histogram.fromBytes(bytes.slice(0, -1))).toBeNull();
    expect(Histogram.fromBytes(new Uint8Array([2, 0, 0, 0, 0]))).toBeNull();
    expect(Histogram.fromBytes(new Uint8Array([3, 1, 0, 0, 0]))).toBeNull();
    // Buckets out of order, past the last bucket, or with a zero count.
    expect(Histogram.fromBytes(fromHex("020000020216011501"))).toBeNull();
    expect(Histogram.fromBytes(fromHex("0200000202a00501"))).toBeNull();
    expect(Histogram.fromBytes(fromHex("02000002021600"))).toBeNull();
  });

  it("returns exact min/max endpoints and keeps ~1% quantile precision in v2", () => {
    const h = new Histogram();
    for (let i = 0; i < 15; i++) h.add(2_500_000);
    h.add(10_111_570);
    expect(h.quantile(0)).toBe(2_500_000);
    expect(h.quantile(0.99)).toBe(10_111_570);
    expect(h.quantile(1)).toBe(10_111_570);

    const spread = new Histogram();
    const vals = Array.from({ length: 10_000 }, (_, i) => 1_000 + (i + 1) * 37);
    for (const v of vals) spread.add(v);
    for (const q of [0.1, 0.5, 0.95, 0.99]) {
      const exact = vals[Math.max(1, Math.ceil(q * vals.length)) - 1];
      const got = spread.quantile(q)!;
      expect(Math.abs(got - exact) / exact).toBeLessThan(0.015);
    }
    const bytes = spread.toBytes()!;
    expect(bytes[0]).toBe(2);
    const decoded = Histogram.fromBytes(bytes)!;
    expect(decoded.count()).toBe(10_000);
    expect(decoded.quantile(0.95)).toBe(spread.quantile(0.95));
  });
});
