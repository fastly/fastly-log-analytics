import { describe, expect, it } from "vitest";
import { Histogram } from "./histogram";

const fromHex = (hex: string) =>
  Uint8Array.from(hex.match(/../g)!.map((b) => parseInt(b, 16)));
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

// Shared with compute/cloud/src/ingest/hist.rs `matches_the_reference_vector`:
// 50, 100, 150, 1000, 1000, 70_000_000 µs.
const VECTOR = "0101016480f6e042000103011602";

describe("Histogram", () => {
  it("reads the Rust reference vector", () => {
    const h = Histogram.fromBytes(fromHex(VECTOR))!;
    expect(h.count()).toBe(6);
    expect(h.min).toBe(50);
    expect(h.max).toBe(70_000_000);
    expect(h.quantile(0.5)).toBe(Math.sqrt(137 * 152));
    expect(h.quantile(0)).toBe(50);
    expect(h.quantile(1)).toBe(70_000_000);
    expect(hex(h.toBytes()!)).toBe(VECTOR);
  });

  it("merges by adding counts and widening the range", () => {
    const a = Histogram.fromBytes(fromHex(VECTOR))!;
    const b = Histogram.fromBytes(fromHex(VECTOR))!;
    a.merge(b);
    expect(a.count()).toBe(12);
    expect(hex(a.toBytes()!)).toBe("0102026480f6e042000203021604");
    const empty = new Histogram();
    expect(empty.quantile(0.5)).toBeNull();
    expect(empty.toBytes()).toBeNull();
    empty.merge(b);
    expect(hex(empty.toBytes()!)).toBe(VECTOR);
  });

  it("rejects malformed blobs", () => {
    const bytes = fromHex(VECTOR);
    expect(Histogram.fromBytes(bytes.slice(0, -1))).toBeNull();
    expect(Histogram.fromBytes(new Uint8Array([1, 0, 0, 0, 0]))).toBeNull();
    expect(Histogram.fromBytes(new Uint8Array([2, 1, 0, 0, 0]))).toBeNull();
    // Buckets out of order, past the last bucket, or with a zero count.
    expect(Histogram.fromBytes(fromHex("010000020216011501"))).toBeNull();
    expect(Histogram.fromBytes(fromHex("01000002028001"))).toBeNull();
    expect(Histogram.fromBytes(fromHex("01000002021600"))).toBeNull();
  });
});
