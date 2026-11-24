import { describe, expect, it } from "vitest";
import {
  KV_BINARY_GIB,
  kvStorageGibMonthsFromHourlyMax,
  priceCdnEgress,
  priceCdnRequests,
  priceComputeRequests,
  priceComputeVcpuMs,
  priceFosClassA,
  priceFosClassB,
  priceKvClassA,
  priceKvClassB,
  priceKvStorageGb,
  priceSecrets,
  priceStorageGb,
} from "./pricing";

describe("Fastly tiered monthly pricing", () => {
  it("applies free tiers across all products", () => {
    expect(priceStorageGb(5)).toBe(0);
    expect(priceFosClassA(1_000)).toBe(0);
    expect(priceFosClassB(10_000)).toBe(0);
    expect(priceCdnEgress(100)).toBe(0);
    expect(priceCdnRequests(1_000_000)).toBe(0);
    expect(priceComputeRequests(10_000_000)).toBe(0);
    expect(priceComputeVcpuMs(100_000_000)).toBe(0);
    expect(priceKvClassA(100_000)).toBe(0);
    expect(priceKvClassB(1_000_000)).toBe(0);
    expect(priceKvStorageGb(1)).toBe(0);
    expect(priceKvStorageGb(3)).toBeCloseTo(1.0, 6);
    expect(kvStorageGibMonthsFromHourlyMax(Array.from({ length: 720 }, () => 2 * KV_BINARY_GIB))).toBeCloseTo(2, 6);
    expect(priceSecrets(10)).toBe(0);
  });

  it("steps through progressive volume tiers", () => {
    // Storage: 5 free, 49,995 @ 0.02, 10,000 @ 0.018
    expect(priceStorageGb(60_000)).toBeCloseTo(49_995 * 0.02 + 10_000 * 0.018, 6);
    // CDN egress NA: 100 free, 9,900 @ 0.12, 5,000 @ 0.08
    expect(priceCdnEgress(15_000, 0.12)).toBeCloseTo(9_900 * 0.12 + 5_000 * 0.08, 6);
    // Compute requests: 10M free, 490M @ 0.50, 100M @ 0.49
    expect(priceComputeRequests(600_000_000)).toBeCloseTo(490 * 0.5 + 100 * 0.49, 6);
    // Compute vCPU: 100M free, 4,900M @ 0.05, 1,000M @ 0.049
    expect(priceComputeVcpuMs(6_000_000_000)).toBeCloseTo(4_900 * 0.05 + 1_000 * 0.049, 6);
    // KV Class A: 100k free, 900k @ 6.50/M, 9M @ 6.00/M, 5M @ 5.50/M
    expect(priceKvClassA(15_000_000)).toBeCloseTo(0.9 * 6.5 + 9 * 6.0 + 5 * 5.5, 6);
    // KV Class B: 1M free, 99M @ 0.55/M, 50M @ 0.50/M
    expect(priceKvClassB(150_000_000)).toBeCloseTo(99 * 0.55 + 50 * 0.5, 6);
    // Secret Store: 10 free, 90 @ 5.00, 50 @ 4.50
    expect(priceSecrets(150)).toBeCloseTo(90 * 5 + 50 * 4.5, 6);
  });

  it("respects custom rate overrides while preserving free tiers", () => {
    expect(priceSecrets(15, 7)).toBeCloseTo(5 * 7, 6);
    expect(priceComputeRequests(20_000_000, 0)).toBe(0);
    expect(priceCdnEgress(200, 0.15)).toBeCloseTo(100 * 0.15, 6);
  });
});
