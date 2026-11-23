import { describe, expect, it } from "vitest";
import fixtures from "./storageBilling.fixtures.json";
import {
  calculateStorageBilling,
  type StorageBillingInterval,
  type StorageBillingSummary,
} from "./storageBilling";

type FixtureCase = {
  name: string;
  from: number;
  to: number;
  intervals: StorageBillingInterval[];
  expected: StorageBillingSummary;
};

describe("calculateStorageBilling", () => {
  const cases = fixtures.cases as FixtureCase[];

  it.each(cases)("$name", ({ intervals, from, to, expected }) => {
    expect(calculateStorageBilling(intervals, from, to)).toEqual(expected);
  });

  it("rejects inverted or non-finite ranges with RangeError", () => {
    expect(() => calculateStorageBilling([], 1000, 500)).toThrow(RangeError);
    expect(() => calculateStorageBilling([], Number.NaN, 1000)).toThrow(RangeError);
    expect(() => calculateStorageBilling([], 0, Number.POSITIVE_INFINITY)).toThrow(RangeError);
  });

  it("clamps negative and non-finite bytes to zero without producing NaN", () => {
    const hour = 3_600_000;
    const summary = calculateStorageBilling(
      [
        {
          id: "neg-bytes",
          dataset: "pq_request",
          kind: "live",
          bytes: -500,
          billedFrom: 0,
          billedUntil: hour,
          complete: true,
          provenance: "cloud:inventory",
        },
        {
          id: "nan-bytes",
          dataset: "pq_request",
          kind: "deleted",
          bytes: Number.NaN,
          billedFrom: 0,
          billedUntil: hour,
          complete: true,
          provenance: "cloud:fos_usage",
        },
        {
          id: "inf-bytes",
          dataset: "pq_request",
          kind: "minimum_billed",
          bytes: Number.POSITIVE_INFINITY,
          billedFrom: 0,
          billedUntil: hour,
          complete: true,
          provenance: "cloud:ingest_files",
        },
      ],
      0,
      hour,
    );

    expect(summary).toEqual({
      liveGbHours: 0,
      deletedGbHours: 0,
      minimumBilledGbHours: 0,
      totalGbHours: 0,
      complete: true,
      provenance: ["cloud:fos_usage", "cloud:ingest_files", "cloud:inventory"],
    });
  });

  it("preserves full floating-point precision without premature rounding", () => {
    const GiB = 1024 ** 3;
    const hour = 3_600_000;
    const summary = calculateStorageBilling(
      [
        {
          id: "fractional",
          dataset: "pq_request",
          kind: "live",
          bytes: GiB,
          billedFrom: 0,
          billedUntil: 1_200_000, // 1/3 hour
          complete: true,
          provenance: "cloud:inventory",
        },
      ],
      0,
      hour,
    );

    expect(summary.liveGbHours).toBe(1_200_000 / hour);
    expect(summary.totalGbHours).toBe(1_200_000 / hour);
  });

  it("ignores intervals with inverted or non-finite billing bounds", () => {
    const GiB = 1024 ** 3;
    const hour = 3_600_000;
    const summary = calculateStorageBilling(
      [
        {
          id: "inverted-bounds",
          dataset: "pq_request",
          kind: "live",
          bytes: GiB,
          billedFrom: 2 * hour,
          billedUntil: hour,
          complete: false,
          provenance: "cloud:inventory",
        },
        {
          id: "nan-bounds",
          dataset: "pq_request",
          kind: "live",
          bytes: GiB,
          billedFrom: Number.NaN,
          billedUntil: hour,
          complete: false,
          provenance: "cloud:inventory",
        },
      ],
      0,
      3 * hour,
    );

    expect(summary).toEqual({
      liveGbHours: 0,
      deletedGbHours: 0,
      minimumBilledGbHours: 0,
      totalGbHours: 0,
      complete: true,
      provenance: [],
    });
  });
});
