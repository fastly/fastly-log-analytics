import { describe, expect, it } from "vitest";
import {
  fnv1a32,
  fnv1a64,
  parseCompactTimestamp,
  parseHivePartitionPath,
  parseOpsParquetKey,
  parseParquetPartitionWindow,
} from "./parquet-key";

describe("parquet-key utilities", () => {
  it("computes FNV-1a 32-bit and 64-bit hashes deterministically", () => {
    expect(fnv1a64([])).toBe(BigInt("0xcbf29ce484222325"));
    expect(fnv1a32(["host", "status"], "v1")).toHaveLength(8);
    expect(fnv1a32(["host", "status"], "v1")).not.toBe(
      fnv1a32(["host", "status"], "v2"),
    );
  });

  it("parses Hive partition paths and returns null on malformed keys", () => {
    expect(
      parseHivePartitionPath(
        "svc/processed/request/year=2026/month=10/day=29/hour=12/window=1-2-r.parquet",
      ),
    ).toEqual({ year: 2026, month: 10, day: 29, hour: 12 });
    expect(
      parseHivePartitionPath(
        "svc/processed/summary_bundle/request/year=2026/month=10/day=29/day_bundle=c1.parquet",
      ),
    ).toEqual({ year: 2026, month: 10, day: 29, hour: undefined });
    expect(parseHivePartitionPath("svc/invalid/path.parquet")).toBeNull();
    expect(
      parseHivePartitionPath("svc/year=2026/month=13/day=40/file.parquet"),
    ).toBeNull();
  });

  it("parses compact UTC timestamps and returns null on malformed strings", () => {
    expect(parseCompactTimestamp("20261029T123045Z")).toBe(
      Date.parse("2026-10-29T12:30:45Z"),
    );
    expect(parseCompactTimestamp("not-a-timestamp")).toBeNull();
    expect(parseCompactTimestamp("20261399T999999Z")).toBeNull();
  });

  it("parses window, summary, hour_bundle, and day_bundle partition windows", () => {
    expect(
      parseParquetPartitionWindow(
        "svc/requests/year=2026/month=10/day=29/hour=12/window=1793275200-1793275260-r-n1.parquet",
      ),
    ).toEqual([1793275200_000, 1793275260_000]);
    expect(
      parseParquetPartitionWindow(
        "svc/processed/summary/request/year=2026/month=10/day=29/hour=12/summary=1793275200-1793275260-r-n1.parquet",
      ),
    ).toEqual([1793275200_000, 1793275260_000]);
    expect(
      parseParquetPartitionWindow(
        "svc/processed/summary_bundle/request/year=2026/month=10/day=29/hour_bundle=12-c1-n1-h01.parquet",
      ),
    ).toEqual([
      Date.parse("2026-10-29T12:00:00Z"),
      Date.parse("2026-10-29T13:00:00Z"),
    ]);
    expect(
      parseParquetPartitionWindow(
        "svc/processed/summary_bundle/request/year=2026/month=10/day=29/day_bundle=c1-n1-h01.parquet",
      ),
    ).toEqual([
      Date.parse("2026-10-29T00:00:00Z"),
      Date.parse("2026-10-30T00:00:00Z"),
    ]);
    expect(parseParquetPartitionWindow("svc/unrecognized.parquet")).toBeNull();
  });

  it("parses ops telemetry Parquet keys and returns null for other tables or malformed keys", () => {
    const windowKey =
      "svc/processed/ops/year=2026/month=10/day=29/hour=12/window=1793275200-1793275260-r-n1.parquet";
    expect(parseOpsParquetKey(windowKey, "ops")).toEqual({
      key: windowKey,
      table: "ops",
      hourMs: Date.parse("2026-10-29T12:00:00Z"),
      startMs: 1793275200_000,
      endMs: 1793275260_000,
      compact: false,
    });
    expect(parseOpsParquetKey(windowKey, "ingest")).toBeNull();
    expect(parseOpsParquetKey("random/file.parquet")).toBeNull();
  });
});
