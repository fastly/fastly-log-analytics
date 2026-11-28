// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { CloudBudgetError, MAX_REGISTERED_BYTES } from "./budget";
import {
  MAX_RANGES_PER_FILE,
  planParquetProjection,
  readParquetFooter,
  shouldFallbackToWhole,
  WHOLE_FALLBACK_HIGH_FANOUT_MAX_BYTES,
  WHOLE_FALLBACK_HIGH_FANOUT_MIN,
  WHOLE_FALLBACK_HIGH_FANOUT_RATIO,
  WHOLE_FALLBACK_MANY_RANGES_MIN,
  WHOLE_FALLBACK_MANY_RANGES_RATIO,
  WHOLE_FALLBACK_RANGE_RATIO,
  wholeReadLimits,
} from "./parquet-projection";
import {
  effectiveProjectedColumnCount,
  prefersWholeForWideProjection,
} from "./projected-read";

describe("P1 deterministic Cloud bounded-scan & file/range work gates", () => {
  it("caps projected HTTP 206 ranges per file at MAX_RANGES_PER_FILE across many row groups", async () => {
    expect(MAX_RANGES_PER_FILE).toBe(64);
    const totalRows = 3000;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      rowGroupSize: 30,
      columnData: [
        {
          name: "timestamp",
          type: "TIMESTAMP",
          data: Array.from({ length: totalRows }, (_, i) => new Date(1_700_000_000_000 + i * 1_000)),
        },
        {
          name: "wide_payload",
          type: "STRING",
          data: Array.from({ length: totalRows }, (_, i) => `w${i}`.padEnd(240, "x")),
        },
        {
          name: "status",
          type: "INT32",
          data: Array.from({ length: totalRows }, (_, i) => (i % 7 === 0 ? 503 : 200)),
        },
      ],
    });
    const footer = await readParquetFooter(bytes.byteLength, async (start, end) =>
      bytes.slice(start, end),
    );
    expect(footer.metadata.row_groups.length).toBeGreaterThan(MAX_RANGES_PER_FILE);
    const plan = planParquetProjection(footer, new Set(["timestamp", "status"]));
    expect(plan.ranges.length).toBeGreaterThan(0);
    expect(plan.ranges.length).toBeLessThanOrEqual(MAX_RANGES_PER_FILE);
    expect(plan.neededBytes).toBeLessThan(bytes.byteLength);
  });

  it("switches high-fanout and high-coverage scans to a single whole-file read cleanly", () => {
    const fileSize = 4 * 1024 * 1024;
    expect(
      shouldFallbackToWhole(
        { ranges: [{ start: 0, end: 100 }] },
        fileSize,
        Math.ceil(fileSize * WHOLE_FALLBACK_RANGE_RATIO),
      ),
    ).toBe(true);
    expect(
      shouldFallbackToWhole(
        { ranges: Array.from({ length: WHOLE_FALLBACK_MANY_RANGES_MIN }, (_, i) => ({ start: i, end: i + 1 })) },
        fileSize,
        Math.ceil(fileSize * WHOLE_FALLBACK_MANY_RANGES_RATIO),
      ),
    ).toBe(true);
    expect(
      shouldFallbackToWhole(
        { ranges: Array.from({ length: WHOLE_FALLBACK_HIGH_FANOUT_MIN }, (_, i) => ({ start: i, end: i + 1 })) },
        WHOLE_FALLBACK_HIGH_FANOUT_MAX_BYTES,
        Math.ceil(WHOLE_FALLBACK_HIGH_FANOUT_MAX_BYTES * WHOLE_FALLBACK_HIGH_FANOUT_RATIO),
      ),
    ).toBe(true);
    expect(
      shouldFallbackToWhole(
        { ranges: Array.from({ length: WHOLE_FALLBACK_MANY_RANGES_MIN }, (_, i) => ({ start: i, end: i + 1 })) },
        MAX_REGISTERED_BYTES + 1,
        MAX_REGISTERED_BYTES,
      ),
    ).toBe(false);
  });

  it("collapses mutually aliased schema names and prefers shared whole reads for wide overlapped compacts", () => {
    expect(
      effectiveProjectedColumnCount([
        "timestamp",
        "ts_ms",
        "cache",
        "cache_status",
        "resp_bytes",
        "resp_body_bytes",
        "resp_body_size",
      ]),
    ).toBe(3);

    const compactFile = {
      size: wholeReadLimits.closedHourUpToBytes + 1024,
      start: "2027-01-15T08:00:00Z",
      end: "2027-01-15T09:00:00Z",
    };
    expect(
      prefersWholeForWideProjection(compactFile, 16, {
        fromMs: Date.parse("2027-01-15T08:15:00Z"),
        toMs: Date.parse("2027-01-15T09:00:00Z"),
      }),
    ).toBe(true);
    expect(
      prefersWholeForWideProjection(compactFile, 16, {
        fromMs: Date.parse("2027-01-15T08:45:00Z"),
        toMs: Date.parse("2027-01-15T09:00:00Z"),
      }),
    ).toBe(false);
    expect(prefersWholeForWideProjection(compactFile, 15)).toBe(false);
  });

  it("carries structured byte/file limits on CloudBudgetError to refuse unbounded scans fail-closed", () => {
    const err = new CloudBudgetError("over", {
      unit: "bytes",
      needed: MAX_REGISTERED_BYTES + 1,
      limit: MAX_REGISTERED_BYTES,
    });
    expect(err).toBeInstanceOf(Error);
    expect(err.detail).toEqual({
      unit: "bytes",
      needed: MAX_REGISTERED_BYTES + 1,
      limit: MAX_REGISTERED_BYTES,
    });
  });
});
