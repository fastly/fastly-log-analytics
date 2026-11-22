import { describe, expect, it } from "vitest";
import { fixtureUsageInput, FIXTURE_NOW } from "./fixtures";
import { bucketUsage, bucketStarts } from "./series";

const HOUR = 3_600_000;
const DAY = 86_400_000;

describe("bucketStarts", () => {
  it("lists every bucket in the range, including quiet ones", () => {
    const to = Date.UTC(2026, 9, 30, 12, 30);
    const starts = bucketStarts(to - 3 * HOUR, to, "hour");
    expect(starts).toEqual([
      Date.UTC(2026, 9, 30, 9),
      Date.UTC(2026, 9, 30, 10),
      Date.UTC(2026, 9, 30, 11),
      Date.UTC(2026, 9, 30, 12),
    ]);
  });

  it("aligns day buckets to UTC midnight", () => {
    const starts = bucketStarts(FIXTURE_NOW - 2 * DAY, FIXTURE_NOW, "day");
    expect(starts).toEqual([
      Date.UTC(2026, 9, 28),
      Date.UTC(2026, 9, 29),
      Date.UTC(2026, 9, 30),
    ]);
  });
});

describe("bucketUsage", () => {
  const from = FIXTURE_NOW - 3 * DAY;
  const series = bucketUsage(fixtureUsageInput.rows, {
    from,
    to: FIXTURE_NOW,
    by: "day",
  });

  it("zero-fills buckets without rows", () => {
    expect(series.map((p) => p.t)).toEqual([
      Date.UTC(2026, 9, 27),
      Date.UTC(2026, 9, 28),
      Date.UTC(2026, 9, 29),
      Date.UTC(2026, 9, 30),
    ]);
    expect(series[0]).toMatchObject({ classA: 0, classB: 0, usd: 0 });
  });

  it("sums Class A and B operations into the bucket holding the row", () => {
    const total = series.reduce((sum, p) => sum + p.classA + p.classB, 0);
    const expected = fixtureUsageInput.rows.fos_usage
      .filter((r) => r.timestamp >= from && r.op_class !== "CDN_HIT")
      .reduce((sum, r) => sum + r.op_count, 0);
    expect(total).toBe(expected);
    const day = Date.UTC(2026, 9, 28);
    const dayClassA = fixtureUsageInput.rows.fos_usage
      .filter(
        (r) =>
          r.op_class === "A" &&
          r.timestamp >= day &&
          r.timestamp < day + DAY,
      )
      .reduce((sum, r) => sum + r.op_count, 0);
    expect(dayClassA).toBeGreaterThan(0);
    expect(series.find((p) => p.t === day)!.classA).toBe(dayClassA);
  });

  it("counts compute requests from api telemetry", () => {
    const requests = series.reduce((sum, p) => sum + p.requests, 0);
    const expected = fixtureUsageInput.rows.api_telemetry.filter(
      (r) => r.source === "compute_api" && r.timestamp >= from,
    ).length;
    expect(requests).toBe(expected);
  });

  it("counts a folded file-read row as the requests it stands for", () => {
    const folded = bucketUsage(
      {
        ...fixtureUsageInput.rows,
        api_telemetry: [
          {
            timestamp: FIXTURE_NOW - DAY,
            source: "compute_api",
            path: "/api/file",
            vcpu_ms: 35,
            files_fetched: 7,
          },
        ],
      },
      { from, to: FIXTURE_NOW, by: "day" },
    );
    expect(folded.reduce((sum, p) => sum + p.requests, 0)).toBe(7);
  });

  it("sums ingested log rows from request_ingest cron_runs into each bucket", () => {
    const withIngest = bucketUsage(
      {
        ...fixtureUsageInput.rows,
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[2],
            rows_written: 1_250,
          },
        ],
      },
      { from, to: FIXTURE_NOW, by: "day" },
    );
    const totalIngested = withIngest.reduce(
      (sum, p) => sum + p.ingestedRows,
      0,
    );
    expect(totalIngested).toBe(1_250);
  });

  it("uses CDN bandwidth buckets for bytesOut when provided", () => {
    const day = Date.UTC(2026, 9, 29);
    const withCdnBw = bucketUsage(
      fixtureUsageInput.rows,
      { from, to: FIXTURE_NOW, by: "day" },
      [{ t: day + 3_600_000, bytes: 42_000_000 }],
    );
    expect(withCdnBw.find((p) => p.t === day)?.bytesOut).toBe(42_000_000);
  });

  it("populates kvClassA, kvClassB, and kvUsd from kvByHour across the timeline", () => {
    const day = Date.UTC(2026, 9, 29);
    const withKv = bucketUsage(
      fixtureUsageInput.rows,
      { from, to: FIXTURE_NOW, by: "day" },
      undefined,
      [{ t: day + 3_600_000, classA: 10_000, classB: 50_000 }],
      {
        classAPerMillion: fixtureUsageInput.settings.kv_class_a_rate_per_million,
        classBPerMillion: fixtureUsageInput.settings.kv_class_b_rate_per_million,
      },
    );
    const point = withKv.find((p) => p.t === day);
    expect(point?.kvClassA).toBe(10_000);
    expect(point?.kvClassB).toBe(50_000);
    expect(point?.kvUsd).toBeCloseTo(
      (10_000 / 1e6) * fixtureUsageInput.settings.kv_class_a_rate_per_million +
        (50_000 / 1e6) * fixtureUsageInput.settings.kv_class_b_rate_per_million,
    );
    const quiet = withKv.find((p) => p.t !== day);
    expect(quiet?.kvClassA).toBe(0);
    expect(quiet?.kvClassB).toBe(0);
    expect(quiet?.kvUsd).toBe(0);
  });

  it("synthesizes FOS ops, bytesOut, usd, and compute requests from cron_runs when fos_usage and api_telemetry are empty", () => {
    const day = Date.UTC(2026, 9, 29);
    const derived = bucketUsage(
      {
        fos_usage: [],
        api_telemetry: [],
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            started_at: day + 3_600_000,
            cron_kind: "request_ingest",
            outcome: "done",
            listed_files: 4,
            converted_files: 4,
            deleted_files: 4,
            parquet_files_written: 1,
            bytes_written: 160_000,
            rows_written: 2_000,
            worker_executions: 1,
            idle_ticks: 5,
          },
        ],
      },
      { from, to: FIXTURE_NOW, by: "day" },
    );
    const point = derived.find((p) => p.t === day);
    expect(point?.classA).toBe(2);
    expect(point?.classB).toBe(4);
    expect(point?.bytesOut).toBeGreaterThan(160_000);
    expect(point?.usd).toBeGreaterThan(0);
    expect(point?.requests).toBe(1 + 1 + 5);
    expect(point?.ingestedRows).toBe(2_000);
  });
});
