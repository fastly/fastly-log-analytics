import { describe, expect, it } from "vitest";
import {
  DEFAULT_COMPACTION,
  DEFAULT_INPUTS,
  DEFAULT_UNITS,
  LIST_PRICE_RATES,
  MONTH_DAYS,
  estimateMonthly,
  measureEstimate,
  rawFilesPerDay,
  type EstimateFeature,
  type EstimateMeasurements,
  type UnitCosts,
} from "./estimate";

const NO_COMPACTION = { ...DEFAULT_COMPACTION, enabled: false };
import { INGEST_ASSUMPTIONS, INGEST_LIMITS } from "./ingestShape";

const MIB = 1024 * 1024;
const BASE_IDS = ["storage", "delivery", "compute", "kv", "secrets", "config"];

const zeroUnits = Object.fromEntries(
  Object.keys(DEFAULT_UNITS).map((key) => [key, 0]),
) as unknown as UnitCosts;
const zeroRates = Object.fromEntries(
  Object.keys(LIST_PRICE_RATES).map((key) => [key, 0]),
) as unknown as typeof LIST_PRICE_RATES;

const atRps = (rps: number, logPeriodS = 60) => ({
  ...DEFAULT_INPUTS,
  requestsPerDay: rps * 86_400,
  edgeRequestsPerDay: rps * 86_400,
  gzBytesPerLine: 88,
  logPeriodS,
});

const line = (estimate: ReturnType<typeof estimateMonthly>, id: string) =>
  estimate.lines.find((l) => l.id === id);
const feature = (estimate: ReturnType<typeof estimateMonthly>, id: EstimateFeature) =>
  estimate.featureUsd[id];

describe("high-RPS ingest costs", () => {
  it("leaves today's estimate unchanged without relay, segments or multipart", () => {
    const defaults = estimateMonthly(
      DEFAULT_INPUTS,
      DEFAULT_UNITS,
      LIST_PRICE_RATES,
      NO_COMPACTION,
      { relay: 0 },
    );
    // Includes the front clock probes minus the Full Site Delivery free tier (100 GB & 1M reqs/mo)
    // and the 10-secret Secret Store free tier ($5 saved on 1 secret).
    expect(defaults.total).toBeCloseTo(16.463325525936515, 9);
    expect(defaults.lines.map((l) => l.id)).toEqual(BASE_IDS);
    const tenK = estimateMonthly(
      atRps(10_000),
      DEFAULT_UNITS,
      LIST_PRICE_RATES,
      NO_COMPACTION,
      { relay: 0 },
    );
    // Plus the clock probes every 30 s, minus the 100 GB ($12) + 1M reqs ($1) + 1 secret ($5) free tiers.
    expect(tenK.total).toBeCloseTo(544.6170493881453 - 18, 9);
    expect(tenK.lines.map((l) => l.id)).toEqual(BASE_IDS);
  });

  it("charges each segment of a split raw file as a worker run with skipped decompression", () => {
    const inputs = atRps(100_000, 300);
    const F = rawFilesPerDay(inputs);
    const gz = (inputs.edgeRequestsPerDay * 88) / F;
    const S = Math.ceil((gz * 8) / INGEST_LIMITS.shardDecompressedBytes);
    expect(S).toBe(8);
    const units = {
      ...zeroUnits,
      workerRequestsPerFile: 0.125,
      windowFilesPerFile: 0.125,
    };
    const requests = estimateMonthly(
      inputs,
      units,
      { ...zeroRates, compute_request_rate_per_million: 1 },
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    expect(feature(requests, "segments")!).toBeCloseTo(
      (F * (S - 0.125) * MONTH_DAYS) / 1e6,
      6,
    );
    const skippedMib =
      ((INGEST_LIMITS.shardDecompressedBytes / MIB) * S * (S - 1)) / 2;
    const vcpu = estimateMonthly(
      inputs,
      units,
      { ...zeroRates, compute_vcpu_rate_per_million_ms: 1 },
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    expect(feature(vcpu, "segments")!).toBeCloseTo(
      (F * skippedMib * INGEST_ASSUMPTIONS.skipVcpuMsPerMib * MONTH_DAYS) / 1e6,
      6,
    );
    const classA = estimateMonthly(
      inputs,
      units,
      { ...zeroRates, class_a_rate_per_1k: 1 },
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    // Each segment writes its own window file and summary: S windows instead of 0.125.
    expect(feature(classA, "segments")!).toBeCloseTo(
      (F * (S - 0.125) * 2 * MONTH_DAYS) / 1000,
      6,
    );
    expect(feature(classA, "multipart")).toBeUndefined();
  });

  it("charges the whole raw file's CDN download again for every segment after the first", () => {
    const inputs = atRps(100_000, 300);
    const egress = { ...zeroRates, cdn_egress_rate_per_gb: 0.12 };
    const estimate = estimateMonthly(
      inputs,
      { ...zeroUnits, workerRequestsPerFile: 0.125, windowFilesPerFile: 0.125 },
      egress,
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    // 100 GB monthly free delivery bandwidth ($12) is shared proportionally across base + segment egress.
    expect(feature(estimate, "segments")!).toBeCloseTo(19_149.564, 3);
    // Delivery carries the base download plus every segment's re-download minus 100 GB free.
    const totalEgressGb = ((inputs.edgeRequestsPerDay * 88) / 1e9) * MONTH_DAYS * 8;
    expect(line(estimate, "delivery")!.usd).toBeCloseTo(
      Math.max(0, totalEgressGb - 100) * 0.12,
      3,
    );
  });

  it("does not split a file that converts whole", () => {
    const estimate = estimateMonthly(
      atRps(100_000),
      DEFAULT_UNITS,
      LIST_PRICE_RATES,
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    expect(feature(estimate, "segments")).toBeUndefined();
  });

  it("charges multipart create, parts and complete for windows above one part", () => {
    const inputs = atRps(100_000);
    const F = rawFilesPerDay(inputs);
    const windows = F * 0.125;
    const bytes =
      (inputs.edgeRequestsPerDay * inputs.parquetBytesPerRow) / windows;
    const parts = Math.ceil(bytes / INGEST_LIMITS.partBytes);
    expect(parts).toBeGreaterThan(1);
    const estimate = estimateMonthly(
      inputs,
      { ...zeroUnits, windowFilesPerFile: 0.125 },
      { ...zeroRates, class_a_rate_per_1k: 1 },
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    expect(feature(estimate, "multipart")!).toBeCloseTo(
      (windows * (parts + 1) * MONTH_DAYS) / 1000,
      6,
    );
    const small = estimateMonthly(
      atRps(10_000),
      { ...zeroUnits, windowFilesPerFile: 0.125 },
      LIST_PRICE_RATES,
    );
    expect(feature(small, "multipart")).toBeUndefined();
  });

  it("adds a relay execution per group of relayed shards only when relaying", () => {
    const inputs = atRps(100_000);
    const direct = estimateMonthly(
      inputs,
      DEFAULT_UNITS,
      LIST_PRICE_RATES,
      DEFAULT_COMPACTION,
      { relay: 0 },
    );
    expect(feature(direct, "relay")).toBeUndefined();
    expect(direct.ingest.relayRequestsPerDay).toBe(0);
    const relayed = estimateMonthly(
      inputs,
      DEFAULT_UNITS,
      LIST_PRICE_RATES,
      DEFAULT_COMPACTION,
      { relay: INGEST_LIMITS.maxRelayShards },
    );
    const { relayRequestsPerDay, shardsPerDay } = relayed.ingest;
    expect(relayRequestsPerDay).toBeGreaterThanOrEqual(
      shardsPerDay / INGEST_LIMITS.maxRelayShards,
    );
    expect(relayRequestsPerDay).toBeLessThan(shardsPerDay / 2);
    // At list/contract rates (20 ms free per request), the 5 ms relay execution
    // sits inside the per-request CPU allowance and bills only the request fee.
    expect(feature(relayed, "relay")!).toBeCloseTo(
      ((relayRequestsPerDay * MONTH_DAYS) / 1e6) *
        LIST_PRICE_RATES.compute_request_rate_per_million,
      9,
    );
    expect(relayed.total - direct.total).toBeCloseTo(
      feature(relayed, "relay")!,
      9,
    );
    const relayedNoAllowance = estimateMonthly(
      inputs,
      DEFAULT_UNITS,
      {
        ...LIST_PRICE_RATES,
        compute_vcpu_free_ms_per_request: 0,
        compute_vcpu_rounding_ms: 0,
      },
      DEFAULT_COMPACTION,
      { relay: INGEST_LIMITS.maxRelayShards },
    );
    expect(feature(relayedNoAllowance, "relay")!).toBeCloseTo(
      ((relayRequestsPerDay * MONTH_DAYS) / 1e6) *
        LIST_PRICE_RATES.compute_request_rate_per_million +
        ((relayRequestsPerDay *
          INGEST_ASSUMPTIONS.relayVcpuMsPerCall *
          MONTH_DAYS) /
          1e6) *
          LIST_PRICE_RATES.compute_vcpu_rate_per_million_ms,
      9,
    );
  });
});

describe("relay prefill", () => {
  const measurements: EstimateMeasurements = {
    days: 7,
    service: {
      log_period: 60,
      sample_rate: 100,
      edge_only: true,
      delete_after: true,
      log_retention_days: 30,
    },
    traffic: null,
    billing: null,
    fos: null,
    files: [],
    parquet: { bytes: 0, rows: 0 },
    fixedStoredBytes: null,
    minBilledDays: 30,
  };

  it("takes the install's ingest_relay setting, else direct dispatch", () => {
    expect(measureEstimate(measurements).relay).toBe(0);
    expect(
      measureEstimate({
        ...measurements,
        service: { ...measurements.service!, ingest_relay: 4 },
      }).relay,
    ).toBe(4);
  });
});
