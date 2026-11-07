import { describe, expect, it } from "vitest";
import {
  COMPACT_GZ_BYTES_PER_LINE,
  COMPACT_RUM_GZ_BYTES_PER_LINE,
  DEFAULT_COMPACTION,
  DEFAULT_INPUTS,
  DEFAULT_UNITS,
  LIST_PRICE_RATES,
  SUMMARY_BASE_BYTES,
  SUMMARY_HALF_ROWS,
  compactionPlan,
  estimateMonthly,
  measureEstimate,
  rawFilesPerDay,
  summaryBytes,
  type EstimateMeasurements,
  type UnitCosts,
} from "./estimate";
import {
  incurredCost,
  summarizeUsage,
  type ComputeBilling,
} from "./summary";
import { fixtureUsageInput } from "./fixtures";

const zeroUnits = Object.fromEntries(
  Object.keys(DEFAULT_UNITS).map((key) => [key, 0]),
) as unknown as UnitCosts;

const zeroRates = Object.fromEntries(
  Object.keys(LIST_PRICE_RATES).map((key) => [key, 0]),
) as unknown as typeof LIST_PRICE_RATES;

const line = (estimate: ReturnType<typeof estimateMonthly>, id: string) =>
  estimate.lines.find((l) => l.id === id)!;

describe("estimateMonthly", () => {
  it("spreads logged lines over the period's writing nodes", () => {
    const at = (linesPerDay: number) => ({
      ...DEFAULT_INPUTS,
      logPeriodS: 10,
      filesPerPeriod: 50,
      requestsPerDay: linesPerDay,
      edgeRequestsPerDay: linesPerDay,
    });
    const sparse = rawFilesPerDay(at(500));
    expect(sparse).toBeLessThanOrEqual(500);
    expect(sparse).toBeCloseTo(500, 0);
    // λ = N lines per period: expected occupied nodes N(1 − 1/e).
    expect(rawFilesPerDay(at(8_640 * 50))).toBeCloseTo(
      8_640 * 50 * (1 - Math.exp(-1)),
    );
    expect(rawFilesPerDay(at(1e10))).toBeCloseTo(8_640 * 50);
    expect(rawFilesPerDay(at(0))).toBe(0);
  });

  it("logs edge requests only when asked, then samples", () => {
    const inputs = {
      ...DEFAULT_INPUTS,
      requestsPerDay: 3_000_000,
      edgeRequestsPerDay: 2_000_000,
      samplePct: 10,
    };
    expect(
      estimateMonthly(inputs, zeroUnits, LIST_PRICE_RATES).linesPerDay,
    ).toBe(200_000);
    expect(
      estimateMonthly(
        { ...inputs, edgeOnly: false },
        zeroUnits,
        LIST_PRICE_RATES,
      ).linesPerDay,
    ).toBe(300_000);
  });

  it("bills deleted raw files for the minimum days and kept ones for retention", () => {
    const inputs = {
      ...DEFAULT_INPUTS,
      edgeRequestsPerDay: 10_000_000,
      gzBytesPerLine: 100,
      parquetBytesPerRow: 0,
      retentionDays: 90,
      minBilledDays: 30,
    };
    const rates = { ...LIST_PRICE_RATES, storage_rate_per_gb_month: 1 };
    // 1 GB of raw a day.
    expect(
      line(estimateMonthly(inputs, zeroUnits, rates), "storage").usd,
    ).toBeCloseTo(30);
    expect(
      line(
        estimateMonthly({ ...inputs, deleteAfter: false }, zeroUnits, rates),
        "storage",
      ).usd,
    ).toBeCloseTo(90);
  });

  it("adds fixed daily cost to per-file and per-line work over 30 days", () => {
    const units = {
      ...zeroUnits,
      appRequestsPerDay: 1_000,
      workerRequestsPerFile: 0.5,
      vcpuMsPer1kLines: 10,
      fosClassAPerFile: 3,
      secrets: 2,
    };
    const inputs = {
      ...DEFAULT_INPUTS,
      edgeRequestsPerDay: 1_000_000,
      filesPerPeriod: 1,
      logPeriodS: 60,
      gzBytesPerLine: 0,
      parquetBytesPerRow: 0,
    };
    const estimate = estimateMonthly(inputs, units, LIST_PRICE_RATES);
    expect(estimate.filesPerDay).toBe(1_440);
    // (1,000 + 0.5 × 1,440) × 30 requests; 10 × 1,000 = 10,000 vCPU ms/day across 720 worker executions (~13.89 ms/exec, within 20 ms free tier => $0 excess CPU).
    expect(line(estimate, "compute").usd).toBeCloseTo(
      (51_600 / 1e6) * 0.5,
    );
    expect(line(estimate, "ops").usd).toBeCloseTo(
      ((3 * 1_440 * 30) / 1000) * 0.0025,
    );
    expect(line(estimate, "secrets").usd).toBe(10);
    expect(estimate.total).toBeCloseTo(
      estimate.lines.reduce((sum, l) => sum + l.usd, 0),
    );
  });

  it("includes fixed daily KV Store operations in telemetryUsd", () => {
    const units = {
      ...zeroUnits,
      kvClassAPerDay: 10_000,
      kvClassBPerDay: 50_000,
    };
    const estimate = estimateMonthly(DEFAULT_INPUTS, units, LIST_PRICE_RATES);
    expect(estimate.telemetryUsd).toBeCloseTo(
      ((10_000 * 30) / 1e6) * LIST_PRICE_RATES.kv_class_a_rate_per_million +
        ((50_000 * 30) / 1e6) * LIST_PRICE_RATES.kv_class_b_rate_per_million,
    );
  });
});

describe("RUM", () => {
  const rum = {
    ...DEFAULT_INPUTS,
    edgeRequestsPerDay: 0,
    rumPageviewsPerDay: 100_000,
    rumSamplePct: 50,
    rumBeaconsPerPageview: 1,
    rumGzBytesPerLine: 200,
    rumRowsPerLine: 8,
    rumParquetBytesPerRow: 50,
    rumLogPeriodS: 300,
    rumRetentionDays: 30,
    minBilledDays: 30,
  };

  it("is off without pageviews and adds no line", () => {
    const estimate = estimateMonthly(
      DEFAULT_INPUTS,
      zeroUnits,
      LIST_PRICE_RATES,
    );
    expect(estimate.lines.find((l) => l.id === "rum")).toBeUndefined();
  });

  it("logs one batched beacon line per sampled flush and bills its requests and storage", () => {
    const rates = {
      ...LIST_PRICE_RATES,
      storage_rate_per_gb_month: 1,
      cdn_request_rate_per_10k: 1,
      class_a_rate_per_1k: 0,
      class_b_rate_per_10k: 0,
      cdn_egress_rate_per_gb: 0,
    };
    const estimate = estimateMonthly(rum, zeroUnits, rates);
    const r = estimate.rum!;
    expect(r.linesPerDay).toBe(50_000);
    expect(r.rowsPerDay).toBe(400_000);
    // 50,000 lines × 200 B raw billed 30 days, 400,000 rows × 50 B kept 30 days.
    expect(r.storedGb).toBeCloseTo(0.01 * 30 + 0.02 * 30);
    expect(line(estimate, "rum").usd).toBeCloseTo(
      r.storedGb + (50_000 * 30) / 10_000,
    );
    expect(estimate.total).toBeCloseTo(
      estimate.lines.reduce((sum, l) => sum + l.usd, 0),
    );
  });

  it("runs rum_ingest once per RUM log period, each listing the raw prefix", () => {
    const rates = { ...zeroRates, class_a_rate_per_1k: 1 };
    const quiet = estimateMonthly(
      { ...rum, rumPageviewsPerDay: 1e-9 },
      zeroUnits,
      rates,
    );
    expect(quiet.rum!.runsPerDay).toBe(288);
    expect(line(quiet, "rum").usd).toBeCloseTo((288 * 30) / 1000, 3);
    expect(
      estimateMonthly({ ...rum, rumLogPeriodS: 60 }, zeroUnits, rates).rum!
        .runsPerDay,
    ).toBe(1_440);
  });

  it("charges per raw RUM file like request files", () => {
    const units = {
      ...zeroUnits,
      fosClassAPerFile: 3,
      workerRequestsPerFile: 1,
    };
    const rates = {
      ...zeroRates,
      class_a_rate_per_1k: 1,
      compute_request_rate_per_million: 1e6,
    };
    const estimate = estimateMonthly(
      { ...rum, filesPerPeriod: 1, rumPageviewsPerDay: 1e9 },
      units,
      rates,
    );
    expect(estimate.rum!.filesPerDay).toBeCloseTo(288);
    expect(line(estimate, "rum").usd).toBeCloseTo(
      ((3 * 288 + 288) * 30) / 1000 + 288 * 30,
    );
  });
});

describe("summary bundles", () => {
  // 1,440 raw files a day of 1,000 lines each, one window file per raw file.
  const inputs = {
    ...DEFAULT_INPUTS,
    edgeRequestsPerDay: 1_440_000,
    filesPerPeriod: 1,
    logPeriodS: 60,
    retentionDays: 30,
  };
  const units = {
    ...zeroUnits,
    windowFilesPerFile: 1,
    summaryVcpuMsPer1kLines: 100,
    summaryMaxKbPerWindow: 380,
  };

  it("bills rewritten partials for the minimum duration, not request retention", () => {
    const rates = { ...zeroRates, storage_rate_per_gb_month: 1 };
    const unitsWithPartials = { ...units, summaryPartialGbPerDay: 0.5 };
    for (const [retentionDays, minBilledDays] of [
      [7, 30],
      [90, 60],
    ]) {
      const settings = { ...inputs, retentionDays, minBilledDays };
      const baseline = estimateMonthly(settings, units, rates);
      const rewritten = estimateMonthly(settings, unitsWithPartials, rates);
      expect(
        line(rewritten, "summaries").usd - line(baseline, "summaries").usd,
      ).toBeCloseTo(0.5 * minBilledDays);
    }
  });

  it("grows from a fixed footer toward a ceiling as windows get busier", () => {
    expect(summaryBytes(0, 380)).toBe(SUMMARY_BASE_BYTES);
    expect(summaryBytes(SUMMARY_HALF_ROWS, 380)).toBeCloseTo(
      SUMMARY_BASE_BYTES + 190_000,
    );
    expect(summaryBytes(1e9, 380)).toBeCloseTo(
      SUMMARY_BASE_BYTES + 380_000,
      -2,
    );
  });

  it("are always on: a summary per window file plus hour and day bundles", () => {
    const estimate = estimateMonthly(inputs, units, LIST_PRICE_RATES);
    const s = estimate.summaries;
    expect(s.windowFilesPerDay).toBe(1_440);
    // One PUT per window summary, 24 hour bundles and 1 day bundle.
    expect(s.classAPerDay).toBe(1_440 + 24 + 1);
    // Hour bundles read every window summary; the day bundle reads 24 hours.
    expect(s.classBPerDay).toBe(1_440 + 24);
    expect(s.vcpuMsPerDay).toBe(100 * 1_440);
    const gbPerDay =
      (1_440 * summaryBytes(1_000, 380) +
        24 * summaryBytes(60_000, 380) +
        summaryBytes(1_440_000, 380)) /
      1e9;
    expect(s.storedGb).toBeCloseTo(gbPerDay * 30);
    // 100 ms per window summary execution: 80 ms excess above the 20 ms contract allowance across 1,440 runs/day.
    const billedSummaryVcpuPerDay = 80 * 1_440;
    expect(line(estimate, "summaries").usd).toBeCloseTo(
      s.storedGb * 0.02 +
        ((s.classAPerDay * 30) / 1000) * 0.0025 +
        ((s.classBPerDay * 30) / 10_000) * 0.004 +
        ((s.classBPerDay * 30) / 10_000) * 0.01 +
        ((billedSummaryVcpuPerDay * 30) / 1000) * 0.00005,
    );
  });
});

describe("compaction", () => {
  // 24 window files an hour of 1 MB each; 64 MB target.
  const inputs = {
    ...DEFAULT_INPUTS,
    edgeRequestsPerDay: 576_000,
    filesPerPeriod: 1,
    logPeriodS: 3_600,
    parquetBytesPerRow: 1_000,
    retentionDays: 30,
    minBilledDays: 30,
  };
  const units = {
    ...zeroUnits,
    windowFilesPerFile: 24,
    compactVcpuMsPerMb: 20,
  };
  const on = { ...DEFAULT_COMPACTION, enabled: true, targetMb: 64 };

  it("is off by default and costs nothing", () => {
    expect(DEFAULT_COMPACTION).toMatchObject({ enabled: false, targetMb: 64 });
    const estimate = estimateMonthly(inputs, units, LIST_PRICE_RATES);
    expect(estimate.compaction).toBeNull();
    expect(estimate.lines.find((l) => l.id === "compaction")).toBeUndefined();
  });

  it("merges up to 13 sources per execution, level by level, until the hour reaches its target count", () => {
    const plan = compactionPlan(24, 1e6, on);
    // 24 → 2 compacts of 13 and 11 sources → 1 compact of 2.
    expect(plan.executions).toBe(3);
    expect(plan.sourcesRead).toBe(26);
    expect(plan.mbRewritten).toBeCloseTo(48);
    expect(plan.deletedBytes).toBeCloseTo(48e6);
    expect(plan.filesLeft).toBe(1);
  });

  it("leaves sources already at the target, and stops below two per merge", () => {
    expect(compactionPlan(24, 70e6, on).executions).toBe(0);
    expect(compactionPlan(24, 0, on).filesLeft).toBe(1);
    // 40 MB sources: two would exceed 64 MB, so none merge.
    expect(compactionPlan(24, 40e6, on).executions).toBe(0);
    // 20 MB sources: three fit under 64 MB.
    const plan = compactionPlan(9, 20e6, on);
    expect(plan.executions).toBe(3);
    expect(plan.filesLeft).toBe(3);
  });

  it("caps one execution's output at the Compute budget", () => {
    const plan = compactionPlan(26, 100e6, {
      ...on,
      targetMb: 1_000,
    });
    // 250 MB output: two 100 MB sources per merge.
    expect(plan.executions).toBe(13);
    expect(plan.filesLeft).toBe(13);
  });

  it("bills every merged-away source and intermediate compact its minimum days", () => {
    const estimate = estimateMonthly(inputs, units, LIST_PRICE_RATES, on);
    const c = estimate.compaction!;
    const parquetGb = (576_000 * 1_000) / 1e9;
    // Every source and the intermediate compacts are deleted early.
    expect(c.extraStoredGb).toBeCloseTo(((parquetGb * 48) / 24) * 30, 6);
    expect(c.executionsPerDay).toBe(3 * 24);
    // Per execution: create, complete, DeleteObjects plus one part per source; and a LIST per hour.
    expect(c.classAPerDay).toBe((26 + 3 * 3) * 24 + 24);
    expect(c.classBPerDay).toBe(26 * 24);
    expect(c.vcpuMsPerDay).toBeCloseTo(48 * 20 * 24);
    const at90 = estimateMonthly(
      { ...inputs, retentionDays: 90 },
      units,
      LIST_PRICE_RATES,
      on,
    ).compaction!;
    expect(at90.extraStoredGb).toBeCloseTo(c.extraStoredGb);
  });

  it("offsets the cost with fewer dashboard file reads", () => {
    const estimate = estimateMonthly(inputs, units, LIST_PRICE_RATES, {
      ...on,
      hoursReadPerDay: 10,
    });
    // 24 files an hour become 1, for each of 10 hours read a day.
    expect(estimate.compaction!.fileReadsSavedPerDay).toBe(230);
    const without = estimateMonthly(inputs, units, LIST_PRICE_RATES, {
      ...on,
      hoursReadPerDay: 0,
    });
    const saved = 230 * 30;
    expect(
      line(without, "compaction").usd - line(estimate, "compaction").usd,
    ).toBeCloseTo((saved / 10_000) * 0.01 + (saved / 10_000) * 0.004);
  });
});

const billing: ComputeBilling = {
  requests: 0,
  executionTimeMs: 0,
  billedTimeMs: 0,
  coveredFrom: null,
  coveredTo: null,
  cdnBandwidthBytes: 7_000_000_000,
  computeBandwidthBytes: 1_000_000_000,
  cdnRequests: 70_000 + 100,
  kvClassA: 7_000 + DEFAULT_UNITS.kvClassAPerFile * 100,
  kvClassB: 70_000 + DEFAULT_UNITS.kvClassBPerFile * 100,
  secrets: 9,
  byRole: {
    app: { requests: 700_000, vcpuMs: 2_100_000 },
    worker: { requests: 50, vcpuMs: 40_000 },
  },
};

const base: EstimateMeasurements = {
  days: 7,
  service: {
    log_period: 10,
    sample_rate: 50,
    edge_only: false,
    delete_after: true,
    log_retention_days: 14,
  },
  traffic: [
    { requests: 3_000, edge_requests: 2_000 },
    { requests: 6_000, edge_requests: 4_000 },
  ],
  billing,
  fos: null,
  // 100 converted files over 50 ten-second periods, 1,000 lines and 1 MB raw total.
  files: [
    ...Array.from({ length: 100 }, (_, i) => ({
      timestamp: Math.floor(i / 2) * 10_000,
      status: "converted",
      raw_size: 10_000,
      rows: 10,
    })),
    { timestamp: 0, status: "failed", raw_size: 99_999, rows: 0 },
  ],
  parquet: { bytes: 60_000, rows: 1_000 },
  fixedStoredBytes: 2_000_000_000,
  minBilledDays: 30,
};

describe("measureEstimate", () => {
  it("keeps unavailable edge traffic unmeasured instead of turning it into zero", () => {
    const measured = measureEstimate({ ...base, traffic: [{ requests: 1000 }] });
    expect(measured.inputs.requestsPerDay).toBe(1000);
    expect(measured.measuredInputs.edgeRequestsPerDay).toBeUndefined();
    expect(measured.inputs.edgeRequestsPerDay).toBe(DEFAULT_INPUTS.edgeRequestsPerDay);
    const zero = measureEstimate({ ...base, traffic: [{ requests: 1000, edge_requests: 0 }] });
    expect(zero.inputs.edgeRequestsPerDay).toBe(0);
    expect(zero.measuredInputs.edgeRequestsPerDay).toBe(true);
  });
  it("takes traffic and logging settings from the logged service", () => {
    const { inputs, measuredInputs } = measureEstimate(base);
    expect(inputs).toMatchObject({
      requestsPerDay: 4_500,
      edgeRequestsPerDay: 3_000,
      samplePct: 50,
      edgeOnly: false,
      deleteAfter: true,
      retentionDays: 14,
      logPeriodS: 10,
      filesPerPeriod: 2,
      gzBytesPerLine: 1_000,
      parquetBytesPerRow: 60,
    });
    expect(measuredInputs.requestsPerDay).toBe(true);
  });

  it("splits billed totals into per-file work and a fixed daily remainder", () => {
    const { units, measuredUnits } = measureEstimate(base);
    expect(units.appRequestsPerDay).toBe(100_000);
    expect(units.appVcpuMsPerDay).toBe(300_000);
    expect(units.workerRequestsPerFile).toBe(0.5);
    expect(units.vcpuMsPer1kLines + units.summaryVcpuMsPer1kLines).toBe(40_000);
    expect(units.frontRequestsPerDay).toBe(DEFAULT_UNITS.frontRequestsPerDay);
    // 1 GB Compute bandwidth over 7 days (front VCL healthcheck probe bandwidth is ignored).
    expect(units.deliveryGbPerDay).toBeCloseTo(1 / 7);
    expect(units.kvClassAPerDay).toBe(1_000);
    expect(units.kvClassBPerDay).toBe(10_000);
    expect(units.secrets).toBe(9);
    expect(units.fixedStoredGb).toBe(2);
    expect(measuredUnits.kvClassAPerFile).toBeUndefined();
  });

  it("divides billed totals by the span Fastly actually reported", () => {
    const DAY = 86_400_000;
    const late = Array.from({ length: 100 }, () => ({
      timestamp: 3 * DAY,
      status: "converted",
      raw_size: 10_000,
      rows: 10,
    }));
    const { units, window } = measureEstimate({
      ...base,
      billing: { ...billing, coveredFrom: 0, coveredTo: 2 * DAY },
      files: [...base.files, ...late],
    });
    expect(window.days).toBe(2);
    expect(window.files).toBe(100);
    expect(units.appRequestsPerDay).toBe(350_000);
    expect(units.workerRequestsPerFile).toBe(0.5);
    expect(units.vcpuMsPer1kLines + units.summaryVcpuMsPer1kLines).toBe(40_000);
    expect(units.deliveryGbPerDay).toBeCloseTo(1 / 2);
  });

  it("prices worker summaries once and keeps bundle maintenance in the billed app total", () => {
    const { units } = measureEstimate({
      ...base,
      billing: {
        ...billing,
        byRole: {
          app: { requests: 700_000, vcpuMs: 2_100_000 },
          worker: { requests: 50, vcpuMs: 400 },
        },
      },
    });
    const estimate = estimateMonthly(
      { ...DEFAULT_INPUTS, edgeRequestsPerDay: 1_000 },
      units,
      { ...zeroRates, compute_vcpu_rate_per_million_ms: 1e6 },
    );
    expect(
      line(estimate, "compute").usd + line(estimate, "summaries").usd,
    ).toBe((300_000 + 400) * 30);
  });

  it("caps the estimated summary share at the measured worker CPU", () => {
    const { units, measuredUnits } = measureEstimate({
      ...base,
      billing: {
        ...billing,
        byRole: { worker: { requests: 50, vcpuMs: 10 } },
      },
    });
    expect(units.vcpuMsPer1kLines).toBe(0);
    expect(units.summaryVcpuMsPer1kLines).toBe(10);
    expect(units.vcpuMsPer1kLines + units.summaryVcpuMsPer1kLines).toBe(10);
    expect(measuredUnits.summaryVcpuMsPer1kLines).toBeUndefined();
    const estimate = estimateMonthly(
      { ...DEFAULT_INPUTS, edgeRequestsPerDay: 1_000 },
      { ...units, appVcpuMsPerDay: 0 },
      { ...zeroRates, compute_vcpu_rate_per_million_ms: 1e6 },
    );
    expect(
      line(estimate, "compute").usd + line(estimate, "summaries").usd,
    ).toBe(10 * 30);
  });

  it("attributes a bill smaller than the per-file default entirely to files", () => {
    const { units, measuredUnits } = measureEstimate({
      ...base,
      billing: { ...billing, kvClassA: 15 },
    });
    expect(units.kvClassAPerFile).toBe(0.15);
    expect(units.kvClassAPerDay).toBe(0);
    expect(measuredUnits.kvClassAPerFile).toBe(true);
  });

  it("keeps storage-operation defaults when usage logging is off", () => {
    const { units, measuredUnits } = measureEstimate(base);
    expect(units.fosClassAPerFile).toBe(DEFAULT_UNITS.fosClassAPerFile);
    expect(measuredUnits.fosClassAPerDay).toBeUndefined();
  });

  it("adds Fastly's unseen logging PUT to each file's logged operations", () => {
    const { units } = measureEstimate({
      ...base,
      fos: [
        { caller: "worker:ingest", op_class: "A", op_count: 100 },
        { caller: "cron:raw_delete", op_class: "A", op_count: 50 },
        { caller: "cron:ingest", op_class: "B", op_count: 100 },
        { caller: "cron:log_flush", op_class: "A", op_count: 70 },
        { caller: "api:query", op_class: "B", op_count: 700 },
        { caller: "api:query", op_class: "CDN_HIT", op_count: 9_999 },
      ].map((row) => ({
        timestamp: 0,
        op: "PUT",
        dataset: "x",
        cdn_cache: "DIRECT" as const,
        bytes_in: 0,
        bytes_out: 0,
        bytes_deleted: 0,
        est_cost_usd: 0,
        ...row,
        op_class: row.op_class as "A" | "B" | "CDN_HIT",
      })),
    });
    expect(units.fosClassAPerFile).toBe(2.5);
    expect(units.fosClassBPerFile).toBe(1);
    expect(units.fosClassAPerDay).toBe(10);
    expect(units.fosClassBPerDay).toBe(100);
  });

  it("keeps per-file defaults and charges billed totals as fixed when nothing converted", () => {
    const { units, measuredUnits, inputs } = measureEstimate({
      ...base,
      files: [],
      parquet: { bytes: 0, rows: 0 },
    });
    expect(units.workerRequestsPerFile).toBe(
      DEFAULT_UNITS.workerRequestsPerFile,
    );
    expect(measuredUnits.vcpuMsPer1kLines).toBeUndefined();
    expect(inputs.gzBytesPerLine).toBe(DEFAULT_INPUTS.gzBytesPerLine);
    expect(units.kvClassAPerDay).toBeCloseTo(
      (7_000 + DEFAULT_UNITS.kvClassAPerFile * 100) / 7,
    );
  });

  it("measures window files per raw file from the ingest runs", () => {
    const { units, measuredUnits } = measureEstimate({
      ...base,
      parquet: { bytes: 60_000, rows: 1_000, files: 25 },
    });
    expect(units.windowFilesPerFile).toBe(0.25);
    expect(measuredUnits.windowFilesPerFile).toBe(true);
  });

  it("starts the compaction toggle from the service's setting", () => {
    const off = measureEstimate(base);
    expect(off.compaction).toEqual(DEFAULT_COMPACTION);
    expect(off.serviceCompaction).toBeNull();
    const setting = { mode: "size_target" as const, size_target_mb: 128 };
    const on = measureEstimate({
      ...base,
      service: { ...base.service!, compaction: setting },
    });
    expect(on.compaction).toEqual({
      ...DEFAULT_COMPACTION,
      enabled: true,
      targetMb: 128,
    });
    expect(on.serviceCompaction).toEqual(setting);
    const explicitOff = measureEstimate({
      ...base,
      service: {
        ...base.service!,
        compaction: { mode: "off", size_target_mb: 32 },
      },
    });
    expect(explicitOff.compaction).toMatchObject({
      enabled: false,
      targetMb: 32,
    });
  });

  it("measures RUM pageviews, rows per line and Parquet bytes from rum_ingest runs", () => {
    const { inputs, measuredInputs } = measureEstimate({
      ...base,
      service: {
        ...base.service!,
        rum_enabled: true,
        rum_log_period: 60,
        rum_sample_rate: 0.5,
        rum_retention_days: 7,
      },
      rum: { lines: 70_000, rows: 560_000, bytes: 28_000_000 },
    });
    expect(inputs).toMatchObject({
      rumLogPeriodS: 60,
      rumSamplePct: 50,
      rumRetentionDays: 7,
      rumRowsPerLine: 8,
      rumParquetBytesPerRow: 50,
      // 10,000 lines a day from half the sessions, one beacon per pageview.
      rumPageviewsPerDay: 20_000,
    });
    expect(measuredInputs.rumPageviewsPerDay).toBe(true);
  });

  it("falls back to every default without an install", () => {
    const { inputs, units } = measureEstimate({
      ...base,
      service: null,
      traffic: null,
      billing: null,
      files: [],
      parquet: { bytes: 0, rows: 0 },
      fixedStoredBytes: null,
    });
    expect(inputs).toEqual(DEFAULT_INPUTS);
    expect(units).toEqual(DEFAULT_UNITS);
    expect(DEFAULT_UNITS.secrets).toBe(1);
    expect(DEFAULT_UNITS.fosClassAPerDay).toBe(2_000 + 720 * 2);
    expect(DEFAULT_UNITS.kvClassAPerDay).toBe(288 * 2);
    expect(DEFAULT_UNITS.kvClassBPerDay).toBe(10_000);
    expect(DEFAULT_UNITS.kvClassAPerFile).toBe(0.25);
    expect(DEFAULT_UNITS.kvClassBPerFile).toBe(0.5);
    expect(DEFAULT_UNITS.workerRequestsPerFile).toBe(0.125);
    expect(DEFAULT_UNITS.windowFilesPerFile).toBe(0.125);
    expect(DEFAULT_UNITS.vcpuMsPer1kLines).toBe(64);
    expect(DEFAULT_UNITS.summaryVcpuMsPer1kLines).toBe(32);
    expect(DEFAULT_INPUTS.parquetBytesPerRow).toBe(88);
    expect(DEFAULT_INPUTS.rumParquetBytesPerRow).toBe(30);
    expect(COMPACT_GZ_BYTES_PER_LINE).toBe(165);
    expect(COMPACT_RUM_GZ_BYTES_PER_LINE).toBe(95);
  });
});

describe("contract CPU terms parity across /calculator and /usage", () => {
  const cpuOnlyRates = {
    ...zeroRates,
    compute_vcpu_free_ms_per_request: 20,
    compute_vcpu_excess_rate_per_second: 0.00005,
    compute_vcpu_rounding_ms: 10,
  };

  it("applies the 20 ms free allowance and nearest-10ms rounding per execution in the calculator", () => {
    // 1,000 app requests/day:
    // At 24 ms/request (4 ms excess => rounds to 0 ms): $0
    const at24 = estimateMonthly(
      { ...DEFAULT_INPUTS, edgeRequestsPerDay: 0, requestsPerDay: 0 },
      { ...zeroUnits, appRequestsPerDay: 1_000, appVcpuMsPerDay: 24_000 },
      cpuOnlyRates,
    );
    expect(line(at24, "compute").usd).toBe(0);

    // At 26 ms/request (6 ms excess => rounds to 10 ms): 10 ms * 1,000/day * 30 days = 300,000 ms => $0.015
    const at26 = estimateMonthly(
      { ...DEFAULT_INPUTS, edgeRequestsPerDay: 0, requestsPerDay: 0 },
      { ...zeroUnits, appRequestsPerDay: 1_000, appVcpuMsPerDay: 26_000 },
      cpuOnlyRates,
    );
    expect(line(at26, "compute").usd).toBeCloseTo((300_000 / 1000) * 0.00005);
  });

  it("produces the exact same CPU cost in /calculator and /usage for the same per-execution input", () => {
    // 100 executions/day over 30 days = 3,000 executions, each spending 46 ms vCPU (26 ms excess => 30 ms billed).
    const calc = estimateMonthly(
      { ...DEFAULT_INPUTS, edgeRequestsPerDay: 0, requestsPerDay: 0 },
      { ...zeroUnits, appRequestsPerDay: 100, appVcpuMsPerDay: 4_600 },
      cpuOnlyRates,
    );
    const calcCpuUsd = line(calc, "compute").usd;

    const usageSummary = summarizeUsage({
      ...fixtureUsageInput,
      days: 30,
      settings: {
        ...fixtureUsageInput.settings,
        ...cpuOnlyRates,
        compute_request_rate_per_million: 0,
      },
      rows: {
        fos_usage: [],
        cron_runs: [],
        cron_ticks: [],
        api_telemetry: [
          {
            timestamp: 1_761_800_000_000,
            source: "compute_api",
            path: "/api/files",
            files_fetched: 3_000,
            vcpu_ms: 3_000 * 46,
          },
        ],
      },
    });
    const usageIncurred = incurredCost({
      settings: {
        ...fixtureUsageInput.settings,
        ...cpuOnlyRates,
        compute_request_rate_per_million: 0,
      },
      hours: 720,
      billedGbHours: 0,
      classA: 0,
      classB: 0,
      loggedCdnBytes: 0,
      loggedComputeRequests: usageSummary.computeRequests,
      loggedComputeVcpuMs: usageSummary.computeVcpuMs,
      loggedComputeSegments: usageSummary.computeSegments,
      billing: null,
    });
    const usageCpuUsd = usageIncurred.lines.find((l) => l.id === "compute")!.usd!;
    expect(calcCpuUsd).toBeCloseTo((3_000 * 30 / 1000) * 0.00005);
    expect(usageCpuUsd).toBeCloseTo(calcCpuUsd);
  });
});
