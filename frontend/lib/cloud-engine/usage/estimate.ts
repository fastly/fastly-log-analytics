import type {
  ComputeBilling,
  FosUsageRow,
  UsageLoggingSettings,
} from "./summary";
import { billedVcpuForExecutions } from "./pricing";
import {
  INGEST_ASSUMPTIONS,
  INGEST_LIMITS,
  ingestShape,
  ingestTick,
} from "./ingestShape";
import { DAY_MS, LIST_RATES } from "./rates";
import {
  KV_BINARY_GIB,
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

export const MONTH_DAYS = 30;
export const DEFAULT_MIN_BILLED_DAYS = LIST_RATES.min_billed_days;

/** The logged service and its logging settings, as in v3's prefill. */
import type { components } from "../cloud-api.generated";

export interface EstimateInputs {
  requestsPerDay: number;
  edgeRequestsPerDay: number;
  /** Logging condition `fastly.ff.visits_this_service == 0`: only edge requests are logged. */
  edgeOnly: boolean;
  samplePct: number;
  logPeriodS: number;
  /** Raw files Fastly writes per log period, about one per POP node with traffic. */
  filesPerPeriod: number;
  gzBytesPerLine: number;
  /** Decompressed ÷ stored raw bytes (zstd); measured from `ingest_files.raw_bytes` when present. */
  rawBytesPerGzByte: number;
  parquetBytesPerRow: number;
  retentionDays: number;
  /** Raw files are deleted after conversion and billed only for the minimum days. */
  deleteAfter: boolean;
  minBilledDays: number;
  /** Pageviews a day on pages carrying the RUM tracker; 0 when RUM is off. */
  rumPageviewsPerDay: number;
  /** Percent of browser sessions the tracker samples (`rum.sample_rate`). */
  rumSamplePct: number;
  /** Batched `/rum-beacon` requests per sampled pageview: one per flush. */
  rumBeaconsPerPageview: number;
  rumGzBytesPerLine: number;
  /** Vitals and error rows one batched beacon line expands into. */
  rumRowsPerLine: number;
  rumParquetBytesPerRow: number;
  rumLogPeriodS: number;
  rumRetentionDays: number;
}

/** What this install costs per day at rest, per raw file and per 1,000 logged lines. */
export interface UnitCosts {
  appRequestsPerDay: number;
  appVcpuMsPerDay: number;
  frontRequestsPerDay: number;
  deliveryGbPerDay: number;
  /** Fastly POPs that run the front clock health checks (`flp_clock`, `flp_flush`). */
  clockProbeSources: number;
  /** Response bytes per clock probe. */
  clockProbeBytes: number;
  fosClassAPerDay: number;
  fosClassBPerDay: number;
  kvClassAPerDay: number;
  kvClassBPerDay: number;
  fixedStoredGb: number;
  secrets: number;
  workerRequestsPerFile: number;
  frontRequestsPerFile: number;
  fosClassAPerFile: number;
  fosClassBPerFile: number;
  kvClassAPerFile: number;
  kvClassBPerFile: number;
  /** Parquet window files the ingest stage writes per raw file; each gets a summary. */
  windowFilesPerFile: number;
  vcpuMsPer1kLines: number;
  /** Worker CPU to write window summaries; maintenance is in the fixed app CPU. */
  summaryVcpuMsPer1kLines: number;
  /** Ceiling a busy window's summary file approaches, above its fixed footer. */
  summaryMaxKbPerWindow: number;
  /** Replaced partial bundles, still charged for the minimum storage duration. */
  summaryPartialGbPerDay: number;
  /** Compaction CPU per MB of column chunks copied into a compact. */
  compactVcpuMsPerMb: number;
}

/** Per-service compaction setting (`svc:<id>` `compaction`), server side in Task 23. */
export type CompactionSetting = components["schemas"]["CompactionSetting"];

/** The calculator's compaction what-if. */
export interface CompactionInputs {
  enabled: boolean;
  targetMb: number;
  /** Hours of request logs fetched as Parquet files a day (dashboard filters and explorer). */
  hoursReadPerDay: number;
}

export const DEFAULT_COMPACTION: CompactionInputs = {
  enabled: true,
  targetMb: 128,
  hoursReadPerDay: 24,
};

/** One execution: 4 secret reads + create + complete + 2 calls per source within 32. */
export const COMPACT_SOURCES_PER_EXECUTION = 13;
/**
 * The request compaction cron job's cadence once caught up (`schedule::COMPACT_INTERVAL_S`).
 * Each run is a compact-only stage execution on the worker service that reads the installed
 * service and the catalog head, and returns without a merge when nothing is due.
 */
export const COMPACT_JOB_INTERVAL_S = 30;
/** vCPU of one compaction job run that finds nothing to merge (assumed). */
export const COMPACT_DISPATCH_VCPU_MS = 8;
/** Output one execution can stream within its heap and 60 s. */
export const COMPACT_MAX_OUTPUT_MB = 250;
/** A one-row summary file: footer and schema of the long table (native, ZSTD(1)). */
export const SUMMARY_BASE_BYTES = 23_000;
/** Rows per window at which a summary reaches half its ceiling (native fit, 1–40,000 rows). */
export const SUMMARY_HALF_ROWS = 2_600;

export type EstimateRates = Pick<
  UsageLoggingSettings,
  | "storage_rate_per_gb_month"
  | "class_a_rate_per_1k"
  | "class_b_rate_per_10k"
  | "cdn_egress_rate_per_gb"
  | "cdn_request_rate_per_10k"
  | "compute_request_rate_per_million"
  | "compute_vcpu_rate_per_million_ms"
  | "compute_vcpu_free_ms_per_request"
  | "compute_vcpu_excess_rate_per_second"
  | "compute_vcpu_rounding_ms"
  | "kv_class_a_rate_per_million"
  | "kv_class_b_rate_per_million"
  | "secret_rate_per_month"
>;

export const LIST_PRICE_RATES: EstimateRates = {
  storage_rate_per_gb_month: LIST_RATES.storage_rate_per_gb_month,
  class_a_rate_per_1k: LIST_RATES.class_a_rate_per_1k,
  class_b_rate_per_10k: LIST_RATES.class_b_rate_per_10k,
  cdn_egress_rate_per_gb: LIST_RATES.cdn_egress_rate_per_gb,
  cdn_request_rate_per_10k: LIST_RATES.cdn_request_rate_per_10k,
  compute_request_rate_per_million: LIST_RATES.compute_request_rate_per_million,
  compute_vcpu_rate_per_million_ms: LIST_RATES.compute_vcpu_rate_per_million_ms,
  compute_vcpu_free_ms_per_request: LIST_RATES.compute_vcpu_free_ms_per_request,
  compute_vcpu_excess_rate_per_second:
    LIST_RATES.compute_vcpu_excess_rate_per_second,
  compute_vcpu_rounding_ms: LIST_RATES.compute_vcpu_rounding_ms,
  kv_class_a_rate_per_million: LIST_RATES.kv_class_a_rate_per_million,
  kv_class_b_rate_per_million: LIST_RATES.kv_class_b_rate_per_million,
  secret_rate_per_month: LIST_RATES.secret_rate_per_month,
};

/** Compact wire format (`zstd` + positional array) raw bytes per request log line. */
export const COMPACT_GZ_BYTES_PER_LINE = 165;
/** Compact wire format (`zstd` + positional array) raw bytes per RUM beacon log line. */
export const COMPACT_RUM_GZ_BYTES_PER_LINE = 95;

/** A small service with a five-minute log period, for visitors without an install. */
export const DEFAULT_INPUTS: EstimateInputs = {
  requestsPerDay: 1_000_000,
  edgeRequestsPerDay: 1_000_000,
  edgeOnly: true,
  samplePct: 100,
  logPeriodS: 300,
  filesPerPeriod: 109,
  gzBytesPerLine: COMPACT_GZ_BYTES_PER_LINE,
  rawBytesPerGzByte: INGEST_LIMITS.rawBytesPerGzByte,
  parquetBytesPerRow: 88,
  retentionDays: 30,
  deleteAfter: true,
  minBilledDays: 30,
  rumPageviewsPerDay: 0,
  rumSamplePct: 100,
  rumBeaconsPerPageview: 1,
  rumGzBytesPerLine: COMPACT_RUM_GZ_BYTES_PER_LINE,
  rumRowsPerLine: 6,
  rumParquetBytesPerRow: 30,
  rumLogPeriodS: 300,
  rumRetentionDays: 30,
};

/**
 * Idle-ingest dispatches coalesce their KV writes (`schedule.rs` `IDLE_WRITE_BATCH`): a batch is
 * flushed once every `IDLE_WRITE_BATCH` idle periods, not every period.
 */
export const IDLE_WRITE_BATCH = 6;

/**
 * Idle-ingest KV Class A writes a day. An idle dispatch runs once per log period (never faster
 * than every 5 s) and writes 2 keys (`cron:state` and the outbox bucket), batched one flush per
 * `IDLE_WRITE_BATCH` periods: 96/day at 300 s, 480/day at 60 s.
 */
export function idleIngestKvClassAPerDay(logPeriodS: number): number {
  return (86_400 / Math.max(5, logPeriodS) / IDLE_WRITE_BATCH) * 2;
}

/**
 * Unit costs measured on the reference install (2026-10-31: 2.3 billed days,
 * 8,159 raw files, 1.0M lines), adjusted for consolidated secrets, low-cardinality
 * Parquet sorting, column-specific encodings (DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT,
 * 64B stats truncation), and deduplicated single-pass inline summary construction.
 * Object Storage operations are code-derived because usage logging was off. The
 * measured prefill replaces each one it can derive. Fixed costs also include
 * a busy-bundle allowance: 720 turns/day, 795 ms vCPU, a 500 KB partial read,
 * one LIST and a partial PUT per turn (deletes are free). Billing replaces these fixed totals.
 *
 * Idle ingest dispatches write 2 KV keys each (`cron:state` and the outbox bucket), batched one
 * flush per `IDLE_WRITE_BATCH` idle periods (`idleIngestKvClassAPerDay`): `kvClassAPerDay` holds
 * the default-period value, and `estimateMonthly` rescales it to the chosen log period.
 * Fixed daily `fosClassAPerDay`, `kvClassBPerDay`, and `fixedStoredGb` also cover event-driven
 * Cloud DR state snapshots (`meta/cloud_dr_state.json`, classified under `pq_ops`), written when
 * service settings or saved views change and folded automatically into measured `fos_usage`.
 */
export const DEFAULT_UNITS: UnitCosts = {
  appRequestsPerDay: 62_000,
  appVcpuMsPerDay: 250_000 + 720 * 795,
  frontRequestsPerDay: 720,
  // Measured on a live install: 9,136,736 front requests a day at 1 s ≈ 2 checks × 53 POPs × 86,400,
  // and 18.7 GB ≈ 2 KB a probe.
  clockProbeSources: 53,
  clockProbeBytes: 2_000,
  deliveryGbPerDay: (720 * 500_000) / 1e9,
  fosClassAPerDay: 2_000 + 720 * 2,
  fosClassBPerDay: 1_000 + 720,
  kvClassAPerDay: idleIngestKvClassAPerDay(DEFAULT_INPUTS.logPeriodS),
  kvClassBPerDay: 10_000,
  fixedStoredGb: 0.006,
  secrets: 1,
  workerRequestsPerFile: 0.125,
  frontRequestsPerFile: 1,
  // 1 Fastly raw-log PUT + windowFilesPerFile window Parquet PUTs + ~1/22 shard intent-manifest
  // PUT (1 + 0.125 + ~0.045 ≈ 1.17). Per-window summaries and compaction PUTs/deletes are counted
  // separately in their own feature records.
  fosClassAPerFile: 1.17,
  fosClassBPerFile: 1,
  kvClassAPerFile: 0.25,
  kvClassBPerFile: 0.5,
  windowFilesPerFile: 0.125,
  vcpuMsPer1kLines: 64,
  summaryVcpuMsPer1kLines: 32,
  summaryMaxKbPerWindow: 380,
  summaryPartialGbPerDay: (720 * 500_000) / 1e9,
  compactVcpuMsPerMb: 22,
};

/** Logged lines per day after the logging condition and sampling. */
export function loggedLinesPerDay(inputs: EstimateInputs): number {
  const requests = inputs.edgeOnly
    ? inputs.edgeRequestsPerDay
    : inputs.requestsPerDay;
  return Math.max(0, requests * (inputs.samplePct / 100));
}

/**
 * Raw files per day. Each writing node emits one file per log period it logged
 * a line in, so λ lines spread over N nodes fill N(1 − e^(−λ/N)) files.
 */
export function rawFilesPerDay(inputs: EstimateInputs): number {
  const lines = loggedLinesPerDay(inputs);
  if (lines <= 0 || inputs.logPeriodS <= 0) return 0;
  const periods = 86_400 / inputs.logPeriodS;
  const nodes = Math.max(1, inputs.filesPerPeriod);
  return periods * nodes * -Math.expm1(-lines / periods / nodes);
}

/** Bytes of one summary file over `rows` rows: a fixed footer growing toward a ceiling. */
export function summaryBytes(rows: number, maxKb: number): number {
  const r = Math.max(0, rows);
  return SUMMARY_BASE_BYTES + (maxKb * 1000 * r) / (r + SUMMARY_HALF_ROWS);
}

export interface CompactionHour {
  executions: number;
  sourcesRead: number;
  /** Multipart parts uploaded across all merge executions (output bytes / part size). */
  partsUploaded: number;
  mbRewritten: number;
  /** Bytes of sources and intermediate compacts deleted after merging. */
  deletedBytes: number;
  filesLeft: number;
}

/**
 * One closed hour of `files` equal window files of `fileBytes`. Each execution merges up to
 * 13 sources below the target without exceeding it or the per-execution output, and merged
 * compacts still below the target become sources again, until the hour holds its target count.
 */
export function compactionPlan(
  files: number,
  fileBytes: number,
  compaction: CompactionInputs,
): CompactionHour {
  const out: CompactionHour = {
    executions: 0,
    sourcesRead: 0,
    partsUploaded: 0,
    mbRewritten: 0,
    deletedBytes: 0,
    filesLeft: Math.max(0, Math.round(files)),
  };
  const cap = Math.min(compaction.targetMb, COMPACT_MAX_OUTPUT_MB) * 1e6;
  const total = out.filesLeft * fileBytes;
  const wanted = Math.max(1, Math.ceil(total / (compaction.targetMb * 1e6)));
  let size = fileBytes;
  while (out.filesLeft > wanted) {
    const fanIn = Math.min(
      COMPACT_SOURCES_PER_EXECUTION,
      Math.floor(cap / size),
    );
    if (fanIn < 2) break;
    const full = Math.floor(out.filesLeft / fanIn);
    const rest = out.filesLeft % fanIn;
    const merged = full * fanIn + (rest > 1 ? rest : 0);
    const executions = full + (rest > 1 ? 1 : 0);
    out.executions += executions;
    out.sourcesRead += merged;
    out.partsUploaded +=
      full * Math.max(1, Math.ceil((fanIn * size) / INGEST_LIMITS.partBytes)) +
      (rest > 1 ? Math.max(1, Math.ceil((rest * size) / INGEST_LIMITS.partBytes)) : 0);
    out.mbRewritten += (merged * size) / 1e6;
    out.deletedBytes += merged * size;
    out.filesLeft = out.filesLeft - merged + executions;
    size = total / out.filesLeft;
  }
  return out;
}

export interface SummaryDay {
  windowFilesPerDay: number;
  classAPerDay: number;
  classBPerDay: number;
  vcpuMsPerDay: number;
  storedGb: number;
}

/** The install's request-ingest fan-out (config `ingest_relay`). */
export interface IngestOptions {
  relay: number;
}

export const DEFAULT_INGEST: IngestOptions = { relay: 0 };

/** Daily work the high-RPS ingest paths add; all zero for small files without relay. */
export interface IngestScaleDay {
  segmentsPerFile: number;
  shardsPerDay: number;
  extraWorkerRequestsPerDay: number;
  extraGetsPerDay: number;
  /** GB a day downloaded again because every segment reads the whole raw file. */
  extraRawGbPerDay: number;
  extraWindowsPerDay: number;
  skipVcpuMsPerDay: number;
  windowParts: number;
  multipartClassAPerDay: number;
  relayRequestsPerDay: number;
  relayVcpuMsPerDay: number;
}

export interface CompactionDay {
  /** Compaction job runs a day (one worker-service request each), merges or not. */
  dispatchesPerDay: number;
  /** Worker executions that merged files. */
  executionsPerDay: number;
  classAPerDay: number;
  classBPerDay: number;
  kvClassAPerDay: number;
  kvClassBPerDay: number;
  vcpuMsPerDay: number;
  /** Steady GB billed for merged-away objects' minimum days. */
  extraStoredGb: number;
  fileReadsSavedPerDay: number;
}

export type EstimateFeature =
  | "ingest"
  | "summaries"
  | "segments"
  | "multipart"
  | "relay"
  | "rum"
  | "compaction";

/** One feature's monthly usage of each Fastly product. vCPU is priced per execution group. */
export interface ProductUsage {
  storageGb: number;
  fosA: number;
  fosB: number;
  egressGb: number;
  frontRequests: number;
  computeRequests: number;
  vcpuUsd: number;
  vcpuMs: number;
  kvA: number;
  kvB: number;
}

const NO_USAGE: ProductUsage = {
  storageGb: 0,
  fosA: 0,
  fosB: 0,
  egressGb: 0,
  frontRequests: 0,
  computeRequests: 0,
  vcpuUsd: 0,
  vcpuMs: 0,
  kvA: 0,
  kvB: 0,
};

/** One line per Fastly product. */
export interface EstimateLine {
  id: "storage" | "delivery" | "compute" | "kv" | "secrets" | "config";
  label: string;
  detail: string;
  /** How the line is derived, for lines that only appear under some inputs. */
  help?: string;
  usd: number;
  /** Priced components, shown under the total; `usd` is their sum. */
  parts?: EstimatePart[];
}

export interface EstimatePart {
  label: string;
  usd: number;
}

/** A line's components and their sum, so the total can't drift from what it lists. */
function priced(parts: EstimatePart[]): { usd: number; parts: EstimatePart[] } {
  return { usd: parts.reduce((sum, part) => sum + part.usd, 0), parts };
}

export interface RumDay {
  beaconsPerDay: number;
  linesPerDay: number;
  rowsPerDay: number;
  filesPerDay: number;
  runsPerDay: number;
  storedGb: number;
}

export interface Estimate {
  linesPerDay: number;
  filesPerDay: number;
  lines: EstimateLine[];
  total: number;
  totals: ProductUsage;
  /** Monthly USD attributable to telemetry and operational logs at rest and in fixed FOS ops. */
  telemetryUsd: number;
  summaries: SummaryDay;
  ingest: IngestScaleDay;
  compaction: CompactionDay | null;
  rum: RumDay | null;
  /** Monthly USD by the feature that drives it, across products; absent when the feature doesn't apply. */
  featureUsd: Partial<Record<EstimateFeature, number>>;
}

const count = (n: number) => Math.round(n).toLocaleString("en-US");
const gb = (n: number) =>
  n < 10 ? n.toFixed(2) : Math.round(n).toLocaleString("en-US");

/** Monthly cost: what the install spends at rest plus per-file and per-line work at this traffic. */
/**
 * The front clock health checks' interval in seconds, as reconcile sets it
 * (`clock_interval_ms` in compute/cloud/src/admin/reconcile.rs): half the shortest log
 * period the clock serves (request, and RUM when it has pageviews, whose ingest runs at least
 * every 5 s), between 1 and 30 s.
 */
export function clockIntervalS(inputs: Pick<EstimateInputs, "logPeriodS" | "rumLogPeriodS" | "rumPageviewsPerDay">): number {
  let period = Math.max(1, inputs.logPeriodS);
  if (inputs.rumPageviewsPerDay > 0) period = Math.min(period, Math.max(5, inputs.rumLogPeriodS));
  return Math.min(30, Math.max(1, period / 2));
}

/**
 * Billed front requests a day from the two clock health checks: every probing POP sends both at
 * the clock interval. Billed totals can't stand in for this, since they hold whatever interval the
 * window ran at.
 */
export function clockProbesPerDay(
  inputs: Pick<EstimateInputs, "logPeriodS" | "rumLogPeriodS" | "rumPageviewsPerDay">,
  units: Pick<UnitCosts, "clockProbeSources">,
): number {
  return (2 * units.clockProbeSources * 86_400) / clockIntervalS(inputs);
}

export function estimateMonthly(
  inputs: EstimateInputs,
  units: UnitCosts,
  rates: EstimateRates,
  compaction: CompactionInputs = DEFAULT_COMPACTION,
  ingest: IngestOptions = DEFAULT_INGEST,
): Estimate {
  const L = loggedLinesPerDay(inputs);
  const F = rawFilesPerDay(inputs);
  const month = (perDay: number) => perDay * MONTH_DAYS;

  const rawGbPerDay = (L * inputs.gzBytesPerLine) / 1e9;
  const parquetGbPerDay = (L * inputs.parquetBytesPerRow) / 1e9;
  const rawGbMonths =
    rawGbPerDay *
    Math.max(
      inputs.deleteAfter ? 0 : inputs.retentionDays,
      inputs.minBilledDays,
    );
  const parquetGbMonths =
    parquetGbPerDay * Math.max(inputs.retentionDays, inputs.minBilledDays);
  const storedGb = rawGbMonths + parquetGbMonths + units.fixedStoredGb;

  const classA = month(units.fosClassAPerDay + units.fosClassAPerFile * F);
  const classB = month(units.fosClassBPerDay + units.fosClassBPerFile * F);
  const probeIntervalS = clockIntervalS(inputs);
  const probesPerDay = clockProbesPerDay(inputs, units);
  const frontRequests = month(
    units.frontRequestsPerDay + units.frontRequestsPerFile * F + probesPerDay,
  );
  const deliveryGb = month(
    units.deliveryGbPerDay + rawGbPerDay + (probesPerDay * units.clockProbeBytes) / 1e9,
  );
  const computeRequests = month(
    units.appRequestsPerDay + units.workerRequestsPerFile * F,
  );
  const vcpuMs = month(
    units.appVcpuMsPerDay + (units.vcpuMsPer1kLines * L) / 1000,
  );
  // Idle-ingest KV writes scale with the log period. The calculator passes DEFAULT_UNITS and lets
  // the period vary, so rescale the default; a measured (or custom) base passes through unchanged.
  const kvClassAPerDay =
    units.kvClassAPerDay === idleIngestKvClassAPerDay(DEFAULT_INPUTS.logPeriodS)
      ? idleIngestKvClassAPerDay(inputs.logPeriodS)
      : units.kvClassAPerDay;
  const kvA = month(kvClassAPerDay + units.kvClassAPerFile * F);
  const kvB = month(units.kvClassBPerDay + units.kvClassBPerFile * F);
  const storedDays = Math.max(inputs.retentionDays, inputs.minBilledDays);
  const classAUsd = (n: number) => (n / 1000) * rates.class_a_rate_per_1k;
  const classBUsd = (n: number) => (n / 10_000) * rates.class_b_rate_per_10k;
  const egressUsd = (egressGb: number) =>
    priceCdnEgress(egressGb, rates.cdn_egress_rate_per_gb);
  const frontUsd = (n: number) =>
    priceCdnRequests(n, rates.cdn_request_rate_per_10k);
  const requestUsd = (n: number) =>
    (n / 1e6) * rates.compute_request_rate_per_million;
  const vcpuExecUsd = (dailyVcpuMs: number, dailyExecutions: number) =>
    month(billedVcpuForExecutions(dailyVcpuMs, dailyExecutions, rates).usd);

  // Window summaries are read through the front service into 24 hour bundles, and those into a day bundle.
  const windowFiles = units.windowFilesPerFile * F;
  const summaryGbPerDay =
    (windowFiles *
      summaryBytes(
        windowFiles > 0 ? L / windowFiles : 0,
        units.summaryMaxKbPerWindow,
      ) +
      24 * summaryBytes(L / 24, units.summaryMaxKbPerWindow) +
      summaryBytes(L, units.summaryMaxKbPerWindow)) /
    1e9;
  const summaries: SummaryDay = {
    windowFilesPerDay: windowFiles,
    classAPerDay: windowFiles + 24 + 1,
    classBPerDay: windowFiles + 24,
    vcpuMsPerDay: (units.summaryVcpuMsPer1kLines * L) / 1000,
    storedGb:
      summaryGbPerDay * storedDays +
      units.summaryPartialGbPerDay * inputs.minBilledDays,
  };

  // A file split into S segments runs S workers, each reading it through the front service,
  // decompressing and skipping the segments before its own, and writing its own windows.
  const shape = ingestShape(L, F, inputs.gzBytesPerLine);
  const S = shape.segments;
  const split = S > 1 ? F : 0;
  const windowsPerShard =
    units.workerRequestsPerFile > 0
      ? units.windowFilesPerFile / units.workerRequestsPerFile
      : 1;
  const extraWindows =
    split * Math.max(0, S * windowsPerShard - units.windowFilesPerFile);
  const allWindows = windowFiles + extraWindows;
  const windowBytes =
    allWindows > 0 ? (L * inputs.parquetBytesPerRow) / allWindows : 0;
  const parts = Math.ceil(windowBytes / INGEST_LIMITS.partBytes);
  const tick = ingestTick(
    shape,
    F / 86_400,
    units.vcpuMsPer1kLines + units.summaryVcpuMsPer1kLines,
    ingest.relay,
  );
  const relayRequests = (tick.relayCallsPerTick * 86_400) / tick.tickSeconds;
  const ingestDay: IngestScaleDay = {
    segmentsPerFile: S,
    shardsPerDay: tick.shardsPerSecond * 86_400,
    extraWorkerRequestsPerDay:
      split * Math.max(0, S - units.workerRequestsPerFile),
    extraGetsPerDay: split * (S - 1),
    extraRawGbPerDay: (split * (S - 1) * shape.objectGz) / 1e9,
    extraWindowsPerDay: extraWindows,
    skipVcpuMsPerDay:
      split *
      (((INGEST_LIMITS.shardDecompressedBytes / 1024 / 1024) * S * (S - 1)) /
        2) *
      INGEST_ASSUMPTIONS.skipVcpuMsPerMib,
    windowParts: Math.max(1, parts),
    multipartClassAPerDay: parts > 1 ? allWindows * (parts + 1) : 0,
    relayRequestsPerDay: relayRequests,
    relayVcpuMsPerDay: relayRequests * INGEST_ASSUMPTIONS.relayVcpuMsPerCall,
  };

  let compactionDay: CompactionDay | null = null;
  if (compaction.enabled) {
    const filesPerHour = windowFiles / 24;
    const hour = compactionPlan(
      filesPerHour,
      filesPerHour > 0 ? (L * inputs.parquetBytesPerRow) / windowFiles : 0,
      compaction,
    );
    // Per execution: create and complete the multipart upload (2 Class A), plus one part PUT per
    // uploaded part; one LIST per closed hour finds the sources. DeleteObject(s) are Class A in
    // Fastly Object Storage, so each hour's merged-away sources cost one DeleteObjects call per
    // 1,000 objects. The source reads stay on Class B. The job itself runs every
    // COMPACT_JOB_INTERVAL_S, reading the installed service and the catalog head each time.
    const dispatches = 86_400 / COMPACT_JOB_INTERVAL_S;
    const deleteBatches = hour.sourcesRead > 0 ? Math.ceil(hour.sourcesRead / 1000) : 0;
    compactionDay = {
      dispatchesPerDay: dispatches,
      executionsPerDay: hour.executions * 24,
      classAPerDay:
        (hour.partsUploaded + 2 * hour.executions + deleteBatches) * 24 +
        (hour.executions > 0 ? 24 : 0),
      classBPerDay: hour.sourcesRead * 24,
      kvClassAPerDay: 2 * hour.executions * 24,
      kvClassBPerDay: hour.executions * 24 + 2 * dispatches,
      vcpuMsPerDay:
        hour.mbRewritten * units.compactVcpuMsPerMb * 24 +
        dispatches * COMPACT_DISPATCH_VCPU_MS,
      extraStoredGb: ((hour.deletedBytes * 24) / 1e9) * inputs.minBilledDays,
      fileReadsSavedPerDay:
        Math.max(0, Math.round(filesPerHour) - hour.filesLeft) *
        compaction.hoursReadPerDay,
    };
  }

  // Each feature's monthly usage of each Fastly product; the breakdown lists products, not features.
  const features: Partial<Record<EstimateFeature, ProductUsage>> = {};
  const record = (id: EstimateFeature, usage: Partial<ProductUsage>) => {
    features[id] = { ...NO_USAGE, ...usage };
  };

  // One worker execution writes both the window Parquet and its summary (worker.rs:582-602), so
  // price their combined vCPU-ms once across the worker executions instead of applying the
  // per-execution free allowance twice. Ingest keeps the window-write share; summaries take the
  // incremental excess, so ingest's worker vCPU + summaries' vCPU == the single combined price.
  const workerExecutionsPerDay = units.workerRequestsPerFile * F;
  const ingestOnlyWorkerVcpuUsd = vcpuExecUsd(
    (units.vcpuMsPer1kLines * L) / 1000,
    workerExecutionsPerDay,
  );
  const combinedWorkerVcpuUsd = vcpuExecUsd(
    ((units.vcpuMsPer1kLines + units.summaryVcpuMsPer1kLines) * L) / 1000,
    workerExecutionsPerDay,
  );

  record("ingest", {
    storageGb: storedGb,
    fosA: classA,
    fosB: classB,
    egressGb: deliveryGb,
    frontRequests,
    computeRequests,
    vcpuUsd:
      vcpuExecUsd(units.appVcpuMsPerDay, units.appRequestsPerDay) +
      ingestOnlyWorkerVcpuUsd,
    vcpuMs,
    kvA,
    kvB,
  });
  record("summaries", {
    storageGb: summaries.storedGb,
    fosA: month(summaries.classAPerDay),
    fosB: month(summaries.classBPerDay),
    frontRequests: month(summaries.classBPerDay),
    vcpuUsd: combinedWorkerVcpuUsd - ingestOnlyWorkerVcpuUsd,
    vcpuMs: month(summaries.vcpuMsPerDay),
  });
  const g = ingestDay;
  if (g.segmentsPerFile > 1) {
    // Raw files too big for one worker are cut into segments: extra worker runs, re-reads of the
    // whole compressed file through the front service, decompress-and-skip vCPU and extra windows.
    const reads = month(g.extraGetsPerDay + g.extraWindowsPerDay);
    record("segments", {
      storageGb:
        ((g.extraWindowsPerDay * SUMMARY_BASE_BYTES) / 1e9) * storedDays,
      fosA: month(2 * g.extraWindowsPerDay),
      fosB: reads,
      egressGb: month(g.extraRawGbPerDay),
      frontRequests: reads,
      computeRequests: month(g.extraWorkerRequestsPerDay),
      vcpuUsd: vcpuExecUsd(
        g.skipVcpuMsPerDay,
        g.extraWorkerRequestsPerDay > 0 ? g.extraWorkerRequestsPerDay : split * S,
      ),
      vcpuMs: month(g.skipVcpuMsPerDay),
    });
  }
  if (g.multipartClassAPerDay > 0) {
    record("multipart", { fosA: month(g.multipartClassAPerDay) });
  }
  if (g.relayRequestsPerDay > 0) {
    record("relay", {
      computeRequests: month(g.relayRequestsPerDay),
      vcpuUsd: vcpuExecUsd(g.relayVcpuMsPerDay, g.relayRequestsPerDay),
      vcpuMs: month(g.relayVcpuMsPerDay),
    });
  }
  let rumDay: RumDay | null = null;
  if (inputs.rumPageviewsPerDay > 0) {
    // Each sampled pageview's metrics and errors go out in one batched beacon per flush,
    // logged as one lean line on the customer's service and converted by rum_ingest.
    const beacons =
      inputs.rumPageviewsPerDay *
      (inputs.rumSamplePct / 100) *
      inputs.rumBeaconsPerPageview;
    const period = Math.max(5, inputs.rumLogPeriodS);
    const files = rawFilesPerDay({
      ...inputs,
      edgeOnly: true,
      edgeRequestsPerDay: beacons,
      samplePct: 100,
      logPeriodS: period,
    });
    const rows = beacons * inputs.rumRowsPerLine;
    const rawGb = (beacons * inputs.rumGzBytesPerLine) / 1e9;
    const rumStoredDays = Math.max(
      inputs.rumRetentionDays,
      inputs.minBilledDays,
    );
    rumDay = {
      beaconsPerDay: beacons,
      linesPerDay: beacons,
      rowsPerDay: rows,
      filesPerDay: files,
      runsPerDay: 86_400 / period,
      storedGb:
        rawGb *
          Math.max(
            inputs.deleteAfter ? 0 : inputs.rumRetentionDays,
            inputs.minBilledDays,
          ) +
        ((rows * inputs.rumParquetBytesPerRow) / 1e9) * rumStoredDays,
    };
    const r = rumDay;
    // One LIST of the raw RUM prefix per due run, plus request-like per-file work.
    record("rum", {
      storageGb: r.storedGb,
      fosA: month(r.runsPerDay + units.fosClassAPerFile * r.filesPerDay),
      fosB: month(units.fosClassBPerFile * r.filesPerDay),
      egressGb: month(rawGb),
      frontRequests: month(r.beaconsPerDay + units.frontRequestsPerFile * r.filesPerDay),
      computeRequests: month(units.workerRequestsPerFile * r.filesPerDay),
      vcpuUsd: vcpuExecUsd(
        (units.vcpuMsPer1kLines * r.linesPerDay) / 1000,
        units.workerRequestsPerFile * r.filesPerDay > 0
          ? units.workerRequestsPerFile * r.filesPerDay
          : r.runsPerDay,
      ),
      vcpuMs: month((units.vcpuMsPer1kLines * r.linesPerDay) / 1000),
      kvA: month(units.kvClassAPerFile * r.filesPerDay),
      kvB: month(units.kvClassBPerFile * r.filesPerDay),
    });
  }
  if (compactionDay) {
    const c = compactionDay;
    // Sources are read through the front service: a front request and a Class B miss each. Each
    // dashboard file read compaction saves is a front request and a Class B miss fewer.
    const saved = month(c.fileReadsSavedPerDay);
    record("compaction", {
      storageGb: c.extraStoredGb,
      fosA: month(c.classAPerDay),
      fosB: month(c.classBPerDay) - saved,
      frontRequests: month(c.classBPerDay) - saved,
      computeRequests: month(c.dispatchesPerDay + c.executionsPerDay),
      vcpuUsd: vcpuExecUsd(c.vcpuMsPerDay, c.dispatchesPerDay + c.executionsPerDay),
      vcpuMs: month(c.vcpuMsPerDay),
      kvA: month(c.kvClassAPerDay),
      kvB: month(c.kvClassBPerDay),
    });
  }

  const all = Object.values(features).reduce<ProductUsage>(
    (sum, u) => {
      for (const key of Object.keys(sum) as (keyof ProductUsage)[]) sum[key] += u[key];
      return sum;
    },
    { ...NO_USAGE },
  );
  const billedVcpuMs =
    rates.compute_vcpu_rate_per_million_ms > 0
      ? (all.vcpuUsd / rates.compute_vcpu_rate_per_million_ms) * 1e6
      : 0;
  const lines: EstimateLine[] = [
    {
      id: "storage",
      label: "Object Storage",
      detail: `${gb(all.storageGb)} GB-months · A ${count(all.fosA)} · B ${count(all.fosB)}`,
      help: "Raw logs, Parquet, summary bundles, RUM and fixed pq_ops storage (telemetry plus event-driven meta/cloud_dr_state.json DR snapshots), merged-away compaction sources billed their minimum days, and every feature's Class A and Class B operations.",
      ...priced([
        { label: "Storage", usd: priceStorageGb(all.storageGb, rates.storage_rate_per_gb_month) },
        { label: "Class A", usd: priceFosClassA(all.fosA, rates.class_a_rate_per_1k) },
        { label: "Class B", usd: priceFosClassB(all.fosB, rates.class_b_rate_per_10k) },
      ]),
    },
    {
      id: "delivery",
      label: "CDN delivery",
      detail: `${gb(all.egressGb)} GB (100 GB free) · ${count(all.frontRequests)} front reqs (1M free), incl. ${count(probesPerDay)} clock probes/day every ${probeIntervalS} s`,
      ...priced([
        { label: "Egress", usd: priceCdnEgress(all.egressGb, rates.cdn_egress_rate_per_gb) },
        { label: "Requests", usd: priceCdnRequests(all.frontRequests, rates.cdn_request_rate_per_10k) },
      ]),
    },
    {
      id: "compute",
      label: "Compute (app + worker)",
      detail: `${count(all.computeRequests)} reqs · ${count(all.vcpuMs)} vCPU ms`,
      ...priced([
        { label: "Requests", usd: priceComputeRequests(all.computeRequests, rates.compute_request_rate_per_million) },
        { label: "vCPU", usd: priceComputeVcpuMs(billedVcpuMs, rates.compute_vcpu_rate_per_million_ms) },
      ]),
    },
    {
      id: "kv",
      label: "KV Store",
      detail: `${gb(0.005 + (windowFiles * storedDays * 250) / KV_BINARY_GIB)} GiB · A ${count(all.kvA)} · B ${count(all.kvB)}`,
      ...priced([
        { label: "Storage", usd: priceKvStorageGb(0.005 + (windowFiles * storedDays * 250) / KV_BINARY_GIB) },
        { label: "Class A", usd: priceKvClassA(all.kvA, rates.kv_class_a_rate_per_million) },
        { label: "Class B", usd: priceKvClassB(all.kvB, rates.kv_class_b_rate_per_million) },
      ]),
    },
    {
      id: "secrets",
      label: "Secret Store",
      detail: `${count(units.secrets)} secrets`,
      usd: priceSecrets(units.secrets, rates.secret_rate_per_month),
    },
    {
      id: "config",
      label: "Config Store",
      detail: "No usage charge",
      usd: 0,
    },
  ];
  const totalEgressUsd = egressUsd(all.egressGb);
  const effectiveEgressUsdPerGb =
    all.egressGb > 0 ? totalEgressUsd / all.egressGb : 0;
  const totalFrontRequestUsd = frontUsd(all.frontRequests);
  const effectiveFrontUsdPerReq =
    all.frontRequests > 0 ? totalFrontRequestUsd / all.frontRequests : 0;
  const featureUsd = (u: ProductUsage) =>
    u.storageGb * rates.storage_rate_per_gb_month +
    classAUsd(u.fosA) +
    classBUsd(u.fosB) +
    u.egressGb * effectiveEgressUsdPerGb +
    u.frontRequests * effectiveFrontUsdPerReq +
    requestUsd(u.computeRequests) +
    u.vcpuUsd +
    (u.kvA / 1e6) * rates.kv_class_a_rate_per_million +
    (u.kvB / 1e6) * rates.kv_class_b_rate_per_million;
  const byFeature = Object.fromEntries(
    Object.entries(features).map(([id, u]) => [id, featureUsd(u)]),
  ) as Partial<Record<EstimateFeature, number>>;
  const telemetryUsd =
    units.fixedStoredGb * rates.storage_rate_per_gb_month +
    classAUsd(month(units.fosClassAPerDay)) +
    classBUsd(month(units.fosClassBPerDay)) +
    (month(kvClassAPerDay) / 1e6) * rates.kv_class_a_rate_per_million +
    (month(units.kvClassBPerDay) / 1e6) * rates.kv_class_b_rate_per_million;
  return {
    linesPerDay: L,
    filesPerDay: F,
    lines,
    total: lines.reduce((sum, line) => sum + line.usd, 0),
    totals: all,
    featureUsd: byFeature,
    telemetryUsd,
    summaries,
    ingest: ingestDay,
    compaction: compactionDay,
    rum: rumDay,
  };
}

/** Callers whose Object Storage operations scale with raw files rather than time. */
export const PER_FILE_CALLERS = [
  "cron:ingest",
  "worker:ingest",
  "cron:raw_delete",
];

export interface IngestFileRow {
  timestamp: number;
  status: string;
  raw_size: number;
  /** Decompressed bytes; absent on rows written before ingest recorded it. */
  raw_bytes?: number;
  rows: number;
}

export interface EstimateMeasurements {
  /** Length of the unit-cost window in days. */
  days: number;
  service: {
    log_period: number;
    sample_rate: number;
    edge_only: boolean;
    delete_after: boolean;
    log_retention_days: number;
    rum_enabled?: boolean;
    rum_log_period?: number;
    /** Fraction of sessions the RUM tracker samples. */
    rum_sample_rate?: number;
    rum_retention_days?: number;
    /** Absent until the server reports the Task 23 setting. */
    compaction?: CompactionSetting;
    /** Config `ingest_relay`; absent before Task 25. */
    ingest_relay?: number;
  } | null;
  /** Fastly-reported traffic per complete UTC day on the logged service. */
  traffic: Array<{ requests?: number; edge_requests?: number }> | null;
  billing: ComputeBilling | null;
  /** `null` when usage logging is off, so Object Storage operations were not recorded. */
  fos: FosUsageRow[] | null;
  files: IngestFileRow[];
  /** Parquet bytes, rows and window files the ingest stage wrote over the window. */
  parquet: { bytes: number; rows: number; files?: number };
  /** Beacon lines read and RUM rows and bytes written by rum_ingest over the window. */
  rum?: { lines: number; rows: number; bytes: number } | null;
  /** Bytes in non-request datasets at the latest storage reconcile. */
  fixedStoredBytes: number | null;
  minBilledDays: number;
}

export type Measured<T> = Partial<Record<keyof T, true>>;

export interface EstimatePrefill {
  inputs: EstimateInputs;
  units: UnitCosts;
  measuredInputs: Measured<EstimateInputs>;
  measuredUnits: Measured<UnitCosts>;
  /** Measured raw files and lines over the window, for the explanation. */
  window: { days: number; files: number; lines: number };
  compaction: CompactionInputs;
  /** The logged service's compaction setting, when the server reports one. */
  serviceCompaction: CompactionSetting | null;
  /** Whether RUM logging is enabled on the logged service. */
  rumEnabled?: boolean;
  /** The install's `ingest_relay`; 0 when not reported. */
  relay: number;
}

const CONVERTED = new Set(["converted", "split"]);

/**
 * Derives the estimator from this install: Fastly's own traffic and billing figures plus the
 * ingest logs. Per-file and per-line units come from the window's conversions; what is left of
 * each billed total is the install's fixed daily cost. Anything without data keeps its default.
 */
export function measureEstimate(m: EstimateMeasurements): EstimatePrefill {
  const inputs = { ...DEFAULT_INPUTS, minBilledDays: m.minBilledDays };
  const units = { ...DEFAULT_UNITS };
  const measuredInputs: Measured<EstimateInputs> = { minBilledDays: true };
  const measuredUnits: Measured<UnitCosts> = {};
  const setInput = <K extends keyof EstimateInputs>(
    key: K,
    value: EstimateInputs[K],
  ) => {
    inputs[key] = value;
    measuredInputs[key] = true;
  };
  const setUnit = (key: keyof UnitCosts, value: number) => {
    if (!Number.isFinite(value)) return;
    units[key] = Math.max(0, value);
    measuredUnits[key] = true;
  };

  const setting = m.service?.compaction ?? null;
  const compaction: CompactionInputs = setting
    ? {
        ...DEFAULT_COMPACTION,
        enabled: setting.mode === "size_target",
        targetMb: setting.size_target_mb,
      }
    : { ...DEFAULT_COMPACTION };
  if (m.service) {
    setInput("logPeriodS", m.service.log_period);
    setInput("samplePct", m.service.sample_rate);
    setInput("edgeOnly", m.service.edge_only);
    setInput("deleteAfter", m.service.delete_after);
    setInput("retentionDays", m.service.log_retention_days);
    if (m.service.rum_enabled) {
      if (m.service.rum_log_period !== undefined)
        setInput("rumLogPeriodS", m.service.rum_log_period);
      if (m.service.rum_sample_rate !== undefined)
        setInput("rumSamplePct", m.service.rum_sample_rate * 100);
      if (m.service.rum_retention_days !== undefined)
        setInput("rumRetentionDays", m.service.rum_retention_days);
    }
  }
  if (m.rum && m.rum.lines > 0) {
    setInput("rumRowsPerLine", m.rum.rows / m.rum.lines);
    if (m.rum.rows > 0)
      setInput("rumParquetBytesPerRow", m.rum.bytes / m.rum.rows);
    const sampled = (inputs.rumSamplePct / 100) * inputs.rumBeaconsPerPageview;
    if (sampled > 0)
      setInput("rumPageviewsPerDay", m.rum.lines / m.days / sampled);
  }
  const days = m.traffic?.filter((day) => day.requests !== undefined) ?? [];
  if (days.length > 0) {
    const avg = (key: "requests" | "edge_requests") =>
      days.reduce((sum, day) => sum + (day[key] ?? 0), 0) / days.length;
    setInput("requestsPerDay", Math.round(avg("requests")));
    if (days.every((day) => day.edge_requests != null))
      setInput("edgeRequestsPerDay", Math.round(avg("edge_requests")));
  }

  const converted = m.files.filter((file) => CONVERTED.has(file.status));
  const F = converted.length;
  const lines = converted.reduce((sum, file) => sum + file.rows, 0);
  const rawBytes = converted.reduce((sum, file) => sum + file.raw_size, 0);
  if (lines > 0) setInput("gzBytesPerLine", rawBytes / lines);
  const sized = converted.filter((file) => (file.raw_bytes ?? 0) > 0 && file.raw_size > 0);
  if (sized.length > 0) {
    setInput(
      "rawBytesPerGzByte",
      sized.reduce((sum, file) => sum + (file.raw_bytes ?? 0), 0) /
        sized.reduce((sum, file) => sum + file.raw_size, 0),
    );
  }
  if (m.parquet.rows > 0) {
    setInput("parquetBytesPerRow", m.parquet.bytes / m.parquet.rows);
  }
  if (F > 0 && m.parquet.files) {
    setUnit("windowFilesPerFile", m.parquet.files / F);
  }
  if (F > 0 && inputs.logPeriodS > 0) {
    const periods = new Set(
      converted.map((file) =>
        Math.floor(file.timestamp / 1000 / inputs.logPeriodS),
      ),
    );
    setInput("filesPerPeriod", Math.max(1, F / periods.size));
  }

  // Billing totals only span the hours Fastly reported, so per-day and per-file
  // units divide by that span and count only the files converted inside it.
  const b = m.billing;
  const covered =
    b?.coveredFrom != null && b.coveredTo != null && b.coveredTo > b.coveredFrom
      ? { from: b.coveredFrom, to: b.coveredTo }
      : null;
  const W = covered
    ? Math.min(m.days, (covered.to - covered.from) / DAY_MS)
    : m.days;
  const billed = covered
    ? converted.filter(
        (file) => file.timestamp >= covered.from && file.timestamp < covered.to,
      )
    : converted;
  const billedF = billed.length;
  const billedLines = billed.reduce((sum, file) => sum + file.rows, 0);
  // Splits a billed total into a fixed daily part and the per-file default; a
  // total below the default's share means files cost less, not a negative fixed part.
  const split = (
    total: number,
    perDay: keyof UnitCosts,
    perFile: keyof UnitCosts,
  ) => {
    const fileShare = units[perFile] * billedF;
    if (billedF > 0 && total < fileShare) {
      setUnit(perFile, total / billedF);
      setUnit(perDay, 0);
    } else {
      setUnit(perDay, (total - fileShare) / W);
    }
  };
  if (b) {
    const app = b.byRole?.app;
    const worker = b.byRole?.worker;
    if (app) {
      setUnit("appRequestsPerDay", app.requests / W);
      setUnit("appVcpuMsPerDay", app.vcpuMs / W);
    }
    if (worker && billedF > 0) {
      setUnit("workerRequestsPerFile", worker.requests / billedF);
    }
    if (worker && billedLines > 0) {
      const total = worker.vcpuMs / (billedLines / 1000);
      units.summaryVcpuMsPer1kLines = Math.min(
        units.summaryVcpuMsPer1kLines,
        total,
      );
      setUnit("vcpuMsPer1kLines", total - units.summaryVcpuMsPer1kLines);
    }
    setUnit("deliveryGbPerDay", b.computeBandwidthBytes / 1e9 / W);
    split(b.kvClassA, "kvClassAPerDay", "kvClassAPerFile");
    split(b.kvClassB, "kvClassBPerDay", "kvClassBPerFile");
    if (b.secrets !== null) setUnit("secrets", b.secrets);
  }

  if (m.fos) {
    let fileA = 0;
    let fileB = 0;
    let fixedA = 0;
    let fixedB = 0;
    let fileFrontReqs = 0;
    let fixedFrontReqs = 0;
    let fixedBytesOut = 0;
    for (const row of m.fos) {
      const perFile = PER_FILE_CALLERS.includes(row.caller);
      if (row.op_class === "A") {
        if (perFile) fileA += row.op_count;
        else fixedA += row.op_count;
      } else if (row.op_class === "B") {
        if (perFile) {
          fileB += row.op_count;
          fileFrontReqs += row.op_count;
        } else {
          fixedB += row.op_count;
          fixedFrontReqs += row.op_count;
          fixedBytesOut += row.bytes_out;
        }
      } else if (row.op_class === "CDN_HIT") {
        if (perFile) {
          fileFrontReqs += row.op_count;
        } else {
          fixedFrontReqs += row.op_count;
          fixedBytesOut += row.bytes_out;
        }
      }
    }
    setUnit("fosClassAPerDay", fixedA / m.days);
    setUnit("fosClassBPerDay", fixedB / m.days);
    setUnit("frontRequestsPerDay", fixedFrontReqs / m.days);
    setUnit(
      "deliveryGbPerDay",
      fixedBytesOut / 1e9 / m.days +
        (b ? b.computeBandwidthBytes / 1e9 / W : 0),
    );
    // Fastly's logging PUT of each raw file is a Class A operation this app never sees. The
    // `worker:ingest` Class A ops already include the per-window summary PUTs, and those same
    // window summaries are counted again in `summaries.classAPerDay`, so subtract the per-file
    // window count (clamped at zero) to avoid double-counting them.
    if (F > 0) {
      setUnit(
        "fosClassAPerFile",
        1 + Math.max(0, fileA / F - units.windowFilesPerFile),
      );
      setUnit("fosClassBPerFile", fileB / F);
      setUnit("frontRequestsPerFile", fileFrontReqs / F);
    }
  }
  if (m.fixedStoredBytes !== null) {
    setUnit("fixedStoredGb", m.fixedStoredBytes / 1e9);
  }

  return {
    inputs,
    units,
    measuredInputs,
    measuredUnits,
    window: { days: W, files: billedF, lines: billedLines },
    compaction,
    serviceCompaction: setting,
    rumEnabled: Boolean(m.service?.rum_enabled),
    relay: m.service?.ingest_relay ?? 0,
  };
}

export const estimateMonthlyUsage = estimateMonthly;
