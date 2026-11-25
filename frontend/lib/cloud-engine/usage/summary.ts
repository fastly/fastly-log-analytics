import { dashboardCacheIdentity } from "../dashboard/sources";
import { createSharedRequests } from "../shared-request";
import { readLogs as clientReadLogs } from "../telemetry/client";
import { reconcileStorage, adminRequest } from "../telemetry/admin";
import type { LogsRange } from "../telemetry/kinds";
import {
  activeOpsUser,
  assertOpsEpoch,
  currentOpsCacheEpoch,
  currentOpsIdentity,
} from "../telemetry/sources";
import { DAY_MS, FREE_SECRETS, LIST_RATES } from "./rates";
import { priceKvStorageGb } from "./pricing";
import type {
  StorageBillingInterval,
  StorageBillingIntervalKind,
} from "./storageBilling";

const readLogs: UsageLogReader = clientReadLogs;

const sharedSettings = createSharedRequests<unknown>();

// A /usage load reads the settings from several sections one after another; reuse a
// settled answer briefly, like the Cloud metadata lookups. Writes clear it.
const SETTINGS_TTL_MS = 15_000;
let settledSettings: { key: string; at: number; value: unknown } | null = null;
let settingsResets = 0;
const activeSettingsControllers = new Set<AbortController>();

function settingsAbortError(): DOMException {
  return new DOMException("The operation was aborted.", "AbortError");
}

export function resetUsageSettingsCache(
  options: { abortActive?: boolean } = {},
): void {
  const abortActive = options.abortActive ?? true;
  settledSettings = null;
  settledInventory.clear();
  settledUsageSummaries.clear();
  settingsResets += 1;
  if (abortActive) {
    for (const controller of activeSettingsControllers) {
      controller.abort(settingsAbortError());
    }
    activeSettingsControllers.clear();
  }
}

function parseTimestampMs(raw: unknown): number | null {
  if (raw === null || raw === undefined || raw === "") return null;
  const numeric = Number(raw);
  if (Number.isFinite(numeric)) return numeric;
  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export interface CloudStorageInventoryResult {
  intervals: StorageBillingInterval[];
  complete: boolean;
  snapshotId: string | null;
  observedAt: number | null;
  provenance: "cloud:inventory" | "cloud:inventory_incomplete" | "cloud:inventory_stale";
}

const INITIAL_INVENTORY_LOOKBACK_DAYS = 2;
const DEFAULT_INVENTORY_LOOKBACK_DAYS = 30;
const INVENTORY_TTL_MS = 60_000;
const sharedInventory = createSharedRequests<CloudStorageInventoryResult>();
const settledInventory = new Map<
  string,
  { at: number; value: CloudStorageInventoryResult }
>();

export async function fetchCloudStorageInventory(
  options: {
    now?: number;
    lookbackDays?: number;
    identity?: { user: string; role: string };
    read?: UsageLogReader;
    signal?: AbortSignal;
  } = {},
): Promise<CloudStorageInventoryResult> {
  const startOpsEpoch = currentOpsCacheEpoch();
  const scope = dashboardCacheIdentity();
  assertOpsEpoch(startOpsEpoch, options.signal);
  const now = options.now ?? Date.now();
  const identity = options.identity ?? currentOpsIdentity();
  const read = options.read ?? readLogs;
  const isDefaultRead = read === readLogs;

  const readInventoryWindow = async (
    lookbackDays: number,
    signal: AbortSignal | undefined,
  ): Promise<CloudStorageInventoryResult> => {
    const result = await read(
      "storage_inventory",
      {
        from: new Date(now - lookbackDays * DAY_MS).toISOString(),
        to: new Date(now).toISOString(),
      },
      { ...identity, signal },
    );
    assertOpsEpoch(startOpsEpoch, signal);
    if (dashboardCacheIdentity() !== scope) {
      throw settingsAbortError();
    }

    type InventoryObject = {
      dataset: string;
      key: string;
      size: number;
      lastModifiedMs: number | null;
    };
    const snapshots = new Map<
      string,
      {
        latestTs: number;
        terminalTs: number | null;
        objects: Map<string, InventoryObject>;
        hasIncompleteRow: boolean;
      }
    >();

    for (const row of result.rows) {
      const snapshotId =
        typeof row.snapshot_id === "string" ? row.snapshot_id.trim() : "";
      if (!snapshotId) continue;
      const rowTs = parseTimestampMs(row.timestamp) ?? 0;
      let snap = snapshots.get(snapshotId);
      if (!snap) {
        snap = {
          latestTs: rowTs,
          terminalTs: null,
          objects: new Map(),
          hasIncompleteRow: false,
        };
        snapshots.set(snapshotId, snap);
      } else if (rowTs > snap.latestTs) {
        snap.latestTs = rowTs;
      }
      if (row.complete === true) {
        snap.terminalTs =
          snap.terminalTs === null ? rowTs : Math.max(snap.terminalTs, rowTs);
        continue;
      }
      const dataset = typeof row.dataset === "string" ? row.dataset : "";
      const key = typeof row.key === "string" ? row.key : "";
      if (!dataset || !key) {
        snap.hasIncompleteRow = true;
        continue;
      }
      const size = Number(row.size);
      const lastModifiedMs = parseTimestampMs(row.last_modified);
      if (!Number.isFinite(size) || size < 0 || lastModifiedMs === null) {
        snap.hasIncompleteRow = true;
      }
      snap.objects.set(`${dataset}:${key}`, {
        dataset,
        key,
        size: Number.isFinite(size) && size >= 0 ? size : 0,
        lastModifiedMs,
      });
    }

    let newestOverall: { id: string; latestTs: number } | null = null;
    let newestComplete: {
      id: string;
      terminalTs: number;
      objects: Map<string, InventoryObject>;
      hasIncompleteRow: boolean;
    } | null = null;

    for (const [id, snap] of snapshots) {
      if (!newestOverall || snap.latestTs > newestOverall.latestTs) {
        newestOverall = { id, latestTs: snap.latestTs };
      }
      if (
        snap.terminalTs !== null &&
        (!newestComplete || snap.terminalTs > newestComplete.terminalTs)
      ) {
        newestComplete = {
          id,
          terminalTs: snap.terminalTs,
          objects: snap.objects,
          hasIncompleteRow: snap.hasIncompleteRow,
        };
      }
    }

    if (!newestComplete) {
      return {
        intervals: [],
        complete: false,
        snapshotId: null,
        observedAt: null,
        provenance: "cloud:inventory_incomplete",
      };
    }

    // The schedule retakes the snapshot every 6 hours; a newer one still listing doesn't make this one
    // wrong, but a complete snapshot older than two runs means the schedule stopped.
    const stale = now - newestComplete.terminalTs > INVENTORY_STALE_MS;
    const intervals: StorageBillingInterval[] = [];
    for (const [id, obj] of newestComplete.objects) {
      const rowComplete = obj.lastModifiedMs !== null;
      const billedFrom = obj.lastModifiedMs ?? newestComplete.terminalTs;
      intervals.push({
        id,
        dataset: obj.dataset,
        kind: "live",
        bytes: obj.size,
        createdAt: obj.lastModifiedMs,
        deletedAt: null,
        billedFrom,
        billedUntil: now,
        complete: rowComplete,
        provenance: "cloud:inventory",
      });
    }

    const complete =
      result.liveTruncated !== true && !stale && !newestComplete.hasIncompleteRow;
    return {
      intervals,
      complete,
      snapshotId: newestComplete.id,
      observedAt: newestComplete.terminalTs,
      provenance: complete
        ? "cloud:inventory"
        : stale
          ? "cloud:inventory_stale"
          : "cloud:inventory_incomplete",
    };
  };

  const load = async (
    signal: AbortSignal | undefined,
  ): Promise<CloudStorageInventoryResult> => {
    if (options.lookbackDays !== undefined) {
      return readInventoryWindow(options.lookbackDays, signal);
    }
    const recent = await readInventoryWindow(
      INITIAL_INVENTORY_LOOKBACK_DAYS,
      signal,
    );
    if (recent.snapshotId !== null) {
      return recent;
    }
    return readInventoryWindow(DEFAULT_INVENTORY_LOOKBACK_DAYS, signal);
  };

  if (!isDefaultRead) {
    return load(options.signal);
  }

  const cacheKey = JSON.stringify([
    startOpsEpoch,
    scope,
    activeOpsUser(),
    identity.user,
    identity.role,
    Math.floor(now / 60_000),
    options.lookbackDays ?? "auto",
  ]);
  const wallNow = Date.now();
  for (const [k, entry] of settledInventory) {
    if (wallNow - entry.at >= INVENTORY_TTL_MS) settledInventory.delete(k);
  }
  const cached = settledInventory.get(cacheKey);
  if (cached) return cached.value;

  return sharedInventory(
    cacheKey,
    async (sharedSignal) => {
      const value = await load(sharedSignal);
      if (
        value.complete &&
        startOpsEpoch === currentOpsCacheEpoch() &&
        dashboardCacheIdentity() === scope
      ) {
        settledInventory.set(cacheKey, { at: Date.now(), value });
      }
      return value;
    },
    { signal: options.signal },
  );
}

/** Two of the server's 6-hour snapshot intervals (`retention::RECONCILE_INTERVAL_S`). */
const INVENTORY_STALE_MS = 2 * 6 * 3_600_000;

export interface IngestOutput {
  key: string;
  bytes: number;
  createdAt: number;
}

export interface IngestDeletedIntervalsResult {
  intervals: StorageBillingInterval[];
  /** Parquet files converted raw files or compaction produced (`output_keys_json`), first write per key. */
  outputs: IngestOutput[];
  /** Parquet keys deleted by compaction or retention (`status = "parquet_deleted"`). */
  deletedParquetKeys?: Set<string>;
  bytes: number;
  complete: boolean;
  provenance: "cloud:ingest_files" | "cloud:ingest_files_incomplete";
}

export async function fetchIngestDeletedIntervals(
  range: { from: number; to: number },
  identity: { user: string; role: string },
  read: UsageLogReader = readLogs,
  options: {
    minimumBilledDays?: number;
    signal?: AbortSignal;
  } = {},
): Promise<IngestDeletedIntervalsResult> {
  const minimumBilledDays = options.minimumBilledDays ?? 30;
  const minWindowMs = minimumBilledDays * DAY_MS;
  const result = await read(
    "ingest_files",
    {
      from: new Date(range.from).toISOString(),
      to: new Date(range.to).toISOString(),
    },
    { ...identity, signal: options.signal },
  );

  type RawLifecycle = {
    earliestTs: number | null;
    sawNonDeleted: boolean;
    knownSize: number | undefined;
    latest: {
      deleted: boolean;
      deletedAt: number | null;
      rowAt: number;
      version: number;
    };
  };

  const lifecycles = new Map<string, RawLifecycle>();
  const outputs = new Map<string, IngestOutput>();
  const deletedParquetKeys = new Set<string>();
  for (const row of result.rows) {
    if (
      (row.status === "converted" || row.status === "compacted") &&
      typeof row.output_keys_json === "string"
    ) {
      const at = parseTimestampMs(row.timestamp);
      let written: unknown = [];
      try {
        written = JSON.parse(row.output_keys_json);
      } catch {
        written = [];
      }
      for (const output of Array.isArray(written) ? written : []) {
        const key = typeof output?.key === "string" ? output.key : "";
        const bytes = Number(output?.bytes);
        if (!key || !Number.isFinite(bytes) || at === null || outputs.has(key)) continue;
        outputs.set(key, { key, bytes, createdAt: at });
      }
    }
    if (
      row.status === "parquet_deleted" &&
      typeof row.output_keys_json === "string"
    ) {
      let deleted: unknown = [];
      try {
        deleted = JSON.parse(row.output_keys_json);
      } catch {
        deleted = [];
      }
      for (const entry of Array.isArray(deleted) ? deleted : []) {
        const key =
          typeof entry === "string"
            ? entry
            : typeof entry?.key === "string"
              ? entry.key
              : "";
        if (key) deletedParquetKeys.add(key);
      }
    }
    const rawKey = typeof row.raw_key === "string" ? row.raw_key : "";
    if (!rawKey) continue;
    const rawSize = row.raw_size;
    const size =
      rawSize !== null && rawSize !== undefined ? Number(rawSize) : NaN;
    const validSize = Number.isFinite(size) && size >= 0 ? size : undefined;
    const ts = parseTimestampMs(row.timestamp);
    const deletedAt = parseTimestampMs(row.raw_deleted_at);
    const rowAt = deletedAt ?? ts ?? 0;
    const version = Number(row.state_version ?? 0);
    const validVersion = Number.isFinite(version) ? version : 0;
    const deleted = row.raw_deleted === true;

    const existing = lifecycles.get(rawKey);
    if (!existing) {
      lifecycles.set(rawKey, {
        earliestTs: ts,
        sawNonDeleted: !deleted,
        knownSize: validSize,
        latest: {
          deleted,
          deletedAt,
          rowAt,
          version: validVersion,
        },
      });
      continue;
    }

    if (ts !== null && (existing.earliestTs === null || ts < existing.earliestTs)) {
      existing.earliestTs = ts;
    }
    if (!deleted) {
      existing.sawNonDeleted = true;
    }
    if (existing.knownSize === undefined && validSize !== undefined) {
      existing.knownSize = validSize;
    }
    if (
      validVersion > existing.latest.version ||
      (validVersion === existing.latest.version && rowAt > existing.latest.rowAt)
    ) {
      existing.latest = {
        deleted,
        deletedAt,
        rowAt,
        version: validVersion,
      };
    }
  }

  const intervals: StorageBillingInterval[] = [];
  let bytes = 0;
  let complete = result.liveTruncated !== true;

  for (const [rawKey, entry] of lifecycles) {
    if (!entry.latest.deleted) continue;
    const deletedAt = entry.latest.deletedAt ?? entry.latest.rowAt;
    if (entry.knownSize === undefined) {
      complete = false;
      continue;
    }
    const hasCreationEvidence =
      entry.sawNonDeleted ||
      (entry.earliestTs !== null && entry.earliestTs < deletedAt);
    const createdAt = entry.earliestTs ?? deletedAt;
    const minFloor = createdAt + minWindowMs;
    const billedUntil = Math.max(deletedAt, minFloor);
    const kind: StorageBillingIntervalKind =
      billedUntil > deletedAt ? "minimum_billed" : "deleted";
    const intervalComplete = hasCreationEvidence && result.liveTruncated !== true;
    if (!intervalComplete) {
      complete = false;
    }
    bytes += entry.knownSize;
    intervals.push({
      id: `raw_request:${rawKey}`,
      dataset: "raw_request",
      kind,
      bytes: entry.knownSize,
      createdAt: hasCreationEvidence ? createdAt : null,
      deletedAt,
      billedFrom: createdAt,
      billedUntil,
      complete: intervalComplete,
      provenance: "cloud:ingest_files",
    });
  }

  return {
    intervals,
    outputs: [...outputs.values()],
    deletedParquetKeys,
    bytes,
    complete,
    provenance: complete
      ? "cloud:ingest_files"
      : "cloud:ingest_files_incomplete",
  };
}

export async function fetchIngestDeletedBytes(
  range: { from: number; to: number },
  identity: { user: string; role: string },
  read: UsageLogReader = readLogs,
  signal?: AbortSignal,
) {
  const result = await read(
    "ingest_files",
    {
      from: new Date(range.from).toISOString(),
      to: new Date(range.to).toISOString(),
    },
    { ...identity, signal },
  );
  const known = new Map<string, number>();
  const latest = new Map<
    string,
    { deleted: boolean; at: number; version: number }
  >();
  for (const row of result.rows) {
    const key = typeof row.raw_key === "string" ? row.raw_key : "";
    if (!key) continue;
    const size = Number(row.raw_size);
    if (Number.isFinite(size) && size >= 0 && !known.has(key)) {
      known.set(key, size);
    }
    const rawAt = row.raw_deleted_at ?? row.timestamp;
    const numericAt = Number(rawAt);
    const at =
      typeof rawAt === "string" && !Number.isFinite(numericAt)
        ? Date.parse(rawAt)
        : numericAt;
    const version = Number(row.state_version ?? 0);
    const candidate = {
      deleted: row.raw_deleted === true,
      at: Number.isFinite(at) ? at : 0,
      version: Number.isFinite(version) ? version : 0,
    };
    const previous = latest.get(key);
    if (
      !previous ||
      candidate.version > previous.version ||
      (candidate.version === previous.version && candidate.at > previous.at)
    ) {
      latest.set(key, candidate);
    }
  }
  let bytes = 0;
  let complete = result.liveTruncated !== true;
  for (const [key, state] of latest) {
    if (!state.deleted || state.at < range.from || state.at > range.to) continue;
    const size = known.get(key);
    if (size === undefined) {
      complete = false;
      continue;
    }
    bytes += size;
  }
  return {
    bytes,
    complete: complete && latest.size > 0,
    provenance: complete && latest.size > 0
      ? "ingest_files_exact"
      : "ingest_files_incomplete",
  } as const;
}

/** Readers of the usage-logging settings share one request per user. */
const readUsageLogging = async (signal?: AbortSignal): Promise<unknown> => {
  const startOpsEpoch = currentOpsCacheEpoch();
  const resets = settingsResets;
  assertOpsEpoch(startOpsEpoch, signal);
  const key = JSON.stringify([
    resets,
    dashboardCacheIdentity(),
    activeOpsUser(),
  ]);
  const cached = settledSettings;
  if (cached?.key === key && Date.now() - cached.at < SETTINGS_TTL_MS) {
    return cached.value;
  }
  const controller = new AbortController();
  activeSettingsControllers.add(controller);
  const onCallerAbort = () =>
    controller.abort(signal?.reason ?? settingsAbortError());
  if (signal) {
    signal.addEventListener("abort", onCallerAbort, { once: true });
  }
  try {
    const value = await sharedSettings(
      key,
      (shared) =>
        adminRequest<unknown>("/api/admin/usage-logging", { signal: shared }),
      { signal: controller.signal },
    );
    assertOpsEpoch(startOpsEpoch, signal);
    // A write that landed while this was in flight may have changed the answer.
    if (resets === settingsResets && startOpsEpoch === currentOpsCacheEpoch()) {
      settledSettings = { key, at: Date.now(), value };
    }
    return value;
  } finally {
    signal?.removeEventListener("abort", onCallerAbort);
    activeSettingsControllers.delete(controller);
  }
};

export type OpClass = "A" | "B" | "CDN_HIT";

export interface UsageLoggingSettings {
  enabled: boolean;
  retention_days: number;
  class_a_rate_per_1k: number;
  class_b_rate_per_10k: number;
  cdn_egress_rate_per_gb: number;
  storage_rate_per_gb_month: number;
  min_billed_days: number;
  compute_request_rate_per_million: number;
  compute_vcpu_rate_per_million_ms: number;
  compute_vcpu_free_ms_per_request: number;
  compute_vcpu_excess_rate_per_second: number;
  compute_vcpu_rounding_ms: number;
  cdn_request_rate_per_10k: number;
  kv_class_a_rate_per_million: number;
  kv_class_b_rate_per_million: number;
  secret_rate_per_month: number;
}

export const DEFAULT_SETTINGS: UsageLoggingSettings = {
  enabled: true,
  retention_days: 30,
  ...LIST_RATES,
};

export { LIST_RATES } from "./rates";

export interface FosUsageRow {
  timestamp: number;
  op: string;
  op_class: OpClass;
  op_count: number;
  dataset: string;
  caller: string;
  cdn_cache: "HIT" | "MISS" | "DIRECT";
  bytes_in: number;
  bytes_out: number;
  bytes_deleted: number;
  bytes_deleted_complete?: boolean;
  est_cost_usd: number;
}

export interface ApiTelemetryRow {
  timestamp: number;
  source: "compute_api" | "fastly_api" | "browser_query";
  path: string;
  vcpu_ms: number;
  /** On a `compute_api` row, the number of file reads a beacon folded into it. */
  files_fetched?: number;
}

export interface CronRunRow {
  started_at: number;
  cron_kind: string;
  outcome: string;
  vcpu_ms: number;
  cache_lookups: number;
  backend_requests_used: number;
  details_json: string;
  /** Idle clock executions (and their vCPU) folded into this run since the job last ran. */
  idle_ticks?: number;
  idle_vcpu_ms?: number;
  /** `<root>-<cron_kind>`: every stage row of one clock, flush or manual root shares `<root>`. */
  run_id?: string;
  trigger?: string;
  /** flp-worker executions the clock execution that wrote this row dispatched. */
  worker_executions?: number;
  listed_files?: number;
  converted_files?: number;
  deleted_files?: number;
  rows_written?: number;
  lines_read?: number;
  bytes_written?: number;
  parquet_files_written?: number;
  kv_ops?: number;
  cache_replaces?: number;
  purges?: number;
}

/** Retired: rows written before the 1 s dispatcher moved idle counts onto cron_runs. */
export interface CronTickRow {
  timestamp: number;
  ticks_count: number;
  vcpu_ms: number;
}

export interface UsageInput {
  now: number;
  days: number;
  settings: UsageLoggingSettings;
  rows: {
    fos_usage: FosUsageRow[];
    api_telemetry: ApiTelemetryRow[];
    cron_runs: CronRunRow[];
    cron_ticks: CronTickRow[];
  };
  /** `null` means the backend does not log the counter. */
  counters: {
    kv_ops: number | null;
    cache_replaces: number | null;
    purges: number | null;
  };
  telemetryCoverage?: {
    fos_usage: boolean;
  };
  retention_days?: Record<string, number>;
}

export interface OpTotals {
  classA: number;
  classB: number;
  cdnHit: number;
  usd: number;
}

export interface ComputeExecutionSegment {
  vcpuMs: number;
  requests: number;
}

export interface RouteTotals {
  key: string;
  requests: number;
  vcpuMs: number;
  billedVcpuMs?: number;
  segments?: ComputeExecutionSegment[];
}

export interface StorageDataset {
  dataset: string;
  bytes: number;
  snapshotBytes: number;
  putBytes: number;
  deletedBytes: number;
  gbPerDay: number;
  retentionDays: number;
  billedGbMonths: number;
}

export interface UsageSummary {
  days: number;
  totals: OpTotals;
  byDay: Array<OpTotals & { day: string }>;
  byCaller: Array<OpTotals & { caller: string }>;
  cdnHitRatio: number | null;
  bytesWritten: number;
  bytesRead: number;
  computeByRoute: RouteTotals[];
  computeByCron: RouteTotals[];
  computeRequests: number;
  computeVcpuMs: number;
  computeBilledVcpuMs?: number;
  computeSegments?: ComputeExecutionSegment[];
  cacheLookups: number;
  kvOps: number | null;
  cacheReplaces: number | null;
  purges: number | null;
  storage: {
    snapshotAt: number | null;
    datasets: StorageDataset[];
    totalBytes: number;
    billedGbMonths: number;
  };
  retentionLagMs: number | null;
  overhead: {
    classA: number;
    classB: number;
    usd: number;
    runs: number;
    vcpuMs: number;
    cacheLookups: number;
  };
  telemetryOps?: OpTotals;
  telemetryBytesRead?: number;
  telemetryKvOps?: number | null;
}

const NON_TELEMETRY_DATASETS = new Set([
  "pq_request",
  "raw_request",
  "pq_rum",
  "raw_rum",
  "summaries",
]);

function isTelemetryCaller(caller: string): boolean {
  return (
    caller === "cron:log_flush" ||
    caller === "cron:log_compact" ||
    caller.startsWith("api:/api/logs") ||
    caller.startsWith("api:/api/telemetry") ||
    caller.startsWith("api:/api/admin/usage") ||
    caller.startsWith("api:/api/admin/logs")
  );
}

function isTelemetryRoute(path: string): boolean {
  return isTelemetryCaller(`api:${path}`);
}

const OVERHEAD_CALLERS = [
  "cron:log_flush",
  "cron:log_compact",
  "cron:retention",
  "cron:summary_bundle",
];
const OVERHEAD_CRON_KINDS = [
  "log_flush",
  "log_compact",
  "retention_sweep",
  "summary_bundle",
];

function exact(value: number): number {
  return Number(value.toPrecision(12));
}

export function perThousandToPerMillion(rate: number): number {
  return exact(rate * 1000);
}

export function perTenThousandToPerMillion(rate: number): number {
  return exact(rate * 100);
}

/**
 * Billed excess vCPU ms for a single Compute Request under the contract terms:
 * `round(max(0, vcpuMs - freeMs) / roundingMs) * roundingMs`.
 */
export function billedVcpuMsPerRequest(
  vcpuMs: number,
  freeMs = 20,
  roundingMs = 10,
): number {
  const excess = Math.max(0, vcpuMs - freeMs);
  return roundingMs > 0 ? Math.round(excess / roundingMs) * roundingMs : excess;
}

export function billedVcpuForExecutions(
  totalVcpuMs: number,
  executions: number,
  settings: Pick<
    UsageLoggingSettings,
    | "compute_vcpu_free_ms_per_request"
    | "compute_vcpu_excess_rate_per_second"
    | "compute_vcpu_rounding_ms"
  > & { compute_vcpu_rate_per_million_ms?: number },
): { billedMs: number; usd: number } {
  if (executions <= 0 || totalVcpuMs <= 0) return { billedMs: 0, usd: 0 };
  const freeMs =
    Number.isFinite(settings.compute_vcpu_free_ms_per_request) &&
    settings.compute_vcpu_free_ms_per_request >= 0
      ? settings.compute_vcpu_free_ms_per_request
      : LIST_RATES.compute_vcpu_free_ms_per_request;
  const roundingMs =
    Number.isFinite(settings.compute_vcpu_rounding_ms) &&
    settings.compute_vcpu_rounding_ms >= 0
      ? settings.compute_vcpu_rounding_ms
      : LIST_RATES.compute_vcpu_rounding_ms;
  const excessRatePerSecond =
    Number.isFinite(settings.compute_vcpu_excess_rate_per_second) &&
    settings.compute_vcpu_excess_rate_per_second > 0
      ? settings.compute_vcpu_excess_rate_per_second
      : Number.isFinite(settings.compute_vcpu_rate_per_million_ms) &&
          (settings.compute_vcpu_rate_per_million_ms ?? 0) > 0
        ? (settings.compute_vcpu_rate_per_million_ms ?? 0) / 1000
        : 0;
  const perExecBilled = billedVcpuMsPerRequest(
    totalVcpuMs / executions,
    freeMs,
    roundingMs,
  );
  const billedMs = perExecBilled * executions;
  const usd = (billedMs / 1000) * excessRatePerSecond;
  return { billedMs, usd };
}

/** S_k GB/day x rate x max(retention, min billed days) / 30 */
export function storageCostUsd(
  gbPerDay: number,
  ratePerGbMonth: number,
  retentionDays: number,
  minBilledDays: number,
): number {
  return (
    (gbPerDay * ratePerGbMonth * Math.max(retentionDays, minBilledDays)) / 30
  );
}

function emptyTotals(): OpTotals {
  return { classA: 0, classB: 0, cdnHit: 0, usd: 0 };
}

function detailNumber(run: CronRunRow, field: "worker_vcpu_ms" | "passes") {
  if (!run.details_json) return 0;
  try {
    const parsed = JSON.parse(run.details_json) as Record<string, unknown>;
    const value = Number(parsed[field] ?? 0);
    return Number.isFinite(value) && value > 0 ? value : 0;
  } catch {
    return 0;
  }
}

/** Worker executions a run dispatched report their vCPU in its details, not in `vcpu_ms`. */
function workerVcpuMs(run: CronRunRow): number {
  return detailNumber(run, "worker_vcpu_ms");
}

/** Stages that run in their own flp-worker execution, so their `vcpu_ms` is the worker's. */
const WORKER_CRON_KINDS = new Set([
  "retention_sweep",
  "log_compact",
  "delete_history",
  "storage_reconcile",
  "summary_bundle",
  "request_compact",
  "delete_data",
  "rum_compact",
]);

/**
 * Splits cron_runs rows into the Compute executions they stand for. Stage rows share their
 * execution's root run id (`<root>-<cron_kind>`); a row without one is its own execution.
 * A clock or `/_flush` root is one app-service execution carrying its tick stages; a manual
 * root ran inside its API trigger request, whose api_telemetry row already holds that vCPU.
 * Worker stages, the worker executions a root dispatched (`worker_executions`, the vCPU in
 * `worker_vcpu_ms`) and compaction passes go to one worker segment per root.
 */
function cronExecutionSegments(
  cron_runs: CronRunRow[],
): ComputeExecutionSegment[] {
  const roots = new Map<
    string,
    { manual: boolean; app: number; worker: ComputeExecutionSegment }
  >();
  cron_runs.forEach((run, i) => {
    const suffix = `-${run.cron_kind}`;
    const id = run.run_id
      ? `id:${run.run_id.endsWith(suffix) ? run.run_id.slice(0, -suffix.length) : run.run_id}`
      : `row:${i}`;
    let root = roots.get(id);
    if (!root) {
      root = {
        manual: run.trigger === "manual",
        app: 0,
        worker: { vcpuMs: 0, requests: 0 },
      };
      roots.set(id, root);
    }
    const vcpu = Number(run.vcpu_ms ?? 0);
    if (run.run_id && WORKER_CRON_KINDS.has(run.cron_kind)) {
      root.worker.vcpuMs += vcpu;
    } else {
      root.app += vcpu;
    }
    root.worker.vcpuMs += workerVcpuMs(run);
    root.worker.requests +=
      Number(run.worker_executions ?? 0) + detailNumber(run, "passes");
  });
  const segments: ComputeExecutionSegment[] = [];
  for (const root of roots.values()) {
    if (!root.manual) segments.push({ vcpuMs: root.app, requests: 1 });
    const { vcpuMs, requests } = root.worker;
    if (vcpuMs > 0 || requests > 0) {
      segments.push({
        vcpuMs,
        requests: vcpuMs > 0 ? Math.max(1, requests) : requests,
      });
    }
  }
  for (const run of cron_runs) {
    const idle = Number(run.idle_ticks ?? 0);
    const idleVcpu = Number(run.idle_vcpu_ms ?? 0);
    if (idle > 0 || idleVcpu > 0) {
      segments.push({ vcpuMs: idleVcpu, requests: Math.max(1, idle) });
    }
  }
  return segments;
}

function addOps(totals: OpTotals, row: FosUsageRow) {
  if (row.op_class === "A") totals.classA += row.op_count;
  else if (row.op_class === "B") totals.classB += row.op_count;
  else totals.cdnHit += row.op_count;
  totals.usd += row.est_cost_usd;
}

function attachGroupSegments(
  entry: RouteTotals,
  segments: ComputeExecutionSegment[],
  settings: UsageLoggingSettings,
) {
  const billedVcpuMs = segments.reduce(
    (sum, s) =>
      sum + billedVcpuForExecutions(s.vcpuMs, s.requests, settings).billedMs,
    0,
  );
  Object.defineProperty(entry, "segments", {
    value: segments,
    writable: true,
    configurable: true,
    enumerable: false,
  });
  Object.defineProperty(entry, "billedVcpuMs", {
    value: billedVcpuMs,
    writable: true,
    configurable: true,
    enumerable: false,
  });
}

function bump(
  map: Map<string, RouteTotals>,
  key: string,
  vcpuMs: number,
  requests = 1,
  segment?: ComputeExecutionSegment,
) {
  let entry = map.get(key);
  if (!entry) {
    entry = { key, requests: 0, vcpuMs: 0 };
    Object.defineProperty(entry, "segments", {
      value: [] as ComputeExecutionSegment[],
      writable: true,
      configurable: true,
      enumerable: false,
    });
  }
  entry.requests += requests;
  entry.vcpuMs += vcpuMs;
  if (segment) {
    entry.segments!.push(segment);
  }
  map.set(key, entry);
}

function ranked(map: Map<string, RouteTotals>): RouteTotals[] {
  return [...map.values()].sort(
    (a, b) => b.vcpuMs - a.vcpuMs || a.key.localeCompare(b.key),
  );
}

/** Bytes deleted since `from`; FOS still bills them for its minimum storage duration. */
export function deletedBytesSince(rows: FosUsageRow[], from: number): number {
  return rows.reduce((sum, row) => (row.timestamp >= from ? sum + row.bytes_deleted : sum), 0);
}

export function deletedBytesAccountingSince(
  rows: FosUsageRow[],
  from: number,
  coverage = false,
) {
  const since = rows.filter((row) => row.timestamp >= from);
  const deletionRows = since.filter(
    (row) => row.op === "DELETE" || row.op === "DELETE_OBJECTS",
  );
  const complete =
    coverage &&
    deletionRows.length > 0 &&
    deletionRows.every((row) => row.bytes_deleted_complete === true);
  return {
    bytes: deletionRows.reduce((sum, row) => sum + row.bytes_deleted, 0),
    complete,
    provenance: complete ? "fos_usage_exact" : "fos_usage_incomplete",
  } as const;
}

export function reconcileSnapshot(runs: CronRunRow[]) {
  const candidates = runs
    .filter(
      (run) =>
        run.cron_kind === "storage_reconcile" &&
        (run.outcome === "done" || run.outcome === "partial"),
    )
    .sort((a, b) => b.started_at - a.started_at);
  if (candidates.length === 0) return null;
  for (const run of candidates) {
    try {
      const parsed = JSON.parse(run.details_json) as {
        datasets?: Record<string, { bytes?: number }>;
      };
      const entries = Object.entries(parsed.datasets ?? {});
      if (entries.length > 0) {
        const datasets: Record<string, number> = {};
        for (const [name, value] of entries) {
          datasets[name] = Number(value?.bytes) || 0;
        }
        return { at: run.started_at, datasets };
      }
    } catch {
      // An unreadable snapshot contributes no per-dataset bytes.
    }
  }
  return { at: candidates[0].started_at, datasets: {} };
}

export function deriveFosUsageFromCronRuns(
  cron_runs: CronRunRow[],
  rates: Pick<
    UsageLoggingSettings,
    "class_a_rate_per_1k" | "class_b_rate_per_10k" | "cdn_egress_rate_per_gb"
  > = DEFAULT_SETTINGS,
): FosUsageRow[] {
  const derived: FosUsageRow[] = [];
  const costA = (count: number) => (count / 1000) * rates.class_a_rate_per_1k;
  const costB = (count: number) => (count / 10000) * rates.class_b_rate_per_10k;
  const push = (
    timestamp: number,
    op: string,
    op_class: OpClass,
    op_count: number,
    dataset: string,
    caller: string,
    cdn_cache: "HIT" | "MISS" | "DIRECT",
    bytes_in: number,
    bytes_out: number,
    bytes_deleted: number,
  ) => {
    if (
      op_count <= 0 &&
      bytes_in <= 0 &&
      bytes_out <= 0 &&
      bytes_deleted <= 0
    ) {
      return;
    }
    const est_cost_usd =
      op_class === "A"
        ? costA(op_count)
        : op_class === "B"
          ? costB(op_count)
          : 0;
    derived.push({
      timestamp,
      op,
      op_class,
      op_count,
      dataset,
      caller,
      cdn_cache,
      bytes_in,
      bytes_out,
      bytes_deleted,
      est_cost_usd,
    });
  };

  for (const run of cron_runs) {
    const ts = run.started_at;
    const listed = run.listed_files ?? 0;
    const converted = run.converted_files ?? 0;
    const deleted = run.deleted_files ?? 0;
    const filesWritten = run.parquet_files_written ?? 0;
    const bytesWritten = run.bytes_written ?? 0;
    const calls = run.backend_requests_used ?? 0;

    switch (run.cron_kind) {
      case "request_ingest": {
        if (
          run.outcome !== "error" &&
          run.outcome !== "busy" &&
          (listed > 0 ||
            converted > 0 ||
            filesWritten > 0 ||
            bytesWritten > 0 ||
            calls > 0)
        ) {
          const listOps = Math.max(1, Math.ceil(listed / 1000));
          push(
            ts,
            "LIST",
            "A",
            listOps,
            "pq_request",
            "cron:ingest",
            "DIRECT",
            0,
            0,
            0,
          );
        }
        if (converted > 0) {
          const rawBytes =
            bytesWritten > 0
              ? Math.round(bytesWritten * 1.5)
              : converted * 1024;
          push(
            ts,
            "GET",
            "B",
            converted,
            "pq_request",
            "worker:ingest",
            "MISS",
            0,
            rawBytes,
            0,
          );
        }
        if (filesWritten > 0 || bytesWritten > 0) {
          push(
            ts,
            "PUT",
            "A",
            Math.max(1, filesWritten),
            "pq_request",
            "worker:ingest",
            "DIRECT",
            bytesWritten,
            0,
            0,
          );
        }
        break;
      }
      case "rum_ingest": {
        if (
          run.outcome !== "error" &&
          run.outcome !== "busy" &&
          (listed > 0 ||
            converted > 0 ||
            filesWritten > 0 ||
            bytesWritten > 0 ||
            calls > 0)
        ) {
          const listOps = Math.max(1, Math.ceil(listed / 1000));
          push(
            ts,
            "LIST",
            "A",
            listOps,
            "pq_rum",
            "cron:rum_ingest",
            "DIRECT",
            0,
            0,
            0,
          );
        }
        if (converted > 0) {
          const rawBytes =
            bytesWritten > 0 ? Math.round(bytesWritten * 1.5) : converted * 512;
          push(
            ts,
            "GET",
            "B",
            converted,
            "pq_rum",
            "worker:rum_ingest",
            "MISS",
            0,
            rawBytes,
            0,
          );
        }
        if (filesWritten > 0 || bytesWritten > 0) {
          push(
            ts,
            "PUT",
            "A",
            Math.max(1, filesWritten),
            "pq_rum",
            "worker:rum_ingest",
            "DIRECT",
            bytesWritten,
            0,
            0,
          );
        }
        break;
      }
      case "raw_delete": {
        if (deleted > 0 || bytesWritten > 0) {
          push(
            ts,
            "DELETE_OBJECTS",
            "A",
            Math.max(1, Math.ceil(deleted / 1000)),
            "raw_request",
            "cron:raw_delete",
            "DIRECT",
            0,
            0,
            bytesWritten,
          );
        }
        break;
      }
      case "log_flush": {
        if (filesWritten > 0 || bytesWritten > 0) {
          const opCount = Math.max(1, filesWritten);
          const estBytes = bytesWritten > 0 ? bytesWritten : opCount * 4096;
          push(
            ts,
            "PUT",
            "A",
            opCount,
            "pq_ops",
            "cron:log_flush",
            "DIRECT",
            estBytes,
            0,
            0,
          );
        }
        break;
      }
      case "log_compact": {
        if (listed > 0 || filesWritten > 0 || deleted > 0 || bytesWritten > 0) {
          push(
            ts,
            "PUT",
            "A",
            1 + filesWritten,
            "pq_ops",
            "cron:log_compact",
            "DIRECT",
            bytesWritten,
            0,
            0,
          );
          if (listed > 0) {
            push(
              ts,
              "GET",
              "B",
              listed,
              "pq_ops",
              "cron:log_compact",
              "MISS",
              0,
              bytesWritten,
              0,
            );
          }
          if (deleted > 0 && bytesWritten > 0) {
            push(
              ts,
              "DELETE_OBJECTS",
              "A",
              Math.max(1, Math.ceil(deleted / 1000)),
              "pq_ops",
              "cron:log_compact",
              "DIRECT",
              0,
              0,
              bytesWritten,
            );
          }
        }
        break;
      }
      case "request_compact":
      case "summary_bundle": {
        if (listed > 0 || filesWritten > 0 || deleted > 0 || bytesWritten > 0) {
          const caller = `cron:${run.cron_kind}`;
          // A bundle's partial or final file is one single-request PUT; a compaction adds its
          // multipart create/complete.
          const overhead = run.cron_kind === "summary_bundle" ? 0 : 1;
          push(
            ts,
            "PUT",
            "A",
            overhead + filesWritten,
            "pq_request",
            caller,
            "DIRECT",
            bytesWritten,
            0,
            0,
          );
          if (listed > 0) {
            push(
              ts,
              "GET",
              "B",
              listed,
              "pq_request",
              caller,
              "MISS",
              0,
              bytesWritten,
              0,
            );
          }
          if (deleted > 0 && bytesWritten > 0) {
            push(
              ts,
              "DELETE_OBJECTS",
              "A",
              Math.max(1, Math.ceil(deleted / 1000)),
              "pq_request",
              caller,
              "DIRECT",
              0,
              0,
              bytesWritten,
            );
          }
        }
        break;
      }
      case "retention_sweep": {
        if (
          (run.outcome === "done" || run.outcome === "partial") &&
          (deleted > 0 || bytesWritten > 0 || calls > 0)
        ) {
          const listOps = Math.max(1, calls - (run.kv_ops ?? 0));
          push(
            ts,
            "LIST",
            "A",
            listOps,
            "pq_ops",
            "cron:retention",
            "DIRECT",
            0,
            0,
            bytesWritten,
          );
        }
        break;
      }
      default:
        break;
    }
  }
  return derived;
}

export function summarizeUsage(input: UsageInput): UsageSummary {
  const { fos_usage, api_telemetry, cron_runs, cron_ticks } = input.rows;
  const effectiveFosUsage =
    fos_usage.length > 0 || !input.settings.enabled
      ? fos_usage
      : deriveFosUsageFromCronRuns(cron_runs, input.settings);
  const totals = emptyTotals();
  const telemetryOps = emptyTotals();
  const days = new Map<string, OpTotals>();
  const callers = new Map<string, OpTotals>();
  let hits = 0;
  let misses = 0;
  let bytesWritten = 0;
  let bytesRead = 0;
  let telemetryBytesRead = 0;
  const overhead = {
    classA: 0,
    classB: 0,
    usd: 0,
    runs: 0,
    vcpuMs: 0,
    cacheLookups: 0,
  };
  for (const row of effectiveFosUsage) {
    addOps(totals, row);
    const day = new Date(row.timestamp).toISOString().slice(0, 10);
    const dayTotals = days.get(day) ?? emptyTotals();
    addOps(dayTotals, row);
    days.set(day, dayTotals);
    const callerTotals = callers.get(row.caller) ?? emptyTotals();
    addOps(callerTotals, row);
    callers.set(row.caller, callerTotals);
    if (row.op === "GET" && row.caller.startsWith("api:")) {
      if (row.cdn_cache === "HIT") hits += row.op_count;
      else if (row.cdn_cache === "MISS") misses += row.op_count;
    }
    bytesWritten += row.bytes_in;
    bytesRead += row.bytes_out;
    if (
      !NON_TELEMETRY_DATASETS.has(row.dataset) ||
      isTelemetryCaller(row.caller)
    ) {
      addOps(telemetryOps, row);
      telemetryBytesRead += row.bytes_out;
    }
    if (OVERHEAD_CALLERS.includes(row.caller)) {
      if (row.op_class === "A") overhead.classA += row.op_count;
      else if (row.op_class === "B") overhead.classB += row.op_count;
      overhead.usd += row.est_cost_usd;
    }
  }

  const routes = new Map<string, RouteTotals>();
  const computeSegments: ComputeExecutionSegment[] = [];
  let computeRequests = 0;
  let computeVcpuMs = 0;
  for (const row of api_telemetry) {
    if (row.source !== "compute_api") continue;
    const requests = Math.max(1, Number(row.files_fetched || 1));
    const vcpu = Number(row.vcpu_ms ?? 0);
    const segment = { vcpuMs: vcpu, requests };
    bump(routes, row.path, vcpu, requests, segment);
    computeSegments.push(segment);
    computeRequests += requests;
    computeVcpuMs += vcpu;
  }
  for (const entry of routes.values()) {
    attachGroupSegments(entry, entry.segments ?? [], input.settings);
  }
  for (const tick of cron_ticks) {
    const ticks = Math.max(1, Number(tick.ticks_count ?? 0));
    const vcpu = Number(tick.vcpu_ms ?? 0);
    computeRequests += tick.ticks_count;
    computeVcpuMs += vcpu;
    if (tick.ticks_count > 0 || vcpu > 0) {
      computeSegments.push({ vcpuMs: vcpu, requests: ticks });
    }
  }

  const crons = new Map<string, RouteTotals>();
  const cronRunSegments = new Map<
    string,
    Map<string, ComputeExecutionSegment>
  >();
  let cacheLookups = 0;
  let loggedKvOps = 0;
  let hasKvOps = false;
  let telemetryKvOps = 0;
  let hasTelemetryKvOps = false;
  let loggedCacheReplaces = 0;
  let hasCacheReplaces = false;
  let loggedPurges = 0;
  let hasPurges = false;
  let anonymousRunIdx = 0;
  for (const run of cron_runs) {
    const runKey = run.run_id ? `id:${run.run_id}` : `row:${anonymousRunIdx++}`;
    const workers = Number(run.worker_executions ?? 0);
    const runVcpu = Number(run.vcpu_ms ?? 0) + workerVcpuMs(run);
    let kindSegments = cronRunSegments.get(run.cron_kind);
    if (!kindSegments) {
      kindSegments = new Map();
      cronRunSegments.set(run.cron_kind, kindSegments);
    }
    const existingKindSeg = kindSegments.get(runKey);
    if (existingKindSeg) {
      existingKindSeg.vcpuMs += runVcpu;
      existingKindSeg.requests += workers;
    } else {
      kindSegments.set(runKey, {
        vcpuMs: runVcpu,
        requests: 1 + workers,
      });
    }
    bump(crons, run.cron_kind, runVcpu);
    cacheLookups += run.cache_lookups;
    if (typeof run.kv_ops === "number") {
      loggedKvOps += run.kv_ops;
      hasKvOps = true;
      if (run.cron_kind === "log_flush" || run.cron_kind === "log_compact") {
        telemetryKvOps += run.kv_ops;
        hasTelemetryKvOps = true;
      }
    }
    if (typeof run.cache_replaces === "number") {
      loggedCacheReplaces += run.cache_replaces;
      hasCacheReplaces = true;
    }
    if (typeof run.purges === "number") {
      loggedPurges += run.purges;
      hasPurges = true;
    }
    if (OVERHEAD_CRON_KINDS.includes(run.cron_kind)) {
      overhead.runs += 1;
      overhead.vcpuMs += run.vcpu_ms;
      overhead.cacheLookups += run.cache_lookups;
    }
  }
  for (const [kind, entry] of crons.entries()) {
    const segs = [...(cronRunSegments.get(kind)?.values() ?? [])];
    attachGroupSegments(entry, segs, input.settings);
  }
  for (const seg of cronExecutionSegments(cron_runs)) {
    computeSegments.push(seg);
    computeRequests += seg.requests;
    computeVcpuMs += seg.vcpuMs;
  }
  const computeBilledVcpuMs = computeSegments.reduce(
    (sum, s) =>
      sum +
      billedVcpuForExecutions(s.vcpuMs, s.requests, input.settings).billedMs,
    0,
  );

  const snapshot = reconcileSnapshot(cron_runs);
  const names = new Set<string>(Object.keys(snapshot?.datasets ?? {}));
  for (const row of effectiveFosUsage) names.add(row.dataset);
  const rumIngestRuns = cron_runs.filter(
    (run) => run.cron_kind === "rum_ingest" && (run.bytes_written ?? 0) > 0,
  );
  if (rumIngestRuns.length > 0) names.add("pq_rum");
  const datasets: StorageDataset[] = [...names].sort().map((dataset) => {
    const after = effectiveFosUsage.filter(
      (row) => row.dataset === dataset && row.timestamp > (snapshot?.at ?? 0),
    );
    let putBytes = after
      .filter((row) => row.op === "PUT")
      .reduce((sum, row) => sum + row.bytes_in, 0);
    const deletedBytes = after.reduce((sum, row) => sum + row.bytes_deleted, 0);
    const snapshotBytes = snapshot?.datasets[dataset] ?? 0;
    let windowPutBytes = effectiveFosUsage
      .filter((row) => row.dataset === dataset && row.op === "PUT")
      .reduce((sum, row) => sum + row.bytes_in, 0);
    if (
      dataset === "pq_rum" &&
      windowPutBytes === 0 &&
      rumIngestRuns.length > 0
    ) {
      windowPutBytes = rumIngestRuns.reduce(
        (sum, run) => sum + (run.bytes_written ?? 0),
        0,
      );
      putBytes = rumIngestRuns
        .filter((run) => run.started_at > (snapshot?.at ?? 0))
        .reduce((sum, run) => sum + (run.bytes_written ?? 0), 0);
    }
    const gbPerDay = windowPutBytes / 1e9 / input.days;
    const retentionDays =
      input.retention_days?.[dataset] ?? input.settings.retention_days;
    return {
      gbPerDay,
      retentionDays,
      billedGbMonths:
        storageCostUsd(
          gbPerDay,
          1,
          retentionDays,
          input.settings.min_billed_days,
        ) * 30,
      dataset,
      snapshotBytes,
      putBytes,
      deletedBytes,
      bytes: Math.max(0, snapshotBytes + putBytes - deletedBytes),
    };
  });

  const lastSweep = cron_runs
    .filter(
      (run) => run.cron_kind === "retention_sweep" && run.outcome === "done",
    )
    .sort((a, b) => b.started_at - a.started_at)[0];

  return {
    days: input.days,
    totals,
    byDay: [...days.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([day, value]) => ({ day, ...value })),
    byCaller: [...callers.entries()]
      .sort(([, a], [, b]) => b.usd - a.usd)
      .map(([caller, value]) => ({ caller, ...value })),
    cdnHitRatio: hits + misses > 0 ? hits / (hits + misses) : null,
    bytesWritten,
    bytesRead,
    computeByRoute: ranked(routes),
    computeByCron: ranked(crons),
    computeRequests,
    computeVcpuMs,
    computeBilledVcpuMs,
    computeSegments,
    cacheLookups,
    kvOps: input.counters.kv_ops ?? (hasKvOps ? loggedKvOps : null),
    cacheReplaces:
      input.counters.cache_replaces ??
      (hasCacheReplaces ? loggedCacheReplaces : null),
    purges: input.counters.purges ?? (hasPurges ? loggedPurges : null),
    storage: {
      snapshotAt: snapshot?.at ?? null,
      datasets,
      totalBytes: datasets.reduce((sum, item) => sum + item.bytes, 0),
      billedGbMonths: datasets.reduce(
        (sum, item) => sum + item.billedGbMonths,
        0,
      ),
    },
    retentionLagMs: lastSweep
      ? Math.max(0, input.now - lastSweep.started_at)
      : null,
    overhead,
    telemetryOps,
    telemetryBytesRead,
    telemetryKvOps: hasTelemetryKvOps ? telemetryKvOps : null,
  };
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Malformed usage response: ${name}`);
  }
  return value as Record<string, unknown>;
}

function num(source: Record<string, unknown>, key: string, name: string) {
  const value = source[key];
  if (value === undefined || value === null) return 0;
  if (typeof value === "bigint") return Number(value);
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Malformed usage response: ${name}.${key}`);
  }
  return value;
}

function optNum(source: Record<string, unknown>, key: string, name: string) {
  const value = source[key];
  if (value === undefined || value === null) return undefined;
  return num(source, key, name);
}

function str(source: Record<string, unknown>, key: string, name: string) {
  const value = source[key];
  if (value === undefined || value === null) return "";
  if (typeof value !== "string") {
    throw new Error(`Malformed usage response: ${name}.${key}`);
  }
  return value;
}

function rows<T>(
  value: Record<string, unknown>[],
  name: string,
  parse: (row: Record<string, unknown>) => T,
): T[] {
  return value.map((row) => parse(record(row, `rows.${name}[]`)));
}

function oneOf<T extends string>(
  row: Record<string, unknown>,
  key: string,
  allowed: readonly T[],
  fallback: T,
  name: string,
): T {
  const value = row[key];
  if (value === undefined || value === null) return fallback;
  if (!allowed.includes(value as T)) {
    throw new Error(`Malformed usage response: ${name}.${key}`);
  }
  return value as T;
}

export const USAGE_KINDS = [
  "fos_usage",
  "api_telemetry",
  "cron_runs",
  "cron_ticks",
] as const;

export type UsageKind = (typeof USAGE_KINDS)[number];
type UsageReadKind = UsageKind | "ingest_files" | "storage_inventory";

export type UsageLogReader = (
  kind: UsageReadKind,
  range: LogsRange,
  options: { user: string; role: string; signal?: AbortSignal },
) => Promise<{
  rows: Record<string, unknown>[];
  liveTruncated?: boolean;
}>;

function parseSettings(raw: unknown): UsageLoggingSettings {
  const settingsRaw = record(raw, "settings");
  if (typeof settingsRaw.enabled !== "boolean") {
    throw new Error("Malformed usage response: settings.enabled");
  }
  const rates = Object.fromEntries(
    Object.entries(LIST_RATES).map(([key, fallback]) => [
      key,
      settingsRaw[key] === undefined || settingsRaw[key] === null
        ? fallback
        : num(settingsRaw, key, "settings"),
    ]),
  ) as Record<keyof typeof LIST_RATES, number>;
  const hasMillionRate =
    settingsRaw.compute_vcpu_rate_per_million_ms !== undefined &&
    settingsRaw.compute_vcpu_rate_per_million_ms !== null;
  const hasExcessRate =
    settingsRaw.compute_vcpu_excess_rate_per_second !== undefined &&
    settingsRaw.compute_vcpu_excess_rate_per_second !== null;
  if (hasMillionRate && !hasExcessRate) {
    rates.compute_vcpu_excess_rate_per_second =
      rates.compute_vcpu_rate_per_million_ms / 1000;
  } else if (hasExcessRate && !hasMillionRate) {
    rates.compute_vcpu_rate_per_million_ms =
      rates.compute_vcpu_excess_rate_per_second * 1000;
  }
  if (rates.compute_vcpu_free_ms_per_request < 0) {
    rates.compute_vcpu_free_ms_per_request =
      LIST_RATES.compute_vcpu_free_ms_per_request;
  }
  if (rates.compute_vcpu_rounding_ms < 0) {
    rates.compute_vcpu_rounding_ms = LIST_RATES.compute_vcpu_rounding_ms;
  }
  return {
    ...rates,
    enabled: settingsRaw.enabled,
    retention_days: num(settingsRaw, "retention_days", "settings"),
    class_a_rate_per_1k: num(settingsRaw, "class_a_rate_per_1k", "settings"),
    class_b_rate_per_10k: num(settingsRaw, "class_b_rate_per_10k", "settings"),
    cdn_egress_rate_per_gb: num(
      settingsRaw,
      "cdn_egress_rate_per_gb",
      "settings",
    ),
    storage_rate_per_gb_month: num(
      settingsRaw,
      "storage_rate_per_gb_month",
      "settings",
    ),
    min_billed_days: num(settingsRaw, "min_billed_days", "settings"),
  };
}

function parseRows(
  raw: Record<UsageKind, Record<string, unknown>[]>,
): UsageInput["rows"] {
  return {
    fos_usage: rows(raw.fos_usage, "fos_usage", (row) => ({
      timestamp: num(row, "timestamp", "fos_usage"),
      op: str(row, "op", "fos_usage"),
      op_class: oneOf(row, "op_class", ["A", "B", "CDN_HIT"], "B", "fos_usage"),
      op_count: num(row, "op_count", "fos_usage"),
      dataset: str(row, "dataset", "fos_usage"),
      caller: str(row, "caller", "fos_usage"),
      cdn_cache: oneOf(
        row,
        "cdn_cache",
        ["HIT", "MISS", "DIRECT"],
        "DIRECT",
        "fos_usage",
      ),
      bytes_in: num(row, "bytes_in", "fos_usage"),
      bytes_out: num(row, "bytes_out", "fos_usage"),
      bytes_deleted: num(row, "bytes_deleted", "fos_usage"),
      bytes_deleted_complete:
        typeof row.bytes_deleted_complete === "boolean"
          ? row.bytes_deleted_complete
          : undefined,
      est_cost_usd: num(row, "est_cost_usd", "fos_usage"),
    })),
    api_telemetry: rows(raw.api_telemetry, "api_telemetry", (row) => ({
      timestamp: num(row, "timestamp", "api_telemetry"),
      source: oneOf(
        row,
        "source",
        ["compute_api", "fastly_api", "browser_query"],
        "compute_api",
        "api_telemetry",
      ),
      path: str(row, "path", "api_telemetry"),
      vcpu_ms: num(row, "vcpu_ms", "api_telemetry"),
      files_fetched: optNum(row, "files_fetched", "api_telemetry"),
    })),
    cron_runs: rows(raw.cron_runs, "cron_runs", (row) => ({
      started_at: num(row, "started_at", "cron_runs"),
      cron_kind: str(row, "cron_kind", "cron_runs"),
      outcome: str(row, "outcome", "cron_runs"),
      vcpu_ms: num(row, "vcpu_ms", "cron_runs"),
      cache_lookups: num(row, "cache_lookups", "cron_runs"),
      backend_requests_used: num(row, "backend_requests_used", "cron_runs"),
      details_json: str(row, "details_json", "cron_runs"),
      idle_ticks: num(row, "idle_ticks", "cron_runs"),
      idle_vcpu_ms: num(row, "idle_vcpu_ms", "cron_runs"),
      run_id: str(row, "run_id", "cron_runs"),
      trigger: str(row, "trigger", "cron_runs"),
      worker_executions: num(row, "worker_executions", "cron_runs"),
      listed_files: optNum(row, "listed_files", "cron_runs"),
      converted_files: optNum(row, "converted_files", "cron_runs"),
      deleted_files: optNum(row, "deleted_files", "cron_runs"),
      rows_written: num(row, "rows_written", "cron_runs"),
      lines_read: num(row, "lines_read", "cron_runs"),
      bytes_written: num(row, "bytes_written", "cron_runs"),
      parquet_files_written: optNum(row, "parquet_files_written", "cron_runs"),
      kv_ops: optNum(row, "kv_ops", "cron_runs"),
      cache_replaces: optNum(row, "cache_replaces", "cron_runs"),
      purges: optNum(row, "purges", "cron_runs"),
    })),
    cron_ticks: rows(raw.cron_ticks, "cron_ticks", (row) => ({
      timestamp: num(row, "timestamp", "cron_ticks"),
      ticks_count: num(row, "ticks_count", "cron_ticks"),
      vcpu_ms: num(row, "vcpu_ms", "cron_ticks"),
    })),
  };
}

/**
 * Builds the summary input from the real readers: the usage-logging settings and the
 * fos_usage, api_telemetry, cron_runs and cron_ticks logs (live rows plus Parquet files).
 * KV, Core Cache replace and purge counts are not logged by the backend, so they stay null.
 */
export async function fetchUsageInput(
  days: number,
  identity: { user?: string; role?: string } = currentOpsIdentity(),
  read: UsageLogReader = readLogs,
  signal?: AbortSignal,
  bounds?: { from: number; to: number },
): Promise<UsageInput> {
  const startOpsEpoch = currentOpsCacheEpoch();
  assertOpsEpoch(startOpsEpoch, signal);
  const resolvedIdentity = currentOpsIdentity(identity);
  const wallNow = Date.now();
  const now = bounds?.to ?? wallNow;
  const from = bounds?.from ?? now - days * DAY_MS;
  const range = {
    from: new Date(from).toISOString(),
    to: new Date(now).toISOString(),
  };
  // When reading a live "now" window (quantized down to the minute), extend cron_runs's
  // upper bound to the current minute ceiling so concurrent useLastSync (1h up to Date.now())
  // slices from this 24h read instead of issuing a second cron_runs read. Rows are still
  // filtered to `<= now` below.
  const cronRunsRange =
    now >= wallNow - 60_000 && now <= wallNow
      ? {
          from: range.from,
          to: new Date(Math.ceil(wallNow / 60_000) * 60_000).toISOString(),
        }
      : range;
  const settingsPromise = readUsageLogging(signal);
  const [settings, ...results] = await Promise.all([
    settingsPromise,
    ...USAGE_KINDS.map((kind) =>
      // Defer fos_usage until settings resolves so a concurrent storageBillingIntervals
      // call registers the wider 30-day fos_usage read first and this read slices from it.
      kind === "fos_usage"
        ? settingsPromise.then(() =>
            read(kind, range, { ...resolvedIdentity, signal }),
          )
        : read(kind, kind === "cron_runs" ? cronRunsRange : range, {
            ...resolvedIdentity,
            signal,
          }),
    ),
  ]);
  assertOpsEpoch(startOpsEpoch, signal);
  const raw = Object.fromEntries(
    USAGE_KINDS.map((kind, index) => [
      kind,
      results[index].rows.filter((row) => {
        const at = Number(row.timestamp ?? row.started_at);
        return !Number.isFinite(at) || (at >= from && at <= now);
      }),
    ]),
  ) as Record<UsageKind, Record<string, unknown>[]>;
  return {
    now,
    days,
    settings: parseSettings(settings),
    rows: parseRows(raw),
    telemetryCoverage: {
      fos_usage:
        parseSettings(settings).enabled && results[0].liveTruncated === false,
    },
    counters: { kv_ops: null, cache_replaces: null, purges: null },
  };
}

export async function fetchUsageSettings(
  signal?: AbortSignal,
): Promise<UsageLoggingSettings> {
  return parseSettings(await readUsageLogging(signal));
}

/** One log kind over `[from, to]`, so each dashboard section loads on its own. */
export async function fetchUsageKind<K extends UsageKind>(
  kind: K,
  range: { from: number; to: number },
  identity: { user?: string; role?: string } = currentOpsIdentity(),
  read: UsageLogReader = readLogs,
  signal?: AbortSignal,
): Promise<UsageInput["rows"][K]> {
  return (await fetchUsageKindWithCoverage(kind, range, identity, read, signal))
    .rows;
}

export async function fetchUsageKindWithCoverage<K extends UsageKind>(
  kind: K,
  range: { from: number; to: number },
  identity: { user?: string; role?: string } = currentOpsIdentity(),
  read: UsageLogReader = readLogs,
  signal?: AbortSignal,
): Promise<{ rows: UsageInput["rows"][K]; complete: boolean }> {
  const startOpsEpoch = currentOpsCacheEpoch();
  assertOpsEpoch(startOpsEpoch, signal);
  const resolvedIdentity = currentOpsIdentity(identity);
  const result = await read(
    kind,
    {
      from: new Date(range.from).toISOString(),
      to: new Date(range.to).toISOString(),
    },
    { ...resolvedIdentity, signal },
  );
  assertOpsEpoch(startOpsEpoch, signal);
  const raw = Object.fromEntries(
    USAGE_KINDS.map((k) => [
      k,
      k === kind
        ? result.rows.filter((row) => {
            const at = Number(row.timestamp ?? row.started_at);
            return !Number.isFinite(at) || (at >= range.from && at <= range.to);
          })
        : [],
    ]),
  ) as Record<UsageKind, Record<string, unknown>[]>;
  return {
    rows: parseRows(raw)[kind],
    complete: result.liveTruncated === false,
  };
}

export interface ComputeHour {
  t: number;
  appRequests: number;
  workerRequests: number;
  appVcpuMs: number;
  workerVcpuMs: number;
  billedMs: number;
}

/** Fastly's own Compute figures for the app and worker services, the numbers it bills. */
export interface ComputeBilling {
  requests: number;
  executionTimeMs: number;
  billedTimeMs: number;
  /** Unix ms span of hourly buckets Platform Metrics returned, or null when it had none. */
  coveredFrom: number | null;
  coveredTo: number | null;
  cdnBandwidthBytes?: number;
  cdnBandwidthByHour?: Array<{ t: number; bytes: number }>;
  /** Delivery bandwidth Fastly bills on the app and worker Compute services. */
  computeBandwidthBytes: number;
  /** Front-service CDN requests; null when the front service was not read. */
  cdnRequests: number | null;
  kvClassA: number;
  kvClassB: number;
  kvStorageGb?: number | null;
  kvByHour?: Array<{ t: number; classA: number; classB: number }>;
  /** Hourly app and worker Compute, merged by hour; absent when the response has no buckets. */
  computeByHour?: ComputeHour[];
  /** Secrets in the install's Secret Store; null when the response lacks it. */
  secrets: number | null;
  /** The same figures per service role, for the cost estimator's per-service units. */
  byRole?: Record<string, { requests: number; vcpuMs: number }>;
}

export function parseComputeBilling(raw: unknown): ComputeBilling {
  const body = raw as { services?: unknown };
  if (!Array.isArray(body?.services)) {
    throw new Error("Malformed billing response: services");
  }
  const billing: ComputeBilling = {
    requests: 0,
    executionTimeMs: 0,
    billedTimeMs: 0,
    coveredFrom: null,
    coveredTo: null,
    computeBandwidthBytes: 0,
    cdnRequests: null,
    kvClassA: 0,
    kvClassB: 0,
    secrets: null,
    byRole: {},
  };
  const kvHours = new Map<number, { classA: number; classB: number }>();
  let hasKvBuckets = false;
  const computeHours = new Map<number, ComputeHour>();
  const computeHour = (t: number) => {
    let hour = computeHours.get(t);
    if (!hour) {
      hour = {
        t,
        appRequests: 0,
        workerRequests: 0,
        appVcpuMs: 0,
        workerVcpuMs: 0,
        billedMs: 0,
      };
      computeHours.set(t, hour);
    }
    return hour;
  };
  for (const service of body.services as Record<string, unknown>[]) {
    if (service.role === "app" || service.role === "worker") {
      const app = service.role === "app";
      const eachBucket = (
        key: string,
        apply: (hour: ComputeHour, value: number) => void,
      ) => {
        const buckets = service[`${key}_buckets`];
        if (!Array.isArray(buckets)) return;
        const ctx = `billing.${key}_buckets`;
        for (const b of buckets as Record<string, unknown>[]) {
          apply(computeHour(num(b, "start_time", ctx) * 1000), num(b, key, ctx));
        }
      };
      eachBucket("compute_requests", (h, v) => {
        if (app) h.appRequests += v;
        else h.workerRequests += v;
      });
      eachBucket("compute_execution_time_ms", (h, v) => {
        if (app) h.appVcpuMs += v;
        else h.workerVcpuMs += v;
      });
      eachBucket("compute_request_time_billed_ms", (h, v) => {
        h.billedMs += v;
      });
    }
    if (typeof service.role === "string") {
      const role = (billing.byRole![service.role] ??= {
        requests: 0,
        vcpuMs: 0,
      });
      role.requests += num(
        service,
        service.role === "front" ? "requests" : "compute_requests",
        "billing",
      );
      role.vcpuMs += num(service, "compute_execution_time_ms", "billing");
    }
    billing.kvClassA += num(service, "kv_store_class_a_operations", "billing");
    billing.kvClassB += num(service, "kv_store_class_b_operations", "billing");
    if (Array.isArray(service.kv_class_a_buckets)) {
      hasKvBuckets = true;
      for (const b of service.kv_class_a_buckets as Record<string, unknown>[]) {
        const t = num(b, "start_time", "billing.kv_class_a_buckets") * 1000;
        const entry = kvHours.get(t) ?? { classA: 0, classB: 0 };
        entry.classA += num(
          b,
          "kv_store_class_a_operations",
          "billing.kv_class_a_buckets",
        );
        kvHours.set(t, entry);
      }
    }
    if (Array.isArray(service.kv_class_b_buckets)) {
      hasKvBuckets = true;
      for (const b of service.kv_class_b_buckets as Record<string, unknown>[]) {
        const t = num(b, "start_time", "billing.kv_class_b_buckets") * 1000;
        const entry = kvHours.get(t) ?? { classA: 0, classB: 0 };
        entry.classB += num(
          b,
          "kv_store_class_b_operations",
          "billing.kv_class_b_buckets",
        );
        kvHours.set(t, entry);
      }
    }
    if (service.role === "front") {
      billing.cdnRequests =
        (billing.cdnRequests ?? 0) + num(service, "requests", "billing");
      billing.cdnBandwidthBytes =
        (billing.cdnBandwidthBytes ?? 0) + num(service, "bandwidth", "billing");
      if (Array.isArray(service.bandwidth_buckets)) {
        billing.cdnBandwidthByHour = [
          ...(billing.cdnBandwidthByHour ?? []),
          ...(service.bandwidth_buckets as Record<string, unknown>[]).map(
            (b) => ({
              t: num(b, "start_time", "billing.bandwidth_buckets") * 1000,
              bytes: num(b, "bandwidth", "billing.bandwidth_buckets"),
            }),
          ),
        ];
      }
    } else {
      billing.computeBandwidthBytes += num(service, "bandwidth", "billing");
      billing.requests += num(service, "compute_requests", "billing");
      billing.executionTimeMs += num(
        service,
        "compute_execution_time_ms",
        "billing",
      );
      billing.billedTimeMs += num(
        service,
        "compute_request_time_billed_ms",
        "billing",
      );
    }
    const from = service.covered_from;
    const to = service.covered_to;
    if (typeof from === "number") {
      billing.coveredFrom = Math.min(
        billing.coveredFrom ?? Infinity,
        from * 1000,
      );
    }
    if (typeof to === "number") {
      billing.coveredTo = Math.max(billing.coveredTo ?? 0, to * 1000);
    }
  }
  if (hasKvBuckets) {
    billing.kvByHour = [...kvHours.entries()]
      .sort(([a], [b]) => a - b)
      .map(([t, v]) => ({ t, classA: v.classA, classB: v.classB }));
  }
  if (computeHours.size > 0) {
    billing.computeByHour = [...computeHours.values()].sort((a, b) => a.t - b.t);
  }
  const store = (raw as { secret_store?: Record<string, unknown> })
    .secret_store;
  if (store && typeof store === "object") {
    billing.secrets = num(store, "secrets", "billing.secret_store");
  }
  return billing;
}

export interface IncurredLine {
  id: "storage" | "ops" | "delivery" | "compute" | "kv" | "secrets" | "config";
  label: string;
  /** USD, or null when the quantity is unavailable to this viewer. */
  usd: number | null;
  detail: string;
  /** "fastly" when the quantity comes from Fastly's Stats/API; "logged" from this app's telemetry. */
  source: "fastly" | "logged" | "none";
  /** The line's priced components, shown under its total (CDN delivery: egress and requests). */
  parts?: Array<{ label: string; usd: number }>;
}

export interface IncurredInput {
  settings: UsageLoggingSettings;
  /** Selected period length in hours. */
  hours: number;
  billedGbHours: number;
  classA: number;
  classB: number;
  cdnHits?: number;
  /** Front-service bytes this app logged reading through the CDN; used without Fastly figures. */
  loggedCdnBytes: number;
  loggedComputeRequests: number;
  loggedComputeVcpuMs: number;
  loggedComputeSegments?: ComputeExecutionSegment[];
  billing: ComputeBilling | null;
  /** Estimated USD attributable to operational logs & developer telemetry over the period. */
  telemetryUsd?: number;
}

/**
 * Computes the subset of incurred USD attributable to operational logs and developer telemetry
 * (telemetry datasets in Object Storage, telemetry FOS operations and CDN egress, KV Store
 * telemetry operations, and telemetry cron/API Compute).
 */
export function telemetryCostUsd(
  summary: UsageSummary,
  settings: UsageLoggingSettings,
  billing?: ComputeBilling | null,
): number {
  const storageGbMonths = summary.storage.datasets
    .filter((d) => !NON_TELEMETRY_DATASETS.has(d.dataset))
    .reduce((sum, d) => sum + d.billedGbMonths, 0);
  const storageUsd = storageGbMonths * settings.storage_rate_per_gb_month;

  const opsSource = summary.telemetryOps
    ? [summary.telemetryOps]
    : summary.byCaller.filter((c) => isTelemetryCaller(c.caller));
  const opsUsd = opsSource.reduce(
    (sum, c) =>
      sum +
      (c.classA / 1000) * settings.class_a_rate_per_1k +
      (c.classB / 10000) * settings.class_b_rate_per_10k,
    0,
  );
  const egressUsd =
    ((summary.telemetryBytesRead ?? 0) / 1e9) * settings.cdn_egress_rate_per_gb;

  const cronCompute = summary.computeByCron.filter(
    (c) => c.key === "log_flush" || c.key === "log_compact",
  );
  const routeCompute = summary.computeByRoute.filter((r) =>
    isTelemetryRoute(r.key),
  );
  const computeRequests =
    cronCompute.reduce((s, c) => s + c.requests, 0) +
    routeCompute.reduce((s, r) => s + r.requests, 0);
  const computeVcpuUsd = [...cronCompute, ...routeCompute].reduce((sum, g) => {
    const segs =
      g.segments && g.segments.length > 0
        ? g.segments
        : [{ vcpuMs: g.vcpuMs, requests: g.requests }];
    return (
      sum +
      segs.reduce(
        (segSum, seg) =>
          segSum +
          billedVcpuForExecutions(seg.vcpuMs, seg.requests, settings).usd,
        0,
      )
    );
  }, 0);
  const computeUsd =
    (computeRequests / 1e6) * settings.compute_request_rate_per_million +
    computeVcpuUsd;

  let kvUsd = 0;
  if (billing && (billing.kvClassA > 0 || billing.kvClassB > 0)) {
    const ingestFiles = summary.byCaller
      .filter((c) => c.caller === "cron:ingest" || c.caller === "worker:ingest")
      .reduce((s, c) => s + c.classB, 0);
    const telemetryKvA = Math.max(0, billing.kvClassA - 0.25 * ingestFiles);
    const telemetryKvB = Math.max(0, billing.kvClassB - 0.5 * ingestFiles);
    kvUsd =
      (telemetryKvA / 1e6) * settings.kv_class_a_rate_per_million +
      (telemetryKvB / 1e6) * settings.kv_class_b_rate_per_million;
  } else if (summary.telemetryKvOps && summary.telemetryKvOps > 0) {
    kvUsd =
      (summary.telemetryKvOps / 1e6) * settings.kv_class_a_rate_per_million;
  }

  return storageUsd + opsUsd + egressUsd + computeUsd + kvUsd;
}

/**
 * Every Fastly charge this install incurs over the period, at marginal list rates: Object Storage
 * capacity and operations, CDN delivery (front requests plus bandwidth on all three services),
 * Compute on the app and worker services, KV Store operations, Secret Store secrets, and the
 * Config Store, which has no usage charge. Fastly's own figures are used when available.
 */
export function incurredCost(input: IncurredInput): {
  total: number;
  telemetryUsd: number;
  lines: IncurredLine[];
} {
  const { settings: r, billing } = input;
  const months = input.hours / 720;
  const fmt = (n: number) => Math.round(n).toLocaleString("en-US");
  const gb = (bytes: number) => bytes / 1e9;
  const lines: IncurredLine[] = [
    {
      id: "storage",
      label: "Object Storage",
      usd: (input.billedGbHours / 720) * r.storage_rate_per_gb_month,
      detail: `${input.billedGbHours.toFixed(2)} GB-hrs`,
      source: "logged",
    },
    {
      id: "ops",
      label: "Storage operations",
      usd:
        (input.classA / 1000) * r.class_a_rate_per_1k +
        (input.classB / 10000) * r.class_b_rate_per_10k,
      detail: `A ${fmt(input.classA)} · B ${fmt(input.classB)}`,
      source: "logged",
    },
  ];
  if (billing) {
    // Fastly bills every request and byte on the front VCL service, its own health probes
    // (`/_tick`, `/_flush`) included, so price its Platform Metrics totals plus Compute egress on
    // app and worker. Logged `/fos/...` file deliveries stand in only when the front wasn't read.
    const frontRead = billing.cdnRequests !== null;
    const bytes =
      (frontRead ? (billing.cdnBandwidthBytes ?? 0) : input.loggedCdnBytes) +
      billing.computeBandwidthBytes;
    const requests = frontRead
      ? (billing.cdnRequests ?? 0)
      : input.classB + (input.cdnHits ?? 0);
    const roleEntries = Object.entries(billing.byRole ?? {}).filter(
      ([role, v]) => role !== "front" && (v.requests > 0 || v.vcpuMs > 0),
    );
    const vcpuUsd =
      roleEntries.length > 0
        ? roleEntries.reduce(
            (sum, [, role]) =>
              sum + billedVcpuForExecutions(role.vcpuMs, role.requests, r).usd,
            0,
          )
        : billedVcpuForExecutions(billing.executionTimeMs, billing.requests, r)
            .usd;
    const egressUsd = gb(bytes) * r.cdn_egress_rate_per_gb;
    const requestsUsd = (requests / 10000) * r.cdn_request_rate_per_10k;
    lines.push(
      {
        id: "delivery",
        label: "CDN delivery",
        usd: egressUsd + requestsUsd,
        detail: `${gb(bytes).toFixed(2)} GB · ${fmt(requests)} front reqs${frontRead ? "" : " (logged)"}`,
        source: frontRead ? "fastly" : "logged",
        parts: [
          { label: "Egress", usd: egressUsd },
          { label: "Requests", usd: requestsUsd },
        ],
      },
      {
        id: "compute",
        label: "Compute (app + worker)",
        usd:
          (billing.requests / 1e6) * r.compute_request_rate_per_million +
          vcpuUsd,
        detail: `${fmt(billing.requests)} reqs · ${fmt(billing.executionTimeMs)} vCPU ms`,
        source: "fastly",
      },
      {
        id: "kv",
        label: "KV Store operations",
        usd:
          priceKvStorageGb(billing.kvStorageGb ?? 0.005) * months +
          (billing.kvClassA / 1e6) * r.kv_class_a_rate_per_million +
          (billing.kvClassB / 1e6) * r.kv_class_b_rate_per_million,
        detail: `${(billing.kvStorageGb ?? 0.005).toFixed(2)} GB (est.) · A ${fmt(billing.kvClassA)} · B ${fmt(billing.kvClassB)}`,
        source: "fastly",
      },
    );
  } else {
    const loggedVcpuUsd =
      input.loggedComputeSegments && input.loggedComputeSegments.length > 0
        ? input.loggedComputeSegments.reduce(
            (sum, seg) =>
              sum + billedVcpuForExecutions(seg.vcpuMs, seg.requests, r).usd,
            0,
          )
        : billedVcpuForExecutions(
            input.loggedComputeVcpuMs,
            input.loggedComputeRequests,
            r,
          ).usd;
    lines.push(
      {
        id: "delivery",
        label: "CDN delivery",
        usd: gb(input.loggedCdnBytes) * r.cdn_egress_rate_per_gb,
        detail: `${gb(input.loggedCdnBytes).toFixed(2)} GB read; requests not logged`,
        source: "logged",
        parts: [
          { label: "Egress", usd: gb(input.loggedCdnBytes) * r.cdn_egress_rate_per_gb },
        ],
      },
      {
        id: "compute",
        label: "Compute (app + worker)",
        usd:
          (input.loggedComputeRequests / 1e6) *
            r.compute_request_rate_per_million +
          loggedVcpuUsd,
        detail: `${fmt(input.loggedComputeRequests)} reqs · ${fmt(input.loggedComputeVcpuMs)} vCPU ms`,
        source: "logged",
      },
      {
        id: "kv",
        label: "KV Store operations",
        usd: null,
        detail: "Admin only (Fastly stats)",
        source: "none",
      },
    );
  }
  const secrets = billing?.secrets ?? null;
  lines.push(
    {
      id: "secrets",
      label: "Secret Store",
      usd:
        secrets === null
          ? null
          : Math.max(0, secrets - FREE_SECRETS) * r.secret_rate_per_month * months,
      detail:
        secrets === null
          ? "Admin only (Fastly API)"
          : `${secrets} secrets × ${(months * 30).toFixed(1)} days`,
      source: secrets === null ? "none" : "fastly",
    },
    {
      id: "config",
      label: "Config Store",
      usd: 0,
      detail: "No usage charge",
      source: "fastly",
    },
  );
  return {
    total: lines.reduce((sum, line) => sum + (line.usd ?? 0), 0),
    telemetryUsd: input.telemetryUsd ?? 0,
    lines,
  };
}

/** Fastly Platform Metrics Compute totals for `[from, to]` (unix ms), hourly resolution. */
export async function fetchComputeBilling(
  range: { from: number; to: number },
  signal?: AbortSignal,
): Promise<ComputeBilling> {
  return parseComputeBilling(
    await adminRequest<unknown>(
      `/api/admin/usage/compute-billing?from=${Math.floor(range.from)}&to=${Math.ceil(range.to)}`,
      { signal },
    ),
  );
}

export async function requestStorageReconcile(): Promise<boolean> {
  const result = await reconcileStorage();
  return result.started;
}

export const buildSevenLineCostTable = incurredCost;

export function billedVcpuForSegments(
  segments: readonly ComputeExecutionSegment[],
  settings: Pick<
    UsageLoggingSettings,
    | "compute_vcpu_free_ms_per_request"
    | "compute_vcpu_excess_rate_per_second"
    | "compute_vcpu_rounding_ms"
  > & { compute_vcpu_rate_per_million_ms?: number } = DEFAULT_SETTINGS,
): { billedMs: number; usd: number } {
  let billedMs = 0;
  let usd = 0;
  for (const seg of segments) {
    const res = billedVcpuForExecutions(seg.vcpuMs, seg.requests, settings);
    billedMs += res.billedMs;
    usd += res.usd;
  }
  return { billedMs, usd };
}

export function filterRowsInWindow<
  T extends { timestamp?: number; started_at?: number },
>(rows: readonly T[], fromMs: number, toMs: number): T[] {
  return rows.filter((row) => {
    const ts = Number(row.timestamp ?? row.started_at ?? 0);
    return Number.isFinite(ts) && ts >= fromMs && ts <= toMs;
  });
}

type UsageSummaryResult = UsageSummary & {
  input: UsageInput;
  summary: UsageSummary;
  lines: IncurredLine[];
  totalUsd: number;
};

const SUMMARY_TTL_MS = 15_000;
const sharedUsageSummaries = createSharedRequests<UsageSummaryResult>();
const settledUsageSummaries = new Map<
  string,
  { at: number; value: UsageSummaryResult }
>();

export async function fetchUsageSummary(options: {
  from: string | number;
  to: string | number;
  signal?: AbortSignal;
}): Promise<UsageSummaryResult> {
  const fromMs =
    typeof options.from === "number" ? options.from : Date.parse(options.from);
  const toMs =
    typeof options.to === "number" ? options.to : Date.parse(options.to);
  const identity = currentOpsIdentity();
  const startOpsEpoch = currentOpsCacheEpoch();
  const key = JSON.stringify([
    fromMs,
    toMs,
    identity.user,
    identity.role,
    activeOpsUser(),
    dashboardCacheIdentity(),
    startOpsEpoch,
  ]);
  const wallNow = Date.now();
  for (const [k, entry] of settledUsageSummaries) {
    if (wallNow - entry.at >= SUMMARY_TTL_MS) settledUsageSummaries.delete(k);
  }
  const cached = settledUsageSummaries.get(key);
  if (cached) return cached.value;
  return sharedUsageSummaries(
    key,
    async (sharedSignal) => {
      const days = Math.max(1, (toMs - fromMs) / DAY_MS);
      const input = await fetchUsageInput(
        days,
        identity,
        readLogs,
        sharedSignal,
        { from: fromMs, to: toMs },
      );
      const summary = summarizeUsage(input);
      const incurred = incurredCost({
        hours: days * 24,
        settings: input.settings,
        billedGbHours: summary.storage.billedGbMonths * 720,
        classA: summary.totals.classA,
        classB: summary.totals.classB,
        cdnHits: summary.totals.cdnHit,
        loggedCdnBytes: summary.bytesRead,
        loggedComputeRequests: summary.computeRequests,
        loggedComputeVcpuMs: summary.computeVcpuMs,
        loggedComputeSegments: summary.computeSegments,
        billing: null,
      });
      const result: UsageSummaryResult = {
        ...summary,
        input,
        summary,
        lines: incurred.lines,
        totalUsd: incurred.total,
      };
      if (startOpsEpoch === currentOpsCacheEpoch()) {
        settledUsageSummaries.set(key, { at: Date.now(), value: result });
      }
      return result;
    },
    { signal: options.signal },
  );
}
