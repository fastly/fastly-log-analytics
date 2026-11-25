import { dashboardCacheIdentity } from "../dashboard/sources";
import { createSharedRequests } from "../shared-request";
import { getLoggedTraffic } from "../telemetry/admin";
import { readLogs } from "../telemetry/client";
import {
  activeOpsUser,
  assertOpsEpoch,
  currentOpsCacheEpoch,
  currentOpsIdentity,
} from "../telemetry/sources";
import { usageComputeBilling } from "./computeUsage";
import {
  measureEstimate,
  type EstimateRates,
  type EstimatePrefill,
} from "./estimate";
import { DAY_MS } from "./rates";
import {
  fetchComputeBilling,
  fetchUsageKind,
  fetchUsageSettings,
  reconcileSnapshot,
  type CronRunRow,
  type UsageLogReader,
} from "./summary";

/** Days of conversions and billing the unit costs are measured over. */
export const UNIT_WINDOW_DAYS = 7;

const PREFILL_TTL_MS = 15_000;
const sharedPrefills = createSharedRequests<MeasuredEstimate>();
const settledPrefills = new Map<string, { at: number; value: MeasuredEstimate }>();

export function resetEstimatePrefillCache(): void {
  settledPrefills.clear();
}

const INGEST_PREFILL_COLUMNS = [
  "timestamp",
  "status",
  "raw_size",
  "raw_bytes",
  "rows",
] as const;

type Reader = (
  kind: "ingest_files",
  range: { from: string; to: string },
  options: {
    user: string;
    role: string;
    signal?: AbortSignal;
    columns?: readonly string[];
    dedupe?: boolean;
    sort?: boolean;
  },
) => Promise<{ rows: Record<string, unknown>[] }>;

function number(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function fixedStoredBytes(runs: CronRunRow[]): number | null {
  const snapshot = reconcileSnapshot(runs);
  if (!snapshot || Object.keys(snapshot.datasets).length === 0) return null;
  return Object.entries(snapshot.datasets)
    .filter(([name]) => !name.includes("request"))
    .reduce((sum, [, bytes]) => sum + number(bytes), 0);
}

export interface MeasuredEstimate extends EstimatePrefill {
  rates: EstimateRates;
  /** Object Storage operations were recorded; otherwise those units are defaults. */
  usageLogging: boolean;
  /** Service's Fastly Object Storage region (e.g. `us-east-1`), used to default the delivery billing zone. */
  fosRegion?: string;
}

async function computeEstimatePrefill(
  resolvedIdentity: { user: string; role: string },
  read: UsageLogReader & Reader,
  signal: AbortSignal | undefined,
  resolvedNow: number,
  hasExplicitNow: boolean,
  startOpsEpoch: number,
): Promise<MeasuredEstimate> {
  const range = { from: resolvedNow - UNIT_WINDOW_DAYS * DAY_MS, to: resolvedNow };
  // When using the default minute-quantized window, extend cron_runs by 60s to the
  // ceiling minute so concurrent 24h usage summary AND unquantized 1h last-sync reads
  // both slice from this single 7d cron_runs read.
  const cronRange = hasExplicitNow
    ? range
    : { from: range.from, to: resolvedNow + 60_000 };
  const iso = {
    from: new Date(range.from).toISOString(),
    to: new Date(range.to).toISOString(),
  };
  const optional = <T>(promise: Promise<T>) =>
    promise.catch((err: unknown) => {
      if (signal?.aborted || startOpsEpoch !== currentOpsCacheEpoch()) {
        throw err;
      }
      return null;
    });
  const settingsPromise = fetchUsageSettings(signal);
  // Yield one microtask after settings resolves so a concurrent /api/usage/current-storage
  // call registers its 31d fos_usage and ingest_files superset reads first.
  const afterStorageBillingStart = settingsPromise.then(async (s) => {
    await Promise.resolve();
    return s;
  });
  const isDefaultRead = read === (readLogs as UsageLogReader & Reader);
  const billingPromise = isDefaultRead
    ? optional(usageComputeBilling(range, signal))
    : optional(fetchComputeBilling(range, signal));
  const [settings, traffic, billing, runs, fos, files] = await Promise.all([
    settingsPromise,
    optional(getLoggedTraffic(signal)),
    billingPromise,
    optional(fetchUsageKind("cron_runs", cronRange, resolvedIdentity, read, signal)),
    afterStorageBillingStart.then((s) =>
      s.enabled
        ? optional(
            fetchUsageKind("fos_usage", range, resolvedIdentity, read, signal),
          )
        : null,
    ),
    afterStorageBillingStart.then(() =>
      optional(
        read("ingest_files", iso, {
          ...resolvedIdentity,
          signal,
          columns: INGEST_PREFILL_COLUMNS,
          dedupe: false,
          sort: false,
        }),
      ),
    ),
  ]);
  assertOpsEpoch(startOpsEpoch, signal);
  const ingest = (runs ?? []).filter(
    (run) => run.cron_kind === "request_ingest",
  );
  const rum = (runs ?? []).filter((run) => run.cron_kind === "rum_ingest");
  const prefill = measureEstimate({
    days: UNIT_WINDOW_DAYS,
    service: traffic?.logging ?? null,
    traffic: traffic?.days ?? null,
    billing,
    fos,
    files: (files?.rows ?? [])
      .filter((row) => {
        const at = number(row.timestamp);
        return at >= range.from && at <= range.to;
      })
      .map((row) => ({
        timestamp: number(row.timestamp),
        status: String(row.status ?? ""),
        raw_size: number(row.raw_size),
        raw_bytes: row.raw_bytes == null ? undefined : number(row.raw_bytes),
        rows: number(row.rows),
      })),
    parquet: {
      bytes: ingest.reduce((sum, run) => sum + (run.bytes_written ?? 0), 0),
      rows: ingest.reduce((sum, run) => sum + (run.rows_written ?? 0), 0),
      files: ingest.reduce(
        (sum, run) => sum + (run.parquet_files_written ?? 0),
        0,
      ),
    },
    rum: runs
      ? {
          lines: rum.reduce((sum, run) => sum + (run.lines_read ?? 0), 0),
          rows: rum.reduce((sum, run) => sum + (run.rows_written ?? 0), 0),
          bytes: rum.reduce((sum, run) => sum + (run.bytes_written ?? 0), 0),
        }
      : null,
    fixedStoredBytes: runs ? fixedStoredBytes(runs) : null,
    minBilledDays: settings.min_billed_days,
  });
  return {
    ...prefill,
    rates: settings,
    usageLogging: fos !== null,
    fosRegion: traffic?.logging?.fos_region,
  };
}

/** Everything the estimator measures from this install; any source that fails keeps its defaults. */
export async function fetchEstimatePrefill(
  identity: { user?: string; role?: string } = currentOpsIdentity(),
  read: UsageLogReader & Reader = readLogs as UsageLogReader & Reader,
  signal?: AbortSignal,
  now?: number,
): Promise<MeasuredEstimate> {
  const startOpsEpoch = currentOpsCacheEpoch();
  assertOpsEpoch(startOpsEpoch, signal);
  const resolvedIdentity = currentOpsIdentity(identity);
  const hasExplicitNow = now !== undefined;
  const resolvedNow = now ?? Math.floor(Date.now() / 60_000) * 60_000;
  const isDefaultRead = read === (readLogs as UsageLogReader & Reader);
  if (!isDefaultRead || hasExplicitNow) {
    return computeEstimatePrefill(
      resolvedIdentity,
      read,
      signal,
      resolvedNow,
      hasExplicitNow,
      startOpsEpoch,
    );
  }
  const cacheKey = JSON.stringify([
    startOpsEpoch,
    dashboardCacheIdentity(),
    activeOpsUser(),
    resolvedIdentity.user,
    resolvedIdentity.role,
    resolvedNow,
  ]);
  const wallNow = Date.now();
  for (const [k, entry] of settledPrefills) {
    if (wallNow - entry.at >= PREFILL_TTL_MS) settledPrefills.delete(k);
  }
  const cached = settledPrefills.get(cacheKey);
  if (cached) return cached.value;
  return sharedPrefills(
    cacheKey,
    async (sharedSignal) => {
      const value = await computeEstimatePrefill(
        resolvedIdentity,
        read,
        sharedSignal,
        resolvedNow,
        false,
        startOpsEpoch,
      );
      if (startOpsEpoch === currentOpsCacheEpoch()) {
        settledPrefills.set(cacheKey, { at: Date.now(), value });
      }
      return value;
    },
    { signal },
  );
}
