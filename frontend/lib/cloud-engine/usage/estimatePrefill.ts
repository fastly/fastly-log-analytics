import { getLoggedTraffic } from "../telemetry/admin";
import { readLogs } from "../telemetry/client";
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

type Reader = (
  kind: "ingest_files",
  range: { from: string; to: string },
  options: { user: string; role: string; signal?: AbortSignal },
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
}

/** Everything the estimator measures from this install; any source that fails keeps its defaults. */
export async function fetchEstimatePrefill(
  identity: { user: string; role: string },
  read: UsageLogReader & Reader = readLogs as UsageLogReader & Reader,
  signal?: AbortSignal,
  now = Date.now(),
): Promise<MeasuredEstimate> {
  const range = { from: now - UNIT_WINDOW_DAYS * DAY_MS, to: now };
  const iso = {
    from: new Date(range.from).toISOString(),
    to: new Date(range.to).toISOString(),
  };
  const optional = <T>(promise: Promise<T>) => promise.catch(() => null);
  const settings = await fetchUsageSettings(signal);
  const [traffic, billing, runs, fos, files] = await Promise.all([
    optional(getLoggedTraffic(signal)),
    optional(fetchComputeBilling(range, signal)),
    optional(fetchUsageKind("cron_runs", range, identity, read, signal)),
    settings.enabled
      ? optional(fetchUsageKind("fos_usage", range, identity, read, signal))
      : Promise.resolve(null),
    optional(read("ingest_files", iso, { ...identity, signal })),
  ]);
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
  return { ...prefill, rates: settings, usageLogging: fos !== null };
}
