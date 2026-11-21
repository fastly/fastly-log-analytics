import { isCloudMode } from '@/lib/cloud-engine/mode';

/**
 * Per-service setting defaults shared by every form, calculator and fallback in the UI.
 *
 * They mirror what both deployments persist when a value is absent: the self-hosted backend
 * (`backend/core/service_defaults.py`, including `DEFAULT_SNAPSHOT_INTERVAL_MINUTES` and
 * `VALID_SNAPSHOT_INTERVAL_MINUTES`) and the Cloud app (`compute/cloud/src/logging/mod.rs`
 * `DEFAULT_LOG_PERIOD` and `MIN_LOG_PERIOD`, `compute/cloud/src/admin/model.rs` `default_retention_days`, sample-rate bounds;
 * `compute/cloud/src/logging/rum.rs` `DEFAULT_RUM_LOG_PERIOD`; usage-log
 * retention in `backend/config.py` `_UsageLoggingConfigModel` and `compute/cloud/src/auth/settings.rs`).
 * Change them together with those sources so the UI never shows a default a backend would not apply.
 */
export const SERVICE_DEFAULTS = {
  logPeriodSeconds: 10,
  rumLogPeriodSeconds: 300,
  sampleRatePercent: 100,
  logRetentionDays: 30,
  rumRetentionDays: 30,
  usageLogRetentionDays: 30,
  snapshotIntervalMinutes: 5,
} as const;

export const SNAPSHOT_INTERVAL_OPTIONS = [5, 15, 30, 60, 360, 1440] as const;
export type SnapshotIntervalMinutes = (typeof SNAPSHOT_INTERVAL_OPTIONS)[number];

export const SNAPSHOT_INTERVAL_LABELS: Record<SnapshotIntervalMinutes, string> = {
  5: '5m',
  15: '15m',
  30: '30m',
  60: '1h',
  360: '6h',
  1440: '24h',
};

export const SNAPSHOT_INTERVAL_SELECT_OPTIONS = SNAPSHOT_INTERVAL_OPTIONS.map((value) => ({
  value: String(value),
  label: SNAPSHOT_INTERVAL_LABELS[value],
}));

export function normalizeSnapshotInterval(value: number | null | undefined): SnapshotIntervalMinutes {
  return (SNAPSHOT_INTERVAL_OPTIONS as readonly number[]).includes(value as number)
    ? (value as SnapshotIntervalMinutes)
    : SERVICE_DEFAULTS.snapshotIntervalMinutes;
}

/** Cloud rejects request and RUM log periods below one minute (`MIN_LOG_PERIOD`). */
const CLOUD_MIN_LOG_PERIOD_SECONDS = 60;

function minLogPeriodSeconds(): number {
  return isCloudMode() ? CLOUD_MIN_LOG_PERIOD_SECONDS : 1;
}

export function allowedLogPeriods<T extends string>(options: readonly T[]): T[] {
  const min = minLogPeriodSeconds();
  return options.filter((value) => Number(value) >= min);
}

export function effectiveLogPeriod(seconds: number | null | undefined): number {
  return Math.max(seconds || SERVICE_DEFAULTS.logPeriodSeconds, minLogPeriodSeconds());
}
