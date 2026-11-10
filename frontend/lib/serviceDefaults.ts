/**
 * Per-service setting defaults shared by every form, calculator and fallback in the UI.
 *
 * They mirror what both deployments persist when a value is absent: the self-hosted backend
 * (`backend/config.py` log_period, `backend/models/common/provision.py` retention) and the Cloud
 * app (`compute/cloud/src/admin/model.rs` `default_retention_days`, sample-rate bounds;
 * `compute/cloud/src/logging/rum.rs` `DEFAULT_RUM_LOG_PERIOD`; usage-log
 * retention in `backend/config.py` `_UsageLoggingConfigModel` and `compute/cloud/src/auth/settings.rs`).
 * Change them together with those sources so the UI never shows a default a backend would not apply.
 */
export const SERVICE_DEFAULTS = {
  logPeriodSeconds: 60,
  rumLogPeriodSeconds: 300,
  sampleRatePercent: 100,
  logRetentionDays: 30,
  rumRetentionDays: 30,
  usageLogRetentionDays: 30,
} as const;
