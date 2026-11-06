export const OPS_KINDS = [
  "cron_runs",
  "cron_ticks",
  "ingest_files",
  "fos_usage",
  "admin_audit",
  "api_telemetry",
  "platform_errors",
] as const;

export const LOG_KINDS = OPS_KINDS;

export type OpsKind = (typeof OPS_KINDS)[number];

/** Kinds held in the Core Cache; only these have an unflushed cache to clear (admin_audit is immutable). */
export const CLEARABLE_KINDS = [
  "fos_usage",
  "api_telemetry",
  "platform_errors",
] as const satisfies readonly OpsKind[];

export type LogRow = Record<string, unknown> & {
  timestamp: number;
  event_id: string;
};

export type LiveStatus = {
  logq_depth?: number;
  logq_oldest_age_s?: number | null;
  dropped?: number;
};

export type LogsStats = {
  listed: number;
  read: number;
  cached: number;
  cdnHits: number;
  skippedCovered: number;
  ignoredCompact: number;
  evicted: number;
  liveRows: number;
};

export type LogsResult = {
  kind: OpsKind;
  from: string;
  to: string;
  rows: LogRow[];
  oldestLiveMs: number | null;
  liveTruncated?: boolean;
  status: LiveStatus | null;
  stats: LogsStats;
};

export type LogsRange = { from: string; to: string };
