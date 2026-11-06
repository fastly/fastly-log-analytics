import type { paths } from "../cloud-api.generated";
import type { OpsKind } from "./kinds";

export type OperationalLogSettings = {
  flush_max_rows: number;
  flush_max_age_s: number;
  flush_min_interval_s: number;
  dev_telemetry_enabled?: boolean;
  browser_query_telemetry: boolean;
  compaction_enabled: boolean;
  retention_days_by_kind: Record<string, number>;
};

export type OperationalLoggingState = {
  settings: OperationalLogSettings;
  defaults: OperationalLogSettings;
  dev_telemetry_available?: boolean;
};

export type OperationalLoggingChange = {
  settings: OperationalLogSettings;
  changed: string[];
  dev_telemetry_available?: boolean;
};

export type FlushResult = {
  status: string;
  rows: number;
  files: number;
  purged: number;
  outbox_depth: number;
};

export type HistoryJob = {
  kind: string;
  older_than_days: number | null;
  cutoff_ts: number;
  status: "deleting" | "purging" | "complete" | string;
  actor: string;
  started_at: number;
  updated_at: number;
  deleted: number;
  error: string | null;
};

export type SweepResult = {
  deleted: number;
  failed: number;
  bytes: number;
  more_pending: boolean;
  datasets: unknown;
  backend_calls: number;
};

export type BackfillJob = {
  status: "running" | "complete" | string;
  actor: string;
  started_at: number;
  updated_at: number;
  from_ts: number | null;
  to_ts: number | null;
  after_key: string;
  to_key: string;
  scanned: number;
  planned: number;
};

export type LoggedTrafficDay = {
  day: number;
  requests?: number;
  edge_requests?: number;
  expected_rows?: number;
  ingested_rows?: number;
};

export type LoggedTraffic = {
  service_id: string;
  from: number;
  to: number;
  days: LoggedTrafficDay[];
  logging?: {
    log_period: number;
    sample_rate: number;
    edge_only: boolean;
    delete_after: boolean;
    log_retention_days: number;
  };
};

export type LogsStatus = {
  delete_history: HistoryJob | null;
  storage_reconcile: { requested_at: number; actor: string } | null;
  last_sweep: { at: number; complete: boolean; next: number } | null;
  backfill?: BackfillJob | null;
};

const MESSAGES: Record<string, string> = {
  settings_conflict:
    "These settings were changed by someone else. Reload the current values and try again.",
  dev_telemetry_unavailable:
    "Developer telemetry is disabled in this environment (set FLP_DEV_TELEMETRY=1 to enable).",
  flush_in_progress: "A flush is already running. Try again shortly.",
  scan_behind:
    "The log scan is behind, so the cache cannot be cleared safely yet. Try again after the next tick.",
  history_job_in_progress: "A history delete is already in progress.",
  backfill_in_progress: "A backfill scan is already running.",
  kind_not_clearable: "That log kind has no cache to clear.",
  service_not_configured: "No service is provisioned yet.",
};

export class AdminApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string | null,
  ) {
    super(message);
    this.name = "AdminApiError";
  }
}

const base = () => process.env.NEXT_PUBLIC_API_URL || "";

export async function adminRequest<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(base() + path, {
    credentials: "same-origin",
    ...init,
  });
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as {
      detail?: unknown;
      error?: unknown;
    } | null;
    const code =
      typeof body?.error === "string"
        ? body.error
        : typeof body?.detail === "string"
          ? body.detail
          : null;
    throw new AdminApiError(
      (code && MESSAGES[code]) ?? code ?? `Request failed (${response.status})`,
      response.status,
      code,
    );
  }
  return (await response.json().catch(() => ({}))) as T;
}

const json = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

export const getOperationalLogging = () =>
  adminRequest<OperationalLoggingState>("/api/admin/operational-logging");

export const patchOperationalLogging = (
  patch: Partial<OperationalLogSettings>,
) =>
  adminRequest<OperationalLoggingChange>(
    "/api/admin/operational-logging",
    json("PATCH", patch),
  );

export const getLogsStatus = () =>
  adminRequest<LogsStatus>("/api/admin/logs/status");

/** Flushes every kind; the endpoint takes no body. */
export const flushLogs = () =>
  adminRequest<FlushResult>("/api/admin/logs/flush", { method: "POST" });

export const clearLogCache = (kind: OpsKind | "all") =>
  adminRequest<{ cleared: string }>(
    "/api/admin/logs/clear-cache",
    json("POST", { kind }),
  );

export const deleteLogHistory = (kind: OpsKind, olderThanDays?: number) =>
  adminRequest<{ job: HistoryJob }>(
    "/api/admin/logs/delete-history",
    json(
      "POST",
      olderThanDays === undefined
        ? { kind }
        : { kind, older_than_days: olderThanDays },
    ),
  );

export const sweepRetention = () =>
  adminRequest<SweepResult>("/api/admin/logs/sweep", { method: "POST" });

export const startBackfill = (options?: {
  from_ts?: number | null;
  to_ts?: number | null;
  restart?: boolean;
}) =>
  adminRequest<{ backfill: BackfillJob }>(
    "/api/admin/logs/backfill",
    json("POST", options ?? {}),
  );

export const getLoggedTraffic = (signal?: AbortSignal) =>
  adminRequest<LoggedTraffic>("/api/admin/usage/logged-traffic", { signal });

export const reconcileStorage = () =>
  adminRequest<{ started: boolean }>("/api/admin/usage/reconcile-storage", {
    method: "POST",
  });

type QuarantinePage =
  paths["/api/admin/quarantine"]["get"]["responses"]["200"]["content"]["application/json"];
export type QuarantineItem = QuarantinePage["items"][number];
export type QuarantinePurgeStep =
  paths["/api/admin/quarantine/purge"]["post"]["responses"]["200"]["content"]["application/json"];
export type QuarantineTarget =
  | { target: "all" }
  | { target: string; record: string };

export const getQuarantinePage = (cursor?: string | null) =>
  adminRequest<QuarantinePage>(
    `/api/admin/quarantine${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`,
  );

/** Every quarantine page, oldest record key first. */
export async function getQuarantine(): Promise<QuarantineItem[]> {
  const items: QuarantineItem[] = [];
  let cursor: string | null = null;
  do {
    const page: QuarantinePage = await getQuarantinePage(cursor);
    items.push(...page.items);
    cursor = page.complete ? null : (page.next_cursor ?? null);
  } while (cursor);
  return items;
}

const MAX_PURGE_STEPS = 500;

const purgeStep = (body: Record<string, unknown>) =>
  adminRequest<QuarantinePurgeStep>(
    "/api/admin/quarantine/purge",
    json("POST", body),
  );

/**
 * Asks for a confirm token, then runs bounded purge steps until the server reports completion.
 * Each step stays inside one request's backend budget and is audited server-side.
 */
export async function purgeQuarantine(
  target: QuarantineTarget,
  onStep?: (step: QuarantinePurgeStep) => void,
): Promise<{ purged: number; failed: number; skipped: number }> {
  const totals = { purged: 0, failed: 0, skipped: 0 };
  const issued = await purgeStep({ ...target });
  let confirm = issued.confirm_token ?? null;
  let cursor: string | null = null;
  for (let steps = 0; confirm && steps < MAX_PURGE_STEPS; steps++) {
    const step = await purgeStep({
      ...target,
      confirm,
      ...(cursor ? { cursor } : {}),
    });
    totals.purged += step.purged ?? 0;
    totals.failed += step.failed ?? 0;
    totals.skipped += step.skipped ?? 0;
    onStep?.(step);
    if (step.complete) break;
    confirm = step.confirm_token ?? null;
    cursor = step.next_cursor ?? null;
  }
  return totals;
}
