/**
 * Mitigation for the intermittent "no data" symptom on the dashboard,
 * charts, and related pages that read `/api/dashboard/aggregates`.
 *
 * Root cause (RC-16 in docs/performance_remediation_plan_final.md): the
 * backend's `metadata_sync` cron rebuilds the per-service Iceberg view
 * every couple of minutes. When a query lands on a pooled DuckDB
 * connection mid-rebind, the per-window aggregations come back empty
 * even though the underlying log table has data. The proper fix is to
 * isolate the cron's writes from the request-path pool (a larger backend
 * change tracked separately).
 *
 * Until that lands, retry once on the client when we detect the symptom:
 * the response says recent log data exists (latest_log_at is set) but
 * every aggregation came back empty. Most retries land after the sync
 * window closes (~hundreds of ms).
 */

export class StaleDashboardViewError extends Error {
  constructor() {
    super('dashboard aggregates response is internally inconsistent (likely stale-view symptom during a metadata_sync tick)')
    this.name = 'StaleDashboardViewError'
  }
}

/**
 * Inspect a `/api/dashboard/aggregates` response and decide whether it
 * looks like the stale-view symptom rather than a legitimate empty window.
 *
 * The discriminator: if `latest_log_at` is set (metadata sees recent data
 * in the table) but every per-field aggregation AND the time series are
 * empty, the view-side query disagrees with the table-level metadata —
 * almost certainly the sync-overlap symptom.
 *
 * If `latest_log_at` is missing, treat empty as legitimate (the service
 * has no data at all).
 */
export function isStaleDashboardAggregates(d: unknown): boolean {
  if (!d || typeof d !== 'object') return false
  const r = d as Record<string, unknown>
  if (!r.latest_log_at) return false
  const timeSeries = (r.time_series ?? r.timeseries) as unknown[] | undefined
  if (Array.isArray(timeSeries) && timeSeries.length > 0) return false
  const fields = r.data as Record<string, { total?: number; top?: unknown[] }> | undefined
  if (!fields) return true
  return Object.values(fields).every(
    (f) => !f || ((f.total ?? 0) === 0 && (!f.top || f.top.length === 0)),
  )
}

/**
 * Wrap an aggregates fetch so it throws on the stale-view symptom. Use
 * together with React Query's `retry` / `retryDelay` options — see
 * `STALE_VIEW_RETRY_OPTIONS` below.
 */
export function throwIfStaleAggregates<T>(data: T): T {
  if (isStaleDashboardAggregates(data)) throw new StaleDashboardViewError()
  return data
}

/**
 * React Query options to pair with `throwIfStaleAggregates`. Retries up
 * to twice, only on the stale-view error (other errors fail fast as
 * before). Backoff is short — the sync window typically clears in well
 * under a second.
 */
export const STALE_VIEW_RETRY_OPTIONS = {
  retry: (failureCount: number, error: Error) =>
    error instanceof StaleDashboardViewError && failureCount < 2,
  retryDelay: (attempt: number) => 400 * (attempt + 1),
} as const
