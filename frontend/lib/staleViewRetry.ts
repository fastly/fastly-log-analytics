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
 * Until that lands, retry on the client when we detect the symptom: the
 * response says recent log data exists (latest_log_at is set) but every
 * aggregation came back empty. Most retries land after the sync window
 * closes (~hundreds of ms).
 *
 * IMPORTANT (window-awareness): `latest_log_at` / `earliest_log_at` are
 * ALL-TIME source extents (from the svcconfig status cache via
 * get_source_extent), NOT scoped to the queried [start_time, end_time].
 * The per-window aggregations ARE window-scoped. Comparing the two
 * without considering the window produces false positives whenever the
 * selected window legitimately contains no data while the service has
 * data elsewhere — most visibly on a fresh install, where the dashboard
 * fires with the default last-24h window before the extents snap and the
 * ingested logs may sit entirely outside it (historical/backfill batch,
 * ingest lag, etc.). A false positive there retries the same empty
 * window twice and then hard-fails the whole page with a confusing
 * "internally inconsistent" error. Pass the queried window so the
 * discriminator only fires when the data span actually overlaps it.
 */

class StaleDashboardViewError extends Error {
  constructor() {
    super('dashboard aggregates response is internally inconsistent (likely stale-view symptom during a metadata_sync tick)')
    this.name = 'StaleDashboardViewError'
  }
}

/** Narrow an unknown error to the stale-view symptom. */
export function isStaleDashboardViewError(err: unknown): err is StaleDashboardViewError {
  return (
    err instanceof StaleDashboardViewError ||
    (err instanceof Error && err.name === 'StaleDashboardViewError')
  )
}

/** Queried time window for an aggregates request (ISO strings). */
export interface AggregatesWindow {
  startTime?: string | null
  endTime?: string | null
}

/**
 * Parse a status-cache extent string to epoch millis. Handles full ISO
 * timestamps and the date-only ("YYYY-MM-DD") shorthand the status cache
 * can emit: for date-only values the `earliest` edge is treated as
 * start-of-day and the `latest` edge as end-of-day (UTC), mirroring the
 * snap parsing in FilterBar. Returns null for missing/invalid input.
 */
function parseExtentMs(value: unknown, edge: 'earliest' | 'latest'): number | null {
  if (typeof value !== 'string' || value.length === 0) return null
  const iso =
    value.length === 10
      ? `${value}${edge === 'earliest' ? 'T00:00:00.000Z' : 'T23:59:59.999Z'}`
      : value
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : t
}

/**
 * Inspect a `/api/dashboard/aggregates` response and decide whether it
 * looks like the stale-view symptom rather than a legitimate empty window.
 *
 * The discriminator: if `latest_log_at` is set (metadata sees data in the
 * table) but every per-field aggregation AND the time series are empty,
 * the view-side query disagrees with the table-level metadata.
 *
 * Two guards keep this from misfiring on legitimately-empty results:
 *   1. `latest_log_at` missing → the service has no data at all → empty
 *      is legitimate.
 *   2. When a `window` is supplied, the data's all-time span
 *      [earliest_log_at, latest_log_at] must actually OVERLAP the queried
 *      [startTime, endTime]. If the window sits entirely outside the data
 *      (default 24h window on a fresh install whose logs predate it, a
 *      historical backfill, or any range with no data), an empty result
 *      is legitimate, so don't flag it.
 */
function isStaleDashboardAggregates(
  d: unknown,
  window?: AggregatesWindow,
  hasFilters = false,
): boolean {
  if (!d || typeof d !== 'object') return false
  // An empty result with an ACTIVE FILTER is a legitimate "no rows match",
  // not the stale-view symptom — the metadata-vs-aggregates contradiction this
  // heuristic detects only holds for an UNfiltered window. Without this guard,
  // any filter that matches zero rows (a masking analyst's masked-IP click, or
  // an admin filtering on a zero-traffic value) trips the stale detector into
  // an infinite 5s poll of the expensive live-SQL path. (RC: the original
  // masked-IP slowness + stuck "Preparing your data" banner.)
  if (hasFilters) return false
  const r = d as Record<string, unknown>
  if (!r.latest_log_at) return false

  // Window-aware overlap guard (see header note). Only applied when both
  // window bounds and the data's latest extent parse cleanly; otherwise
  // fall through to the legacy emptiness check so behaviour is unchanged
  // for callers that don't pass a window.
  if (window?.startTime && window?.endTime) {
    const winStart = Date.parse(window.startTime)
    const winEnd = Date.parse(window.endTime)
    const dataLatest = parseExtentMs(r.latest_log_at, 'latest')
    if (Number.isFinite(winStart) && Number.isFinite(winEnd) && dataLatest !== null) {
      const dataEarliest = parseExtentMs(r.earliest_log_at, 'earliest') ?? dataLatest
      const overlaps = dataEarliest <= winEnd && dataLatest >= winStart
      if (!overlaps) return false
    }
  }

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
 * together with React Query's `retry` / `retryDelay` / `refetchInterval`
 * options — see `STALE_VIEW_RETRY_OPTIONS` below. Pass the queried
 * `window` so out-of-window empty results aren't misclassified as stale,
 * and `hasFilters` (true when the request carries any filter) so an empty
 * *filtered* result is treated as a legitimate "no rows match" rather than
 * the stale-view symptom.
 */
export function throwIfStaleAggregates<T>(
  data: T,
  window?: AggregatesWindow,
  hasFilters = false,
): T {
  if (isStaleDashboardAggregates(data, window, hasFilters)) throw new StaleDashboardViewError()
  return data
}

/**
 * Slow background poll interval (ms). When a stale-view symptom outlives
 * the fast retry budget below — most notably on a fresh install, whose
 * first Iceberg view / rollup build can take minutes while the status
 * cache already reports `latest_log_at` — `STALE_VIEW_RETRY_OPTIONS`
 * keeps re-fetching at this cadence until the view catches up, instead of
 * sitting on a terminal error and hard-failing the page.
 */
export const STALE_VIEW_POLL_MS = 5000

/**
 * React Query options to pair with `throwIfStaleAggregates`:
 *   - `retry` / `retryDelay`: retry up to twice, only on the stale-view
 *     error (other errors fail fast). Backoff is short — a mid-sync tick
 *     typically clears in well under a second.
 *   - `refetchInterval`: once the retries are exhausted and the query is
 *     parked on a stale-view error, keep polling slowly until the view is
 *     consistent. Returns false (no poll) for non-stale errors and for
 *     success, so this only affects the benign stale-view case.
 */
export const STALE_VIEW_RETRY_OPTIONS = {
  retry: (failureCount: number, error: Error) =>
    isStaleDashboardViewError(error) && failureCount < 2,
  retryDelay: (attempt: number) => 400 * (attempt + 1),
  refetchInterval: (query: { state: { error: unknown } }) =>
    isStaleDashboardViewError(query.state.error) ? STALE_VIEW_POLL_MS : false,
} as const
