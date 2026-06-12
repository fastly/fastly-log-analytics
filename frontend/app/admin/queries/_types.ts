/**
 * Shared types for the Live Query Monitor admin page.
 *
 * Kept as a flat `.ts` (not generated from the OpenAPI types) so the
 * sub-section components can import a single canonical shape — the
 * generated `paths["/api/admin/queries"]["get"]["responses"][200]…` chain
 * is unergonomic when you need to reuse the row shape in 4 different
 * components.
 */

export type AttributionKind = 'analyst' | 'admin' | 'cron' | 'system'

export interface Attribution {
  kind: AttributionKind
  label: string
  principal_id: string | null
  caller_qualname: string
  caller_file: string
  request_path: string | null
  request_id: string | null
  cron_job: string | null
  cron_run_id: string | null
  pool_slot: string | null
}

export interface ActiveRow {
  query_id: number
  db_type: 'DuckDB' | 'SQLite'
  sql_preview: string
  sql: string | null
  sql_len: number
  attribution: Attribution
  service_id: string | null
  started_at_utc: number
  duration_ms: number
  cancellable: boolean
  cancelled_at: number | null
}

export interface CompletedRow extends Omit<ActiveRow, 'cancellable' | 'cancelled_at'> {
  ended_at_utc: number
  outcome: 'ok' | 'error' | 'cancelled'
  error_type: string | null
  error_message: string | null
}

export interface SnapshotResponse {
  last_seq: number
  active: ActiveRow[]
  completed: CompletedRow[]
}

export interface SummaryResponse {
  active_total: number
  by_db_type: Record<string, number>
  longest_ms: number
}

export interface CancelResponse {
  state: 'cancelled' | 'not_found' | 'already_finished' | 'connection_gone'
  query_id: number
}

export interface MonitorConfig {
  enabled: boolean
}

export type ViewMode = 'all' | 'live' | 'past'

/** Active row plus an optional `_completed` field for rows promoted from
 *  the just-finished window. The table component renders these as faded
 *  rows with an outcome badge instead of a Kill button. */
export type ActiveOrPromotedRow = ActiveRow & { _completed?: CompletedRow }
