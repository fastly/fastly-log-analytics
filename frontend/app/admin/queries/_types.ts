/**
 * Shared types for the Live Query Monitor admin page.
 *
 * The row interfaces below are deliberate narrowings of the generated
 * wire schemas (which are all-optional/nullable — the exclude_unset
 * artifact): the query-registry producer always emits the full shape,
 * and the literal unions (db_type, outcome, kind) drive UI switches.
 * Each narrowing is pinned to its generated schema by the
 * `Expect<WireParity<...>>` guards at the bottom of this file, so a
 * backend field rename or type change fails typecheck here.
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
  /** Memory still held by the DuckDB connection at deregister time, in MB.
   *  `null` for SQLite rows and for any DuckDB row where the probe failed
   *  (closed connection, version mismatch). Frontend skips the column when
   *  every visible row is `null`. */
  peak_memory_mb: number | null
}

export interface SnapshotResponse {
  last_seq: number
  active: ActiveRow[]
  completed: CompletedRow[]
}

import type { components } from '@/types/api.generated'
import type { Expect, WireParity } from '@/types/api'

export type SummaryResponse = components['schemas']['SummaryResponse']
export type CancelResponse = components['schemas']['CancelResponse']

// Wire-parity guards for the hand-narrowed rows above (see types/api.ts).
export type _AttributionParity = Expect<WireParity<Attribution, components['schemas']['QueryAttribution']>>
export type _ActiveRowParity = Expect<WireParity<ActiveRow, components['schemas']['ActiveQueryRow']>>
export type _CompletedRowParity = Expect<WireParity<CompletedRow, components['schemas']['CompletedQueryRow']>>
export type _SnapshotParity = Expect<WireParity<SnapshotResponse, components['schemas']['SnapshotResponse']>>

export interface MonitorConfig {
  enabled: boolean
}

export type ViewMode = 'all' | 'live' | 'past'

/** DB-engine filter. ``'all'`` shows both; the other two narrow to a single
 *  engine and apply page-wide (Active + Slow + Recently Completed). */
export type DbFilter = 'all' | 'DuckDB' | 'SQLite'

/** Active row plus an optional `_completed` field for rows promoted from
 *  the just-finished window. The table component renders these as faded
 *  rows with an outcome badge instead of a Kill button.
 *
 *  Cron-grouping markers:
 *  - ``_groupedCount`` — set on the representative row of a collapsed group
 *    OR on the head of an expanded one. Drives the ``×N`` badge.
 *  - ``_isGroupHead`` — true on the leading row of an expanded group; tells
 *    the badge to render in "expanded" state (chevron flipped).
 *  - ``_expandedChild`` — true on sibling rows revealed by expanding a
 *    group. Renders with a left indent + muted background so the visual
 *    grouping is obvious. */
export type ActiveOrPromotedRow = ActiveRow & {
  _completed?: CompletedRow
  _groupedCount?: number
  _isGroupHead?: boolean
  _expandedChild?: boolean
}

/** CompletedRow extended with the same cron-grouping markers. */
export type GroupedCompletedRow = CompletedRow & {
  _groupedCount?: number
  _isGroupHead?: boolean
  _expandedChild?: boolean
}
