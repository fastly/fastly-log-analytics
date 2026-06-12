'use client'

import { Badge } from '@/components/ui/badge'

import { durationColor, formatDuration, formatMemoryMb, kindBadgeVariant } from '../_helpers'
import type { CompletedRow } from '../_types'

/** Shared table for completed-query rows. Used by:
 *  - Recently Completed (default order: newest first)
 *  - Notable Slow Queries (pass `preserveOrder` so the caller's
 *    duration-desc sort is preserved)
 *
 *  Capped at 50 rows on render — completed history server-side is bounded
 *  to 200 (deque maxlen); 50 fills several screens without slowing the
 *  300ms refresh cycle's diff.
 *
 *  The Memory column only renders when at least one visible row has a
 *  `peak_memory_mb` value. SQLite rows and DuckDB rows where the probe
 *  failed are always null, so an all-null view collapses the column out
 *  entirely. */
export function CompletedTable({
  rows,
  emptyMessage = 'No completed queries yet.',
  preserveOrder = false,
}: {
  rows: CompletedRow[]
  emptyMessage?: string
  preserveOrder?: boolean
}) {
  if (rows.length === 0) {
    return <div className="p-6 text-center text-sm text-muted-foreground">{emptyMessage}</div>
  }
  const sorted = preserveOrder
    ? rows.slice(0, 50)
    : [...rows].sort((a, b) => b.query_id - a.query_id).slice(0, 50)
  const showMemory = sorted.some((r) => r.peak_memory_mb !== null && r.peak_memory_mb !== undefined)

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b text-xs text-muted-foreground">
          <tr className="text-left">
            <th className="px-3 py-2">Outcome</th>
            <th className="px-3 py-2">Source</th>
            <th className="px-3 py-2">Caller</th>
            <th className="px-3 py-2">DB</th>
            <th className="px-3 py-2 text-right">Duration</th>
            {showMemory && <th className="px-3 py-2 text-right">Memory</th>}
            <th className="px-3 py-2">SQL</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr key={row.query_id} className="border-b hover:bg-muted/30">
              <td className="px-3 py-2">
                <Badge
                  variant={
                    row.outcome === 'ok'
                      ? 'outline'
                      : row.outcome === 'cancelled'
                        ? 'secondary'
                        : 'destructive'
                  }
                  className="capitalize"
                >
                  {row.outcome}
                </Badge>
                {row.error_type && <span className="text-xs text-red-600 ml-2">{row.error_type}</span>}
              </td>
              <td className="px-3 py-2">
                <div className="flex items-center gap-2">
                  <Badge variant={kindBadgeVariant(row.attribution.kind)} className="capitalize">
                    {row.attribution.kind}
                  </Badge>
                  <span className="truncate max-w-xs text-xs" title={row.attribution.label}>
                    {row.attribution.label}
                  </span>
                </div>
              </td>
              <td
                className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs"
                title={`${row.attribution.caller_qualname}\n${row.attribution.caller_file}`}
              >
                <div className="truncate">{row.attribution.caller_qualname}</div>
                <div className="truncate text-[10px] opacity-60">{row.attribution.caller_file}</div>
              </td>
              <td className="px-3 py-2 text-xs">{row.db_type}</td>
              <td className={`px-3 py-2 text-right font-mono ${durationColor(row.duration_ms)}`}>
                {formatDuration(row.duration_ms)}
              </td>
              {showMemory && (
                <td className="px-3 py-2 text-right font-mono text-xs text-muted-foreground tabular-nums">
                  {formatMemoryMb(row.peak_memory_mb) || '—'}
                </td>
              )}
              <td
                className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-md"
                title={row.sql_preview}
              >
                {row.sql_preview}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
