'use client'

/**
 * Row-detail dialog for the Live Query Monitor.
 *
 * The custom inline expand-drawer the page used to ship was replaced when
 * the tables moved onto the project-standard ``<DataTable>`` — DataTable
 * doesn't render expanded rows out of the box. The Dialog is the
 * project's standard "show me a full detail view" primitive (no Sheet
 * component exists in ``components/ui/``), and it keeps the row table
 * clean while still surfacing every attribution field + the full SQL.
 *
 * The dialog re-polls the per-row endpoint every 2 s so the live duration
 * keeps ticking while the operator reads the SQL.
 */

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Check, Copy, X } from 'lucide-react'
import { adminFetch } from '@/lib/api'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

import { durationColor, formatDuration, formatMemoryMb, kindBadgeVariant } from '../_helpers'
import type { ActiveOrPromotedRow, ActiveRow, CompletedRow } from '../_types'

type AnyRow = ActiveOrPromotedRow | CompletedRow

function isCompleted(row: AnyRow): row is CompletedRow {
  return 'outcome' in row
}

function isActivePromoted(row: AnyRow): row is ActiveOrPromotedRow {
  return 'cancellable' in row
}

export function RowDetailDialog({
  row,
  onClose,
  onKill,
  cancellingQid,
}: {
  row: AnyRow | null
  onClose: () => void
  onKill?: (row: ActiveRow) => void
  cancellingQid?: number | null
}) {
  // Re-fetch the full SQL for live rows; the snapshot endpoint only ships
  // the 200-char preview. For completed/promoted rows the per-row endpoint
  // 404s (registry only knows active queries) so we fall back to whatever
  // sql_preview we have.
  const isLive = row !== null && isActivePromoted(row) && !row._completed
  const { data: fullRow } = useQuery<AnyRow>({
    queryKey: ['admin', 'query-monitor', 'detail', row?.query_id ?? null],
    queryFn: async ({ signal }) => {
      const r = await adminFetch(`/api/admin/queries/${row!.query_id}`, { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: row !== null && isLive,
    refetchInterval: 2000,
    refetchIntervalInBackground: false,
  })

  if (!row) return null
  const display = (fullRow ?? row) as AnyRow
  const sql = display.sql ?? row.sql_preview
  const attr = display.attribution
  const completed = isCompleted(display) ? display : null
  const ap = isActivePromoted(display) ? display : null
  const cancelling = ap !== null && cancellingQid !== undefined && cancellingQid === ap.query_id
  const canKill = ap !== null && ap.cancellable && ap.cancelled_at === null && onKill !== undefined

  return (
    <Dialog open={row !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize">
              {attr.kind}
            </Badge>
            <span className="truncate">{attr.label}</span>
            {completed && (
              <Badge
                variant={
                  completed.outcome === 'ok'
                    ? 'outline'
                    : completed.outcome === 'cancelled'
                      ? 'secondary'
                      : 'destructive'
                }
                className="capitalize ml-2"
              >
                {completed.outcome}
                {completed.error_type && `: ${completed.error_type}`}
              </Badge>
            )}
          </DialogTitle>
          <DialogDescription className="sr-only">
            Query {display.query_id} details
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-2 gap-3 text-xs">
          <div>
            <span className="text-muted-foreground">Caller:</span>{' '}
            <span className="font-mono">
              {attr.caller_qualname} <span className="opacity-60">({attr.caller_file})</span>
            </span>
          </div>
          <div>
            <span className="text-muted-foreground">DB:</span> {display.db_type}
          </div>
          <div>
            <span className="text-muted-foreground">Service:</span>{' '}
            <span className="font-mono">{display.service_id ?? '—'}</span>
          </div>
          <div>
            <span className="text-muted-foreground">Pool slot:</span>{' '}
            <span className="font-mono">{attr.pool_slot ?? '—'}</span>
          </div>
          <div>
            <span className="text-muted-foreground">Request:</span>{' '}
            {attr.request_path || '—'}
            {attr.request_id && (
              <span className="opacity-60"> · {attr.request_id.slice(0, 8)}</span>
            )}
          </div>
          <div>
            <span className="text-muted-foreground">Duration:</span>{' '}
            <span className={`font-mono ${durationColor(display.duration_ms)}`}>
              {formatDuration(display.duration_ms)}
            </span>
          </div>
          {attr.cron_job && (
            <div>
              <span className="text-muted-foreground">Cron:</span> {attr.cron_job}
              {attr.cron_run_id && ` (run ${attr.cron_run_id})`}
            </div>
          )}
          {completed?.peak_memory_mb != null && (
            <div>
              <span className="text-muted-foreground">Peak memory:</span>{' '}
              <span className="font-mono">{formatMemoryMb(completed.peak_memory_mb)}</span>
            </div>
          )}
          {completed?.error_message && (
            <div className="col-span-2">
              <span className="text-muted-foreground">Error:</span>{' '}
              <span className="font-mono text-red-600">{completed.error_message}</span>
            </div>
          )}
        </div>

        <div className="relative">
          <CopySqlButton sql={sql} />
          <pre className="bg-muted/50 border rounded p-3 pr-12 text-xs overflow-auto whitespace-pre-wrap font-mono max-h-96">
            {sql}
          </pre>
        </div>
        {row.sql_len > 200 && !fullRow && isLive && (
          <div className="text-xs text-muted-foreground">Loading full SQL ({row.sql_len} chars)…</div>
        )}

        <DialogFooter>
          {canKill && (
            <Button
              variant="destructive"
              size="sm"
              disabled={cancelling}
              onClick={() => onKill!(ap!)}
            >
              {cancelling ? 'Cancelling…' : (<><X className="h-3 w-3 mr-1" /> Kill</>)}
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** Tiny floating Copy button anchored to the top-right of the SQL <pre>.
 *  Flashes a checkmark on success and reverts after 1.5 s — enough to
 *  confirm the click without lingering UI noise. Falls back silently if
 *  the Clipboard API is unavailable (e.g. insecure context); copying SQL
 *  is convenience, not safety-critical. */
function CopySqlButton({ sql }: { sql: string }) {
  const [copied, setCopied] = React.useState(false)
  React.useEffect(() => {
    if (!copied) return
    const t = setTimeout(() => setCopied(false), 1500)
    return () => clearTimeout(t)
  }, [copied])
  const onClick = async () => {
    if (typeof navigator === 'undefined' || !navigator.clipboard) return
    try {
      await navigator.clipboard.writeText(sql)
      setCopied(true)
    } catch {
      // ignore — common in non-secure contexts and on permission denial
    }
  }
  return (
    <Button
      variant="outline"
      size="sm"
      className="absolute top-1.5 right-1.5 h-7 px-2 text-xs"
      onClick={onClick}
      title="Copy SQL to clipboard"
    >
      {copied ? (
        <>
          <Check className="h-3 w-3 mr-1" /> Copied
        </>
      ) : (
        <>
          <Copy className="h-3 w-3 mr-1" /> Copy
        </>
      )}
    </Button>
  )
}
