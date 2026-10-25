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
import { Keyboard, RefreshCw, X } from 'lucide-react'
import { adminJson } from '@/lib/api'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { HelpDialog } from '@/components/ui/confirm-dialog'

import {
  durationColor,
  formatDurationMs,
  formatMemoryMb,
  kindBadgeVariant,
  type ActiveOrPromotedRow,
  type ActiveRow,
  type CompletedRow,
  type SnapshotResponse,
  type SummaryResponse,
} from '../_hooks/useFilteredActive'
import { queryKeys } from '@/lib/query-keys'

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
    queryKey: queryKeys.admin.queryMonitorDetail(row?.query_id ?? null),
    queryFn: async ({ signal }) => {
      return adminJson<'/api/admin/queries/{qid}', ActiveRow>(
        '/api/admin/queries/{qid}',
        `/api/admin/queries/${row!.query_id}`,
        { signal },
      )
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
              {formatDurationMs(display.duration_ms)}
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
          <CopyButton
            text={sql}
            label="Copy"
            copiedLabel="Copied"
            variant="outline"
            size="sm"
            className="absolute top-1.5 right-1.5 h-7 px-2 text-xs"
            title="Copy SQL to clipboard"
            timeoutMs={1500}
          />
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

export function FilterChipRow<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T
  onChange: (v: T) => void
  options: { value: T; label: string }[]
}) {
  return (
    <div className="flex items-center gap-1">
      {options.map((o) => (
        <Button
          key={o.value}
          variant={value === o.value ? 'default' : 'outline'}
          size="sm"
          className="h-7 px-2 text-xs capitalize"
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </Button>
      ))}
    </div>
  )
}

export function PollingIndicator({
  visible,
  isFetching,
  isError,
  paused = false,
}: {
  visible: boolean
  isFetching: boolean
  isError: boolean
  paused?: boolean
}) {
  if (isError) return <span className="text-xs text-red-500 ml-2">Error — retrying</span>
  if (paused) return <span className="text-xs text-amber-600 ml-2 font-medium">Paused</span>
  if (!visible) return <span className="text-xs text-muted-foreground ml-2">Paused (tab hidden)</span>
  return (
    <span className="flex items-center gap-1 text-xs text-muted-foreground ml-2">
      <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : 'opacity-50'}`} />
      Live
    </span>
  )
}

export function ShortcutsHelp({
  open,
  onOpenChange,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <HelpDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Keyboard shortcuts"
      icon={<Keyboard className="h-4 w-4" />}
    >
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm text-foreground">
        <Kbd>/</Kbd>
        <dd>Focus the search field</dd>

        <Kbd>.</Kbd>
        <dd>Pause / resume the snapshot poll (stops rows from shifting under you)</dd>

        <Kbd>Esc</Kbd>
        <dd>Close the row detail dialog, kill confirm, or this help overlay</dd>

        <Kbd>?</Kbd>
        <dd>Show this help overlay</dd>
      </dl>
      <p className="mt-4 text-xs text-muted-foreground">
        Shortcuts are disabled while typing in the search box (except <kbd>Esc</kbd>, which always
        works).
      </p>
    </HelpDialog>
  )
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <dt className="self-center">
      <kbd className="inline-flex items-center justify-center min-w-[1.5rem] h-6 px-1.5 rounded border border-border bg-muted/50 text-xs font-mono">
        {children}
      </kbd>
    </dt>
  )
}

export function SummaryStrip({ snapshot }: { snapshot: SnapshotResponse | undefined }) {
  const data = React.useMemo<SummaryResponse | null>(() => {
    if (!snapshot) return null
    const by_db_type: Record<string, number> = {}
    let longest_ms = 0
    for (const row of snapshot.active) {
      by_db_type[row.db_type] = (by_db_type[row.db_type] ?? 0) + 1
      if (row.duration_ms > longest_ms) longest_ms = row.duration_ms
    }
    return {
      active_total: snapshot.active.length,
      by_db_type,
      longest_ms,
    }
  }, [snapshot])
  const liveLabel = React.useMemo(() => {
    if (!data) return ''
    return `${data.active_total} active ${data.active_total === 1 ? 'query' : 'queries'}`
  }, [data?.active_total])
  if (!data) return null
  return (
    <div className="flex items-center gap-3 text-sm">
      <Badge variant={data.active_total > 0 ? 'default' : 'outline'} className="gap-1">
        <span className="font-medium">{data.active_total}</span> active
      </Badge>
      {Object.entries(data.by_db_type).map(([db, n]) => (
        <Badge key={db} variant="outline" className="gap-1">
          {db} <span className="font-medium">{n}</span>
        </Badge>
      ))}
      {data.longest_ms > 0 && (
        <span className={`text-xs ${durationColor(data.longest_ms)}`}>
          longest: {formatDurationMs(data.longest_ms)}
        </span>
      )}
      <div role="status" aria-live="polite" className="sr-only">
        {liveLabel}
      </div>
    </div>
  )
}
