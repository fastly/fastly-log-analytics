'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef, Row } from '@tanstack/react-table'
import {
  ChevronDown,
  ChevronRight,
  Keyboard,
  RefreshCw,
  X,
} from 'lucide-react'
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton, HelpDialog } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { adminJson, queryKeys } from '@/lib/api'
import {
  type ActiveOrPromotedRow,
  type ActiveRow,
  type AnyRow,
  type AttributedRow,
  type CompletedRow,
  type GroupedCompletedRow,
  type SnapshotResponse,
  type SummaryResponse,
  durationColor,
  formatDurationMs,
  formatMemoryMb,
  kindBadgeVariant,
} from './useQueryMonitorState'

const STUCK_LIVE_MS = 30_000

const outcomeVariant = (o: string): 'outline' | 'secondary' | 'destructive' =>
  o === 'ok' ? 'outline' : o === 'cancelled' ? 'secondary' : 'destructive'

function buildSourceCell(onToggleGroup: (runId: string) => void) {
  return function SourceCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
    const attr = row.original.attribution
    const groupCount = row.original._groupedCount
    const isGroupHead = !!row.original._isGroupHead
    return (
      <div
        className={`flex items-center gap-2 min-w-0 ${
          row.original._expandedChild ? 'pl-5' : ''
        }`}
      >
        <Badge variant={kindBadgeVariant(attr.kind)} className="capitalize shrink-0">
          {attr.kind}
        </Badge>
        <span className="truncate text-xs" title={attr.label}>
          {attr.label}
        </span>
        {groupCount && groupCount > 1 ? (
          <button
            type="button"
            className="shrink-0 inline-flex items-center gap-0.5 rounded-md border border-input bg-secondary px-1.5 py-0.5 font-mono text-[10px] hover:bg-secondary/80"
            title={
              isGroupHead
                ? `Collapse this cron run (${groupCount} queries)`
                : `Expand this cron run (${groupCount} queries)`
            }
            onClick={(e) => {
              e.stopPropagation()
              if (attr.cron_run_id) onToggleGroup(attr.cron_run_id)
            }}
          >
            {isGroupHead ? (
              <ChevronDown className="h-2.5 w-2.5" />
            ) : (
              <ChevronRight className="h-2.5 w-2.5" />
            )}
            ×{groupCount}
          </button>
        ) : null}
      </div>
    )
  }
}

function callerCell<T extends AttributedRow>({ row }: { row: Row<T> }) {
  const attr = row.original.attribution
  return (
    <div
      className="font-mono text-xs text-muted-foreground"
      title={`${attr.caller_qualname}\n${attr.caller_file}`}
    >
      <div className="truncate">{attr.caller_qualname}</div>
      <div className="truncate text-[10px]">{attr.caller_file}</div>
    </div>
  )
}

function buildServiceCol<T extends AttributedRow>(): ColumnDef<T> {
  return {
    id: 'service_id',
    accessorFn: (r) => r.service_id ?? '',
    header: 'Service',
    size: 200,
    cell: ({ row }) => (
      <span className="text-xs font-mono truncate" title={row.original.service_id ?? ''}>
        {row.original.service_id ?? '—'}
      </span>
    ),
  }
}

function buildSharedAttributionColumns<T extends AttributedRow>(
  onToggleGroup: (runId: string) => void,
): ColumnDef<T>[] {
  return [
    {
      id: 'source',
      accessorFn: (r) => r.attribution.label,
      header: 'Source',
      size: 280,
      cell: buildSourceCell(onToggleGroup),
    },
    {
      id: 'caller',
      accessorFn: (r) => r.attribution.caller_qualname,
      header: 'Caller',
      size: 280,
      cell: callerCell,
    },
    {
      id: 'db_type',
      accessorKey: 'db_type',
      header: 'DB',
      size: 80,
      cell: ({ row }) => <span className="text-xs">{row.original.db_type}</span>,
    },
  ]
}

function activeDurationCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
  const r = row.original
  const isCancelled = r.cancelled_at !== null
  const promoted = !!r._completed
  const stuck = !promoted && !isCancelled && r.duration_ms >= STUCK_LIVE_MS
  return (
    <span
      className={`inline-flex items-center gap-1.5 font-mono ${durationColor(r.duration_ms)} ${stuck ? 'animate-pulse font-bold' : ''}`}
      title={
        stuck
          ? `Live query running for ${formatDurationMs(r.duration_ms)} — investigate`
          : undefined
      }
    >
      {!promoted && !isCancelled && (
        <span className="relative flex h-2 w-2" aria-hidden="true">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
        </span>
      )}
      {formatDurationMs(r.duration_ms)}
    </span>
  )
}

function buildActionsCell(
  onKill: (row: ActiveRow) => void,
  cancellingQid: number | null,
) {
  return function ActionsCell({ row }: { row: Row<ActiveOrPromotedRow> }) {
    const r = row.original
    if (r._completed) {
      const outcome = r._completed.outcome
      return (
        <Badge variant={outcomeVariant(outcome)} className="capitalize">
          {outcome === 'ok' ? '✓ done' : outcome}
        </Badge>
      )
    }
    if (r.cancelled_at !== null) {
      return <span className="text-xs text-muted-foreground">cancelling…</span>
    }
    if (!r.cancellable) {
      return <span className="text-xs text-muted-foreground">—</span>
    }
    const cancelling = cancellingQid === r.query_id
    return (
      <Button
        variant="destructive"
        size="sm"
        className="h-7 px-2"
        disabled={cancelling}
        title="Interrupt the currently-running statement. A caller that retries will start a new query you may need to cancel again."
        onClick={(e) => {
          e.stopPropagation()
          onKill(r)
        }}
      >
        {cancelling ? (
          'Cancelling…'
        ) : (
          <>
            <X className="h-3 w-3 mr-1" /> Kill
          </>
        )}
      </Button>
    )
  }
}

export function buildActiveColumns(deps: {
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  showService: boolean
  showPool: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<ActiveOrPromotedRow>[] {
  const cols = buildSharedAttributionColumns<ActiveOrPromotedRow>(deps.onToggleGroup)
  if (deps.showService) cols.push(buildServiceCol())
  if (deps.showPool) {
    cols.push({
      id: 'pool_slot',
      accessorFn: (r) => r.attribution.pool_slot ?? '',
      header: 'Pool',
      size: 200,
      cell: ({ row }) => (
        <span className="text-xs font-mono">
          {row.original.attribution.pool_slot ?? '—'}
        </span>
      ),
    })
  }
  cols.push(
    {
      id: 'duration_ms',
      accessorKey: 'duration_ms',
      header: 'Duration',
      size: 120,
      cell: activeDurationCell,
    },
    {
      id: 'actions',
      header: 'Actions',
      enableSorting: false,
      size: 110,
      cell: buildActionsCell(deps.onKill, deps.cancellingQid),
    },
  )
  return cols
}

export function buildCompletedColumns(opts: {
  showMemory: boolean
  showService: boolean
  onToggleGroup: (runId: string) => void
}): ColumnDef<GroupedCompletedRow>[] {
  const cols: ColumnDef<GroupedCompletedRow>[] = [
    {
      id: 'outcome',
      accessorKey: 'outcome',
      header: 'Outcome',
      size: 130,
      cell: ({ row }) => {
        const r = row.original
        return (
          <div className="flex items-center gap-2">
            <Badge variant={outcomeVariant(r.outcome)} className="capitalize">
              {r.outcome}
            </Badge>
            {r.error_type && (
              <span
                className="text-xs text-red-600 truncate"
                title={r.error_message ?? ''}
              >
                {r.error_type}
              </span>
            )}
          </div>
        )
      },
    },
    ...buildSharedAttributionColumns<GroupedCompletedRow>(opts.onToggleGroup),
  ]
  if (opts.showService) cols.push(buildServiceCol())
  cols.push({
    id: 'duration_ms',
    accessorKey: 'duration_ms',
    header: 'Duration',
    size: 110,
    cell: ({ row }) => (
      <span className={`font-mono text-xs ${durationColor(row.original.duration_ms)}`}>
        {formatDurationMs(row.original.duration_ms)}
      </span>
    ),
  })
  if (opts.showMemory) {
    cols.push({
      id: 'peak_memory_mb',
      accessorKey: 'peak_memory_mb',
      header: 'Memory',
      size: 100,
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground tabular-nums">
          {formatMemoryMb(row.original.peak_memory_mb) || '—'}
        </span>
      ),
    })
  }
  cols.push({
    id: 'sql',
    accessorKey: 'sql_preview',
    header: 'SQL',
    enableSorting: false,
    size: 400,
    cell: ({ row }) => (
      <span
        className="font-mono text-xs text-muted-foreground truncate block"
        title={row.original.sql_preview}
      >
        {row.original.sql_preview}
      </span>
    ),
  })
  return cols
}

const hasAnyVal = <T,>(rows: T[], pick: (r: T) => unknown) =>
  rows.some((r) => pick(r) != null)

export function ActiveTable({
  rows,
  onRowClick,
  onKill,
  cancellingQid,
  onToggleGroup,
}: {
  rows: ActiveOrPromotedRow[]
  onRowClick: (row: ActiveOrPromotedRow) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  onToggleGroup: (runId: string) => void
}) {
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const showPool = hasAnyVal(rows, (r) => r.attribution.pool_slot)
  const columns = React.useMemo(
    () =>
      buildActiveColumns({
        onKill,
        cancellingQid,
        showService,
        showPool,
        onToggleGroup,
      }),
    [onKill, cancellingQid, showService, showPool, onToggleGroup],
  )
  return (
    <DataTable<ActiveOrPromotedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={false}
      getRowClassName={(row) =>
        row._expandedChild
          ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30'
          : row._completed
            ? 'bg-muted/30'
            : row.cancelled_at !== null
              ? 'bg-muted/50'
              : 'bg-primary/5 border-l-2 border-l-primary/60'
      }
      emptyMessage="No active queries. Long-running queries will appear here in real time."
      tableCaption="Active and just-finished queries"
    />
  )
}

export function CompletedTable({
  rows,
  onRowClick,
  emptyMessage = 'No completed queries yet.',
  initialSorting,
  onToggleGroup,
}: {
  rows: GroupedCompletedRow[]
  onRowClick: (row: GroupedCompletedRow) => void
  emptyMessage?: string
  initialSorting?: { id: string; desc: boolean }[]
  onToggleGroup: (runId: string) => void
}) {
  const showMemory = hasAnyVal(rows, (r) => r.peak_memory_mb)
  const showService = hasAnyVal(rows, (r) => r.service_id)
  const columns = React.useMemo(
    () => buildCompletedColumns({ showMemory, showService, onToggleGroup }),
    [showMemory, showService, onToggleGroup],
  )
  return (
    <DataTable<GroupedCompletedRow, unknown>
      columns={columns}
      data={rows}
      onRowClick={onRowClick}
      hideToolbar
      showPagination={rows.length > 50}
      getRowClassName={(row) =>
        row._expandedChild ? 'bg-muted/30 border-l-2 border-l-muted-foreground/30' : ''
      }
      emptyMessage={emptyMessage}
      initialSorting={initialSorting ?? [{ id: 'duration_ms', desc: true }]}
      tableCaption="Completed queries"
    />
  )
}

const isCompleted = (row: AnyRow): row is CompletedRow => 'outcome' in row
const isActivePromoted = (row: AnyRow): row is ActiveOrPromotedRow => 'cancellable' in row

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
  const isLive = row !== null && isActivePromoted(row) && !row._completed
  const { data: fullRow } = useQuery<AnyRow>({
    queryKey: queryKeys.admin.queryMonitorDetail(row?.query_id ?? null),
    queryFn: ({ signal }) =>
      adminJson<'/api/admin/queries/{qid}', ActiveRow>(
        '/api/admin/queries/{qid}',
        `/api/admin/queries/${row!.query_id}`,
        { signal },
      ),
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
  const cancelling =
    ap !== null && cancellingQid !== undefined && cancellingQid === ap.query_id
  const canKill =
    ap !== null && ap.cancellable && ap.cancelled_at === null && onKill !== undefined

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
                variant={outcomeVariant(completed.outcome)}
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
              {attr.caller_qualname}{' '}
              <span className="opacity-60">({attr.caller_file})</span>
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
              <span className="font-mono">
                {formatMemoryMb(completed.peak_memory_mb)}
              </span>
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
          <div className="text-xs text-muted-foreground">
            Loading full SQL ({row.sql_len} chars)…
          </div>
        )}
        <DialogFooter>
          {canKill && (
            <Button
              variant="destructive"
              size="sm"
              disabled={cancelling}
              onClick={() => onKill!(ap!)}
            >
              {cancelling ? (
                'Cancelling…'
              ) : (
                <>
                  <X className="h-3 w-3 mr-1" /> Kill
                </>
              )}
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
        {[
          ['/', 'Focus the search field'],
          ['.', 'Pause / resume the snapshot poll (stops rows from shifting under you)'],
          ['Esc', 'Close the row detail dialog, kill confirm, or this help overlay'],
          ['?', 'Show this help overlay'],
        ].map(([k, desc]) => (
          <React.Fragment key={k}>
            <dt className="self-center">
              <kbd className="inline-flex items-center justify-center min-w-[1.5rem] h-6 px-1.5 rounded border border-border bg-muted/50 text-xs font-mono">
                {k}
              </kbd>
            </dt>
            <dd>{desc}</dd>
          </React.Fragment>
        ))}
      </dl>
      <p className="mt-4 text-xs text-muted-foreground">
        Shortcuts are disabled while typing in the search box (except <kbd>Esc</kbd>, which
        always works).
      </p>
    </HelpDialog>
  )
}

export function SummaryStrip({
  snapshot,
}: {
  snapshot: SnapshotResponse | undefined
}) {
  const data = React.useMemo<SummaryResponse | null>(() => {
    if (!snapshot) return null
    const by_db_type: Record<string, number> = {}
    let longest_ms = 0
    for (const row of snapshot.active) {
      by_db_type[row.db_type] = (by_db_type[row.db_type] ?? 0) + 1
      if (row.duration_ms > longest_ms) longest_ms = row.duration_ms
    }
    return { active_total: snapshot.active.length, by_db_type, longest_ms }
  }, [snapshot])

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
        {data.active_total} active {data.active_total === 1 ? 'query' : 'queries'}
      </div>
    </div>
  )
}
