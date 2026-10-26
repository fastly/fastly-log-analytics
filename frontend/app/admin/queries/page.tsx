'use client'

/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button. Admin-only (the route lives under /api/admin/* so
 * RemoteAccessMiddleware structurally blocks analyst sessions).
 */

import * as React from 'react'
import { formatCount } from '@/lib/format'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { adminJson, adminJsonResult, extractApiError } from '@/lib/api'
import { AlertTriangle, Check, Copy, Group, Keyboard, Pause, Play, RefreshCw, Search, X } from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConfirmDialog, CopyButton, HelpDialog, PageHeader } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'

import {
  durationColor,
  formatDurationMs,
  formatMemoryMb,
  kindBadgeVariant,
  matchesQueryRowFilters,
  useDocumentVisible,
  useFilteredActive,
  useKeyboardShortcuts,
  useQueryMonitorUrlState,
  type ActiveOrPromotedRow,
  type ActiveRow,
  type AttributionKind,
  type CancelResponse,
  type CompletedRow,
  type DbFilter,
  type MonitorConfig,
  type ShortcutBinding,
  type SlowQueriesResult,
  type SnapshotResponse,
  type SummaryResponse,
  type ViewMode,
} from './_hooks/useFilteredActive'
import { ActiveTable, CompletedTable } from './_sections/queryColumns'
import { useCopyToClipboard } from '@/hooks/useSSE'
import { queryKeys } from '@/lib/query-keys'

type AnyRow = ActiveOrPromotedRow | CompletedRow

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
    queryFn: async ({ signal }) =>
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
          <DialogDescription className="sr-only">Query {display.query_id} details</DialogDescription>
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
            {attr.request_id && <span className="opacity-60"> · {attr.request_id.slice(0, 8)}</span>}
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
            <Button variant="destructive" size="sm" disabled={cancelling} onClick={() => onKill!(ap!)}>
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

export function ShortcutsHelp({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  return (
    <HelpDialog open={open} onOpenChange={onOpenChange} title="Keyboard shortcuts" icon={<Keyboard className="h-4 w-4" />}>
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
        Shortcuts are disabled while typing in the search box (except <kbd>Esc</kbd>, which always works).
      </p>
    </HelpDialog>
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

const DEFAULT_SLOW_THRESHOLD_MS = 500
const formatThresholdMs = (ms: number, space = false) =>
  ms < 1000 ? `${ms}${space ? ' ' : ''}ms` : `${ms / 1000}${space ? ' ' : ''}s`
const DB_FILTER_OPTIONS: { value: DbFilter; label: string }[] = [
  { value: 'all', label: 'All DBs' },
  { value: 'DuckDB', label: 'DuckDB' },
  { value: 'Postgres', label: 'Postgres' },
]
const KIND_FILTER_OPTIONS: { value: AttributionKind | 'all'; label: string }[] = (
  ['all', 'analyst', 'admin', 'cron', 'system'] as const
).map((o) => ({ value: o, label: o }))

type SlowHistoryMode = 'recent' | 'past_1h' | 'past_6h' | 'past_12h' | 'past_24h' | 'past_7d'

const SLOW_HISTORY_META: Record<
  SlowHistoryMode,
  { hours: number; buttonLabel: string; title: string; emptyWindow: string; reportLabel: string }
> = {
  recent: { hours: 24, buttonLabel: 'Recent', title: 'Live in-memory ring (~10–30 min window, clears on restart)', emptyWindow: '', reportLabel: 'Recent In-Memory Ring' },
  past_1h: { hours: 1, buttonLabel: '1 h', title: 'Persistent history — last 1 h', emptyWindow: '1 hour', reportLabel: 'Past 1h' },
  past_6h: { hours: 6, buttonLabel: '6 h', title: 'Persistent history — last 6 h', emptyWindow: '6 hours', reportLabel: 'Past 6h' },
  past_12h: { hours: 12, buttonLabel: '12 h', title: 'Persistent history — last 12 h', emptyWindow: '12 hours', reportLabel: 'Past 12h' },
  past_24h: { hours: 24, buttonLabel: '24 h', title: 'Persistent history — last 24 h', emptyWindow: '24 hours', reportLabel: 'Past 24h' },
  past_7d: { hours: 168, buttonLabel: '7 d', title: 'Persistent history — last 7 d', emptyWindow: '7 days', reportLabel: 'Past 7d' },
}
const SLOW_HISTORY_MODES = Object.keys(SLOW_HISTORY_META) as SlowHistoryMode[]
const COMPLETED_BUFFER_CAP = 500

export default function QueryMonitorPage() {
  const queryClient = useQueryClient()
  const visible = useDocumentVisible()
  const {
    search,
    kindFilter,
    dbFilter,
    viewMode,
    slowThresholdMs,
    groupCrons,
    setSearch,
    setKindFilter,
    setDbFilter,
    setViewMode,
    setSlowThresholdMs,
    setGroupCrons,
  } = useQueryMonitorUrlState(DEFAULT_SLOW_THRESHOLD_MS)
  const [confirmKill, setConfirmKill] = React.useState<ActiveRow | null>(null)
  const [actionError, setActionError] = React.useState<string>('')
  const [slowHistoryMode, setSlowHistoryMode] = React.useState<SlowHistoryMode>('recent')
  const [paused, setPaused] = React.useState(false)
  const [expandedRunIds, setExpandedRunIds] = React.useState<ReadonlySet<string>>(() => new Set())
  const toggleGroup = React.useCallback((runId: string) => {
    setExpandedRunIds((prev) => {
      const next = new Set(prev)
      if (next.has(runId)) next.delete(runId)
      else next.add(runId)
      return next
    })
  }, [])
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false)
  const [detailRow, setDetailRow] = React.useState<AnyRow | null>(null)
  const searchInputRef = React.useRef<HTMLInputElement>(null)

  const { data: cfg } = useQuery<MonitorConfig>({
    queryKey: queryKeys.admin.queryMonitorConfig(),
    queryFn: ({ signal }) => adminJson('/api/admin/app-config/query-monitor', '/api/admin/app-config/query-monitor', { signal }),
    staleTime: 60_000,
  })

  const enabled = cfg?.enabled !== false
  const includeCompleted = viewMode !== 'live'

  const lastSeqRef = React.useRef(0)
  const completedRef = React.useRef<Map<number, CompletedRow>>(new Map())
  const resetPendingRef = React.useRef(false)

  const snapshotQuery = useQuery<SnapshotResponse>({
    queryKey: queryKeys.admin.queryMonitorSnapshot(includeCompleted),
    queryFn: async ({ signal }) => {
      if (resetPendingRef.current) {
        resetPendingRef.current = false
        lastSeqRef.current = 0
        completedRef.current = new Map()
      }
      const params = new URLSearchParams()
      if (includeCompleted) params.set('include_completed', 'true')
      if (lastSeqRef.current > 0) {
        params.set('since_seq', String(lastSeqRef.current))
      } else if (includeCompleted) {
        params.set('cold_limit', '50')
      }
      const qs = params.toString()
      const delta = await adminJson<'/api/admin/queries', SnapshotResponse>(
        '/api/admin/queries',
        `/api/admin/queries${qs ? '?' + qs : ''}`,
        { signal },
      )
      if (delta.last_seq > lastSeqRef.current) lastSeqRef.current = delta.last_seq
      const buffer = completedRef.current
      for (const row of delta.completed ?? []) buffer.set(row.query_id, row)
      if (buffer.size > COMPLETED_BUFFER_CAP) {
        const sortedIds = [...buffer.keys()].sort((a, b) => a - b)
        const toRemove = sortedIds.length - COMPLETED_BUFFER_CAP
        for (let k = 0; k < toRemove; k++) buffer.delete(sortedIds[k])
      }
      return { last_seq: delta.last_seq, active: delta.active, completed: [...buffer.values()] }
    },
    enabled: visible && enabled && !paused,
    refetchInterval: (query) => {
      const d = query.state.data as SnapshotResponse | undefined
      return d?.active && d.active.length > 0 ? 300 : 1500
    },
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => {
    resetPendingRef.current = true
    queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() })
  }, [viewMode, queryClient])

  const wasVisibleRef = React.useRef(visible)
  React.useEffect(() => {
    if (visible && !wasVisibleRef.current) {
      resetPendingRef.current = true
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() })
    }
    wasVisibleRef.current = visible
  }, [visible, queryClient])

  const bufferedSnapshot = snapshotQuery.data

  const cancelMutation = useMutation({
    mutationFn: async (qid: number): Promise<CancelResponse> => {
      const r = await adminJsonResult<'/api/admin/queries/{qid}/cancel', CancelResponse, 'post'>(
        '/api/admin/queries/{qid}/cancel',
        `/api/admin/queries/${qid}/cancel`,
        { method: 'POST' },
      )
      if (!r.ok) throw new Error(r.errorText || `status ${r.status}`)
      return r.data
    },
    onSuccess: (res) => {
      setActionError('')
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() })
      if (res.state === 'connection_gone') {
        setActionError('That query already finished — its connection is gone, so there was nothing left to interrupt.')
      } else if (res.state === 'not_found') {
        setActionError('That query is no longer active — it completed or aged out of the live buffer before the kill landed.')
      } else if (res.state !== 'cancelled' && res.state !== 'already_finished') {
        setActionError(`Cancel returned: ${res.state}`)
      }
    },
    onError: (err: Error) => setActionError(extractApiError(err)),
  })

  const historicalQuery = useQuery<SlowQueriesResult>({
    queryKey: queryKeys.admin.queryMonitorSlowHistory(slowHistoryMode, slowThresholdMs),
    queryFn: ({ signal }) =>
      adminJson<'/api/admin/slow-queries', SlowQueriesResult>(
        '/api/admin/slow-queries',
        `/api/admin/slow-queries?since_hours=${SLOW_HISTORY_META[slowHistoryMode].hours}&threshold_ms=${slowThresholdMs}&sort=duration&limit=200`,
        { signal },
      ),
    enabled: slowHistoryMode !== 'recent' && enabled,
    staleTime: 30_000,
  })

  const { filteredActive, completed, slowQueries } = useFilteredActive({
    snapshot: bufferedSnapshot,
    search,
    kindFilter,
    dbFilter,
    slowThresholdMs,
    groupCrons,
    expandedRunIds,
  })

  const filteredHistoricalRows = React.useMemo(() => {
    const raw = historicalQuery.data?.rows ?? []
    const q = search.trim().toLowerCase()
    return raw.filter((r) => matchesQueryRowFilters(r, kindFilter, dbFilter, q))
  }, [historicalQuery.data?.rows, search, kindFilter, dbFilter])

  const { copied: copiedQueries, copy: copyText } = useCopyToClipboard(1500)

  const handleCopyQueries = React.useCallback(async () => {
    const queries = slowHistoryMode === 'recent' ? slowQueries : filteredHistoricalRows
    if (queries.length === 0) return

    const toIso = (ts?: number | null) =>
      ts ? new Date(ts < 10_000_000_000 ? ts * 1000 : ts).toISOString() : '—'

    const lines = [
      '==================================================',
      'NOTABLE SLOW QUERIES REPORT',
      '==================================================',
      `Time Window:    ${SLOW_HISTORY_META[slowHistoryMode].reportLabel}`,
      `Threshold:      ≥ ${slowThresholdMs} ms`,
      `Total Queries:  ${queries.length}`,
      `Exported At:    ${new Date().toISOString()}`,
      '',
    ]

    queries.forEach((row, i) => {
      const attr = row.attribution
      lines.push(
        '--------------------------------------------------',
        `[Query #${i + 1}] ID: ${row.query_id} | DB: ${row.db_type} | Duration: ${formatCount(row.duration_ms)} ms | Outcome: ${row.outcome}`,
        `Timestamp:      Started ${toIso(row.started_at_utc)} · Ended ${toIso(row.ended_at_utc)}`,
        `Attribution:    Kind: ${attr.kind} · Caller: ${attr.caller_qualname} (${attr.caller_file})`,
      )
      if (attr.cron_job) lines.push(`Cron:           ${attr.cron_job}${attr.cron_run_id ? ` (Run: ${attr.cron_run_id})` : ''}`)
      if (attr.request_path) lines.push(`HTTP Request:   ${attr.request_path}${attr.request_id ? ` (ID: ${attr.request_id})` : ''}`)
      if (row.service_id) lines.push(`Service:        ${row.service_id}`)
      if (row.peak_memory_mb != null) lines.push(`Peak Memory:    ${row.peak_memory_mb.toFixed(1)} MB`)
      if (row.error_message) lines.push(`Error:          ${row.error_message}`)
      lines.push('', 'SQL:', row.sql || row.sql_preview, '')
    })
    lines.push('==================================================', '')

    await copyText(lines.join('\n'))
  }, [slowHistoryMode, slowQueries, filteredHistoricalRows, slowThresholdMs, copyText])

  const requestKill = React.useCallback(
    (row: ActiveRow) => {
      setActionError('')
      if (row.attribution.kind === 'cron' || row.attribution.kind === 'system') {
        setConfirmKill(row)
      } else {
        cancelMutation.mutate(row.query_id)
      }
    },
    [cancelMutation],
  )

  const shortcuts = React.useMemo<ShortcutBinding[]>(
    () => [
      {
        key: '/',
        description: 'Focus the search field',
        handler: (e) => {
          e.preventDefault()
          searchInputRef.current?.focus()
          searchInputRef.current?.select()
        },
      },
      {
        key: '?',
        description: 'Show keyboard shortcuts',
        handler: (e) => {
          e.preventDefault()
          setShortcutsOpen(true)
        },
      },
      {
        key: '.',
        description: 'Pause / resume the snapshot poll',
        handler: (e) => {
          e.preventDefault()
          setPaused((p) => !p)
        },
      },
      {
        key: 'Escape',
        description: 'Close dialog / overlay',
        allowInForms: true,
        handler: () => {
          if (shortcutsOpen) return setShortcutsOpen(false)
          if (detailRow) return setDetailRow(null)
          if (confirmKill) return setConfirmKill(null)
          if (document.activeElement === searchInputRef.current) searchInputRef.current?.blur()
        },
      },
    ],
    [shortcutsOpen, confirmKill, detailRow],
  )

  const [isManualRefreshing, setIsManualRefreshing] = React.useState(false)

  const handleManualRefresh = React.useCallback(() => {
    setIsManualRefreshing(true)
    resetPendingRef.current = true
    const pending: Promise<unknown>[] = [
      queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() }),
    ]
    if (slowHistoryMode !== 'recent') pending.push(historicalQuery.refetch())
    void Promise.allSettled(pending).then(() => setIsManualRefreshing(false))
  }, [queryClient, slowHistoryMode, historicalQuery])

  useKeyboardShortcuts(shortcuts, enabled)

  const isInitialLoading = snapshotQuery.isPending && !bufferedSnapshot
  const isRefreshingActive = isManualRefreshing && snapshotQuery.isFetching
  const isRefreshingSlow =
    (isManualRefreshing && (snapshotQuery.isFetching || historicalQuery.isFetching)) ||
    (slowHistoryMode !== 'recent' && historicalQuery.isFetching && !!historicalQuery.data)
  const slowRows = slowHistoryMode === 'recent' ? slowQueries : filteredHistoricalRows

  return (
    <div className="space-y-6" data-testid="admin-queries-root">
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and Postgres query. Click a row to see the full SQL."
      >
        <BackToAdminLink variant="secondary" prefetch={false} />
      </PageHeader>

      {!enabled && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Live Query Monitor is disabled</AlertTitle>
          <AlertDescription>
            Set <code>QUERY_MONITOR_ENABLED=1</code> in the backend environment to enable.
          </AlertDescription>
        </Alert>
      )}

      {enabled && (
        <>
          <div
            data-testid="admin-queries-summary-reservation"
            className="flex items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
          >
            <SummaryStrip snapshot={bufferedSnapshot} />
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-xs"
                data-testid="admin-queries-refresh-btn"
                onClick={handleManualRefresh}
                disabled={snapshotQuery.isFetching}
                title="Refresh query snapshot"
              >
                <RefreshCw className={`h-3 w-3 mr-1 ${isRefreshingActive ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-8 px-2"
                onClick={() => setShortcutsOpen(true)}
                title="Keyboard shortcuts (?)"
              >
                <Keyboard className="h-4 w-4 text-muted-foreground" />
                <span className="sr-only">Show keyboard shortcuts</span>
              </Button>
            </div>
          </div>

          {actionError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{actionError}</AlertDescription>
            </Alert>
          )}

          <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as ViewMode)}>
            <TabsList>
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="live">Live only</TabsTrigger>
              <TabsTrigger value="past">Past only</TabsTrigger>
            </TabsList>
          </Tabs>

          {viewMode !== 'past' && (
            <div
              data-testid="admin-queries-active-reservation"
              className="min-h-[480px] [contain-intrinsic-size:auto_480px] [content-visibility:auto]"
            >
              <Card className="min-h-[480px]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3 gap-2">
                  <CardTitle className="text-base flex items-center gap-2 whitespace-nowrap shrink-0">
                    Active &amp; Just-Finished
                    <Badge variant="secondary" className="whitespace-nowrap">
                      {filteredActive.filter((r) => !r._completed).length} active
                      {filteredActive.some((r) => r._completed) &&
                        ` + ${filteredActive.filter((r) => r._completed).length} just-finished`}
                    </Badge>
                    <PollingIndicator
                      visible={visible}
                      isFetching={snapshotQuery.isFetching}
                      isError={snapshotQuery.isError}
                      paused={paused}
                    />
                  </CardTitle>
                  <div className="flex items-center gap-2 shrink-0">
                    <Button
                      variant={paused ? 'default' : 'outline'}
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => {
                        if (paused) {
                          setPaused(false)
                          queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() })
                        } else {
                          setPaused(true)
                        }
                      }}
                      title={paused ? 'Resume polling (.)' : 'Pause polling (.)'}
                    >
                      {paused ? <><Play className="h-3 w-3 mr-1" /> Resume</> : <><Pause className="h-3 w-3 mr-1" /> Pause</>}
                    </Button>
                    <FilterChipRow value={dbFilter} onChange={setDbFilter} options={DB_FILTER_OPTIONS} />
                    <FilterChipRow value={kindFilter} onChange={setKindFilter} options={KIND_FILTER_OPTIONS} />
                    <Button
                      variant={groupCrons ? 'default' : 'outline'}
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => setGroupCrons((v) => !v)}
                      title={
                        groupCrons
                          ? 'Cron rows from the same run are collapsed. Click to expand.'
                          : 'Cron rows are shown individually. Click to group by run.'
                      }
                    >
                      <Group className="h-3 w-3 mr-1" />
                      Group crons
                    </Button>
                    <div className="relative">
                      <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                      <Input
                        ref={searchInputRef}
                        placeholder="Filter by SQL or caller…"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="h-8 w-48 pl-7 text-sm"
                      />
                    </div>
                  </div>
                </CardHeader>
                <CardContent
                  className={`p-0 transition-opacity duration-150 ${
                    isRefreshingActive ? 'opacity-40 pointer-events-none' : ''
                  }`}
                >
                  {isInitialLoading ? (
                    <div data-testid="admin-queries-active-skeleton">
                      <Skeleton className="h-10 w-full rounded-none" />
                      <div className="h-[400px] flex items-center justify-center text-xs text-muted-foreground">
                        Inspecting DuckDB connection pool...
                      </div>
                    </div>
                  ) : (
                    <ActiveTable
                      rows={filteredActive}
                      onRowClick={(row) => setDetailRow(row)}
                      onKill={requestKill}
                      cancellingQid={cancelMutation.variables ?? null}
                      onToggleGroup={toggleGroup}
                    />
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {viewMode !== 'live' && (
            <div
              data-testid="admin-queries-slow-reservation"
              className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
            >
              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    Notable Slow Queries
                    <Badge variant="outline">{slowRows.length}</Badge>
                    <span className="text-xs text-muted-foreground font-normal">
                      ≥ {formatThresholdMs(slowThresholdMs, slowThresholdMs < 1000)}, sorted slowest first
                    </span>
                    {slowHistoryMode !== 'recent' && historicalQuery.isFetching && (
                      <span className="text-xs text-muted-foreground italic">Loading query telemetry...</span>
                    )}
                  </CardTitle>
                  <div className="flex items-center gap-1">
                    <div className="flex items-center gap-1 mr-2">
                      {SLOW_HISTORY_MODES.map((m) => (
                        <Button
                          key={m}
                          variant={slowHistoryMode === m ? 'default' : 'outline'}
                          size="sm"
                          className="h-7 px-2 text-xs"
                          onClick={() => setSlowHistoryMode(m)}
                          title={SLOW_HISTORY_META[m].title}
                        >
                          {SLOW_HISTORY_META[m].buttonLabel}
                        </Button>
                      ))}
                    </div>
                    {[100, 500, 1000, 2000, 5000].map((ms) => (
                      <Button
                        key={ms}
                        variant={slowThresholdMs === ms ? 'default' : 'outline'}
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={() => setSlowThresholdMs(ms)}
                      >
                        {formatThresholdMs(ms)}
                      </Button>
                    ))}
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 px-2 text-xs ml-2 font-medium shrink-0"
                      disabled={slowRows.length === 0}
                      onClick={handleCopyQueries}
                      title="Copy slow queries report to clipboard"
                    >
                      {copiedQueries ? (
                        <>
                          <Check className="h-3 w-3 mr-1 text-emerald-500 animate-in fade-in zoom-in-95 duration-150" />
                          Copied!
                        </>
                      ) : (
                        <>
                          <Copy className="h-3 w-3 mr-1" />
                          Copy queries
                        </>
                      )}
                    </Button>
                  </div>
                </CardHeader>
                <CardContent
                  className={`p-0 transition-opacity duration-150 ${
                    isRefreshingSlow ? 'opacity-40 pointer-events-none' : ''
                  }`}
                >
                  {isInitialLoading ? (
                    <div data-testid="admin-queries-slow-skeleton">
                      <Skeleton className="h-10 w-full rounded-none" />
                      <div className="h-24 flex items-center justify-center text-xs text-muted-foreground">
                        Loading query telemetry...
                      </div>
                    </div>
                  ) : (
                    <CompletedTable
                      rows={slowRows}
                      onRowClick={(row) => setDetailRow(row)}
                      emptyMessage={
                        slowHistoryMode === 'recent'
                          ? `No queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in recent history.`
                          : historicalQuery.isFetching
                            ? 'Loading query telemetry...'
                            : `No persisted queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in the last ${SLOW_HISTORY_META[slowHistoryMode].emptyWindow}.`
                      }
                      initialSorting={[{ id: 'duration_ms', desc: true }]}
                      onToggleGroup={toggleGroup}
                    />
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {viewMode !== 'live' && (
            <div
              data-testid="admin-queries-completed-reservation"
              className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
            >
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    Recently Completed
                    <Badge variant="outline">{completed.length}</Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent
                  className={`p-0 transition-opacity duration-150 ${
                    isRefreshingActive ? 'opacity-40 pointer-events-none' : ''
                  }`}
                >
                  <CompletedTable
                    rows={completed}
                    onRowClick={(row) => setDetailRow(row)}
                    onToggleGroup={toggleGroup}
                  />
                </CardContent>
              </Card>
            </div>
          )}
        </>
      )}

      <RowDetailDialog
        row={detailRow}
        onClose={() => setDetailRow(null)}
        onKill={(row) => {
          setDetailRow(null)
          requestKill(row)
        }}
        cancellingQid={cancelMutation.variables ?? null}
      />

      {confirmKill && (
        <ConfirmDialog
          open={!!confirmKill}
          onOpenChange={(open) => !open && setConfirmKill(null)}
          title={`Cancel ${confirmKill.attribution.kind} query?`}
          description={
            (confirmKill.attribution.kind === 'cron'
              ? `This is a background ${confirmKill.attribution.cron_job || 'cron'} job. Cancelling may leave its work partial; the next tick will reconcile.`
              : `This is a system query (${confirmKill.attribution.caller_qualname}). Cancelling is rarely the right action.`)
            + ' Kill interrupts only the currently-running statement — a caller that retries will start a new query you may need to cancel again.'
          }
          confirmLabel="Cancel query"
          isDangerous
          onConfirm={() => {
            cancelMutation.mutate(confirmKill.query_id)
            setConfirmKill(null)
          }}
        />
      )}

      <ShortcutsHelp open={shortcutsOpen} onOpenChange={setShortcutsOpen} />
    </div>
  )
}
