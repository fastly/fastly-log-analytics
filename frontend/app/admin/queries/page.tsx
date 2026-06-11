'use client'

/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button. Admin-only (the route lives under /api/admin/* so
 * RemoteAccessMiddleware structurally blocks analyst sessions).
 *
 * Polling cadence is adaptive — 1s while queries are active, 2s when idle,
 * paused entirely when the tab is hidden (TanStack Query's
 * refetchIntervalInBackground default). Each row fetches its full SQL
 * lazily via /api/admin/queries/{qid} so the steady-state poll payload
 * stays tiny.
 */

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowLeft, X, ChevronDown, ChevronRight, Search, RefreshCw } from 'lucide-react'
import Link from 'next/link'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { PageHeader } from '@/components/ui/page-header'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { client, extractApiError } from '@/lib/api'

// ── Types ───────────────────────────────────────────────────────────────────

type AttributionKind = 'analyst' | 'admin' | 'cron' | 'system'

interface Attribution {
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

interface ActiveRow {
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

interface CompletedRow extends Omit<ActiveRow, 'cancellable' | 'cancelled_at'> {
  ended_at_utc: number
  outcome: 'ok' | 'error' | 'cancelled'
  error_type: string | null
  error_message: string | null
}

interface SnapshotResponse {
  last_seq: number
  active: ActiveRow[]
  completed: CompletedRow[]
}

interface SummaryResponse {
  active_total: number
  by_db_type: Record<string, number>
  longest_ms: number
}

interface CancelResponse {
  state: 'cancelled' | 'not_found' | 'already_finished' | 'connection_gone'
  query_id: number
}

interface MonitorConfig {
  enabled: boolean
}

// ── Hooks ───────────────────────────────────────────────────────────────────

function useDocumentVisible() {
  const [visible, setVisible] = React.useState(
    typeof document !== 'undefined' ? document.visibilityState !== 'hidden' : true,
  )
  React.useEffect(() => {
    const onVis = () => setVisible(document.visibilityState !== 'hidden')
    document.addEventListener('visibilitychange', onVis)
    return () => document.removeEventListener('visibilitychange', onVis)
  }, [])
  return visible
}

// ── Color/format helpers ────────────────────────────────────────────────────

function durationColor(ms: number): string {
  if (ms < 500) return 'text-emerald-600 dark:text-emerald-400'
  if (ms < 2000) return 'text-amber-600 dark:text-amber-400'
  if (ms < 10_000) return 'text-orange-600 dark:text-orange-400'
  return 'text-red-600 dark:text-red-400'
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`
  const mins = Math.floor(ms / 60_000)
  const secs = Math.round((ms % 60_000) / 1000)
  return `${mins}m ${secs}s`
}

function kindBadgeVariant(kind: AttributionKind): 'default' | 'secondary' | 'destructive' | 'outline' {
  switch (kind) {
    case 'analyst':
      return 'default'
    case 'admin':
      return 'secondary'
    case 'cron':
      return 'outline'
    case 'system':
      return 'outline'
  }
}

// ── The page ────────────────────────────────────────────────────────────────

export default function QueryMonitorPage() {
  const queryClient = useQueryClient()
  const visible = useDocumentVisible()
  const [expandedQid, setExpandedQid] = React.useState<number | null>(null)
  const [search, setSearch] = React.useState('')
  const [kindFilter, setKindFilter] = React.useState<AttributionKind | 'all'>('all')
  const [confirmKill, setConfirmKill] = React.useState<ActiveRow | null>(null)
  const [actionError, setActionError] = React.useState<string>('')

  // Feature-flag check; if disabled, render a clear empty state.
  const { data: cfg } = useQuery<MonitorConfig>({
    queryKey: ['admin', 'query-monitor', 'config'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/app-config/query-monitor', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    staleTime: 60_000,
  })

  const enabled = cfg?.enabled !== false

  // 300ms while the page is open. The snapshot endpoint returns in <1ms
  // server-side; real analyst/cron queries finish in 0.2-30ms (verified on
  // prod 2026-06-11), so anything slower than 300ms polling means the
  // Active list reads empty even when the system is busy. The cost is one
  // tiny GET every 300ms per admin tab — order of nothing.
  const snapshotQuery = useQuery<SnapshotResponse>({
    queryKey: ['admin', 'query-monitor', 'snapshot'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/queries?include_completed=true', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: visible && enabled,
    refetchInterval: 300,
    refetchIntervalInBackground: false,
  })

  const cancelMutation = useMutation({
    mutationFn: async (qid: number): Promise<CancelResponse> => {
      const r = await fetch(`/api/admin/queries/${qid}/cancel`, { method: 'POST' })
      if (!r.ok) {
        const body = await r.text().catch(() => '')
        throw new Error(body || `status ${r.status}`)
      }
      return r.json()
    },
    onSuccess: (res) => {
      setActionError('')
      // Force an immediate refetch so the cancellation appears in the UI
      // without waiting for the next polling tick.
      queryClient.invalidateQueries({ queryKey: ['admin', 'query-monitor', 'snapshot'] })
      if (res.state !== 'cancelled' && res.state !== 'already_finished') {
        setActionError(`Cancel returned: ${res.state}`)
      }
    },
    onError: (err: Error) => setActionError(extractApiError(err) || err.message),
  })

  // "Just finished" — anything that completed in the last 3 seconds. Promoted
  // into the Active section as a faded row with the outcome pill so the user
  // gets visual feedback even when real queries are sub-300ms. Without this
  // the Active list reads empty on typical traffic (verified on prod 2026-
  // 06-11: p50 query duration 0.2ms, max 29ms — far below any poll cadence).
  const JUST_FINISHED_WINDOW_S = 3
  const justFinished = React.useMemo(() => {
    const completed = snapshotQuery.data?.completed ?? []
    const cutoff = Date.now() / 1000 - JUST_FINISHED_WINDOW_S
    return completed.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshotQuery.data])

  // Filter / search the active list (active rows + just-finished promotions).
  const filteredActive = React.useMemo(() => {
    type Row = ActiveRow & { _completed?: CompletedRow }
    const active: Row[] = (snapshotQuery.data?.active ?? []).map((r) => ({ ...r }))
    const justRows: Row[] = justFinished.map((c) => ({
      query_id: c.query_id,
      db_type: c.db_type,
      sql_preview: c.sql_preview,
      sql: c.sql,
      sql_len: c.sql_len,
      attribution: c.attribution,
      service_id: c.service_id,
      started_at_utc: c.started_at_utc,
      duration_ms: c.duration_ms,
      cancellable: false,
      cancelled_at: null,
      _completed: c,
    }))
    // Newest first, no dupes (a row could theoretically appear in both).
    const seen = new Set<number>()
    const combined: Row[] = []
    for (const r of [...active, ...justRows]) {
      if (seen.has(r.query_id)) continue
      seen.add(r.query_id)
      combined.push(r)
    }
    const q = search.trim().toLowerCase()
    return combined.filter((r) => {
      if (kindFilter !== 'all' && r.attribution.kind !== kindFilter) return false
      if (!q) return true
      return (
        r.sql_preview.toLowerCase().includes(q) ||
        r.attribution.caller_qualname.toLowerCase().includes(q) ||
        r.attribution.caller_file.toLowerCase().includes(q) ||
        r.attribution.label.toLowerCase().includes(q)
      )
    })
  }, [snapshotQuery.data, justFinished, search, kindFilter])

  const completed = snapshotQuery.data?.completed ?? []

  const requestKill = (row: ActiveRow) => {
    setActionError('')
    if (row.attribution.kind === 'cron' || row.attribution.kind === 'system') {
      setConfirmKill(row)
    } else {
      cancelMutation.mutate(row.query_id)
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and SQLite query. Click a row to expand."
      >
        <Link href="/admin" className={buttonVariants({ variant: 'secondary', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" /> Back to Admin
        </Link>
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
          <SummaryStrip />

          {actionError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{actionError}</AlertDescription>
            </Alert>
          )}

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                Active &amp; Just-Finished
                <Badge variant="secondary">
                  {(snapshotQuery.data?.active?.length ?? 0)} active
                  {justFinished.length > 0 && ` + ${justFinished.length} just-finished`}
                </Badge>
                <PollingIndicator
                  visible={visible}
                  isFetching={snapshotQuery.isFetching}
                  isError={snapshotQuery.isError}
                />
              </CardTitle>
              <div className="flex items-center gap-2">
                <FilterChips value={kindFilter} onChange={setKindFilter} />
                <div className="relative">
                  <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    placeholder="Filter by SQL or caller…"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    className="h-8 w-64 pl-7 text-sm"
                  />
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <ActiveTable
                rows={filteredActive}
                expandedQid={expandedQid}
                onToggleRow={(qid) => setExpandedQid(expandedQid === qid ? null : qid)}
                onKill={requestKill}
                cancellingQid={cancelMutation.variables ?? null}
              />
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                Recently Completed
                <Badge variant="outline">{completed.length}</Badge>
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <CompletedTable rows={completed} />
            </CardContent>
          </Card>
        </>
      )}

      {confirmKill && (
        <ConfirmDialog
          open={!!confirmKill}
          onOpenChange={(open) => !open && setConfirmKill(null)}
          title={`Cancel ${confirmKill.attribution.kind} query?`}
          description={
            confirmKill.attribution.kind === 'cron'
              ? `This is a background ${confirmKill.attribution.cron_job || 'cron'} job. Cancelling may leave its work partial; the next tick will reconcile.`
              : `This is a system query (${confirmKill.attribution.caller_qualname}). Cancelling is rarely the right action.`
          }
          confirmLabel="Cancel query"
          isDangerous
          onConfirm={() => {
            cancelMutation.mutate(confirmKill.query_id)
            setConfirmKill(null)
          }}
        />
      )}
    </div>
  )
}

// ── Subcomponents ───────────────────────────────────────────────────────────

function SummaryStrip() {
  const visible = useDocumentVisible()
  const { data } = useQuery<SummaryResponse>({
    queryKey: ['admin', 'query-monitor', 'summary'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/queries/summary', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: visible,
    // Same 300ms cadence as the snapshot — without it the badge lags the
    // table and the page feels inconsistent.
    refetchInterval: 300,
    refetchIntervalInBackground: false,
  })
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
          longest: {formatDuration(data.longest_ms)}
        </span>
      )}
    </div>
  )
}

function FilterChips({
  value,
  onChange,
}: {
  value: AttributionKind | 'all'
  onChange: (v: AttributionKind | 'all') => void
}) {
  const opts: (AttributionKind | 'all')[] = ['all', 'analyst', 'admin', 'cron', 'system']
  return (
    <div className="flex items-center gap-1">
      {opts.map((opt) => (
        <Button
          key={opt}
          variant={value === opt ? 'default' : 'outline'}
          size="sm"
          className="h-7 px-2 text-xs capitalize"
          onClick={() => onChange(opt)}
        >
          {opt}
        </Button>
      ))}
    </div>
  )
}

function PollingIndicator({
  visible,
  isFetching,
  isError,
}: {
  visible: boolean
  isFetching: boolean
  isError: boolean
}) {
  if (isError) return <span className="text-xs text-red-500 ml-2">Error — retrying</span>
  if (!visible) return <span className="text-xs text-muted-foreground ml-2">Paused (tab hidden)</span>
  return (
    <span className="flex items-center gap-1 text-xs text-muted-foreground ml-2">
      <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : 'opacity-50'}`} />
      Live
    </span>
  )
}

type ActiveOrPromotedRow = ActiveRow & { _completed?: CompletedRow }

function ActiveTable({
  rows,
  expandedQid,
  onToggleRow,
  onKill,
  cancellingQid,
}: {
  rows: ActiveOrPromotedRow[]
  expandedQid: number | null
  onToggleRow: (qid: number) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
}) {
  if (rows.length === 0) {
    return (
      <div className="p-8 text-center text-sm text-muted-foreground">
        No active queries. Long-running queries will appear here in real time.
      </div>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b text-xs text-muted-foreground">
          <tr className="text-left">
            <th className="px-3 py-2 w-6"></th>
            <th className="px-3 py-2">Source</th>
            <th className="px-3 py-2">Caller</th>
            <th className="px-3 py-2">DB</th>
            <th className="px-3 py-2">Service</th>
            <th className="px-3 py-2">Pool</th>
            <th className="px-3 py-2 text-right">Duration</th>
            <th className="px-3 py-2 text-right w-24">Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const expanded = expandedQid === row.query_id
            const cancelling = cancellingQid === row.query_id
            const isCancelled = row.cancelled_at !== null
            const promoted = !!row._completed
            return (
              <React.Fragment key={row.query_id}>
                <tr
                  className={`border-b hover:bg-muted/30 cursor-pointer ${isCancelled ? 'opacity-60' : ''} ${promoted ? 'opacity-60 bg-muted/10' : ''}`}
                  onClick={() => onToggleRow(row.query_id)}
                >
                  <td className="px-3 py-2">
                    {expanded ? (
                      <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                    ) : (
                      <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex items-center gap-2">
                      <Badge variant={kindBadgeVariant(row.attribution.kind)} className="capitalize">
                        {row.attribution.kind}
                      </Badge>
                      <span className="truncate max-w-xs" title={row.attribution.label}>
                        {row.attribution.label}
                      </span>
                    </div>
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs" title={row.attribution.caller_file}>
                    {row.attribution.caller_file}
                  </td>
                  <td className="px-3 py-2">{row.db_type}</td>
                  <td className="px-3 py-2 text-xs">{row.service_id ?? '—'}</td>
                  <td className="px-3 py-2 text-xs font-mono">{row.attribution.pool_slot ?? '—'}</td>
                  <td className={`px-3 py-2 text-right font-mono ${durationColor(row.duration_ms)}`}>
                    {formatDuration(row.duration_ms)}
                  </td>
                  <td className="px-3 py-2 text-right">
                    {promoted ? (
                      <Badge
                        variant={
                          row._completed!.outcome === 'ok'
                            ? 'outline'
                            : row._completed!.outcome === 'cancelled'
                            ? 'secondary'
                            : 'destructive'
                        }
                        className="capitalize"
                      >
                        {row._completed!.outcome === 'ok' ? '✓ done' : row._completed!.outcome}
                      </Badge>
                    ) : row.cancellable && !isCancelled ? (
                      <Button
                        variant="destructive"
                        size="sm"
                        className="h-7 px-2"
                        disabled={cancelling}
                        onClick={(e) => {
                          e.stopPropagation()
                          onKill(row)
                        }}
                      >
                        {cancelling ? 'Cancelling…' : (<><X className="h-3 w-3 mr-1" /> Kill</>)}
                      </Button>
                    ) : isCancelled ? (
                      <span className="text-xs text-muted-foreground">cancelling…</span>
                    ) : (
                      <span className="text-xs text-muted-foreground">—</span>
                    )}
                  </td>
                </tr>
                {expanded && <ExpandedRow row={row} />}
              </React.Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function CompletedTable({ rows }: { rows: CompletedRow[] }) {
  if (rows.length === 0) {
    return (
      <div className="p-6 text-center text-sm text-muted-foreground">
        No completed queries yet.
      </div>
    )
  }
  // Show newest first
  const sorted = [...rows].sort((a, b) => b.query_id - a.query_id).slice(0, 50)
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
            <th className="px-3 py-2">SQL</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr key={row.query_id} className="border-b hover:bg-muted/30">
              <td className="px-3 py-2">
                <Badge
                  variant={
                    row.outcome === 'ok' ? 'outline' : row.outcome === 'cancelled' ? 'secondary' : 'destructive'
                  }
                  className="capitalize"
                >
                  {row.outcome}
                </Badge>
                {row.error_type && (
                  <span className="text-xs text-red-600 ml-2">{row.error_type}</span>
                )}
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
              <td className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs" title={row.attribution.caller_file}>
                {row.attribution.caller_file}
              </td>
              <td className="px-3 py-2 text-xs">{row.db_type}</td>
              <td className={`px-3 py-2 text-right font-mono ${durationColor(row.duration_ms)}`}>
                {formatDuration(row.duration_ms)}
              </td>
              <td className="px-3 py-2 font-mono text-xs text-muted-foreground truncate max-w-md" title={row.sql_preview}>
                {row.sql_preview}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ExpandedRow({ row }: { row: ActiveRow }) {
  // Fetch full SQL lazily; falls back to the preview if the row finished.
  const { data: fullRow } = useQuery({
    queryKey: ['admin', 'query-monitor', 'detail', row.query_id],
    queryFn: async ({ signal }) => {
      const r = await fetch(`/api/admin/queries/${row.query_id}`, { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json() as Promise<ActiveRow>
    },
    // Refetch every 2s for as long as the row stays expanded — its
    // duration ticks up live.
    refetchInterval: 2000,
    refetchIntervalInBackground: false,
  })
  const sql = fullRow?.sql ?? row.sql_preview
  const attr = row.attribution

  return (
    <tr className="bg-muted/20">
      <td colSpan={8} className="px-3 py-3">
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div>
              <span className="text-muted-foreground">Caller:</span>{' '}
              <span className="font-mono">{attr.caller_qualname} ({attr.caller_file})</span>
            </div>
            <div>
              <span className="text-muted-foreground">Request:</span>{' '}
              {attr.request_path || '—'}{attr.request_id ? ` · ${attr.request_id.slice(0, 8)}` : ''}
            </div>
            {attr.cron_job && (
              <div>
                <span className="text-muted-foreground">Cron:</span> {attr.cron_job}
                {attr.cron_run_id && ` (run ${attr.cron_run_id})`}
              </div>
            )}
            {attr.pool_slot && (
              <div>
                <span className="text-muted-foreground">Pool slot:</span> <span className="font-mono">{attr.pool_slot}</span>
              </div>
            )}
          </div>
          <pre className="bg-background border rounded p-3 text-xs overflow-x-auto whitespace-pre-wrap font-mono max-h-64">
            {sql}
          </pre>
          {row.sql_len > 200 && !fullRow?.sql && (
            <div className="text-xs text-muted-foreground">Loading full SQL ({row.sql_len} chars)…</div>
          )}
        </div>
      </td>
    </tr>
  )
}
