'use client'

/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button. Admin-only (the route lives under /api/admin/* so
 * RemoteAccessMiddleware structurally blocks analyst sessions).
 *
 * This file is the orchestrator: data wiring + layout. Derived state
 * (filtered/promoted/slow row sets) lives in `_hooks/useFilteredActive`;
 * URL sync in `_hooks/useQueryMonitorUrlSync`; layout details in
 * `_sections/`; shared types/helpers in `_types.ts` / `_helpers.ts`.
 *
 * Tables render through the project-standard `<DataTable>` (column
 * reorder, hide/show, resize, sort). Row click opens `RowDetailDialog`
 * for the full SQL + attribution. The prior custom HTML tables and
 * inline expand drawer were retired in favour of consistency with every
 * other admin table on the dashboard.
 */

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowLeft, Group, Keyboard, Pause, Play, Search } from 'lucide-react'
import Link from 'next/link'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Input } from '@/components/ui/input'
import { PageHeader } from '@/components/ui/page-header'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { extractApiError } from '@/lib/api'

import { useDocumentVisible } from './_helpers'
import { useFilteredActive } from './_hooks/useFilteredActive'
import { useKeyboardShortcuts, type ShortcutBinding } from './_hooks/useKeyboardShortcuts'
import { useQueryMonitorUrlSync } from './_hooks/useQueryMonitorUrlSync'
import { ActiveTable } from './_sections/ActiveTable'
import { CompletedTable } from './_sections/CompletedTable'
import { DbFilterChips } from './_sections/DbFilterChips'
import { FilterChips } from './_sections/FilterChips'
import { PollingIndicator } from './_sections/PollingIndicator'
import { RowDetailDialog } from './_sections/RowDetailDialog'
import { ShortcutsHelp } from './_sections/ShortcutsHelp'
import { SummaryStrip } from './_sections/SummaryStrip'
import type {
  ActiveOrPromotedRow,
  ActiveRow,
  AttributionKind,
  CancelResponse,
  CompletedRow,
  DbFilter,
  MonitorConfig,
  SnapshotResponse,
  ViewMode,
} from './_types'

const DEFAULT_SLOW_THRESHOLD_MS = 500

type DetailRow = ActiveOrPromotedRow | CompletedRow

export default function QueryMonitorPage() {
  const queryClient = useQueryClient()
  const visible = useDocumentVisible()
  const [search, setSearch] = React.useState('')
  const [kindFilter, setKindFilter] = React.useState<AttributionKind | 'all'>('all')
  const [dbFilter, setDbFilter] = React.useState<DbFilter>('all')
  const [confirmKill, setConfirmKill] = React.useState<ActiveRow | null>(null)
  const [actionError, setActionError] = React.useState<string>('')
  const [viewMode, setViewMode] = React.useState<ViewMode>('all')
  const [slowThresholdMs, setSlowThresholdMs] = React.useState(DEFAULT_SLOW_THRESHOLD_MS)
  // 'recent' = the in-memory ring buffer (~2000 entries, ~10-30 min on a
  // busy service, cleared on restart). 'past_24h' / 'past_7d' = the
  // persistent slow_queries SQLite table. Default to 'recent' because
  // it's the fastest path and what the operator usually wants ("what
  // just happened"); historical view is a deeper-dive toggle.
  const [slowHistoryMode, setSlowHistoryMode] = React.useState<'recent' | 'past_24h' | 'past_7d'>(
    'recent',
  )
  // Cron-grouping collapses rows from the same cron run into a single
  // representative row with a ×N badge — default on because a single tick
  // can spawn dozens of identical queries that otherwise drown out the
  // user's own activity.
  const [groupCrons, setGroupCrons] = React.useState(true)
  // Manual pause stops the 300ms snapshot poll so an admin can read a row
  // mid-incident without it shifting under them. Distinct from the
  // tab-visibility auto-pause; this one survives focus changes.
  const [paused, setPaused] = React.useState(false)
  // Per-run expansion state for cron-grouping. Transient (no URL persist) —
  // the expanded set should reset on hard navigation since the rows it
  // points at won't exist anyway. Stable identity via useCallback so the
  // column builder's useMemo doesn't churn each render.
  const [expandedRunIds, setExpandedRunIds] = React.useState<ReadonlySet<string>>(
    () => new Set(),
  )
  const toggleGroup = React.useCallback((runId: string) => {
    setExpandedRunIds((prev) => {
      const next = new Set(prev)
      if (next.has(runId)) next.delete(runId)
      else next.add(runId)
      return next
    })
  }, [])
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false)
  const [detailRow, setDetailRow] = React.useState<DetailRow | null>(null)
  const searchInputRef = React.useRef<HTMLInputElement>(null)

  useQueryMonitorUrlSync(
    { search, kindFilter, dbFilter, viewMode, slowThresholdMs, groupCrons },
    { setSearch, setKindFilter, setDbFilter, setViewMode, setSlowThresholdMs, setGroupCrons },
    DEFAULT_SLOW_THRESHOLD_MS,
  )

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
    enabled: visible && enabled && !paused,
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
      queryClient.invalidateQueries({ queryKey: ['admin', 'query-monitor', 'snapshot'] })
      if (res.state !== 'cancelled' && res.state !== 'already_finished') {
        setActionError(`Cancel returned: ${res.state}`)
      }
    },
    onError: (err: Error) => setActionError(extractApiError(err) || err.message),
  })

  // Historical slow queries — only fetches when toggled away from
  // 'recent'. Background polling stays off (this isn't a live view);
  // staleTime is 30s so toggling between 24h / 7d back-to-back doesn't
  // re-hit SQLite if the data was just fetched.
  const historicalQuery = useQuery<{ rows: CompletedRow[] }>({
    queryKey: ['admin', 'query-monitor', 'slow-history', slowHistoryMode, slowThresholdMs],
    queryFn: async ({ signal }) => {
      const sinceHours = slowHistoryMode === 'past_7d' ? 168 : 24
      const r = await fetch(
        `/api/admin/slow-queries?since_hours=${sinceHours}&threshold_ms=${slowThresholdMs}&sort=duration&limit=200`,
        { signal },
      )
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: slowHistoryMode !== 'recent' && enabled,
    staleTime: 30_000,
  })

  const { justFinished, filteredActive, completed, slowQueries } = useFilteredActive({
    snapshot: snapshotQuery.data,
    search,
    kindFilter,
    dbFilter,
    slowThresholdMs,
    groupCrons,
    expandedRunIds,
  })

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

  // Row-level shortcuts (j/k/Enter/x) lived with the prior custom table
  // and didn't survive the move to <DataTable>. The remaining shortcuts
  // are page-level (search focus, dialog open/close, help).
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
          if (shortcutsOpen) {
            setShortcutsOpen(false)
            return
          }
          if (detailRow) {
            setDetailRow(null)
            return
          }
          if (confirmKill) {
            setConfirmKill(null)
            return
          }
          if (document.activeElement === searchInputRef.current) {
            searchInputRef.current?.blur()
          }
        },
      },
    ],
    [shortcutsOpen, confirmKill, detailRow],
  )

  useKeyboardShortcuts(shortcuts, enabled)

  return (
    <div className="space-y-6">
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and SQLite query. Click a row to see the full SQL."
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
          <div className="flex items-center justify-between gap-3">
            <SummaryStrip />
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
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Active &amp; Just-Finished
                  <Badge variant="secondary">
                    {/* Count the rows the user can actually see (post-filter),
                       not the unfiltered totals. The prior version showed
                       totals and produced a confusing "44 just-finished but
                       empty table" mismatch when a filter (db / kind) was
                       hiding everything. */}
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
                <div className="flex items-center gap-2">
                  <Button
                    variant={paused ? 'default' : 'outline'}
                    size="sm"
                    className="h-7 px-2 text-xs"
                    onClick={() => {
                      if (paused) {
                        // Resume → immediately fetch so the user gets fresh
                        // data on the click rather than waiting up to 300ms.
                        setPaused(false)
                        queryClient.invalidateQueries({
                          queryKey: ['admin', 'query-monitor', 'snapshot'],
                        })
                      } else {
                        setPaused(true)
                      }
                    }}
                    title={paused ? 'Resume polling (.)' : 'Pause polling (.)'}
                  >
                    {paused ? (
                      <>
                        <Play className="h-3 w-3 mr-1" /> Resume
                      </>
                    ) : (
                      <>
                        <Pause className="h-3 w-3 mr-1" /> Pause
                      </>
                    )}
                  </Button>
                  <DbFilterChips value={dbFilter} onChange={setDbFilter} />
                  <FilterChips value={kindFilter} onChange={setKindFilter} />
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
                      className="h-8 w-64 pl-7 text-sm"
                    />
                  </div>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <ActiveTable
                  rows={filteredActive}
                  onRowClick={(row) => setDetailRow(row)}
                  onKill={requestKill}
                  cancellingQid={cancelMutation.variables ?? null}
                  onToggleGroup={toggleGroup}
                />
              </CardContent>
            </Card>
          )}

          {viewMode !== 'live' && (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Notable Slow Queries
                  <Badge variant="outline">
                    {slowHistoryMode === 'recent'
                      ? slowQueries.length
                      : (historicalQuery.data?.rows ?? []).length}
                  </Badge>
                  <span className="text-xs text-muted-foreground font-normal">
                    ≥ {slowThresholdMs < 1000 ? `${slowThresholdMs} ms` : `${slowThresholdMs / 1000}s`},
                    sorted slowest first
                  </span>
                  {slowHistoryMode !== 'recent' && historicalQuery.isFetching && (
                    <span className="text-xs text-muted-foreground italic">loading…</span>
                  )}
                </CardTitle>
                <div className="flex items-center gap-1">
                  {/* History-window toggle. 'recent' is the in-memory
                      ring buffer (no fetch — what the page already had);
                      the other two query the persistent slow_queries
                      SQLite table via /api/admin/slow-queries. */}
                  <div className="flex items-center gap-1 mr-2">
                    {(['recent', 'past_24h', 'past_7d'] as const).map((m) => (
                      <Button
                        key={m}
                        variant={slowHistoryMode === m ? 'default' : 'outline'}
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={() => setSlowHistoryMode(m)}
                        title={
                          m === 'recent'
                            ? 'Live in-memory ring (~10–30 min window, clears on restart)'
                            : m === 'past_24h'
                              ? 'Persistent history — last 24 h'
                              : 'Persistent history — last 7 d'
                        }
                      >
                        {m === 'recent' ? 'Recent' : m === 'past_24h' ? '24 h' : '7 d'}
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
                      {ms < 1000 ? `${ms}ms` : `${ms / 1000}s`}
                    </Button>
                  ))}
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <CompletedTable
                  rows={
                    slowHistoryMode === 'recent'
                      ? slowQueries
                      : (historicalQuery.data?.rows ?? [])
                  }
                  onRowClick={(row) => setDetailRow(row)}
                  emptyMessage={
                    slowHistoryMode === 'recent'
                      ? `No queries ≥ ${slowThresholdMs < 1000 ? slowThresholdMs + ' ms' : slowThresholdMs / 1000 + ' s'} in recent history.`
                      : historicalQuery.isFetching
                        ? 'Loading…'
                        : `No persisted queries ≥ ${slowThresholdMs < 1000 ? slowThresholdMs + ' ms' : slowThresholdMs / 1000 + ' s'} in the last ${slowHistoryMode === 'past_7d' ? '7 days' : '24 hours'}.`
                  }
                  initialSorting={[{ id: 'duration_ms', desc: true }]}
                  onToggleGroup={toggleGroup}
                />
              </CardContent>
            </Card>
          )}

          {viewMode !== 'live' && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Recently Completed
                  <Badge variant="outline">{completed.length}</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <CompletedTable
                  rows={completed}
                  onRowClick={(row) => setDetailRow(row)}
                  onToggleGroup={toggleGroup}
                />
              </CardContent>
            </Card>
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

      <ShortcutsHelp open={shortcutsOpen} onOpenChange={setShortcutsOpen} />
    </div>
  )
}
