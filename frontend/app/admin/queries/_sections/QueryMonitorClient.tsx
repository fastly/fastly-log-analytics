'use client'
/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button, plus shared TanStack ColumnDef builders for ActiveTable and CompletedTable.
 */
import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  Keyboard,
  RefreshCw,
} from 'lucide-react'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ConfirmDialog, PageHeader } from '@/components/ui/confirm-dialog'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useCopyToClipboard } from '@/hooks/useSSE'
import { adminJson, adminJsonResult, extractApiError, queryKeys } from '@/lib/api'
import {
  type ActiveRow,
  type AnyRow,
  type CancelResponse,
  type CompletedRow,
  type MonitorConfig,
  type ShortcutBinding,
  type SlowHistoryMode,
  type SlowQueriesResult,
  type SnapshotResponse,
  type ViewMode,
  COMPLETED_BUFFER_CAP,
  DEFAULT_SLOW_THRESHOLD_MS,
  SLOW_HISTORY_META,
  matchesQueryRowFilters,
  useDocumentVisible,
  useFilteredActive,
  useKeyboardShortcuts,
  useQueryMonitorUrlState,
} from './useQueryMonitorState'
import {
  RowDetailDialog,
  ShortcutsHelp,
  SummaryStrip,
} from './QueryMonitorComponents'
import {
  ActiveQueriesCard,
  CompletedQueriesCard,
  SlowQueriesCard,
  formatSlowQueriesReport,
} from './QueryMonitorSections'

export * from './useQueryMonitorState'
export * from './QueryMonitorComponents'
export * from './QueryMonitorSections'

export function QueryMonitorPage() {
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
  const [detailRow, setDetailRow] = React.useState<AnyRow | null>(null)
  const searchInputRef = React.useRef<HTMLInputElement>(null)

  const { data: cfg } = useQuery<MonitorConfig>({
    queryKey: queryKeys.admin.queryMonitorConfig(),
    queryFn: ({ signal }) =>
      adminJson(
        '/api/admin/app-config/query-monitor',
        '/api/admin/app-config/query-monitor',
        { signal },
      ),
    staleTime: 60_000,
  })
  const enabled = cfg?.enabled !== false
  const includeCompleted = viewMode !== 'live'
  const lastSeqRef = React.useRef(0)
  const completedRef = React.useRef<Map<number, CompletedRow>>(new Map())
  const resetPendingRef = React.useRef(false)

  const invalidateSnapshots = React.useCallback(
    () => queryClient.invalidateQueries({ queryKey: queryKeys.admin.queryMonitorSnapshotAll() }),
    [queryClient],
  )

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
      if (lastSeqRef.current > 0) params.set('since_seq', String(lastSeqRef.current))
      else if (includeCompleted) params.set('cold_limit', '50')
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
        for (let k = 0; k < sortedIds.length - COMPLETED_BUFFER_CAP; k++) {
          buffer.delete(sortedIds[k])
        }
      }
      return {
        last_seq: delta.last_seq,
        active: delta.active,
        completed: [...buffer.values()],
      }
    },
    enabled: visible && enabled && !paused,
    refetchInterval: (query) =>
      (query.state.data as SnapshotResponse | undefined)?.active?.length ? 300 : 1500,
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => {
    resetPendingRef.current = true
    void invalidateSnapshots()
  }, [viewMode, invalidateSnapshots])

  const wasVisibleRef = React.useRef(visible)
  React.useEffect(() => {
    if (visible && !wasVisibleRef.current) {
      resetPendingRef.current = true
      void invalidateSnapshots()
    }
    wasVisibleRef.current = visible
  }, [visible, invalidateSnapshots])

  const bufferedSnapshot = snapshotQuery.data

  const cancelMutation = useMutation({
    mutationFn: async (qid: number): Promise<CancelResponse> => {
      const r = await adminJsonResult<
        '/api/admin/queries/{qid}/cancel',
        CancelResponse,
        'post'
      >('/api/admin/queries/{qid}/cancel', `/api/admin/queries/${qid}/cancel`, {
        method: 'POST',
      })
      if (!r.ok) throw new Error(r.errorText || `status ${r.status}`)
      return r.data
    },
    onSuccess: (res) => {
      setActionError('')
      void invalidateSnapshots()
      if (res.state === 'connection_gone') {
        setActionError(
          'That query already finished — its connection is gone, so there was nothing left to interrupt.',
        )
      } else if (res.state === 'not_found') {
        setActionError(
          'That query is no longer active — it completed or aged out of the live buffer before the kill landed.',
        )
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
    paused,
  })

  const filteredHistoricalRows = React.useMemo(() => {
    const q = search.trim().toLowerCase()
    return (historicalQuery.data?.rows ?? []).filter((r) =>
      matchesQueryRowFilters(r, kindFilter, dbFilter, q),
    )
  }, [historicalQuery.data?.rows, search, kindFilter, dbFilter])

  const { copied: copiedQueries, copy: copyText } = useCopyToClipboard(1500)
  const handleCopyQueries = React.useCallback(async () => {
    const queries = slowHistoryMode === 'recent' ? slowQueries : filteredHistoricalRows
    if (queries.length === 0) return
    await copyText(formatSlowQueriesReport(queries, slowHistoryMode, slowThresholdMs))
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
          if (document.activeElement === searchInputRef.current) {
            searchInputRef.current?.blur()
          }
        },
      },
    ],
    [shortcutsOpen, confirmKill, detailRow],
  )

  const [isManualRefreshing, setIsManualRefreshing] = React.useState(false)
  const handleManualRefresh = React.useCallback(() => {
    setIsManualRefreshing(true)
    resetPendingRef.current = true
    const pending: Promise<unknown>[] = [invalidateSnapshots()]
    if (slowHistoryMode !== 'recent') pending.push(historicalQuery.refetch())
    void Promise.allSettled(pending).then(() => setIsManualRefreshing(false))
  }, [invalidateSnapshots, slowHistoryMode, historicalQuery])

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
                <RefreshCw
                  className={`h-3 w-3 mr-1 ${isRefreshingActive ? 'animate-spin' : ''}`}
                />
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
            <ActiveQueriesCard
              filteredActive={filteredActive}
              visible={visible}
              isFetching={snapshotQuery.isFetching}
              isError={snapshotQuery.isError}
              paused={paused}
              onTogglePaused={() => {
                if (paused) {
                  setPaused(false)
                  void invalidateSnapshots()
                } else {
                  setPaused(true)
                }
              }}
              dbFilter={dbFilter}
              onDbFilterChange={setDbFilter}
              kindFilter={kindFilter}
              onKindFilterChange={setKindFilter}
              groupCrons={groupCrons}
              onToggleGroupCrons={() => setGroupCrons((v) => !v)}
              search={search}
              onSearchChange={setSearch}
              searchInputRef={searchInputRef}
              isRefreshingActive={isRefreshingActive}
              isInitialLoading={isInitialLoading}
              onSelectRow={(row) => setDetailRow(row)}
              onKill={requestKill}
              cancellingQid={cancelMutation.variables ?? null}
              onToggleGroup={toggleGroup}
            />
          )}
          {viewMode !== 'live' && (
            <SlowQueriesCard
              slowRows={slowRows}
              slowThresholdMs={slowThresholdMs}
              onSlowThresholdChange={setSlowThresholdMs}
              slowHistoryMode={slowHistoryMode}
              onSlowHistoryModeChange={setSlowHistoryMode}
              isFetchingHistorical={historicalQuery.isFetching}
              isRefreshingSlow={isRefreshingSlow}
              isInitialLoading={isInitialLoading}
              copiedQueries={copiedQueries}
              onCopyQueries={handleCopyQueries}
              onSelectRow={(row) => setDetailRow(row)}
              onToggleGroup={toggleGroup}
            />
          )}
          {viewMode !== 'live' && (
            <CompletedQueriesCard
              completed={completed}
              isRefreshingActive={isRefreshingActive}
              onSelectRow={(row) => setDetailRow(row)}
              onToggleGroup={toggleGroup}
            />
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
              : `This is a system query (${confirmKill.attribution.caller_qualname}). Cancelling is rarely the right action.`) +
            ' Kill interrupts only the currently-running statement — a caller that retries will start a new query you may need to cancel again.'
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

export const QueryMonitorClient = QueryMonitorPage

export default QueryMonitorPage
