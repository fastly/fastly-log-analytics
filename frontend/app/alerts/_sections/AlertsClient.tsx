'use client'

import React from 'react'
import { useSearchParams } from 'next/navigation'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { ReportShell } from '@/components/ReportShell'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent } from '@/components/ui/card'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  Bell,
  BellOff,
  CheckCircle2,
  Plus,
  AlertTriangle,
  Clock,
  Zap,
  BellPlus,
  Info,
  Loader2,
  Play,
} from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { showToastWithAction, showToast } from '@/lib/toast'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { AlertsList, ALERTS_AVAILABLE_COLUMNS } from './AlertsList'
import { CreateAlertForm } from './AlertEditor'

type Alert = components["schemas"]["Alert"]
export type AlertStatusFilter = 'all' | 'alerting' | 'ok' | 'muted'

const VALID_STATUS_FILTERS = new Set<AlertStatusFilter>(['all', 'alerting', 'ok', 'muted'])

export default function AlertsClient() {
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()
  const searchParams = useSearchParams()
  // Mirrors AppLayout's analyst derivation: a user is "analyst" if their
  // active service is read-only OR if bootstrap flagged them as a remote
  // share-invited analyst. Backend gates POST/PUT/DELETE /api/alerts/* on
  // the same condition (H-1 family), so any modify control would silently
  // 403 — hide them entirely instead of letting the user click through to
  // a failure.
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()
  const [isFormOpen, setIsFormOpen] = React.useState(false)
  const [editingAlert, setEditingAlert] = React.useState<Alert | null>(null)
  const [deleteTarget, setDeleteTarget] = React.useState<string | null>(null)
  const [isEvaluating, setIsEvaluating] = React.useState(false)

  const initialStatusParam = (searchParams?.get('status') || 'all').toLowerCase() as AlertStatusFilter
  const [statusFilter, setStatusFilter] = React.useState<AlertStatusFilter>(
    VALID_STATUS_FILTERS.has(initialStatusParam) ? initialStatusParam : 'all',
  )

  // The filter follows `?status=` whenever it changes (adjusted during render, keyed on the param).
  const statusParam = (searchParams?.get('status') || '').toLowerCase()
  const [seenStatusParam, setSeenStatusParam] = React.useState(statusParam)
  if (statusParam !== seenStatusParam) {
    setSeenStatusParam(statusParam)
    if (VALID_STATUS_FILTERS.has(statusParam as AlertStatusFilter)) {
      setStatusFilter(statusParam as AlertStatusFilter)
    }
  }

  // IDs hidden from the list while the post-confirm undo window is open.
  // The DELETE fires after the toast duration unless the user hits Undo,
  // which removes the id and cancels the per-id timer.
  const [pendingDeleteIds, setPendingDeleteIds] = React.useState<Set<string>>(() => new Set())
  const pendingTimersRef = React.useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const { full } = useDateFormat()

  // Defer the logging-settings fetch to browser idle. The only field
  // consumed is `period`, which feeds the alerts useQuery's
  // refetchInterval and has a safe 30 s default — postponing the
  // 3-Fastly-call chain off the cold path saves 70–700 ms of backend
  // wait on every cold admin load without changing the eventual cadence.
  const [logSettingsIdle, setLogSettingsIdle] = React.useState(false)
  React.useEffect(() => {
    if (typeof window === 'undefined') return
    const cb = () => setLogSettingsIdle(true)
    const ric = window.requestIdleCallback
    if (typeof ric === 'function') {
      const id = ric(cb)
      return () => { try { window.cancelIdleCallback?.(id) } catch {} }
    }
    const id = window.setTimeout(cb, 200)
    return () => window.clearTimeout(id)
  }, [])

  const { data: loggingSettings } = useQuery({
    queryKey: ['loggingSettings', activeServiceId],
    queryFn: async ({ signal }) => {
      if (!activeServiceId) return null
      const { data } = await client.GET("/api/services/{service_id}/logging-settings", {
        signal,
        params: { path: { service_id: activeServiceId } }
      })
      return data
    },
    // Analysts never edit alerts and the redirect dance can't surface
    // logging settings to them anyway — skip the call entirely so the
    // Fastly Stats chain doesn't burn a request on a doomed page load.
    enabled: !!activeServiceId && !isAnalyst && logSettingsIdle,
    // M4: this endpoint chains 3 sequential Fastly calls (~200ms total)
    // — and on a cold cache the upstream Fastly latency can spike to
    // 700-900 ms. The `period` field we read out of it is the logging
    // tile's evaluation interval; that only changes when the admin
    // edits service config, not within an interactive session. Bump
    // staleTime from 30 s to 5 min so the call drops out of the
    // /alerts cold path entirely after first render.
    staleTime: 5 * 60_000,
  })

  const logPeriodSeconds = loggingSettings?.period || 30

  const { data: alertsRes, isLoading, isFetching, refetch, error: alertsError } = useQuery({
    queryKey: ['alerts', activeServiceId],
    queryFn: async ({ signal }) => {
      if (activeServiceId) {
        const { data } = await client.GET("/api/alerts/{service_id}", {
          signal,
          params: { path: { service_id: activeServiceId } }
        })
        return data
      } else {
        const { data } = await client.GET("/api/alerts/", { signal })
        return data
      }
    },
    placeholderData: (prev) => prev ?? queryClient.getQueryData(['alerts', null]),
    // Analysts don't manage alerts. Without this gate the page on the
    // analyst-fronting Fastly path fires /api/alerts (slow, often
    // 503-ing the first byte timeout) only for the redirect dance to
    // immediately bounce them to /dashboard — stop the request at the
    // source.
    enabled: !isAnalyst,
    refetchInterval: logPeriodSeconds * 1000,
    // Drop background polling — alerts are a foreground UI; the focus
    // refetch below picks up changes when the operator returns to the
    // tab. Backgrounded tabs at 1000-deep tab parks were eating an
    // entire backend worker on metadata.db reads otherwise.
    refetchIntervalInBackground: false,
  })

  // Refetch on focus so re-entering the tab surfaces fresh alert state
  // without polling in the background. The default behavior would
  // refresh on focus AND on a refetchInterval cadence; pairing focus
  // with the background-off above gets the right shape.
  React.useEffect(() => {
    const onFocus = () => {
      queryClient.invalidateQueries({ queryKey: ['alerts', activeServiceId] })
    }
    window.addEventListener('focus', onFocus)
    return () => window.removeEventListener('focus', onFocus)
  }, [queryClient, activeServiceId])

  const allAlerts = alertsRes?.data || []
  const alerts = React.useMemo(
    () => allAlerts.filter((a) => !pendingDeleteIds.has(a.id!)),
    [allAlerts, pendingDeleteIds],
  )

  const summaryCounts = React.useMemo(() => {
    let alerting = 0
    let ok = 0
    let muted = 0
    for (const a of alerts) {
      if (a.enabled === false) {
        muted += 1
      } else if (a.last_triggered_at) {
        alerting += 1
      } else {
        ok += 1
      }
    }
    return {
      total: alerts.length,
      alerting,
      ok,
      muted,
    }
  }, [alerts])

  const filteredAlerts = React.useMemo(() => {
    if (statusFilter === 'all') return alerts
    if (statusFilter === 'muted') return alerts.filter((a) => a.enabled === false)
    if (statusFilter === 'alerting') return alerts.filter((a) => a.enabled !== false && Boolean(a.last_triggered_at))
    if (statusFilter === 'ok') return alerts.filter((a) => a.enabled !== false && !a.last_triggered_at)
    return alerts
  }, [alerts, statusFilter])

  const lastCheckedLabel = alertsRes?.evaluated_at ? full(alertsRes.evaluated_at) : 'Just now'

  // Clear pending timers on unmount so the DELETE doesn't fire against a
  // stale closure after the page is gone. Side effect: navigating away
  // mid-window cancels the delete — acceptable since the operator never
  // saw the change commit; they can re-delete on the next visit.
  React.useEffect(() => {
    const timers = pendingTimersRef.current
    return () => {
      for (const [, t] of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])

  const confirmDelete = React.useCallback(() => {
    if (!deleteTarget) return
    const alertId = deleteTarget
    const alertName = allAlerts.find((a) => a.id === alertId)?.name ?? 'Alert'
    setDeleteTarget(null)
    setPendingDeleteIds((prev) => {
      const next = new Set(prev)
      next.add(alertId)
      return next
    })
    const timer = setTimeout(async () => {
      pendingTimersRef.current.delete(alertId)
      try {
        await client.DELETE("/api/alerts/{alert_id}", {
          params: { path: { alert_id: alertId } }
        })
        queryClient.invalidateQueries({ queryKey: ['alerts'] })
      } catch {
        // Restore the row on failure so the operator can retry; the global
        // mutation-error toast (U-4) surfaces the failure separately.
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(alertId)
          return next
        })
        showToast(`Failed to delete "${alertName}"`, 'error')
      }
    }, 5500)
    pendingTimersRef.current.set(alertId, timer)
    showToastWithAction(`Deleted "${alertName}"`, {
      actionLabel: 'Undo',
      onAction: () => {
        const t = pendingTimersRef.current.get(alertId)
        if (t) {
          clearTimeout(t)
          pendingTimersRef.current.delete(alertId)
        }
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(alertId)
          return next
        })
      },
      durationMs: 5500,
    })
  }, [deleteTarget, allAlerts, queryClient])

  const handleEdit = React.useCallback((alert: Alert) => {
    if (isAnalyst) return
    setEditingAlert(alert)
    setIsFormOpen(true)
  }, [isAnalyst])

  const handleCreate = React.useCallback(() => {
    if (isAnalyst) return
    setEditingAlert(null)
    setIsFormOpen(true)
  }, [isAnalyst])

  const handleEvaluateNow = React.useCallback(async () => {
    if (isAnalyst || !activeServiceId) return
    setIsEvaluating(true)
    try {
      const { data } = await client.POST("/api/alerts/evaluate", {
        body: { service_id: activeServiceId }
      })
      const evaluated = data?.data?.evaluated ?? 0
      const triggered = data?.data?.triggered ?? 0
      showToast(`Evaluated ${evaluated} rule(s); ${triggered} triggered.`, 'success')
      await queryClient.invalidateQueries({ queryKey: ['alerts'] })
    } catch (error) {
      showToast(extractApiError(error) || 'Failed to evaluate alerts', 'error')
    } finally {
      setIsEvaluating(false)
    }
  }, [isAnalyst, activeServiceId, queryClient])

  return (
    <ReportShell
      title="Alerts"
      headerActions={
        <div className="flex items-center gap-3 flex-wrap">
          <Badge variant="secondary" className="gap-2 font-mono text-xs min-w-[180px]">
            <Clock className="w-3 h-3 text-muted-foreground shrink-0" />
            <span className="truncate">{`Last Checked: ${lastCheckedLabel}`}</span>
          </Badge>
          {!isAnalyst && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleEvaluateNow}
              disabled={!activeServiceId || isEvaluating || isFetching || isLoading}
              data-testid="alerts-evaluate-btn"
            >
              {isEvaluating ? (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Play className="w-4 h-4 mr-2" />
              )}
              Evaluate Rules
            </Button>
          )}
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching || isLoading}
            data-testid="alerts-refresh-btn"
          >
            <Loader2 className={`w-4 h-4 mr-2 ${isFetching ? 'animate-spin' : ''}`} />
            Refresh Now
          </Button>
          {!isAnalyst && (
            <Button onClick={handleCreate} data-testid="alerts-create-btn">
              <Plus className="w-4 h-4 mr-2" />
              Create Alert
            </Button>
          )}
        </div>
      }
      description={
        <div className="flex items-center gap-2 flex-wrap">
          <span>Configure threshold-based alerts and webhook notifications.</span>
          <Popover>
            <PopoverTrigger>
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-md hover:bg-accent transition-colors">
                <Clock className="h-3 w-3" />
                <span>Evaluated every {logPeriodSeconds}s</span>
                <Info className="h-3 w-3" />
              </div>
            </PopoverTrigger>
            <PopoverContent className="w-80 text-sm">
              <div className="space-y-2">
                <h4 className="font-medium flex items-center gap-2">
                  <Zap className="h-4 w-4 text-yellow-500" />
                  Evaluation Frequency
                </h4>
                <p className="text-muted-foreground">
                  Alerts are evaluated dynamically based on your Fastly service&apos;s log batching period (currently <strong>{logPeriodSeconds} seconds</strong>).
                </p>
                <p className="text-muted-foreground">
                  This ensures alerts trigger the moment a new batch of logs arrives from Fastly, without uselessly spamming the database in between batches.
                </p>
                <div className="bg-muted p-2 rounded text-xs mt-2 text-muted-foreground">
                  Note: Alert queries execute in the background server process, so they do not appear in the DuckDB Queries debug panel.
                </div>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      }
      icon={Bell}
      requireService={false}
      isReadyOverride={true}
    >
      <div data-testid="alerts-page-root" className="space-y-6">
        <div
          data-testid="alerts-summary-grid"
          className="grid grid-cols-2 md:grid-cols-4 gap-4 min-h-[96px] [contain-intrinsic-size:96px] [content-visibility:auto]"
        >
          <Card className="py-3">
            <CardContent className="px-4 py-0 flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground font-medium">Total Rules</p>
                <p className="text-2xl font-bold tabular-nums" data-testid="alerts-count-total">
                  {summaryCounts.total}
                </p>
              </div>
              <Bell className="h-5 w-5 text-muted-foreground" />
            </CardContent>
          </Card>
          <Card className="py-3">
            <CardContent className="px-4 py-0 flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground font-medium">Alerting</p>
                <p className="text-2xl font-bold tabular-nums text-red-500" data-testid="alerts-count-alerting">
                  {summaryCounts.alerting}
                </p>
              </div>
              <AlertTriangle className="h-5 w-5 text-red-500" />
            </CardContent>
          </Card>
          <Card className="py-3">
            <CardContent className="px-4 py-0 flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground font-medium">Healthy (OK)</p>
                <p className="text-2xl font-bold tabular-nums text-emerald-500" data-testid="alerts-count-ok">
                  {summaryCounts.ok}
                </p>
              </div>
              <CheckCircle2 className="h-5 w-5 text-emerald-500" />
            </CardContent>
          </Card>
          <Card className="py-3">
            <CardContent className="px-4 py-0 flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground font-medium">Muted</p>
                <p className="text-2xl font-bold tabular-nums text-muted-foreground" data-testid="alerts-count-muted">
                  {summaryCounts.muted}
                </p>
              </div>
              <BellOff className="h-5 w-5 text-muted-foreground" />
            </CardContent>
          </Card>
        </div>

        <Dialog open={isFormOpen} onOpenChange={setIsFormOpen}>
          <DialogContent className="sm:max-w-5xl max-h-[90vh] flex flex-col p-6">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                {editingAlert ? <Bell className="h-5 w-5 text-primary" /> : <BellPlus className="h-5 w-5 text-primary" />}
                {editingAlert ? `Edit Alert: ${editingAlert.name}` : 'Create New Alert'}
              </DialogTitle>
              <DialogDescription>
                Threshold alerts evaluate logs in real-time every {logPeriodSeconds} seconds.
              </DialogDescription>
            </DialogHeader>
            <CreateAlertForm
              initialAlert={editingAlert}
              onSuccess={() => setIsFormOpen(false)}
            />
          </DialogContent>
        </Dialog>

        <div
          data-testid="alerts-list-card"
          className="min-h-[350px] [contain-intrinsic-size:350px] [content-visibility:auto]"
        >
          <AnalyticsCard
            title="Configured Alerts"
            icon={<Bell className="h-4 w-4" />}
            isLoading={isLoading}
            isFetching={false}
            loadingMessage="Fetching configured alert rules..."
            error={alertsError as AnalyticsCardError | null}
            isEmpty={false}
            className="min-h-[350px]"
            contentClassName="p-0 min-h-[280px]"
            headerAction={
              <div className="flex items-center gap-2 flex-wrap">
                <div
                  role="group"
                  aria-label="Filter alerts by status"
                  data-testid="alerts-status-filter"
                  className="inline-flex items-center rounded-md border bg-muted/40 p-0.5 text-xs"
                >
                  {(['all', 'alerting', 'ok', 'muted'] as AlertStatusFilter[]).map((st) => (
                    <button
                      key={st}
                      type="button"
                      data-testid={`alerts-filter-${st}`}
                      aria-pressed={statusFilter === st}
                      onClick={() => setStatusFilter(st)}
                      className={`px-2.5 py-1 rounded-sm font-medium capitalize transition-colors ${
                        statusFilter === st
                          ? 'bg-background text-foreground shadow-xs'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      {st === 'ok' ? 'OK' : st}
                    </button>
                  ))}
                </div>
                <ColumnVisibilityDropdown
                  columns={ALERTS_AVAILABLE_COLUMNS}
                  visibility={columnVisibility}
                  onChange={(id, visible) => setColumnVisibility(prev => ({ ...prev, [id]: visible }))}
                />
              </div>
            }
          >
            <div className={`min-h-[280px] ${isFetching && !isLoading ? 'opacity-40 pointer-events-none transition-opacity' : ''}`}>
              {isLoading ? (
                <div className="min-h-[280px]" />
              ) : filteredAlerts.length === 0 ? (
                <div
                  data-testid="alerts-empty-state"
                  className="flex flex-col items-center justify-center min-h-[280px] py-12 px-4 text-center gap-3"
                >
                  <Bell className="h-8 w-8 text-muted-foreground/60" aria-hidden="true" />
                  <div className="space-y-1 max-w-md">
                    <h3 className="text-sm font-semibold">
                      {alerts.length === 0 ? 'No alert rules configured' : `No ${statusFilter} alert rules`}
                    </h3>
                    <p className="text-xs text-muted-foreground">
                      {alerts.length === 0
                        ? 'Create threshold or anomaly detection rules to receive Slack, PagerDuty, or Webhook notifications when traffic or error rates spike.'
                        : 'No configured alert rules match the selected status filter.'}
                    </p>
                  </div>
                  {alerts.length === 0 && !isAnalyst ? (
                    <Button size="sm" onClick={handleCreate} data-testid="alerts-empty-create-btn">
                      <Plus className="w-4 h-4 mr-1.5" />
                      Add Alert
                    </Button>
                  ) : alerts.length > 0 && statusFilter !== 'all' ? (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => setStatusFilter('all')}
                      data-testid="alerts-clear-filter-btn"
                    >
                      Show All Rules
                    </Button>
                  ) : null}
                </div>
              ) : (
                <AlertsList
                  alerts={filteredAlerts}
                  columnVisibility={columnVisibility}
                  setColumnVisibility={setColumnVisibility}
                  onEdit={handleEdit}
                  onDelete={setDeleteTarget}
                  isAnalyst={isAnalyst}
                />
              )}
            </div>
          </AnalyticsCard>
        </div>

        <ConfirmDialog
          open={deleteTarget !== null}
          onOpenChange={(open) => { if (!open) setDeleteTarget(null) }}
          onConfirm={confirmDelete}
          isDangerous
          title="Delete Alert"
          description="Are you sure you want to delete this alert?"
          confirmLabel="Delete"
        />

        {!activeServiceId && bootstrapResolved && (
          <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20 text-yellow-600 dark:text-yellow-400 flex items-center gap-3 mt-6">
            <AlertTriangle className="h-5 w-5" />
            <p className="text-sm">
              Showing alerts for <strong>all services</strong>. Select a service to filter.
            </p>
          </div>
        )}
      </div>
    </ReportShell>
  )
}
