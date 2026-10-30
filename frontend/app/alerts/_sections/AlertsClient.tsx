'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  Bell,
  BellOff,
  BellPlus,
  CheckCircle2,
  Clock,
  Info,
  Loader2,
  Play,
  Plus,
  Zap,
} from 'lucide-react'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import {
  useEffectiveServiceId,
  useBootstrapResolved,
  useIsAnalyst,
} from '@/hooks/useBootstrap'
import { useDateFormat, useDeferredDeleteWithUndo } from '@/hooks/useSSE'
import { ReportShell } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent } from '@/components/ui/card'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { showToast } from '@/lib/utils'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import type { VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { AlertPreview } from './AlertPreview'
import { CreateAlertForm } from './CreateAlertForm'
import { AlertsList, ALERTS_AVAILABLE_COLUMNS } from './AlertsList'

export { AlertPreview, CreateAlertForm }

type Alert = components['schemas']['Alert']

const ALERT_STATUS_FILTERS = ['all', 'alerting', 'ok', 'muted'] as const
const alertStatusParser = parseAsStringLiteral(ALERT_STATUS_FILTERS).withDefault('all')

export default function AlertsClient() {
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()
  const [isFormOpen, setIsFormOpen] = React.useState(false)
  const [editingAlert, setEditingAlert] = React.useState<Alert | null>(null)
  const [deleteTarget, setDeleteTarget] = React.useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useQueryState('status', alertStatusParser)
  const { pendingDeleteIds, scheduleDelete } = useDeferredDeleteWithUndo()
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const { full } = useDateFormat()

  const [logSettingsIdle, setLogSettingsIdle] = React.useState(false)
  React.useEffect(() => {
    if (typeof window === 'undefined') return
    const cb = () => setLogSettingsIdle(true)
    const ric = window.requestIdleCallback
    if (typeof ric === 'function') {
      const id = ric(cb)
      return () => {
        try {
          window.cancelIdleCallback?.(id)
        } catch {}
      }
    }
    const id = window.setTimeout(cb, 200)
    return () => window.clearTimeout(id)
  }, [])

  const { data: loggingSettings } = useQuery({
    queryKey: ['loggingSettings', activeServiceId],
    queryFn: async ({ signal }) =>
      activeServiceId
        ? unwrapClient(
            client.GET('/api/services/{service_id}/logging-settings', {
              signal,
              params: { path: { service_id: activeServiceId } },
            }),
          )
        : null,
    enabled: !!activeServiceId && !isAnalyst && logSettingsIdle,
    staleTime: 5 * 60_000,
  })
  const logPeriodSeconds = loggingSettings?.period || 30

  const {
    data: alertsRes,
    isLoading,
    isFetching,
    refetch,
    error: alertsError,
  } = useQuery({
    queryKey: ['alerts', activeServiceId],
    queryFn: async ({ signal }) =>
      activeServiceId
        ? unwrapClient(
            client.GET('/api/alerts/{service_id}', {
              signal,
              params: { path: { service_id: activeServiceId } },
            }),
          )
        : unwrapClient(client.GET('/api/alerts/', { signal })),
    placeholderData: (prev) => prev ?? queryClient.getQueryData(['alerts', null]),
    enabled: !isAnalyst,
    refetchInterval: logPeriodSeconds * 1000,
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => {
    const onFocus = () =>
      queryClient.invalidateQueries({ queryKey: ['alerts', activeServiceId] })
    window.addEventListener('focus', onFocus)
    return () => window.removeEventListener('focus', onFocus)
  }, [queryClient, activeServiceId])

  const allAlerts = React.useMemo(() => alertsRes?.data || [], [alertsRes?.data])
  const alerts = React.useMemo(
    () => allAlerts.filter((a) => !pendingDeleteIds.has(a.id!)),
    [allAlerts, pendingDeleteIds],
  )

  const summaryCounts = React.useMemo(() => {
    let alerting = 0
    let ok = 0
    let muted = 0
    for (const a of alerts) {
      if (a.enabled === false) muted += 1
      else if (a.last_triggered_at) alerting += 1
      else ok += 1
    }
    return { total: alerts.length, alerting, ok, muted }
  }, [alerts])

  const filteredAlerts = React.useMemo(() => {
    if (statusFilter === 'muted') return alerts.filter((a) => a.enabled === false)
    if (statusFilter === 'alerting') {
      return alerts.filter((a) => a.enabled !== false && Boolean(a.last_triggered_at))
    }
    if (statusFilter === 'ok') {
      return alerts.filter((a) => a.enabled !== false && !a.last_triggered_at)
    }
    return alerts
  }, [alerts, statusFilter])

  const lastCheckedLabel = alertsRes?.evaluated_at ? full(alertsRes.evaluated_at) : 'Just now'

  const confirmDelete = React.useCallback(() => {
    if (!deleteTarget) return
    const alertId = deleteTarget
    const alertName = allAlerts.find((a) => a.id === alertId)?.name ?? 'Alert'
    setDeleteTarget(null)
    scheduleDelete({
      id: alertId,
      toastMessage: `Deleted "${alertName}"`,
      errorMessage: `Failed to delete "${alertName}"`,
      performDelete: async () => {
        await unwrapClient(
          client.DELETE('/api/alerts/{alert_id}', {
            params: { path: { alert_id: alertId } },
          }),
        )
        queryClient.invalidateQueries({ queryKey: ['alerts'] })
      },
    })
  }, [deleteTarget, allAlerts, queryClient, scheduleDelete])

  const handleEdit = React.useCallback(
    (alert: Alert) => {
      if (!isAnalyst) {
        setEditingAlert(alert)
        setIsFormOpen(true)
      }
    },
    [isAnalyst],
  )
  const handleCreate = React.useCallback(() => {
    if (!isAnalyst) {
      setEditingAlert(null)
      setIsFormOpen(true)
    }
  }, [isAnalyst])

  const evaluateMutation = useMutation({
    mutationFn: async (serviceId: string) =>
      unwrapClient(
        client.POST('/api/alerts/evaluate', { body: { service_id: serviceId } }),
      ),
    onSuccess: async (data) => {
      showToast(
        `Evaluated ${data?.data?.evaluated ?? 0} rule(s); ${data?.data?.triggered ?? 0} triggered.`,
        'success',
      )
      await queryClient.invalidateQueries({ queryKey: ['alerts'] })
    },
    onError: (error) =>
      showToast(extractApiError(error) || 'Failed to evaluate alerts', 'error'),
  })
  const isEvaluating = evaluateMutation.isPending
  const handleEvaluateNow = React.useCallback(() => {
    if (!isAnalyst && activeServiceId) evaluateMutation.mutate(activeServiceId)
  }, [isAnalyst, activeServiceId, evaluateMutation])

  const summaryCards = [
    {
      label: 'Total Rules',
      value: summaryCounts.total,
      testId: 'alerts-count-total',
      icon: Bell,
      color: 'text-muted-foreground',
      valColor: '',
    },
    {
      label: 'Alerting',
      value: summaryCounts.alerting,
      testId: 'alerts-count-alerting',
      icon: AlertTriangle,
      color: 'text-red-500',
      valColor: 'text-red-500',
    },
    {
      label: 'Healthy (OK)',
      value: summaryCounts.ok,
      testId: 'alerts-count-ok',
      icon: CheckCircle2,
      color: 'text-emerald-500',
      valColor: 'text-emerald-500',
    },
    {
      label: 'Muted',
      value: summaryCounts.muted,
      testId: 'alerts-count-muted',
      icon: BellOff,
      color: 'text-muted-foreground',
      valColor: 'text-muted-foreground',
    },
  ]

  return (
    <ReportShell
      title="Alerts"
      icon={Bell}
      requireService={false}
      isReadyOverride={true}
      headerActions={
        <div className="flex items-center gap-3 flex-wrap">
          <Badge variant="secondary" className="gap-2 font-mono text-xs w-[290px] justify-center">
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
                  Alerts are evaluated dynamically based on your Fastly service&apos;s log
                  batching period (currently <strong>{logPeriodSeconds} seconds</strong>).
                </p>
                <p className="text-muted-foreground">
                  This ensures alerts trigger the moment a new batch of logs arrives from
                  Fastly, without uselessly spamming the database in between batches.
                </p>
                <div className="bg-muted p-2 rounded text-xs mt-2 text-muted-foreground">
                  Note: Alert queries execute in the background server process, so they do not
                  appear in the DuckDB Queries debug panel.
                </div>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      }
    >
      <div
        data-testid="alerts-page-root"
        className="space-y-6 min-h-[820px] [contain-intrinsic-size:auto_820px]"
      >
        <div
          data-testid="alerts-summary-grid"
          className="grid grid-cols-2 md:grid-cols-4 gap-4 min-h-[96px] [contain-intrinsic-size:96px] [content-visibility:auto]"
        >
          {summaryCards.map(({ label, value, testId, icon: Icon, color, valColor }) => (
            <Card key={testId} className="py-3">
              <CardContent className="px-4 py-0 flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground font-medium">{label}</p>
                  <p
                    className={`text-2xl font-bold tabular-nums ${valColor}`}
                    data-testid={testId}
                  >
                    {value}
                  </p>
                </div>
                <Icon className={`h-5 w-5 ${color}`} />
              </CardContent>
            </Card>
          ))}
        </div>

        <Dialog open={isFormOpen} onOpenChange={setIsFormOpen}>
          <DialogContent className="sm:max-w-5xl max-h-[90vh] flex flex-col p-6">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                {editingAlert ? (
                  <Bell className="h-5 w-5 text-primary" />
                ) : (
                  <BellPlus className="h-5 w-5 text-primary" />
                )}
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
                  {ALERT_STATUS_FILTERS.map((st) => (
                    <button
                      key={st}
                      type="button"
                      data-testid={`alerts-filter-${st}`}
                      aria-pressed={statusFilter === st}
                      onClick={() => void setStatusFilter(st)}
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
                  onChange={(id, visible) =>
                    setColumnVisibility((prev) => ({ ...prev, [id]: visible }))
                  }
                />
              </div>
            }
          >
            <div
              className={`min-h-[280px] ${
                isFetching && !isLoading
                  ? 'opacity-40 pointer-events-none transition-opacity'
                  : ''
              }`}
            >
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
                      {alerts.length === 0
                        ? 'No alert rules configured'
                        : `No ${statusFilter} alert rules`}
                    </h3>
                    <p className="text-xs text-muted-foreground">
                      {alerts.length === 0
                        ? 'Create threshold or anomaly detection rules to receive Slack, PagerDuty, or Webhook notifications when traffic or error rates spike.'
                        : 'No configured alert rules match the selected status filter.'}
                    </p>
                  </div>
                  {alerts.length === 0 && !isAnalyst ? (
                    <Button
                      size="sm"
                      onClick={handleCreate}
                      data-testid="alerts-empty-create-btn"
                    >
                      <Plus className="w-4 h-4 mr-1.5" />
                      Add Alert
                    </Button>
                  ) : alerts.length > 0 && statusFilter !== 'all' ? (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => void setStatusFilter('all')}
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
          onOpenChange={(open) => {
            if (!open) setDeleteTarget(null)
          }}
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
