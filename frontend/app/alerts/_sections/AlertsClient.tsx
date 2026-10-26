'use client'

import React from 'react'
import { useSearchParams } from 'next/navigation'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { z } from 'zod'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useEffectiveServiceId, useBootstrapResolved, useIsAnalyst } from '@/hooks/useBootstrap'
import { useDateFormat, useDeferredDeleteWithUndo } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { ReportShell } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Card, CardContent } from '@/components/ui/card'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import {
  Activity,
  AlertTriangle,
  Bell,
  BellOff,
  BellPlus,
  CheckCircle2,
  Clock,
  Info,
  Loader2,
  Pencil,
  Play,
  Plus,
  Send,
  Trash2,
  Zap,
} from 'lucide-react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { showToast } from '@/lib/utils'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { CreateAlertForm } from './AlertEditor'

type Alert = components['schemas']['Alert']

const alertStatusFilterSchema = z.enum(['all', 'alerting', 'ok', 'muted'])
type AlertStatusFilter = z.infer<typeof alertStatusFilterSchema>

export const ALERTS_AVAILABLE_COLUMNS = [
  { id: 'name', label: 'Alert Name' },
  { id: 'category', label: 'Category' },
  { id: 'metric', label: 'Metric' },
  { id: 'condition', label: 'Condition' },
  { id: 'last_triggered_at', label: 'Last Triggered' },
  { id: 'enabled', label: 'Enabled?' },
]

const METRIC_TO_DASHBOARD: Record<string, string> = {
  '5xx_rate': '5xx',
  '4xx_rate': '4xx',
  specific_status_rate: 'requests',
  bandwidth: 'throughput',
  ttfb: 'ttfb_client',
}

interface AlertsListProps {
  alerts: Alert[]
  columnVisibility: VisibilityState
  setColumnVisibility: React.Dispatch<React.SetStateAction<VisibilityState>>
  onEdit: (alert: Alert) => void
  onDelete: (alertId: string) => void
  isAnalyst?: boolean
}

export function AlertsList({
  alerts,
  columnVisibility,
  setColumnVisibility,
  onEdit,
  onDelete,
  isAnalyst = false,
}: AlertsListProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()
  const { relative, full, abbr } = useDateFormat()

  const toggleMutation = useMutation({
    mutationFn: async ({ alert, newEnabled }: { alert: Alert; newEnabled: boolean }) => {
      await unwrapClient(
        client.PATCH('/api/alerts/{alert_id}/enabled', {
          params: { path: { alert_id: alert.id! } },
          body: { enabled: newEnabled },
        }),
      )
    },
    onMutate: async ({ alert, newEnabled }) => {
      const queryKey = ['alerts', activeServiceId]
      await queryClient.cancelQueries({ queryKey })
      const previous = queryClient.getQueryData(queryKey)
      queryClient.setQueryData(queryKey, (old: { data?: Alert[] } | undefined) => ({
        ...old,
        data: old?.data?.map((a) => (a.id === alert.id ? { ...a, enabled: newEnabled } : a)),
      }))
      return { previous, queryKey }
    },
    onError: (err, _vars, context) => {
      console.error('Failed to toggle alert', err)
      if (context?.queryKey) queryClient.setQueryData(context.queryKey, context.previous)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
    },
  })
  const togglingId = toggleMutation.isPending ? (toggleMutation.variables?.alert.id ?? null) : null
  const toggleEnabled = React.useCallback(
    (alert: Alert, newEnabled: boolean) => toggleMutation.mutate({ alert, newEnabled }),
    [toggleMutation],
  )

  const testMutation = useMutation({
    mutationFn: async (alert: Alert) => {
      const { data, error } = await client.POST('/api/alerts/{alert_id}/test', {
        params: { path: { alert_id: alert.id! } },
        body: {
          service_id: activeServiceId || alert.service_id,
          channel_type: 'webhook',
          dry_run: true,
        },
      })
      if (error) {
        showToast(extractApiError(error) || 'Failed to test alert notification', 'error')
        return
      }
      const dispatched = data?.data?.targets_tested ?? 0
      showToast(
        dispatched > 0
          ? `Validated ${dispatched} notification channel${dispatched === 1 ? '' : 's'}`
          : 'Alert rule validated (no external channels configured)',
        'success',
      )
    },
    onError: (err) => {
      console.error('Failed to test alert notification', err)
      showToast(extractApiError(err) || 'Failed to test alert notification', 'error')
    },
  })
  const testingId = testMutation.isPending ? (testMutation.variables?.id ?? null) : null
  const testNotification = React.useCallback(
    (alert: Alert) => {
      if (alert.id) testMutation.mutate(alert)
    },
    [testMutation],
  )

  const columns = React.useMemo<ColumnDef<Alert>[]>(
    () => [
      {
        accessorKey: 'name',
        header: 'Alert Name',
        cell: (info) => <span className="font-medium">{info.row.original.name}</span>,
      },
      {
        accessorKey: 'category',
        header: 'Category',
        cell: (info) => (
          <Badge variant="secondary" className="capitalize">
            {info.row.original.category?.replace('_', ' ') || 'Reliability'}
          </Badge>
        ),
      },
      {
        accessorKey: 'metric',
        header: 'Metric',
        cell: (info) => {
          const { metric: val, status_codes: codes, evaluation_scope: scope } = info.row.original
          const display =
            val === 'specific_status' && codes
              ? `Status ${codes.join(', ')}`
              : val === 'specific_status_rate' && codes
                ? `Status ${codes.join(', ')} Rate`
                : val.replace(/_/g, ' ')
          return (
            <div className="flex items-center">
              <span className="capitalize text-sm font-medium">{display}</span>
              {scope === 'edge' && (
                <Badge variant="outline" className="ml-2 text-[10px] h-4 px-1 py-0 font-normal">
                  Edge
                </Badge>
              )}
              {scope === 'origin' && (
                <Badge
                  variant="outline"
                  className="ml-2 text-[10px] h-4 px-1 py-0 font-normal border-orange-500/50 text-orange-600 dark:text-orange-400"
                >
                  Origin
                </Badge>
              )}
            </div>
          )
        },
      },
      {
        id: 'condition',
        header: 'Condition',
        cell: (info) => {
          const a = info.row.original
          const windowStr = a.window_min < 1 ? `${Math.round(a.window_min * 60)}s` : `${a.window_min}m`
          const evalType = a.evaluation_type || 'absolute'
          if (evalType === 'absolute') {
            return (
              <span className="text-sm font-mono">
                {a.operator} {a.threshold} (last {windowStr})
              </span>
            )
          }
          if (evalType === 'anomaly_zscore') {
            const zThresh = a.zscore_threshold !== undefined ? a.zscore_threshold : a.threshold || 3.0
            const baseDays = a.baseline_period_days || 7
            return (
              <span
                className="text-sm font-mono flex items-center gap-1"
                title={`Z-Score > ${zThresh} vs rolling baseline over last ${baseDays} days`}
              >
                <span className="text-purple-600 dark:text-purple-400 font-bold">Z-Score</span> &gt; {zThresh}
                <span className="text-muted-foreground text-[10px]">({baseDays}d baseline)</span>
              </span>
            )
          }
          const compStr = a.comparison_period_min
            ? a.comparison_period_min >= 1440
              ? `${a.comparison_period_min / 1440}d`
              : a.comparison_period_min >= 60
                ? `${a.comparison_period_min / 60}h`
                : `${a.comparison_period_min}m`
            : '?'
          return (
            <span className="text-sm font-mono flex items-center gap-1">
              {evalType === 'relative_increase' ? '↑' : '↓'} &gt; {a.threshold}%
              <span className="text-muted-foreground text-[10px]"> vs {compStr} ago</span>
            </span>
          )
        },
      },
      {
        accessorKey: 'last_triggered_at',
        header: 'Last Triggered',
        cell: (info) => {
          const alert = info.row.original
          const val = alert.last_triggered_at
          if (!val) return <span className="text-muted-foreground text-xs italic">Never</span>

          const end = new Date(val)
          const start = new Date(end.getTime() - alert.window_min * 60 * 1000)
          const params = new URLSearchParams({
            start_time: start.toISOString(),
            end_time: end.toISOString(),
            metric: METRIC_TO_DASHBOARD[alert.metric] ?? alert.metric,
          })
          if ((alert.metric === 'specific_status' || alert.metric === 'specific_status_rate') && alert.status_codes) {
            alert.status_codes.forEach((code: number) => params.append('filter_status', String(code)))
          }
          if (alert.evaluation_scope === 'edge') params.append('filter_edge', 'true')
          else if (alert.evaluation_scope === 'origin') params.append('filter_edge', 'false')
          if (activeServiceId) params.set('service', activeServiceId)

          return (
            <div className="flex flex-col gap-1">
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <div className="flex flex-col">
                        <span className="text-xs text-red-500 font-bold flex items-center gap-1">
                          <AlertTriangle className="h-3 w-3" />
                          {relative(val)}
                        </span>
                      </div>
                    }
                  />
                  <TooltipContent className="text-xs">
                    {full(val)} {abbr()}
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <a
                href={`/dashboard?${params.toString()}`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[10px] text-primary hover:underline flex items-center gap-1 w-fit"
              >
                <Activity className="h-3 w-3" />
                View on Dashboard
              </a>
            </div>
          )
        },
      },
      {
        accessorKey: 'enabled',
        header: 'Enabled?',
        cell: (info) => {
          const a = info.row.original
          if (isAnalyst) {
            return (
              <Badge
                variant={a.enabled ? 'secondary' : 'outline'}
                className="text-[10px]"
                data-testid={`analyst-alert-enabled-${a.id}`}
              >
                {a.enabled ? 'On' : 'Off'}
              </Badge>
            )
          }
          const isPending = togglingId === a.id
          return (
            <Switch
              checked={a.enabled}
              onCheckedChange={(checked) => toggleEnabled(a, checked)}
              disabled={isPending}
              data-testid={`toggle-alert-${a.id}`}
              aria-label={`Toggle alert ${a.name}`}
              className={isPending ? 'opacity-50 cursor-wait' : undefined}
            />
          )
        },
      },
      ...(isAnalyst
        ? []
        : [
            {
              id: 'actions',
              header: '',
              cell: (info) => {
                const a = info.row.original
                return (
                  <div className="flex items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label="Test alert notification"
                      data-testid={`test-alert-${a.id}`}
                      className="h-8 w-8 text-muted-foreground hover:text-primary"
                      onClick={() => testNotification(a)}
                      disabled={testingId === a.id}
                      title="Test alert notification"
                    >
                      <Send className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label="Edit alert"
                      data-testid={`edit-alert-${a.id}`}
                      className="h-8 w-8 text-muted-foreground hover:text-primary"
                      onClick={() => onEdit(a)}
                      title="Edit alert"
                    >
                      <Pencil className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label="Delete alert"
                      data-testid={`delete-alert-${a.id}`}
                      className="h-8 w-8 text-muted-foreground hover:text-destructive"
                      onClick={() => a.id && onDelete(a.id)}
                      title="Delete alert"
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                )
              },
            } satisfies ColumnDef<Alert>,
          ]),
    ],
    [togglingId, testingId, relative, full, abbr, toggleEnabled, testNotification, onEdit, onDelete, isAnalyst, activeServiceId],
  )

  return (
    <DataTable
      columns={columns}
      data={alerts || []}
      hideToolbar={true}
      columnVisibility={columnVisibility}
      onColumnVisibilityChange={setColumnVisibility}
    />
  )
}

export default function AlertsClient() {
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()
  const searchParams = useSearchParams()
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()
  const [isFormOpen, setIsFormOpen] = React.useState(false)
  const [editingAlert, setEditingAlert] = React.useState<Alert | null>(null)
  const [deleteTarget, setDeleteTarget] = React.useState<string | null>(null)

  const initialParsed = alertStatusFilterSchema.safeParse((searchParams?.get('status') || 'all').toLowerCase())
  const [statusFilter, setStatusFilter] = React.useState<AlertStatusFilter>(
    initialParsed.success ? initialParsed.data : 'all',
  )

  const statusParam = (searchParams?.get('status') || '').toLowerCase()
  const [seenStatusParam, setSeenStatusParam] = React.useState(statusParam)
  if (statusParam !== seenStatusParam) {
    setSeenStatusParam(statusParam)
    const parsed = alertStatusFilterSchema.safeParse(statusParam)
    if (parsed.success) setStatusFilter(parsed.data)
  }

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
    queryFn: async ({ signal }) => {
      if (!activeServiceId) return null
      return unwrapClient(
        client.GET('/api/services/{service_id}/logging-settings', {
          signal,
          params: { path: { service_id: activeServiceId } },
        }),
      )
    },
    enabled: !!activeServiceId && !isAnalyst && logSettingsIdle,
    staleTime: 5 * 60_000,
  })

  const logPeriodSeconds = loggingSettings?.period || 30

  const { data: alertsRes, isLoading, isFetching, refetch, error: alertsError } = useQuery({
    queryKey: ['alerts', activeServiceId],
    queryFn: async ({ signal }) =>
      activeServiceId
        ? unwrapClient(client.GET('/api/alerts/{service_id}', { signal, params: { path: { service_id: activeServiceId } } }))
        : unwrapClient(client.GET('/api/alerts/', { signal })),
    placeholderData: (prev) => prev ?? queryClient.getQueryData(['alerts', null]),
    enabled: !isAnalyst,
    refetchInterval: logPeriodSeconds * 1000,
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => {
    const onFocus = () => queryClient.invalidateQueries({ queryKey: ['alerts', activeServiceId] })
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
      if (a.enabled === false) muted += 1
      else if (a.last_triggered_at) alerting += 1
      else ok += 1
    }
    return { total: alerts.length, alerting, ok, muted }
  }, [alerts])

  const filteredAlerts = React.useMemo(() => {
    if (statusFilter === 'muted') return alerts.filter((a) => a.enabled === false)
    if (statusFilter === 'alerting') return alerts.filter((a) => a.enabled !== false && Boolean(a.last_triggered_at))
    if (statusFilter === 'ok') return alerts.filter((a) => a.enabled !== false && !a.last_triggered_at)
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
      performDelete: async () => {
        await unwrapClient(client.DELETE('/api/alerts/{alert_id}', { params: { path: { alert_id: alertId } } }))
        queryClient.invalidateQueries({ queryKey: ['alerts'] })
      },
      errorMessage: `Failed to delete "${alertName}"`,
    })
  }, [deleteTarget, allAlerts, queryClient, scheduleDelete])

  const handleEdit = React.useCallback(
    (alert: Alert) => {
      if (isAnalyst) return
      setEditingAlert(alert)
      setIsFormOpen(true)
    },
    [isAnalyst],
  )

  const handleCreate = React.useCallback(() => {
    if (isAnalyst) return
    setEditingAlert(null)
    setIsFormOpen(true)
  }, [isAnalyst])

  const evaluateMutation = useMutation({
    mutationFn: async (serviceId: string) =>
      unwrapClient(client.POST('/api/alerts/evaluate', { body: { service_id: serviceId } })),
    onSuccess: async (data) => {
      const evaluated = data?.data?.evaluated ?? 0
      const triggered = data?.data?.triggered ?? 0
      showToast(`Evaluated ${evaluated} rule(s); ${triggered} triggered.`, 'success')
      await queryClient.invalidateQueries({ queryKey: ['alerts'] })
    },
    onError: (error) => {
      showToast(extractApiError(error) || 'Failed to evaluate alerts', 'error')
    },
  })
  const isEvaluating = evaluateMutation.isPending

  const handleEvaluateNow = React.useCallback(() => {
    if (!isAnalyst && activeServiceId) evaluateMutation.mutate(activeServiceId)
  }, [isAnalyst, activeServiceId, evaluateMutation])

  const summaryCards = [
    { label: 'Total Rules', value: summaryCounts.total, testId: 'alerts-count-total', icon: Bell, color: 'text-muted-foreground', valColor: '' },
    { label: 'Alerting', value: summaryCounts.alerting, testId: 'alerts-count-alerting', icon: AlertTriangle, color: 'text-red-500', valColor: 'text-red-500' },
    { label: 'Healthy (OK)', value: summaryCounts.ok, testId: 'alerts-count-ok', icon: CheckCircle2, color: 'text-emerald-500', valColor: 'text-emerald-500' },
    { label: 'Muted', value: summaryCounts.muted, testId: 'alerts-count-muted', icon: BellOff, color: 'text-muted-foreground', valColor: 'text-muted-foreground' },
  ]

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
              {isEvaluating ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Play className="w-4 h-4 mr-2" />}
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
          {summaryCards.map(({ label, value, testId, icon: Icon, color, valColor }) => (
            <Card key={testId} className="py-3">
              <CardContent className="px-4 py-0 flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground font-medium">{label}</p>
                  <p className={`text-2xl font-bold tabular-nums ${valColor}`} data-testid={testId}>
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
                {editingAlert ? <Bell className="h-5 w-5 text-primary" /> : <BellPlus className="h-5 w-5 text-primary" />}
                {editingAlert ? `Edit Alert: ${editingAlert.name}` : 'Create New Alert'}
              </DialogTitle>
              <DialogDescription>
                Threshold alerts evaluate logs in real-time every {logPeriodSeconds} seconds.
              </DialogDescription>
            </DialogHeader>
            <CreateAlertForm initialAlert={editingAlert} onSuccess={() => setIsFormOpen(false)} />
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
                  onChange={(id, visible) => setColumnVisibility((prev) => ({ ...prev, [id]: visible }))}
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
