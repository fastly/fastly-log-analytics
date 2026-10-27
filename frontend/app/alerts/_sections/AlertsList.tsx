'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Activity, AlertTriangle, Pencil, Send, Trash2 } from 'lucide-react'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useDateFormat } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { showToast } from '@/lib/utils'
import { DataTable } from '@/components/DataTable'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'

type Alert = components['schemas']['Alert']

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

export interface AlertsListProps {
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
      queryClient.setQueryData(
        queryKey,
        (old: { data?: Alert[] } | undefined) => ({
          ...old,
          data: old?.data?.map((a) =>
            a.id === alert.id ? { ...a, enabled: newEnabled } : a,
          ),
        }),
      )
      return { previous, queryKey }
    },
    onError: (err, _vars, context) => {
      console.error('Failed to toggle alert', err)
      if (context?.queryKey) {
        queryClient.setQueryData(context.queryKey, context.previous)
      }
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['alerts'] }),
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
          const {
            metric: val,
            status_codes: codes,
            evaluation_scope: scope,
          } = info.row.original
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
                <Badge
                  variant="outline"
                  className="ml-2 text-[10px] h-4 px-1 py-0 font-normal"
                >
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
          const windowStr =
            a.window_min < 1 ? `${Math.round(a.window_min * 60)}s` : `${a.window_min}m`
          const evalType = a.evaluation_type || 'absolute'
          if (evalType === 'absolute') {
            return (
              <span className="text-sm font-mono">
                {a.operator} {a.threshold} (last {windowStr})
              </span>
            )
          }
          if (evalType === 'anomaly_zscore') {
            const zThresh =
              a.zscore_threshold !== undefined ? a.zscore_threshold : a.threshold || 3.0
            const baseDays = a.baseline_period_days || 7
            return (
              <span
                className="text-sm font-mono flex items-center gap-1"
                title={`Z-Score > ${zThresh} vs rolling baseline over last ${baseDays} days`}
              >
                <span className="text-purple-600 dark:text-purple-400 font-bold">Z-Score</span>{' '}
                &gt; {zThresh}
                <span className="text-muted-foreground text-[10px]">
                  ({baseDays}d baseline)
                </span>
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
          if (
            (alert.metric === 'specific_status' ||
              alert.metric === 'specific_status_rate') &&
            alert.status_codes
          ) {
            alert.status_codes.forEach((code: number) =>
              params.append('filter_status', String(code)),
            )
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
    [
      togglingId,
      testingId,
      relative,
      full,
      abbr,
      toggleEnabled,
      testNotification,
      onEdit,
      onDelete,
      isAnalyst,
      activeServiceId,
    ],
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
