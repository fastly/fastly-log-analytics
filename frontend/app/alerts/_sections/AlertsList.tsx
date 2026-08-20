'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { DataTable } from '@/components/DataTable/DataTable'
import {
  AlertTriangle,
  Activity,
  Trash2,
  Pencil,
} from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'

type Alert = components["schemas"]["Alert"]

export const ALERTS_AVAILABLE_COLUMNS = [
  { id: 'name', label: 'Alert Name' },
  { id: 'category', label: 'Category' },
  { id: 'metric', label: 'Metric' },
  { id: 'condition', label: 'Condition' },
  { id: 'last_triggered_at', label: 'Last Triggered' },
  { id: 'enabled', label: 'Enabled?' },
]

interface AlertsListProps {
  alerts: Alert[]
  columnVisibility: VisibilityState
  setColumnVisibility: React.Dispatch<React.SetStateAction<VisibilityState>>
  onEdit: (alert: Alert) => void
  onDelete: (alertId: string) => void
  /**
   * When true, hide mutate controls (toggle, edit, delete) — backend
   * gates the underlying PATCH/DELETE endpoints on the same role and any
   * click would silently 403. Passed from AlertsPage.
   */
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
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const [togglingId, setTogglingId] = React.useState<string | null>(null)
  const { relative, full, abbr } = useDateFormat()

  const toggleEnabled = React.useCallback(async (alert: Alert, newEnabled: boolean) => {
    const queryKey = ['alerts', activeServiceId]

    // Cancel any in-flight refetches so they don't overwrite the optimistic update
    await queryClient.cancelQueries({ queryKey })

    const previous = queryClient.getQueryData(queryKey)
    queryClient.setQueryData(queryKey, (old: any) => ({
      ...old,
      data: old?.data?.map((a: Alert) =>
        a.id === alert.id ? { ...a, enabled: newEnabled } : a
      ),
    }))

    setTogglingId(alert.id!)
    try {
      await client.PATCH("/api/alerts/{alert_id}/enabled", {
        params: { path: { alert_id: alert.id! } },
        body: { enabled: newEnabled }
      })
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
    } catch (err) {
      console.error('Failed to toggle alert', err)
      queryClient.setQueryData(queryKey, previous)
    } finally {
      setTogglingId(null)
    }
  }, [activeServiceId, queryClient])

  const columns = React.useMemo(() => [
    {
      accessorKey: 'name',
      header: 'Alert Name',
      cell: (info: any) => <span className="font-medium">{info.getValue()}</span>
    },
    {
      accessorKey: 'category',
      header: 'Category',
      cell: (info: any) => (
        <Badge variant="secondary" className="capitalize">
          {info.getValue()?.replace('_', ' ') || 'Reliability'}
        </Badge>
      )
    },
    {
      accessorKey: 'metric',
      header: 'Metric',
      cell: (info: any) => {
        const val = info.getValue()
        const codes = info.row.original.status_codes
        const scope = info.row.original.evaluation_scope
        let display = val.replace(/_/g, ' ')
        if (val === 'specific_status' && codes) {
           display = `Status ${codes.join(', ')}`
        } else if (val === 'specific_status_rate' && codes) {
           display = `Status ${codes.join(', ')} Rate`
        }

        let scopeBadge = null
        if (scope === 'edge') {
          scopeBadge = <Badge variant="outline" className="ml-2 text-[10px] h-4 px-1 py-0 font-normal">Edge</Badge>
        } else if (scope === 'origin') {
          scopeBadge = <Badge variant="outline" className="ml-2 text-[10px] h-4 px-1 py-0 font-normal border-orange-500/50 text-orange-600 dark:text-orange-400">Origin</Badge>
        }

        return (
          <div className="flex items-center">
            <span className="capitalize text-sm font-medium">{display}</span>
            {scopeBadge}
          </div>
        )
      }
    },
    {
      id: 'condition',
      header: 'Condition',
      cell: (info: any) => {
        const a = info.row.original
        const windowStr = a.window_min < 1 ? `${Math.round(a.window_min * 60)}s` : `${a.window_min}m`
        const evalType = a.evaluation_type || 'absolute'

        if (evalType === 'absolute') {
          return (
            <span className="text-sm font-mono">
              {a.operator} {a.threshold} (last {windowStr})
            </span>
          )
        } else if (evalType === 'anomaly_zscore') {
          const zThresh = a.zscore_threshold !== undefined ? a.zscore_threshold : (a.threshold || 3.0)
          const baseDays = a.baseline_period_days || 7
          return (
            <span className="text-sm font-mono flex items-center gap-1" title={`Z-Score > ${zThresh} vs rolling baseline over last ${baseDays} days`}>
              <span className="text-purple-600 dark:text-purple-400 font-bold">Z-Score</span> &gt; {zThresh}
              <span className="text-muted-foreground text-[10px]">({baseDays}d baseline)</span>
            </span>
          )
        } else {
          const isIncrease = evalType === 'relative_increase'
          const compStr = a.comparison_period_min ? (a.comparison_period_min >= 1440 ? `${a.comparison_period_min/1440}d` : `${a.comparison_period_min >= 60 ? a.comparison_period_min/60 + 'h' : a.comparison_period_min + 'm'}`) : '?'
          return (
            <span className="text-sm font-mono flex items-center gap-1">
              {isIncrease ? '↑' : '↓'} &gt; {a.threshold}%
              <span className="text-muted-foreground text-[10px]"> vs {compStr} ago</span>
            </span>
          )
        }
      }
    },
    {
      accessorKey: 'last_triggered_at',
      header: 'Last Triggered',
      cell: (info: any) => {
        const val = info.getValue()
        if (!val) return <span className="text-muted-foreground text-xs italic">Never</span>
        const alert = info.row.original

        // Build the dashboard link
        const params = new URLSearchParams()
        const end = new Date(val)
        const start = new Date(end.getTime() - alert.window_min * 60 * 1000)

        params.set('start_time', start.toISOString())
        params.set('end_time', end.toISOString())

        // Map alert metric to dashboard metric
        let dashboardMetric = alert.metric
        if (alert.metric === '5xx_rate') dashboardMetric = '5xx'
        if (alert.metric === '4xx_rate') dashboardMetric = '4xx'
        if (alert.metric === 'specific_status_rate') dashboardMetric = 'requests'
        if (alert.metric === 'bandwidth') dashboardMetric = 'throughput'
        if (alert.metric === 'ttfb') dashboardMetric = 'ttfb_client'

        params.set('metric', dashboardMetric)

        if ((alert.metric === 'specific_status' || alert.metric === 'specific_status_rate') && alert.status_codes) {
          alert.status_codes.forEach((code: number) => {
            params.append('filter_status', String(code))
          })
        }

        if (alert.evaluation_scope === 'edge') {
          params.append('filter_edge', 'true')
        } else if (alert.evaluation_scope === 'origin') {
          params.append('filter_edge', 'false')
        }

        if (activeServiceId) params.set('service', activeServiceId)
        const dashboardLink = `/dashboard?${params.toString()}`

        return (
          <div className="flex flex-col gap-1">
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger render={
                  <div className="flex flex-col ">
                    <span className="text-xs text-red-500 font-bold flex items-center gap-1">
                      <AlertTriangle className="h-3 w-3" />
                      {relative(val)}
                    </span>
                  </div>
                } />
                <TooltipContent className="text-xs">
                  {full(val)} {abbr()}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <a
              href={dashboardLink}
              target="_blank"
              rel="noopener noreferrer"
              className="text-[10px] text-primary hover:underline flex items-center gap-1 w-fit"
            >
              <Activity className="h-3 w-3" />
              View on Dashboard
            </a>
          </div>
        )
      }
    },
    {
      accessorKey: 'enabled',
      header: 'Enabled?',
      cell: (info: any) => {
        const isPending = togglingId === info.row.original.id
        // Analysts get a read-only display: the PATCH /enabled endpoint
        // 403s for them, so showing an active Switch invites a click
        // that does nothing.
        if (isAnalyst) {
          return (
            <Badge variant={info.getValue() ? 'secondary' : 'outline'} className="text-[10px]">
              {info.getValue() ? 'On' : 'Off'}
            </Badge>
          )
        }
        return (
          <Switch
            checked={info.getValue()}
            onCheckedChange={(checked) => toggleEnabled(info.row.original, checked)}
            disabled={isPending}
            className={isPending ? 'opacity-50 cursor-wait' : undefined}
          />
        )
      }
    },
    ...(isAnalyst
      ? []
      : [{
        id: 'actions',
        header: '',
        cell: (info: any) => (
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              aria-label="Edit alert"
              className="h-8 w-8 text-muted-foreground hover:text-primary"
              onClick={() => onEdit(info.row.original)}
              title="Edit alert"
            >
              <Pencil className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              aria-label="Delete alert"
              className="h-8 w-8 text-muted-foreground hover:text-destructive"
              onClick={() => onDelete(info.row.original.id)}
              title="Delete alert"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        )
      }])
  ], [togglingId, relative, full, abbr, toggleEnabled, onEdit, onDelete, isAnalyst])

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
