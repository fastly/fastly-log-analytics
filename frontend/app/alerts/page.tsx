'use client'

import React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { ReportShell } from '@/components/ReportShell'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { DataTable } from '@/components/DataTable/DataTable'
import {
  Bell,
  Plus,
  Trash2,
  AlertTriangle,
  Clock,
  Activity,
  Zap,
  BellPlus,
  Info,
  Loader2,
  Pencil
} from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { PlotlyChart } from '@/components/PlotlyChart'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { CHART_LAYOUT_DEFAULTS } from '@/lib/constants'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { ButtonGroup } from '@/components/ui/button-group'
import { useTimeLayout } from '@/lib/chart-helpers'
import { useTimezoneStore } from '@/stores/timezoneStore'

type Alert = components["schemas"]["Alert"]

export default function AlertsPage() {
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()
  const [isFormOpen, setIsFormOpen] = React.useState(false)
  const [editingAlert, setEditingAlert] = React.useState<Alert | null>(null)
  const [deleteTarget, setDeleteTarget] = React.useState<string | null>(null)
  const [isDeleting, setIsDeleting] = React.useState(false)
  const [togglingId, setTogglingId] = React.useState<string | null>(null)
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const { relative, full, abbr } = useDateFormat()

  const { data: loggingSettings } = useQuery({
    queryKey: ['loggingSettings', activeServiceId],
    queryFn: async ({ signal }) => {
      if (!activeServiceId) return null
      const { data } = await client.GET("/api/services/{service_id}/logging-settings", {
        signal,
        params: { path: { service_id: activeServiceId } }
      })
      return data as any
    },
    enabled: !!activeServiceId
  })

  const logPeriodSeconds = (loggingSettings as any)?.period || 30

  const { data: alertsRes, isLoading, isFetching, refetch } = useQuery({
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
    refetchInterval: logPeriodSeconds * 1000,
  })

  const alerts = alertsRes?.data || []
  const lastChecked = alertsRes?.evaluated_at || new Date().toISOString()

  const confirmDelete = React.useCallback(async () => {
    if (!deleteTarget) return
    setIsDeleting(true)
    try {
      await client.DELETE("/api/alerts/{alert_id}", {
        params: { path: { alert_id: deleteTarget } }
      })
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
    } finally {
      setIsDeleting(false)
      setDeleteTarget(null)
    }
  }, [deleteTarget, queryClient])

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

  const handleEdit = React.useCallback((alert: Alert) => {
    setEditingAlert(alert)
    setIsFormOpen(true)
  }, [])

  const handleCreate = React.useCallback(() => {
    setEditingAlert(null)
    setIsFormOpen(true)
  }, [])

  const availableColumns = React.useMemo(() => [
    { id: 'name', label: 'Alert Name' },
    { id: 'category', label: 'Category' },
    { id: 'metric', label: 'Metric' },
    { id: 'condition', label: 'Condition' },
    { id: 'last_triggered_at', label: 'Last Triggered' },
    { id: 'enabled', label: 'Enabled?' },
  ], [])

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
    {
      id: 'actions',
      header: '',
      cell: (info: any) => (
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 text-muted-foreground hover:text-primary"
            onClick={() => handleEdit(info.row.original)}
            title="Edit alert"
          >
            <Pencil className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 text-muted-foreground hover:text-destructive"
            onClick={() => setDeleteTarget(info.row.original.id)}
            title="Delete alert"
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      )
    }
  ], [togglingId, relative, full, abbr, toggleEnabled, handleEdit, activeServiceId])


  return (
    <ReportShell
      title="Alerts"
      headerActions={
        <div className="flex items-center gap-4">
          <Badge variant="secondary" className="gap-2 font-mono text-xs">
            <Clock className="w-3 h-3 text-muted-foreground" />
            {isFetching ? 'Refreshing...' : `Last Checked: ${full(lastChecked)}`}
          </Badge>
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching || isLoading}>
             <Loader2 className={`w-4 h-4 mr-2 ${isFetching ? 'animate-spin' : 'hidden'}`} />
            Refresh Now
          </Button>
          <Button onClick={handleCreate}>
            <Plus className="w-4 h-4 mr-2" />
            Create Alert
          </Button>
        </div>
      }
      description={
        <div className="flex items-center gap-2">
          <span>Configure threshold-based alerts and webhook notifications.</span>
          {activeServiceId && (
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
                    Alerts are evaluated dynamically based on your Fastly service's log batching period (currently <strong>{logPeriodSeconds} seconds</strong>).
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
          )}
        </div>
      }
      icon={Bell}
      requireService={false}
      isReadyOverride={true}
    >
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
      <AnalyticsCard
        title="Configured Alerts"
        icon={<Bell className="h-4 w-4" />}
        isLoading={isLoading}
        className="min-h-[300px]"
        contentClassName="p-0"
        headerAction={
          <ColumnVisibilityDropdown
            columns={availableColumns}
            visibility={columnVisibility}
            onChange={(id, visible) => setColumnVisibility(prev => ({ ...prev, [id]: visible }))}
          />
        }
      >
        <DataTable
          columns={columns}
          data={alerts || []}
          hideToolbar={true}
          columnVisibility={columnVisibility}
          onColumnVisibilityChange={setColumnVisibility}
        />
      </AnalyticsCard>

      <ConfirmDialog
        open={deleteTarget !== null}
        onOpenChange={(open) => { if (!open && !isDeleting) setDeleteTarget(null) }}
        onConfirm={confirmDelete}
        isPending={isDeleting}
        isDangerous
        title="Delete Alert"
        description="Are you sure you want to delete this alert? This action cannot be undone."
        confirmLabel="Delete"
      />

      {!activeServiceId && (
        <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20 text-yellow-600 dark:text-yellow-400 flex items-center gap-3 mt-6">
          <AlertTriangle className="h-5 w-5" />
          <p className="text-sm">
            Showing alerts for <strong>all services</strong>. Select a service to filter.
          </p>
        </div>
      )}
    </ReportShell>
  )
}

function CreateAlertForm({ initialAlert, onSuccess }: { initialAlert?: Alert | null, onSuccess: () => void }) {
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()
  const { data: catalog } = useLogFieldsCatalog()
  
  const [name, setName] = React.useState(initialAlert?.name || '')
  const [category, setCategory] = React.useState((initialAlert?.category as any) || 'traffic')
  const [metric, setMetric] = React.useState((initialAlert?.metric as any) || 'requests')
  const [evalType, setEvalType] = React.useState((initialAlert?.evaluation_type as any) || 'absolute')
  const [evalScope, setEvalScope] = React.useState((initialAlert?.evaluation_scope as any) || 'all')
  const [operator, setOperator] = React.useState(initialAlert?.operator || '>')
  const [threshold, setThreshold] = React.useState(initialAlert?.threshold?.toString() || '')
  const [windowMin, setWindowMin] = React.useState(initialAlert?.window_min?.toString() || '5')
  const [compPeriodMin, setCompPeriodMin] = React.useState(initialAlert?.comparison_period_min?.toString() || '60')
  const [statusCodesStr, setStatusCodesStr] = React.useState(initialAlert?.status_codes?.join(', ') || '')
  const [webhookUrl, setWebhookUrl] = React.useState(initialAlert?.webhook_url || '')
  const [isSaving, setIsSaving] = React.useState(false)
  const [previewData, setPreviewData] = React.useState<any>(null)
  const [isPreviewLoading, setIsPreviewLoading] = React.useState(false)
  const [lookbackHours, setLookbackHours] = React.useState(24)

  // Fetch preview data on change
  React.useEffect(() => {
    if (!activeServiceId) return

    const fetchPreview = async () => {
      setIsPreviewLoading(true)
      try {
        let parsedCodes: number[] | undefined = undefined
        if ((metric === 'specific_status' || metric === 'specific_status_rate') && statusCodesStr) {
          parsedCodes = statusCodesStr.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))
        }

        const { data } = await client.POST("/api/alerts/preview", {
          params: { query: { lookback_hours: lookbackHours } },
          body: {
            service_id: activeServiceId,
            name: 'Preview',
            category,
            metric,
            evaluation_type: evalType,
            evaluation_scope: evalScope,
            operator,
            threshold: parseFloat(threshold) || 0,
            window_min: parseFloat(windowMin),
            comparison_period_min: evalType !== 'absolute' ? parseFloat(compPeriodMin) : undefined,
            status_codes: parsedCodes,
            enabled: true
          }
        })
        if (data) {
          setPreviewData((data as any).data)
        }
      } catch (err) {
        console.error('Preview fetch failed', err)
      } finally {
        setIsPreviewLoading(false)
      }
    }

    const timer = setTimeout(fetchPreview, 500)
    return () => clearTimeout(timer)
  }, [activeServiceId, metric, category, evalType, evalScope, windowMin, compPeriodMin, statusCodesStr, threshold, lookbackHours])

  const metricField = React.useMemo(() => catalog?.fields?.find(f => f.id === metric), [catalog, metric])

  const { timezone } = useTimezoneStore()
  const startTime = React.useMemo(() => previewData?.times?.[0], [previewData])
  const endTime = React.useMemo(() => previewData?.times?.[previewData?.times?.length - 1], [previewData])
  const timeLayout = useTimeLayout(startTime, endTime, timezone)

  const getHoverTemplate = React.useCallback((m: string, label?: string) => {
    const pre = label ? `${label}: ` : ''
    const field = m === metric ? metricField : catalog?.fields?.find(f => f.id === m)
    const unit = field?.unit || ''
    const precision = field?.precision ?? (m === 'requests' ? 0 : 1)
    const format = precision > 0 ? `.${precision}f` : ','
    return `${pre}%{y:${format}}${unit}<extra></extra>`
  }, [catalog, metric, metricField])

  // Dynamic metrics based on category
  const metricsByCategory: Record<string, {value: string, label: string}[]> = {
    reliability: [
      { value: '5xx', label: '5xx Count' },
      { value: '5xx_rate', label: '5xx Rate (%)' },
      { value: '4xx', label: '4xx Count' },
      { value: '4xx_rate', label: '4xx Rate (%)' },
      { value: 'specific_status', label: 'Specific Status Codes' },
      { value: 'specific_status_rate', label: 'Specific Status Codes Rate (%)' },
    ],
    traffic: [
      { value: 'requests', label: 'Request Count' },
      { value: 'bandwidth', label: 'Bandwidth (Bytes)' },
    ],
    performance: [
      { value: 'p95_latency', label: 'Edge P95 Latency (ms)' },
      { value: 'ttfb', label: 'Origin TTFB (ms)' },
    ],
    caching: [
      { value: 'hit_rate', label: 'Cache Hit Rate (%)' },
    ]
  }

  // Handle category change -> reset metric
  const handleCategoryChange = (val: string | null) => {
    if (!val) return
    setCategory(val as any)
    setMetric(metricsByCategory[val][0].value as any)
  }

  // Handle eval type change -> reset operator
  const handleEvalTypeChange = (val: string | null) => {
    if (!val) return
    setEvalType(val as any)
    if (val !== 'absolute') {
      setOperator('>') // Relatives are usually increases
    }
  }

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!activeServiceId || !name || !threshold) return
    
    // Parse status codes
    let parsedCodes: number[] | undefined = undefined
    if ((metric === 'specific_status' || metric === 'specific_status_rate') && statusCodesStr) {
      parsedCodes = statusCodesStr.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))
    }
    
    setIsSaving(true)
    try {
      await client.POST("/api/alerts/", {
        body: {
          id: initialAlert?.id,
          service_id: activeServiceId,
          name,
          category,
          metric,
          evaluation_type: evalType,
          evaluation_scope: evalScope,
          operator,
          threshold: parseFloat(threshold),
          window_min: parseFloat(windowMin),
          comparison_period_min: evalType !== 'absolute' ? parseFloat(compPeriodMin) : undefined,
          status_codes: parsedCodes,
          webhook_url: webhookUrl || undefined,
          enabled: initialAlert ? initialAlert.enabled : true
        } as any
      })
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
      onSuccess()
    } catch (error) {
      console.error('Failed to create alert', error)
    } finally {
      setIsSaving(false)
    }
  }

  const LabelWithInfo = ({ htmlFor, children, tooltip }: { htmlFor?: string, children: React.ReactNode, tooltip: React.ReactNode }) => (
    <div className="flex items-center gap-1.5">
      <Label htmlFor={htmlFor}>{children}</Label>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger type="button" tabIndex={-1} className="text-muted-foreground hover:text-foreground">
            <Info className="h-3.5 w-3.5" />
          </TooltipTrigger>
          <TooltipContent className="max-w-[300px] text-xs">
            {tooltip}
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  )

  return (
    <form onSubmit={handleSave} className="flex flex-col overflow-hidden">
      <div className="grid md:grid-cols-2 gap-6 py-4 overflow-y-auto px-1 flex-1">
        {/* Left Column: Form Fields */}
        <div className="space-y-4 pr-2">
          <div className="grid gap-2">
            <LabelWithInfo htmlFor="alert-name" tooltip="A descriptive name for your alert, which will appear in notifications and the dashboard.">
              Alert Name
            </LabelWithInfo>
            <Input 
              id="alert-name" 
              placeholder="e.g. High 5xx Error Rate" 
              value={name}
              onChange={e => setName(e.target.value)}
              required
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2">
              <LabelWithInfo tooltip="Groups alerts logically. Does not affect evaluation logic.">
                Category
              </LabelWithInfo>
              <Select value={category} onValueChange={handleCategoryChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="reliability">Reliability (Errors)</SelectItem>
                  <SelectItem value="traffic">Traffic (Requests/BW)</SelectItem>
                  <SelectItem value="performance">Performance (Latency)</SelectItem>
                  <SelectItem value="caching">Caching</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <LabelWithInfo tooltip="The specific data point to measure. Rate metrics represent a percentage of total traffic.">
                Metric
              </LabelWithInfo>
              <Select value={metric} onValueChange={(v) => v && setMetric(v as any)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {metricsByCategory[category]?.map(m => (
                     <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          
          {(metric === 'specific_status' || metric === 'specific_status_rate') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
               <LabelWithInfo htmlFor="status-codes" tooltip="Enter one or more HTTP status codes (e.g., 503, 504) to match exactly against the log status field.">
                 HTTP Status Codes
               </LabelWithInfo>
               <Input
                 id="status-codes"
                 placeholder="e.g. 503, 504"
                 value={statusCodesStr}
                 onChange={e => setStatusCodesStr(e.target.value)}
                 required
               />
               <p className="text-[10px] text-muted-foreground">Comma-separated list of HTTP status codes to track.</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2">
              <LabelWithInfo tooltip="Restricts the alert to a specific traffic scope. 'Edge Only' filters for edge responses. 'Origin Only' filters for requests that went to your origin.">
                Evaluation Scope
              </LabelWithInfo>
              <Select value={evalScope} onValueChange={(v) => v && setEvalScope(v as any)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Requests</SelectItem>
                  <SelectItem value="edge">Edge Only</SelectItem>
                  <SelectItem value="origin">Origin Only</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <LabelWithInfo tooltip={<><b>Absolute</b> triggers if the value crosses a hard limit.<br/><br/><b>Relative</b> compares the current window to the <i>exact same duration</i> in the past (the baseline).</>}>
                Evaluation Type
              </LabelWithInfo>
              <Select value={evalType} onValueChange={handleEvalTypeChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="absolute">Absolute Threshold</SelectItem>
                  <SelectItem value="relative_increase">Relative Increase (%)</SelectItem>
                  <SelectItem value="relative_decrease">Relative Decrease (%)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          
          {evalType !== 'absolute' && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabelWithInfo tooltip="How far back to look for the baseline. If comparing the last 5m to 1 hour ago, it measures against the 5-minute window that ended 60 minutes ago.">
                Baseline Comparison Period
              </LabelWithInfo>
              <Select value={compPeriodMin} onValueChange={v => v && setCompPeriodMin(v)}>
                 <SelectTrigger>
                   <SelectValue />
                 </SelectTrigger>
                 <SelectContent>
                   <SelectItem value="10">10 minutes ago</SelectItem>
                   <SelectItem value="60">1 hour ago</SelectItem>
                   <SelectItem value="1440">1 day ago</SelectItem>
                   <SelectItem value="10080">1 week ago</SelectItem>
                 </SelectContent>
              </Select>
              <p className="text-[10px] text-muted-foreground">Alert will compare the current window to the exact same window this duration ago.</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4 border-t pt-4">
            <div className="grid gap-2">
              <LabelWithInfo tooltip="The mathematical condition to trigger the alert.">
                Operator
              </LabelWithInfo>
              <Select value={operator} onValueChange={(v) => v && setOperator(v)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value=">">{'>'}</SelectItem>
                  <SelectItem value="<">{'<'}</SelectItem>
                  <SelectItem value=">=">{'>='}</SelectItem>
                  <SelectItem value="<=">{'<='}</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <LabelWithInfo htmlFor="threshold" tooltip="The numeric value to breach. For rate/relative metrics, this is a percentage.">
                Threshold {evalType !== 'absolute' || metric.endsWith('_rate') ? '(%)' : ''}
              </LabelWithInfo>
              <Input 
                id="threshold" 
                type="number" 
                step="any"
                placeholder={evalType !== 'absolute' ? "e.g. 50 (for 50% increase)" : "e.g. 100"} 
                value={threshold}
                onChange={e => setThreshold(e.target.value)}
                required
              />
            </div>
          </div>

          <div className="grid gap-2">
            <LabelWithInfo htmlFor="window" tooltip="The length of time to aggregate data over before evaluating the threshold. A longer window prevents flapping on brief spikes.">
              Evaluation Window
            </LabelWithInfo>
            <Select value={windowMin} onValueChange={(v) => v && setWindowMin(v)}>
              <SelectTrigger id="window">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="0.5">Last 30 seconds</SelectItem>
                <SelectItem value="1">Last 1 minute</SelectItem>
                <SelectItem value="5">Last 5 minutes</SelectItem>
                <SelectItem value="15">Last 15 minutes</SelectItem>
                <SelectItem value="60">Last 1 hour</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="grid gap-2 border-t pt-4">
            <LabelWithInfo htmlFor="webhook" tooltip="An endpoint to receive an HTTP POST when the alert triggers. Supported natively by Slack, Teams, and Discord.">
              Webhook URL (Optional)
            </LabelWithInfo>
            <Input 
              id="webhook" 
              placeholder="https://hooks.slack.com/services/..." 
              value={webhookUrl}
              onChange={e => setWebhookUrl(e.target.value)}
            />
            <p className="text-[10px] text-muted-foreground italic">
              A JSON POST with a 'text' field will be sent to this URL when triggered.
            </p>
          </div>
        </div>

        {/* Right Column: Live Chart Preview */}
        <div className="flex flex-col min-h-[300px]">
          <div className="flex items-center justify-between mb-2">
            <Label>Live Preview</Label>
            <ButtonGroup>
              {[1, 3, 6, 12, 24].map(h => (
                <Button
                  key={h}
                  type="button"
                  variant={lookbackHours === h ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => setLookbackHours(h)}
                  className={`h-6 text-[10px] px-2 shadow-none transition-colors ${lookbackHours === h ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'hover:text-primary hover:bg-muted'}`}
                >
                  {h}h
                </Button>
              ))}
            </ButtonGroup>
          </div>
          <div className="flex-1 border border-border/50 rounded-md p-4 bg-muted/10 relative flex flex-col">
             {isPreviewLoading && (
               <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/50 rounded-md">
                  <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
               </div>
             )}
             {previewData && previewData.times && previewData.times.length > 0 ? (
               <div className="flex-1 w-full relative">
                  <PlotlyChart
                    data={[
                      {
                         x: previewData.times,
                         y: previewData.values,
                         type: (metric === 'requests' || metric === '5xx' || metric === '4xx' || metric === 'specific_status') ? 'bar' : 'scatter',
                         mode: (metric === 'requests' || metric === '5xx' || metric === '4xx' || metric === 'specific_status') ? undefined : 'lines+markers',
                         name: 'Current',
                         marker: { color: '#3b82f6' },
                         line: { color: '#3b82f6', width: 2 },
                         hovertemplate: getHoverTemplate(metric, 'Current')
                      },
                      ...(previewData.type === 'relative' && previewData.hist_values ? [{
                         x: previewData.times,
                         y: previewData.hist_values,
                         type: 'scatter',
                         mode: 'lines',
                         name: 'Baseline',
                         line: { color: '#a1a1aa', width: 2, dash: 'dot' },
                         hovertemplate: getHoverTemplate(metric, 'Baseline')
                      }] : []),
                      // If absolute, overlay the threshold as a horizontal line
                      ...(previewData.type === 'absolute' && parseFloat(threshold) ? [{
                         x: [previewData.times[0], previewData.times[previewData.times.length - 1]],
                         y: [parseFloat(threshold), parseFloat(threshold)],
                         type: 'scatter',
                         mode: 'lines',
                         name: 'Threshold',
                         line: { color: 'hsl(var(--destructive))', width: 2, dash: 'dash' },
                         hoverinfo: 'none'
                      }] : []),
                      // If relative, overlay the calculated threshold line
                      ...(previewData.type === 'relative' && previewData.hist_values && parseFloat(threshold) ? [{
                        x: previewData.times,
                        y: previewData.hist_values.map((v: number) => {
                          const t = parseFloat(threshold)
                          return evalType === 'relative_increase' ? v * (1 + t/100) : v * (1 - t/100)
                        }),
                        type: 'scatter',
                        mode: 'lines',
                        name: 'Threshold',
                        line: { color: 'hsl(var(--destructive))', width: 2, dash: 'dash' },
                        hoverinfo: 'none'
                     }] : [])
                    ]}
                    layout={{
                      ...timeLayout,
                      margin: { t: 10, r: 10, l: 40, b: 30 },
                      paper_bgcolor: 'transparent',
                      plot_bgcolor: 'transparent',
                      xaxis: { 
                         ...timeLayout.xaxis,
                         showgrid: false,
                         zeroline: false
                      },
                      yaxis: { 
                         title: metricField?.unit || (metric === 'requests' ? 'reqs' : ''),
                         ticksuffix: metricField?.unit || '',
                         separatethousands: true,
                         exponentformat: 'none',
                         showgrid: true,
                         gridcolor: 'hsl(var(--border))',
                         zeroline: false
                      },
                      dragmode: false
                    }}
                    config={{ displayModeBar: false }}
                  />
               </div>
             ) : (
               <div className="flex-1 flex flex-col items-center justify-center text-sm text-muted-foreground">
                 <Bell className="w-8 h-8 mb-2 opacity-20" />
                 <p>No data available for preview.</p>
                 <p className="text-xs opacity-60 mt-1">Adjust metric or window to see data.</p>
               </div>
             )}
          </div>
        </div>
      </div>

      <DialogFooter className="pt-4 mt-auto border-t">
        <Button type="button" variant="outline" onClick={onSuccess}>Cancel</Button>
        <Button type="submit" disabled={isSaving}>
          {isSaving ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
          {initialAlert ? 'Save Changes' : 'Create Alert'}
        </Button>
      </DialogFooter>
    </form>
  )
}
