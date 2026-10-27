'use client'
import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useEffectiveServiceId, useBootstrapResolved, useIsAnalyst, useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { useDateFormat, useDeferredDeleteWithUndo } from '@/hooks/useSSE'
import { useDebounce } from '@/hooks/useDashboardCards'
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { ReportShell } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Activity, AlertTriangle, Bell, BellOff, BellPlus, CheckCircle2, Clock, Info, Loader2, Pencil, Play, Plus, Send, Trash2, Zap } from 'lucide-react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ConfirmDialog, LabelWithInfo } from '@/components/ui/confirm-dialog'
import { PlotlyChart } from '@/components/PlotlyChart'
import { showToast, CHART_COLORS, useTimeLayout, formatDate, keyByOccurrence, oneOf } from '@/lib/utils'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import type { ColumnDef, VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'

type Alert = components['schemas']['Alert']
type AlertChannel = components['schemas']['AlertChannel']
type AlertPreviewData = components['schemas']['AlertPreviewData']
type Category = Alert['category']
type Metric = Alert['metric']
type EvalType = Alert['evaluation_type']
type EvalScope = Alert['evaluation_scope']
type Operator = Alert['operator']
type ChannelType = AlertChannel['type']

const ALERT_STATUS_FILTERS = ['all', 'alerting', 'ok', 'muted'] as const
const alertStatusParser = parseAsStringLiteral(ALERT_STATUS_FILTERS).withDefault('all')

const ALERTS_AVAILABLE_COLUMNS = [
  { id: 'name', label: 'Alert Name' }, { id: 'category', label: 'Category' }, { id: 'metric', label: 'Metric' },
  { id: 'condition', label: 'Condition' }, { id: 'last_triggered_at', label: 'Last Triggered' }, { id: 'enabled', label: 'Enabled?' },
]

const METRIC_TO_DASHBOARD: Record<string, string> = {
  '5xx_rate': '5xx', '4xx_rate': '4xx', specific_status_rate: 'requests', bandwidth: 'throughput', ttfb: 'ttfb_client',
}

const CATEGORIES: readonly Category[] = ['reliability', 'performance', 'traffic', 'caching']
const EVAL_TYPES: readonly EvalType[] = ['absolute', 'relative_increase', 'relative_decrease', 'anomaly_zscore']
const EVAL_SCOPES: readonly EvalScope[] = ['all', 'edge', 'origin']
const OPERATORS: readonly Operator[] = ['>', '<', '>=', '<=']
const CHANNEL_TYPES: readonly ChannelType[] = ['slack', 'pagerduty', 'webhook']

const METRICS_BY_CATEGORY: Record<Category, { value: Metric; label: string }[]> = {
  reliability: [
    { value: '5xx', label: '5xx Count' }, { value: '5xx_rate', label: '5xx Rate (%)' },
    { value: '4xx', label: '4xx Count' }, { value: '4xx_rate', label: '4xx Rate (%)' },
    { value: 'specific_status', label: 'Specific Status Codes' }, { value: 'specific_status_rate', label: 'Specific Status Codes Rate (%)' },
  ],
  traffic: [{ value: 'requests', label: 'Request Count' }, { value: 'bandwidth', label: 'Bandwidth (Bytes)' }],
  performance: [{ value: 'p95_latency', label: 'Edge P95 Latency (ms)' }, { value: 'ttfb', label: 'Origin TTFB (ms)' }],
  caching: [{ value: 'hit_rate', label: 'Cache Hit Rate (%)' }],
}
const ALL_METRICS: readonly Metric[] = Object.values(METRICS_BY_CATEGORY).flatMap((ms) => ms.map((m) => m.value))

const alertFormSchema = z.object({
  name: z.string(), category: z.custom<Category>(), metric: z.custom<Metric>(), evalType: z.custom<EvalType>(),
  evalScope: z.custom<EvalScope>(), operator: z.custom<Operator>(), threshold: z.string(), windowMin: z.string(),
  compPeriodMin: z.string(), statusCodesStr: z.string(), webhookUrl: z.string(), zscoreThreshold: z.string(), baselinePeriodDays: z.string(),
}).superRefine((v, ctx) => {
  if (v.name.trim() === '') ctx.addIssue({ code: 'custom', path: ['name'], message: 'Name is required.' })
  const thresholdField = v.evalType === 'anomaly_zscore' ? 'zscoreThreshold' : 'threshold'
  if (v[thresholdField].trim() === '') ctx.addIssue({ code: 'custom', path: [thresholdField], message: 'Threshold is required.' })
})

type AlertFormValues = z.infer<typeof alertFormSchema>

interface AlertPreviewProps {
  previewData: AlertPreviewData | null; isPreviewLoading: boolean; lookbackHours: number
  setLookbackHours: (h: number) => void; metric: string; evalType: string; threshold: string
}

export function AlertPreview({ previewData, isPreviewLoading, lookbackHours, setLookbackHours, metric, evalType, threshold }: AlertPreviewProps) {
  const { data: catalog } = useLogFieldsCatalog()
  const metricField = React.useMemo(() => catalog?.fields?.find((f) => f.id === metric), [catalog, metric])
  const { timezone } = useTimezoneStore()
  const startTime = React.useMemo(() => previewData?.times?.[0], [previewData])
  const endTime = React.useMemo(() => previewData?.times?.[previewData?.times?.length - 1], [previewData])
  const timeLayout = useTimeLayout(startTime, endTime, timezone)
  const x = React.useMemo(() => (previewData?.times || []).map((t: string) => formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')), [previewData, timezone])

  const getHoverTemplate = React.useCallback((m: string, label?: string) => {
    const pre = label ? `${label}: ` : ''
    const field = m === metric ? metricField : catalog?.fields?.find((f) => f.id === m)
    const unit = field?.unit || ''
    const precision = field?.precision ?? (m === 'requests' ? 0 : 1)
    const format = precision > 0 ? `.${precision}f` : ','
    return `${pre}%{y:${format}}${unit}<extra></extra>`
  }, [catalog, metric, metricField])

  const isBarMetric = metric === 'requests' || metric === '5xx' || metric === '4xx' || metric === 'specific_status'
  const lineTrace = (name: string, y: number[], color: string, dash: string, hover?: string, xs = x) => ({
    x: xs, y, type: 'scatter' as const, mode: 'lines' as const, name, line: { color, width: 2, dash },
    ...(hover ? { hovertemplate: hover } : { hoverinfo: 'none' as const }),
  })

  return (
    <div className="flex flex-col min-h-[300px]" data-testid="alert-preview">
      <div className="flex items-center justify-between mb-2">
        <Label>Live Preview</Label>
        <ButtonGroup>
          {[1, 3, 6, 12, 24].map((h) => (
            <Button
              key={h} type="button" variant={lookbackHours === h ? 'default' : 'ghost'} size="sm" onClick={() => setLookbackHours(h)}
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
                  x, y: previewData.values, type: isBarMetric ? 'bar' : 'scatter', mode: isBarMetric ? undefined : 'lines+markers',
                  name: 'Current', marker: { color: CHART_COLORS.info }, line: { color: CHART_COLORS.info, width: 2 },
                  hovertemplate: getHoverTemplate(metric, 'Current'),
                },
                ...(previewData.type === 'relative' && previewData.hist_values
                  ? [lineTrace('Baseline', previewData.hist_values, '#a1a1aa', 'dot', getHoverTemplate(metric, 'Baseline'))]
                  : []),
                ...(previewData.type === 'anomaly_zscore' && previewData.hist_values
                  ? [lineTrace('Baseline Mean', previewData.hist_values, '#a1a1aa', 'dot', getHoverTemplate(metric, 'Baseline Mean'))]
                  : []),
                ...(previewData.type === 'anomaly_zscore' && previewData.threshold_values
                  ? [lineTrace(`Threshold (${threshold}σ)`, previewData.threshold_values, 'hsl(var(--destructive))', 'dash', getHoverTemplate(metric, 'Threshold'))]
                  : []),
                ...(previewData.type === 'absolute' && parseFloat(threshold)
                  ? [lineTrace('Threshold', [parseFloat(threshold), parseFloat(threshold)], 'hsl(var(--destructive))', 'dash', undefined, [x[0], x[x.length - 1]])]
                  : []),
                ...(previewData.type === 'relative' && previewData.hist_values && parseFloat(threshold)
                  ? [lineTrace('Threshold', previewData.hist_values.map((v: number) => {
                      const t = parseFloat(threshold)
                      return evalType === 'relative_increase' ? v * (1 + t / 100) : v * (1 - t / 100)
                    }), 'hsl(var(--destructive))', 'dash')]
                  : []),
              ]}
              layout={{
                ...timeLayout, margin: { t: 10, r: 10, l: 40, b: 30 }, paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
                xaxis: { ...timeLayout.xaxis, showgrid: false, zeroline: false, fixedrange: true },
                yaxis: {
                  title: metricField?.unit || (metric === 'requests' ? 'reqs' : ''), ticksuffix: metricField?.unit || '',
                  separatethousands: true, exponentformat: 'none', showgrid: true, gridcolor: 'hsl(var(--border))', zeroline: false,
                },
                dragmode: false,
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
  )
}

function LabeledSelect({ htmlFor, label, info, value, onValueChange, options }: {
  htmlFor?: string; label: string; info: React.ReactNode; value: string
  onValueChange: (val: string | null) => void; options: readonly { value: string; label: string }[]
}) {
  return (
    <div className="grid gap-2">
      <LabelWithInfo htmlFor={htmlFor} label={label} info={info} />
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger id={htmlFor}><SelectValue /></SelectTrigger>
        <SelectContent>{options.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}</SelectContent>
      </Select>
    </div>
  )
}

const CATEGORY_OPTIONS = [
  { value: 'reliability', label: 'Reliability (Errors)' }, { value: 'traffic', label: 'Traffic (Requests/BW)' },
  { value: 'performance', label: 'Performance (Latency)' }, { value: 'caching', label: 'Caching' },
] as const
const SCOPE_OPTIONS = [{ value: 'all', label: 'All Requests' }, { value: 'edge', label: 'Edge Only' }, { value: 'origin', label: 'Origin Only' }] as const
const EVAL_TYPE_OPTIONS = [
  { value: 'absolute', label: 'Absolute Threshold' }, { value: 'relative_increase', label: 'Relative Increase (%)' },
  { value: 'relative_decrease', label: 'Relative Decrease (%)' }, { value: 'anomaly_zscore', label: 'Anomaly Detection (Z-Score rolling baseline)' },
] as const
const COMP_PERIOD_OPTIONS = [
  { value: '10', label: '10 minutes ago' }, { value: '60', label: '1 hour ago' },
  { value: '1440', label: '1 day ago' }, { value: '10080', label: '1 week ago' },
] as const
const BASELINE_DAYS_OPTIONS = [{ value: '7', label: '7 Days' }, { value: '14', label: '14 Days' }, { value: '30', label: '30 Days' }] as const
const OPERATOR_OPTIONS = OPERATORS.map((op) => ({ value: op, label: op }))
const WINDOW_OPTIONS = [
  { value: '0.5', label: 'Last 30 seconds' }, { value: '1', label: 'Last 1 minute' },
  { value: '5', label: 'Last 5 minutes' }, { value: '15', label: 'Last 15 minutes' }, { value: '60', label: 'Last 1 hour' },
] as const

function parseStatusCodes(metric: string, raw: string): number[] | undefined {
  if ((metric === 'specific_status' || metric === 'specific_status_rate') && raw) {
    return raw.split(',').map((s) => parseInt(s.trim())).filter((n) => !isNaN(n))
  }
  return undefined
}

export function CreateAlertForm({ initialAlert, onSuccess }: { initialAlert?: Alert | null; onSuccess: () => void }) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()

  const { register, handleSubmit, setValue, control, formState: { errors, isValid } } = useForm<AlertFormValues>({
    resolver: zodResolver(alertFormSchema),
    mode: 'onTouched',
    defaultValues: {
      name: initialAlert?.name || '', category: initialAlert?.category ?? 'traffic', metric: initialAlert?.metric ?? 'requests',
      evalType: initialAlert?.evaluation_type ?? 'absolute', evalScope: initialAlert?.evaluation_scope ?? 'all',
      operator: initialAlert?.operator ?? '>', threshold: initialAlert?.threshold?.toString() || '',
      windowMin: initialAlert?.window_min?.toString() || '5', compPeriodMin: initialAlert?.comparison_period_min?.toString() || '60',
      statusCodesStr: initialAlert?.status_codes?.join(', ') || '', webhookUrl: initialAlert?.webhook_url || '',
      zscoreThreshold: initialAlert?.zscore_threshold?.toString() || '3.0', baselinePeriodDays: initialAlert?.baseline_period_days?.toString() || '7',
    },
  })
  const {
    name, category, metric, evalType, evalScope, operator, threshold,
    windowMin, compPeriodMin, statusCodesStr, webhookUrl, zscoreThreshold, baselinePeriodDays,
  } = useWatch({ control }) as AlertFormValues
  const [channels, setChannels] = React.useState<AlertChannel[]>(initialAlert?.channels || [])
  const [newChannelType, setNewChannelType] = React.useState<ChannelType>('slack')
  const [newChannelUrl, setNewChannelUrl] = React.useState('')
  const [lookbackHours, setLookbackHours] = React.useState(24)

  const nameError = errors.name?.message ?? null
  const thresholdError = errors.threshold?.message ?? null

  const handleAddChannel = () => {
    if (!newChannelUrl.trim()) return
    setChannels([...channels, { type: newChannelType, url: newChannelUrl.trim() }])
    setNewChannelUrl('')
  }
  const handleRemoveChannel = (channel: AlertChannel) => setChannels(channels.filter((c) => c !== channel))

  const previewParams = React.useMemo(
    () => ({
      activeServiceId, metric, category, evalType, evalScope, operator,
      windowMin, compPeriodMin, statusCodesStr, threshold, zscoreThreshold, baselinePeriodDays, lookbackHours,
    }),
    [activeServiceId, metric, category, evalType, evalScope, operator, windowMin, compPeriodMin, statusCodesStr, threshold, zscoreThreshold, baselinePeriodDays, lookbackHours],
  )
  const debouncedPreviewParams = useDebounce(previewParams, 500)

  const previewQuery = useQuery<AlertPreviewData | null>({
    queryKey: ['alert-preview', debouncedPreviewParams],
    enabled: Boolean(debouncedPreviewParams.activeServiceId) && !isAnalyst,
    retry: false,
    queryFn: async () => {
      const p = debouncedPreviewParams
      if (!p.activeServiceId) return null
      const isZ = p.evalType === 'anomaly_zscore'
      const data = await unwrapClient(client.POST('/api/alerts/preview', {
        params: { query: { lookback_hours: p.lookbackHours } },
        body: {
          service_id: p.activeServiceId, name: 'Preview', category: p.category, metric: p.metric,
          evaluation_type: p.evalType, evaluation_scope: p.evalScope, operator: isZ ? '>' : p.operator,
          threshold: isZ ? (parseFloat(p.zscoreThreshold) || 3.0) : (parseFloat(p.threshold) || 0),
          window_min: parseFloat(p.windowMin),
          comparison_period_min: (p.evalType !== 'absolute' && !isZ) ? parseFloat(p.compPeriodMin) : undefined,
          status_codes: parseStatusCodes(p.metric, p.statusCodesStr), enabled: true,
          zscore_threshold: isZ ? (parseFloat(p.zscoreThreshold) || 3.0) : undefined,
          baseline_period_days: isZ ? (parseInt(p.baselinePeriodDays) || 7) : undefined,
        },
      }))
      return data?.data ?? null
    },
  })

  const handleCategoryChange = (val: string | null) => {
    const next = val ? oneOf(CATEGORIES, val) : undefined
    if (!next) return
    setValue('category', next)
    setValue('metric', METRICS_BY_CATEGORY[next][0].value)
  }

  const handleEvalTypeChange = (val: string | null) => {
    const next = val ? oneOf(EVAL_TYPES, val) : undefined
    if (!next) return
    setValue('evalType', next, { shouldValidate: true })
    if (val === 'anomaly_zscore' || val !== 'absolute') setValue('operator', '>')
  }

  const saveMutation = useMutation({
    mutationFn: async (payload: Alert) => {
      if (initialAlert?.id) {
        await unwrapClient(client.PUT('/api/alerts/{alert_id}', { params: { path: { alert_id: initialAlert.id } }, body: payload }))
      } else {
        await unwrapClient(client.POST('/api/alerts/', { body: payload }))
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
      onSuccess()
    },
    onError: (error) => {
      console.error('Failed to create alert', error)
      showToast(extractApiError(error) || 'Failed to save alert', 'error')
    },
  })
  const isSaving = saveMutation.isPending

  const handleSave = handleSubmit(async () => {
    if (!activeServiceId) return
    const isZ = evalType === 'anomaly_zscore'
    const payload: Alert = {
      id: initialAlert?.id, service_id: activeServiceId, name, category, metric,
      evaluation_type: evalType, evaluation_scope: evalScope, operator: isZ ? '>' : operator,
      threshold: isZ ? parseFloat(zscoreThreshold) : parseFloat(threshold),
      window_min: parseFloat(windowMin),
      comparison_period_min: (evalType !== 'absolute' && !isZ) ? parseFloat(compPeriodMin) : undefined,
      status_codes: parseStatusCodes(metric, statusCodesStr), webhook_url: webhookUrl || undefined, channels,
      zscore_threshold: isZ ? parseFloat(zscoreThreshold) : undefined,
      baseline_period_days: isZ ? parseInt(baselinePeriodDays) : undefined,
      enabled: initialAlert ? initialAlert.enabled : true,
    }
    await saveMutation.mutateAsync(payload).catch(() => {})
  })

  return (
    <form onSubmit={handleSave} className="flex flex-col overflow-hidden">
      <div className="grid md:grid-cols-2 gap-6 py-4 overflow-y-auto px-1 flex-1">
        <div className="space-y-4 pr-2">
          <div className="grid gap-2">
            <LabelWithInfo htmlFor="alert-name" label="Alert Name" info="A descriptive name for your alert, which will appear in notifications and the dashboard." />
            <Input
              id="alert-name" placeholder="e.g. High 5xx Error Rate" {...register('name')}
              aria-invalid={nameError !== null} aria-describedby={nameError ? 'alert-name-error' : undefined} required
            />
            {nameError && <p id="alert-name-error" role="alert" className="text-[11px] text-destructive">{nameError}</p>}
          </div>

          <div className="grid grid-cols-2 gap-4">
            <LabeledSelect label="Category" info="Groups alerts logically. Does not affect evaluation logic." value={category} onValueChange={handleCategoryChange} options={CATEGORY_OPTIONS} />
            <LabeledSelect
              label="Metric" info="The specific data point to measure. Rate metrics represent a percentage of total traffic." value={metric}
              onValueChange={(v) => { const m = v ? oneOf(ALL_METRICS, v) : undefined; if (m) setValue('metric', m) }} options={METRICS_BY_CATEGORY[category]}
            />
          </div>

          {(metric === 'specific_status' || metric === 'specific_status_rate') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabelWithInfo htmlFor="status-codes" label="HTTP Status Codes" info="Enter one or more HTTP status codes (e.g., 503, 504) to match exactly against the log status field." />
              <Input id="status-codes" placeholder="e.g. 503, 504" {...register('statusCodesStr')} required />
              <p className="text-[10px] text-muted-foreground">Comma-separated list of HTTP status codes to track.</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <LabeledSelect
              label="Evaluation Scope"
              info="Restricts the alert to a specific traffic scope. 'Edge Only' filters for edge responses. 'Origin Only' filters for requests that went to your origin."
              value={evalScope} onValueChange={(v) => { const sc = v ? oneOf(EVAL_SCOPES, v) : undefined; if (sc) setValue('evalScope', sc) }} options={SCOPE_OPTIONS}
            />
            <LabeledSelect
              label="Evaluation Type"
              info={<><b>Absolute</b> triggers if the value crosses a hard limit.<br /><br /><b>Relative</b> compares the current window to the <i>exact same duration</i> in the past (the baseline).</>}
              value={evalType} onValueChange={handleEvalTypeChange} options={EVAL_TYPE_OPTIONS}
            />
          </div>

          {(evalType !== 'absolute' && evalType !== 'anomaly_zscore') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabeledSelect
                label="Baseline Comparison Period"
                info="How far back to look for the baseline. If comparing the last 5m to 1 hour ago, it measures against the 5-minute window that ended 60 minutes ago."
                value={compPeriodMin} onValueChange={(v) => v && setValue('compPeriodMin', v)} options={COMP_PERIOD_OPTIONS}
              />
              <p className="text-[10px] text-muted-foreground">Alert will compare the current window to the exact same window this duration ago.</p>
            </div>
          )}

          {evalType === 'anomaly_zscore' ? (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <div className="grid gap-2">
                <LabelWithInfo htmlFor="zscore-threshold" label="Z-Score Threshold" info="How many standard deviations above the rolling hourly baseline the current window must be to trigger. Standard value is 3.0." />
                <Input id="zscore-threshold" type="number" step="0.1" min="0" placeholder="e.g. 3.0" {...register('zscoreThreshold')} required />
              </div>
              <LabeledSelect
                label="Baseline Period (Days)" info="The length of the rolling historical baseline period to compute standard deviation and mean over."
                value={baselinePeriodDays} onValueChange={(v) => v && setValue('baselinePeriodDays', v)} options={BASELINE_DAYS_OPTIONS}
              />
            </div>
          ) : (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <LabeledSelect
                label="Operator" info="The mathematical condition to trigger the alert." value={operator}
                onValueChange={(v) => { const op = v ? oneOf(OPERATORS, v) : undefined; if (op) setValue('operator', op) }} options={OPERATOR_OPTIONS}
              />
              <div className="grid gap-2">
                <LabelWithInfo htmlFor="threshold" label={`Threshold ${evalType !== 'absolute' || metric.endsWith('_rate') ? '(%)' : ''}`} info="The numeric value to breach. For rate/relative metrics, this is a percentage." />
                <Input
                  id="threshold" type="number" step="any" placeholder={evalType !== 'absolute' ? 'e.g. 50 (for 50% increase)' : 'e.g. 100'}
                  {...register('threshold')} aria-invalid={thresholdError !== null} aria-describedby={thresholdError ? 'threshold-error' : undefined} required
                />
                {thresholdError && <p id="threshold-error" role="alert" className="text-[11px] text-destructive">{thresholdError}</p>}
              </div>
            </div>
          )}

          <LabeledSelect
            htmlFor="window" label="Evaluation Window"
            info="The length of time to aggregate data over before evaluating the threshold. A longer window prevents flapping on brief spikes."
            value={windowMin} onValueChange={(v) => v && setValue('windowMin', v)} options={WINDOW_OPTIONS}
          />

          <div className="grid gap-4 border-t pt-4">
            <LabelWithInfo label="Alert Notification Channels" info="Configure one or more alert delivery channels (e.g. Slack, PagerDuty, generic Webhooks) for this alert." />
            {channels.length > 0 ? (
              <div className="space-y-2 max-h-[150px] overflow-y-auto pr-1">
                {keyByOccurrence(channels, (c) => `${c.type}|${c.url}`).map(({ key, item: chan }) => (
                  <div key={key} className="flex items-center justify-between p-2 bg-muted/50 rounded-md border border-border/50 text-xs">
                    <div className="flex items-center gap-2 overflow-hidden">
                      <span className="font-semibold capitalize px-1.5 py-0.5 rounded bg-accent/60 text-[10px]">{chan.type}</span>
                      <span className="truncate text-muted-foreground max-w-[200px]" title={chan.url}>{chan.url}</span>
                    </div>
                    <Button type="button" variant="ghost" size="sm" className="h-6 w-6 p-0 text-destructive hover:bg-destructive/10" onClick={() => handleRemoveChannel(chan)}>×</Button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-[10px] text-muted-foreground italic">No channels configured. Legacy Webhook URL (if defined below) will be used.</p>
            )}

            <div className="flex gap-2">
              <Select value={newChannelType} onValueChange={(v) => { const ct = v ? oneOf(CHANNEL_TYPES, v) : undefined; if (ct) setNewChannelType(ct) }}>
                <SelectTrigger className="w-[110px] text-xs h-8"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="slack">Slack</SelectItem>
                  <SelectItem value="pagerduty">PagerDuty</SelectItem>
                  <SelectItem value="webhook">Webhook</SelectItem>
                </SelectContent>
              </Select>
              <Input
                placeholder={newChannelType === 'slack' ? 'Slack webhook URL...' : newChannelType === 'pagerduty' ? 'PagerDuty Integration URL...' : 'Generic webhook URL...'}
                value={newChannelUrl} onChange={(e) => setNewChannelUrl(e.target.value)} className="h-8 text-xs flex-1"
              />
              <Button type="button" variant="outline" size="sm" className="h-8 text-xs" onClick={handleAddChannel} disabled={!newChannelUrl.trim()}>Add</Button>
            </div>
          </div>

          <div className="grid gap-2 border-t pt-4">
            <LabelWithInfo htmlFor="webhook" label="Legacy Webhook URL (Optional / Prefer Channels)" info="Legacy single-destination webhook URL. Use Alert Notification Channels above instead for flexible multi-destination routing." />
            <Input id="webhook" placeholder="https://hooks.slack.com/services/..." {...register('webhookUrl')} />
            <p className="text-[10px] text-muted-foreground italic">Legacy fallback endpoint. We recommend using the dedicated notification channels configured above.</p>
          </div>
        </div>

        <AlertPreview
          previewData={previewQuery.data ?? null} isPreviewLoading={previewQuery.isFetching}
          lookbackHours={lookbackHours} setLookbackHours={setLookbackHours} metric={metric}
          evalType={evalType} threshold={evalType === 'anomaly_zscore' ? zscoreThreshold : threshold}
        />
      </div>

      <DialogFooter className="pt-4 mt-auto border-t">
        <Button type="button" variant="outline" onClick={onSuccess}>Cancel</Button>
        <Button type="submit" disabled={isSaving || !isValid} title={!isValid ? 'Fill in Name and Threshold to enable submit.' : undefined}>
          {isSaving ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
          {initialAlert ? 'Save Changes' : 'Create Alert'}
        </Button>
      </DialogFooter>
    </form>
  )
}

interface AlertsListProps {
  alerts: Alert[]; columnVisibility: VisibilityState; setColumnVisibility: React.Dispatch<React.SetStateAction<VisibilityState>>
  onEdit: (alert: Alert) => void; onDelete: (alertId: string) => void; isAnalyst?: boolean
}

function AlertsList({ alerts, columnVisibility, setColumnVisibility, onEdit, onDelete, isAnalyst = false }: AlertsListProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()
  const { relative, full, abbr } = useDateFormat()

  const toggleMutation = useMutation({
    mutationFn: async ({ alert, newEnabled }: { alert: Alert; newEnabled: boolean }) => {
      await unwrapClient(client.PATCH('/api/alerts/{alert_id}/enabled', { params: { path: { alert_id: alert.id! } }, body: { enabled: newEnabled } }))
    },
    onMutate: async ({ alert, newEnabled }) => {
      const queryKey = ['alerts', activeServiceId]
      await queryClient.cancelQueries({ queryKey })
      const previous = queryClient.getQueryData(queryKey)
      queryClient.setQueryData(queryKey, (old: { data?: Alert[] } | undefined) => ({
        ...old, data: old?.data?.map((a) => (a.id === alert.id ? { ...a, enabled: newEnabled } : a)),
      }))
      return { previous, queryKey }
    },
    onError: (err, _vars, context) => {
      console.error('Failed to toggle alert', err)
      if (context?.queryKey) queryClient.setQueryData(context.queryKey, context.previous)
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['alerts'] }),
  })
  const togglingId = toggleMutation.isPending ? (toggleMutation.variables?.alert.id ?? null) : null
  const toggleEnabled = React.useCallback((alert: Alert, newEnabled: boolean) => toggleMutation.mutate({ alert, newEnabled }), [toggleMutation])

  const testMutation = useMutation({
    mutationFn: async (alert: Alert) => {
      const { data, error } = await client.POST('/api/alerts/{alert_id}/test', {
        params: { path: { alert_id: alert.id! } },
        body: { service_id: activeServiceId || alert.service_id, channel_type: 'webhook', dry_run: true },
      })
      if (error) {
        showToast(extractApiError(error) || 'Failed to test alert notification', 'error')
        return
      }
      const dispatched = data?.data?.targets_tested ?? 0
      showToast(dispatched > 0 ? `Validated ${dispatched} notification channel${dispatched === 1 ? '' : 's'}` : 'Alert rule validated (no external channels configured)', 'success')
    },
    onError: (err) => {
      console.error('Failed to test alert notification', err)
      showToast(extractApiError(err) || 'Failed to test alert notification', 'error')
    },
  })
  const testingId = testMutation.isPending ? (testMutation.variables?.id ?? null) : null
  const testNotification = React.useCallback((alert: Alert) => { if (alert.id) testMutation.mutate(alert) }, [testMutation])

  const columns = React.useMemo<ColumnDef<Alert>[]>(
    () => [
      { accessorKey: 'name', header: 'Alert Name', cell: (info) => <span className="font-medium">{info.row.original.name}</span> },
      { accessorKey: 'category', header: 'Category', cell: (info) => <Badge variant="secondary" className="capitalize">{info.row.original.category?.replace('_', ' ') || 'Reliability'}</Badge> },
      {
        accessorKey: 'metric', header: 'Metric',
        cell: (info) => {
          const { metric: val, status_codes: codes, evaluation_scope: scope } = info.row.original
          const display = val === 'specific_status' && codes ? `Status ${codes.join(', ')}` : val === 'specific_status_rate' && codes ? `Status ${codes.join(', ')} Rate` : val.replace(/_/g, ' ')
          return (
            <div className="flex items-center">
              <span className="capitalize text-sm font-medium">{display}</span>
              {scope === 'edge' && <Badge variant="outline" className="ml-2 text-[10px] h-4 px-1 py-0 font-normal">Edge</Badge>}
              {scope === 'origin' && <Badge variant="outline" className="ml-2 text-[10px] h-4 px-1 py-0 font-normal border-orange-500/50 text-orange-600 dark:text-orange-400">Origin</Badge>}
            </div>
          )
        },
      },
      {
        id: 'condition', header: 'Condition',
        cell: (info) => {
          const a = info.row.original
          const windowStr = a.window_min < 1 ? `${Math.round(a.window_min * 60)}s` : `${a.window_min}m`
          const evalType = a.evaluation_type || 'absolute'
          if (evalType === 'absolute') return <span className="text-sm font-mono">{a.operator} {a.threshold} (last {windowStr})</span>
          if (evalType === 'anomaly_zscore') {
            const zThresh = a.zscore_threshold !== undefined ? a.zscore_threshold : a.threshold || 3.0
            const baseDays = a.baseline_period_days || 7
            return (
              <span className="text-sm font-mono flex items-center gap-1" title={`Z-Score > ${zThresh} vs rolling baseline over last ${baseDays} days`}>
                <span className="text-purple-600 dark:text-purple-400 font-bold">Z-Score</span> &gt; {zThresh}
                <span className="text-muted-foreground text-[10px]">({baseDays}d baseline)</span>
              </span>
            )
          }
          const compStr = a.comparison_period_min ? (a.comparison_period_min >= 1440 ? `${a.comparison_period_min / 1440}d` : a.comparison_period_min >= 60 ? `${a.comparison_period_min / 60}h` : `${a.comparison_period_min}m`) : '?'
          return (
            <span className="text-sm font-mono flex items-center gap-1">
              {evalType === 'relative_increase' ? '↑' : '↓'} &gt; {a.threshold}%
              <span className="text-muted-foreground text-[10px]"> vs {compStr} ago</span>
            </span>
          )
        },
      },
      {
        accessorKey: 'last_triggered_at', header: 'Last Triggered',
        cell: (info) => {
          const alert = info.row.original
          const val = alert.last_triggered_at
          if (!val) return <span className="text-muted-foreground text-xs italic">Never</span>
          const end = new Date(val)
          const start = new Date(end.getTime() - alert.window_min * 60 * 1000)
          const params = new URLSearchParams({ start_time: start.toISOString(), end_time: end.toISOString(), metric: METRIC_TO_DASHBOARD[alert.metric] ?? alert.metric })
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
                  <TooltipTrigger render={<div className="flex flex-col"><span className="text-xs text-red-500 font-bold flex items-center gap-1"><AlertTriangle className="h-3 w-3" />{relative(val)}</span></div>} />
                  <TooltipContent className="text-xs">{full(val)} {abbr()}</TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <a href={`/dashboard?${params.toString()}`} target="_blank" rel="noopener noreferrer" className="text-[10px] text-primary hover:underline flex items-center gap-1 w-fit">
                <Activity className="h-3 w-3" />View on Dashboard
              </a>
            </div>
          )
        },
      },
      {
        accessorKey: 'enabled', header: 'Enabled?',
        cell: (info) => {
          const a = info.row.original
          if (isAnalyst) return <Badge variant={a.enabled ? 'secondary' : 'outline'} className="text-[10px]" data-testid={`analyst-alert-enabled-${a.id}`}>{a.enabled ? 'On' : 'Off'}</Badge>
          const isPending = togglingId === a.id
          return (
            <Switch
              checked={a.enabled} onCheckedChange={(checked) => toggleEnabled(a, checked)} disabled={isPending}
              data-testid={`toggle-alert-${a.id}`} aria-label={`Toggle alert ${a.name}`} className={isPending ? 'opacity-50 cursor-wait' : undefined}
            />
          )
        },
      },
      ...(isAnalyst ? [] : [{
        id: 'actions', header: '',
        cell: (info) => {
          const a = info.row.original
          return (
            <div className="flex items-center gap-1">
              <Button variant="ghost" size="icon" aria-label="Test alert notification" data-testid={`test-alert-${a.id}`} className="h-8 w-8 text-muted-foreground hover:text-primary" onClick={() => testNotification(a)} disabled={testingId === a.id} title="Test alert notification"><Send className="h-4 w-4" /></Button>
              <Button variant="ghost" size="icon" aria-label="Edit alert" data-testid={`edit-alert-${a.id}`} className="h-8 w-8 text-muted-foreground hover:text-primary" onClick={() => onEdit(a)} title="Edit alert"><Pencil className="h-4 w-4" /></Button>
              <Button variant="ghost" size="icon" aria-label="Delete alert" data-testid={`delete-alert-${a.id}`} className="h-8 w-8 text-muted-foreground hover:text-destructive" onClick={() => a.id && onDelete(a.id)} title="Delete alert"><Trash2 className="h-4 w-4" /></Button>
            </div>
          )
        },
      } satisfies ColumnDef<Alert>]),
    ],
    [togglingId, testingId, relative, full, abbr, toggleEnabled, testNotification, onEdit, onDelete, isAnalyst, activeServiceId],
  )

  return <DataTable columns={columns} data={alerts || []} hideToolbar={true} columnVisibility={columnVisibility} onColumnVisibilityChange={setColumnVisibility} />
}

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
      return () => { try { window.cancelIdleCallback?.(id) } catch {} }
    }
    const id = window.setTimeout(cb, 200)
    return () => window.clearTimeout(id)
  }, [])

  const { data: loggingSettings } = useQuery({
    queryKey: ['loggingSettings', activeServiceId],
    queryFn: async ({ signal }) => activeServiceId ? unwrapClient(client.GET('/api/services/{service_id}/logging-settings', { signal, params: { path: { service_id: activeServiceId } } })) : null,
    enabled: !!activeServiceId && !isAnalyst && logSettingsIdle,
    staleTime: 5 * 60_000,
  })
  const logPeriodSeconds = loggingSettings?.period || 30

  const { data: alertsRes, isLoading, isFetching, refetch, error: alertsError } = useQuery({
    queryKey: ['alerts', activeServiceId],
    queryFn: async ({ signal }) => activeServiceId
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

  const allAlerts = React.useMemo(() => alertsRes?.data || [], [alertsRes?.data])
  const alerts = React.useMemo(() => allAlerts.filter((a) => !pendingDeleteIds.has(a.id!)), [allAlerts, pendingDeleteIds])

  const summaryCounts = React.useMemo(() => {
    let alerting = 0, ok = 0, muted = 0
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
      id: alertId, toastMessage: `Deleted "${alertName}"`, errorMessage: `Failed to delete "${alertName}"`,
      performDelete: async () => {
        await unwrapClient(client.DELETE('/api/alerts/{alert_id}', { params: { path: { alert_id: alertId } } }))
        queryClient.invalidateQueries({ queryKey: ['alerts'] })
      },
    })
  }, [deleteTarget, allAlerts, queryClient, scheduleDelete])

  const handleEdit = React.useCallback((alert: Alert) => { if (!isAnalyst) { setEditingAlert(alert); setIsFormOpen(true) } }, [isAnalyst])
  const handleCreate = React.useCallback(() => { if (!isAnalyst) { setEditingAlert(null); setIsFormOpen(true) } }, [isAnalyst])

  const evaluateMutation = useMutation({
    mutationFn: async (serviceId: string) => unwrapClient(client.POST('/api/alerts/evaluate', { body: { service_id: serviceId } })),
    onSuccess: async (data) => {
      showToast(`Evaluated ${data?.data?.evaluated ?? 0} rule(s); ${data?.data?.triggered ?? 0} triggered.`, 'success')
      await queryClient.invalidateQueries({ queryKey: ['alerts'] })
    },
    onError: (error) => showToast(extractApiError(error) || 'Failed to evaluate alerts', 'error'),
  })
  const isEvaluating = evaluateMutation.isPending
  const handleEvaluateNow = React.useCallback(() => { if (!isAnalyst && activeServiceId) evaluateMutation.mutate(activeServiceId) }, [isAnalyst, activeServiceId, evaluateMutation])

  const summaryCards = [
    { label: 'Total Rules', value: summaryCounts.total, testId: 'alerts-count-total', icon: Bell, color: 'text-muted-foreground', valColor: '' },
    { label: 'Alerting', value: summaryCounts.alerting, testId: 'alerts-count-alerting', icon: AlertTriangle, color: 'text-red-500', valColor: 'text-red-500' },
    { label: 'Healthy (OK)', value: summaryCounts.ok, testId: 'alerts-count-ok', icon: CheckCircle2, color: 'text-emerald-500', valColor: 'text-emerald-500' },
    { label: 'Muted', value: summaryCounts.muted, testId: 'alerts-count-muted', icon: BellOff, color: 'text-muted-foreground', valColor: 'text-muted-foreground' },
  ]

  return (
    <ReportShell
      title="Alerts" icon={Bell} requireService={false} isReadyOverride={true}
      headerActions={
        <div className="flex items-center gap-3 flex-wrap">
          <Badge variant="secondary" className="gap-2 font-mono text-xs min-w-[180px]">
            <Clock className="w-3 h-3 text-muted-foreground shrink-0" />
            <span className="truncate">{`Last Checked: ${lastCheckedLabel}`}</span>
          </Badge>
          {!isAnalyst && (
            <Button variant="outline" size="sm" onClick={handleEvaluateNow} disabled={!activeServiceId || isEvaluating || isFetching || isLoading} data-testid="alerts-evaluate-btn">
              {isEvaluating ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Play className="w-4 h-4 mr-2" />}Evaluate Rules
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching || isLoading} data-testid="alerts-refresh-btn">
            <Loader2 className={`w-4 h-4 mr-2 ${isFetching ? 'animate-spin' : ''}`} />Refresh Now
          </Button>
          {!isAnalyst && <Button onClick={handleCreate} data-testid="alerts-create-btn"><Plus className="w-4 h-4 mr-2" />Create Alert</Button>}
        </div>
      }
      description={
        <div className="flex items-center gap-2 flex-wrap">
          <span>Configure threshold-based alerts and webhook notifications.</span>
          <Popover>
            <PopoverTrigger>
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-md hover:bg-accent transition-colors">
                <Clock className="h-3 w-3" /><span>Evaluated every {logPeriodSeconds}s</span><Info className="h-3 w-3" />
              </div>
            </PopoverTrigger>
            <PopoverContent className="w-80 text-sm">
              <div className="space-y-2">
                <h4 className="font-medium flex items-center gap-2"><Zap className="h-4 w-4 text-yellow-500" />Evaluation Frequency</h4>
                <p className="text-muted-foreground">Alerts are evaluated dynamically based on your Fastly service&apos;s log batching period (currently <strong>{logPeriodSeconds} seconds</strong>).</p>
                <p className="text-muted-foreground">This ensures alerts trigger the moment a new batch of logs arrives from Fastly, without uselessly spamming the database in between batches.</p>
                <div className="bg-muted p-2 rounded text-xs mt-2 text-muted-foreground">
                  Note: Alert queries execute in the background server process, so they do not appear in the DuckDB Queries debug panel.
                </div>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      }
    >
      <div data-testid="alerts-page-root" className="space-y-6">
        <div data-testid="alerts-summary-grid" className="grid grid-cols-2 md:grid-cols-4 gap-4 min-h-[96px] [contain-intrinsic-size:96px] [content-visibility:auto]">
          {summaryCards.map(({ label, value, testId, icon: Icon, color, valColor }) => (
            <Card key={testId} className="py-3">
              <CardContent className="px-4 py-0 flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground font-medium">{label}</p>
                  <p className={`text-2xl font-bold tabular-nums ${valColor}`} data-testid={testId}>{value}</p>
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
              <DialogDescription>Threshold alerts evaluate logs in real-time every {logPeriodSeconds} seconds.</DialogDescription>
            </DialogHeader>
            <CreateAlertForm initialAlert={editingAlert} onSuccess={() => setIsFormOpen(false)} />
          </DialogContent>
        </Dialog>

        <div data-testid="alerts-list-card" className="min-h-[350px] [contain-intrinsic-size:350px] [content-visibility:auto]">
          <AnalyticsCard
            title="Configured Alerts" icon={<Bell className="h-4 w-4" />} isLoading={isLoading} isFetching={false}
            loadingMessage="Fetching configured alert rules..." error={alertsError as AnalyticsCardError | null}
            isEmpty={false} className="min-h-[350px]" contentClassName="p-0 min-h-[280px]"
            headerAction={
              <div className="flex items-center gap-2 flex-wrap">
                <div role="group" aria-label="Filter alerts by status" data-testid="alerts-status-filter" className="inline-flex items-center rounded-md border bg-muted/40 p-0.5 text-xs">
                  {ALERT_STATUS_FILTERS.map((st) => (
                    <button
                      key={st} type="button" data-testid={`alerts-filter-${st}`} aria-pressed={statusFilter === st} onClick={() => void setStatusFilter(st)}
                      className={`px-2.5 py-1 rounded-sm font-medium capitalize transition-colors ${statusFilter === st ? 'bg-background text-foreground shadow-xs' : 'text-muted-foreground hover:text-foreground'}`}
                    >
                      {st === 'ok' ? 'OK' : st}
                    </button>
                  ))}
                </div>
                <ColumnVisibilityDropdown
                  columns={ALERTS_AVAILABLE_COLUMNS} visibility={columnVisibility}
                  onChange={(id, visible) => setColumnVisibility((prev) => ({ ...prev, [id]: visible }))}
                />
              </div>
            }
          >
            <div className={`min-h-[280px] ${isFetching && !isLoading ? 'opacity-40 pointer-events-none transition-opacity' : ''}`}>
              {isLoading ? (
                <div className="min-h-[280px]" />
              ) : filteredAlerts.length === 0 ? (
                <div data-testid="alerts-empty-state" className="flex flex-col items-center justify-center min-h-[280px] py-12 px-4 text-center gap-3">
                  <Bell className="h-8 w-8 text-muted-foreground/60" aria-hidden="true" />
                  <div className="space-y-1 max-w-md">
                    <h3 className="text-sm font-semibold">{alerts.length === 0 ? 'No alert rules configured' : `No ${statusFilter} alert rules`}</h3>
                    <p className="text-xs text-muted-foreground">
                      {alerts.length === 0
                        ? 'Create threshold or anomaly detection rules to receive Slack, PagerDuty, or Webhook notifications when traffic or error rates spike.'
                        : 'No configured alert rules match the selected status filter.'}
                    </p>
                  </div>
                  {alerts.length === 0 && !isAnalyst ? (
                    <Button size="sm" onClick={handleCreate} data-testid="alerts-empty-create-btn"><Plus className="w-4 h-4 mr-1.5" />Add Alert</Button>
                  ) : alerts.length > 0 && statusFilter !== 'all' ? (
                    <Button size="sm" variant="outline" onClick={() => void setStatusFilter('all')} data-testid="alerts-clear-filter-btn">Show All Rules</Button>
                  ) : null}
                </div>
              ) : (
                <AlertsList alerts={filteredAlerts} columnVisibility={columnVisibility} setColumnVisibility={setColumnVisibility} onEdit={handleEdit} onDelete={setDeleteTarget} isAnalyst={isAnalyst} />
              )}
            </div>
          </AnalyticsCard>
        </div>

        <ConfirmDialog
          open={deleteTarget !== null} onOpenChange={(open) => { if (!open) setDeleteTarget(null) }}
          onConfirm={confirmDelete} isDangerous title="Delete Alert" description="Are you sure you want to delete this alert?" confirmLabel="Delete"
        />

        {!activeServiceId && bootstrapResolved && (
          <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20 text-yellow-600 dark:text-yellow-400 flex items-center gap-3 mt-6">
            <AlertTriangle className="h-5 w-5" />
            <p className="text-sm">Showing alerts for <strong>all services</strong>. Select a service to filter.</p>
          </div>
        )}
      </div>
    </ReportShell>
  )
}
