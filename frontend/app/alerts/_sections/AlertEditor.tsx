'use client'

import React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { showToast } from '@/lib/toast'
import { useServiceStore } from '@/stores/serviceStore'
import { useDebounce } from '@/hooks/useFilterPayload'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { Button } from '@/components/ui/button'
import { Loader2 } from 'lucide-react'
import { DialogFooter } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { LabelWithInfo } from '@/components/ui/label-with-info'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import type { components } from '@/types/api.generated'
import { AlertPreview } from './AlertPreview'
import { keyByOccurrence, oneOf } from '@/lib/utils'

type Alert = components["schemas"]["Alert"]
type AlertChannel = components["schemas"]["AlertChannel"]
type AlertPreviewData = components["schemas"]["AlertPreviewData"]
type Category = Alert["category"]
type Metric = Alert["metric"]
type EvalType = Alert["evaluation_type"]
type EvalScope = Alert["evaluation_scope"]
type Operator = Alert["operator"]
type ChannelType = AlertChannel["type"]

const CATEGORIES: readonly Category[] = ['reliability', 'performance', 'traffic', 'caching']
const EVAL_TYPES: readonly EvalType[] = ['absolute', 'relative_increase', 'relative_decrease', 'anomaly_zscore']
const EVAL_SCOPES: readonly EvalScope[] = ['all', 'edge', 'origin']
const OPERATORS: readonly Operator[] = ['>', '<', '>=', '<=']
const CHANNEL_TYPES: readonly ChannelType[] = ['slack', 'pagerduty', 'webhook']

// Metrics offered per category (changing the category resets the metric to the first entry).
const METRICS_BY_CATEGORY: Record<Category, { value: Metric; label: string }[]> = {
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
  caching: [{ value: 'hit_rate', label: 'Cache Hit Rate (%)' }],
}
const ALL_METRICS: readonly Metric[] = Object.values(METRICS_BY_CATEGORY).flatMap((ms) => ms.map((m) => m.value))

const alertFormSchema = z
  .object({
    name: z.string(),
    category: z.custom<Category>(),
    metric: z.custom<Metric>(),
    evalType: z.custom<EvalType>(),
    evalScope: z.custom<EvalScope>(),
    operator: z.custom<Operator>(),
    threshold: z.string(),
    windowMin: z.string(),
    compPeriodMin: z.string(),
    statusCodesStr: z.string(),
    webhookUrl: z.string(),
    zscoreThreshold: z.string(),
    baselinePeriodDays: z.string(),
  })
  .superRefine((v, ctx) => {
    if (v.name.trim() === '') {
      ctx.addIssue({ code: 'custom', path: ['name'], message: 'Name is required.' })
    }
    // Z-score alerts take their threshold from a different input.
    const thresholdField = v.evalType === 'anomaly_zscore' ? 'zscoreThreshold' : 'threshold'
    if (v[thresholdField].trim() === '') {
      ctx.addIssue({ code: 'custom', path: [thresholdField], message: 'Threshold is required.' })
    }
  })

type AlertFormValues = z.infer<typeof alertFormSchema>

export function CreateAlertForm({ initialAlert, onSuccess }: { initialAlert?: Alert | null, onSuccess: () => void }) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  // Defensive guard: AlertsPage already hides the dialog trigger for
  // analysts, but if the form mounts via any other path (deep-link,
  // older code path) the POST /api/alerts/preview call would silently
  // 403 — skip it entirely. Backend gates the same endpoint.
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()

  // Errors only surface after blur (onTouched) or a submit attempt, so required
  // fields don't shout on first render.
  const { register, handleSubmit, setValue, control, formState: { errors, isValid } } = useForm<AlertFormValues>({
    resolver: zodResolver(alertFormSchema),
    mode: 'onTouched',
    defaultValues: {
      name: initialAlert?.name || '',
      category: initialAlert?.category ?? 'traffic',
      metric: initialAlert?.metric ?? 'requests',
      evalType: initialAlert?.evaluation_type ?? 'absolute',
      evalScope: initialAlert?.evaluation_scope ?? 'all',
      operator: initialAlert?.operator ?? '>',
      threshold: initialAlert?.threshold?.toString() || '',
      windowMin: initialAlert?.window_min?.toString() || '5',
      compPeriodMin: initialAlert?.comparison_period_min?.toString() || '60',
      statusCodesStr: initialAlert?.status_codes?.join(', ') || '',
      webhookUrl: initialAlert?.webhook_url || '',
      zscoreThreshold: initialAlert?.zscore_threshold?.toString() || '3.0',
      baselinePeriodDays: initialAlert?.baseline_period_days?.toString() || '7',
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
  const handleRemoveChannel = (channel: AlertChannel) => {
    setChannels(channels.filter((c) => c !== channel))
  }

  const previewParams = React.useMemo(
    () => ({
      activeServiceId,
      metric,
      category,
      evalType,
      evalScope,
      operator,
      windowMin,
      compPeriodMin,
      statusCodesStr,
      threshold,
      zscoreThreshold,
      baselinePeriodDays,
      lookbackHours,
    }),
    [
      activeServiceId,
      metric,
      category,
      evalType,
      evalScope,
      operator,
      windowMin,
      compPeriodMin,
      statusCodesStr,
      threshold,
      zscoreThreshold,
      baselinePeriodDays,
      lookbackHours,
    ],
  )
  const debouncedPreviewParams = useDebounce(previewParams, 500)

  const previewQuery = useQuery<AlertPreviewData | null>({
    queryKey: ['alert-preview', debouncedPreviewParams],
    enabled: Boolean(debouncedPreviewParams.activeServiceId) && !isAnalyst,
    retry: false,
    queryFn: async () => {
      const p = debouncedPreviewParams
      if (!p.activeServiceId) return null
      let parsedCodes: number[] | undefined = undefined
      if ((p.metric === 'specific_status' || p.metric === 'specific_status_rate') && p.statusCodesStr) {
        parsedCodes = p.statusCodesStr.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))
      }

      const data = await unwrapClient(client.POST("/api/alerts/preview", {
        params: { query: { lookback_hours: p.lookbackHours } },
        body: {
          service_id: p.activeServiceId,
          name: 'Preview',
          category: p.category,
          metric: p.metric,
          evaluation_type: p.evalType,
          evaluation_scope: p.evalScope,
          operator: p.evalType === 'anomaly_zscore' ? '>' : p.operator,
          threshold: p.evalType === 'anomaly_zscore' ? (parseFloat(p.zscoreThreshold) || 3.0) : (parseFloat(p.threshold) || 0),
          window_min: parseFloat(p.windowMin),
          comparison_period_min: (p.evalType !== 'absolute' && p.evalType !== 'anomaly_zscore') ? parseFloat(p.compPeriodMin) : undefined,
          status_codes: parsedCodes,
          enabled: true,
          zscore_threshold: p.evalType === 'anomaly_zscore' ? (parseFloat(p.zscoreThreshold) || 3.0) : undefined,
          baseline_period_days: p.evalType === 'anomaly_zscore' ? (parseInt(p.baselinePeriodDays) || 7) : undefined,
        },
      }))
      return data?.data ?? null
    },
  })
  const previewData = previewQuery.data ?? null
  const isPreviewLoading = previewQuery.isFetching

  // Handle category change -> reset metric
  const handleCategoryChange = (val: string | null) => {
    const next = val ? oneOf(CATEGORIES, val) : undefined
    if (!next) return
    setValue('category', next)
    setValue('metric', METRICS_BY_CATEGORY[next][0].value)
  }

  // Handle eval type change -> reset operator
  const handleEvalTypeChange = (val: string | null) => {
    const next = val ? oneOf(EVAL_TYPES, val) : undefined
    if (!next) return
    setValue('evalType', next, { shouldValidate: true })
    if (val === 'anomaly_zscore') {
      setValue('operator', '>')
    } else if (val !== 'absolute') {
      setValue('operator', '>') // Relatives are usually increases
    }
  }

  const saveMutation = useMutation({
    mutationFn: async (payload: Alert) => {
      if (initialAlert?.id) {
        await unwrapClient(client.PUT("/api/alerts/{alert_id}", {
          params: { path: { alert_id: initialAlert.id } },
          body: payload,
        }))
      } else {
        await unwrapClient(client.POST("/api/alerts/", {
          body: payload,
        }))
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
      onSuccess()
    },
    onError: (error) => {
      // U-4 (audit): POST mutations bypass the middleware's generic mutation
      // toast (POST is reserved for analytics queries there); surface failure
      // explicitly so the modal closing isn't the only signal the analyst sees.
      console.error('Failed to create alert', error)
      showToast(extractApiError(error) || 'Failed to save alert', 'error')
    },
  })
  const isSaving = saveMutation.isPending

  const handleSave = handleSubmit(async () => {
    // Invalid submits never get here: handleSubmit surfaces the inline errors.
    if (!activeServiceId) return

    let parsedCodes: number[] | undefined = undefined
    if ((metric === 'specific_status' || metric === 'specific_status_rate') && statusCodesStr) {
      parsedCodes = statusCodesStr.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))
    }

    const payload: Alert = {
      id: initialAlert?.id,
      service_id: activeServiceId,
      name,
      category,
      metric,
      evaluation_type: evalType,
      evaluation_scope: evalScope,
      operator: evalType === 'anomaly_zscore' ? '>' : operator,
      threshold: evalType === 'anomaly_zscore' ? parseFloat(zscoreThreshold) : parseFloat(threshold),
      window_min: parseFloat(windowMin),
      comparison_period_min: (evalType !== 'absolute' && evalType !== 'anomaly_zscore') ? parseFloat(compPeriodMin) : undefined,
      status_codes: parsedCodes,
      webhook_url: webhookUrl || undefined,
      channels: channels,
      zscore_threshold: evalType === 'anomaly_zscore' ? parseFloat(zscoreThreshold) : undefined,
      baseline_period_days: evalType === 'anomaly_zscore' ? parseInt(baselinePeriodDays) : undefined,
      enabled: initialAlert ? initialAlert.enabled : true
    }

    await saveMutation.mutateAsync(payload).catch(() => {})
  })

  return (
    <form onSubmit={handleSave} className="flex flex-col overflow-hidden">
      <div className="grid md:grid-cols-2 gap-6 py-4 overflow-y-auto px-1 flex-1">
        {/* Left Column: Form Fields */}
        <div className="space-y-4 pr-2">
          <div className="grid gap-2">
            <LabelWithInfo htmlFor="alert-name" label="Alert Name" info="A descriptive name for your alert, which will appear in notifications and the dashboard." />
            <Input
              id="alert-name"
              placeholder="e.g. High 5xx Error Rate"
              {...register('name')}
              aria-invalid={nameError !== null}
              aria-describedby={nameError ? 'alert-name-error' : undefined}
              required
            />
            {nameError && (
              <p id="alert-name-error" role="alert" className="text-[11px] text-destructive">
                {nameError}
              </p>
            )}
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2">
              <LabelWithInfo label="Category" info="Groups alerts logically. Does not affect evaluation logic." />
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
              <LabelWithInfo label="Metric" info="The specific data point to measure. Rate metrics represent a percentage of total traffic." />
              <Select value={metric} onValueChange={(v) => { const m = v ? oneOf(ALL_METRICS, v) : undefined; if (m) setValue('metric', m) }}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {METRICS_BY_CATEGORY[category].map(m => (
                     <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {(metric === 'specific_status' || metric === 'specific_status_rate') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
               <LabelWithInfo htmlFor="status-codes" label="HTTP Status Codes" info="Enter one or more HTTP status codes (e.g., 503, 504) to match exactly against the log status field." />
               <Input
                 id="status-codes"
                 placeholder="e.g. 503, 504"
                 {...register('statusCodesStr')}
                 required
               />
               <p className="text-[10px] text-muted-foreground">Comma-separated list of HTTP status codes to track.</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2">
              <LabelWithInfo label="Evaluation Scope" info="Restricts the alert to a specific traffic scope. 'Edge Only' filters for edge responses. 'Origin Only' filters for requests that went to your origin." />
              <Select value={evalScope} onValueChange={(v) => { const sc = v ? oneOf(EVAL_SCOPES, v) : undefined; if (sc) setValue('evalScope', sc) }}>
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
              <LabelWithInfo label="Evaluation Type" info={<><b>Absolute</b> triggers if the value crosses a hard limit.<br/><br/><b>Relative</b> compares the current window to the <i>exact same duration</i> in the past (the baseline).</>} />
              <Select value={evalType} onValueChange={handleEvalTypeChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="absolute">Absolute Threshold</SelectItem>
                  <SelectItem value="relative_increase">Relative Increase (%)</SelectItem>
                  <SelectItem value="relative_decrease">Relative Decrease (%)</SelectItem>
                  <SelectItem value="anomaly_zscore">Anomaly Detection (Z-Score rolling baseline)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {(evalType !== 'absolute' && evalType !== 'anomaly_zscore') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabelWithInfo label="Baseline Comparison Period" info="How far back to look for the baseline. If comparing the last 5m to 1 hour ago, it measures against the 5-minute window that ended 60 minutes ago." />
              <Select value={compPeriodMin} onValueChange={v => v && setValue('compPeriodMin', v)}>
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

          {evalType === 'anomaly_zscore' ? (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <div className="grid gap-2">
                <LabelWithInfo htmlFor="zscore-threshold" label="Z-Score Threshold" info="How many standard deviations above the rolling hourly baseline the current window must be to trigger. Standard value is 3.0." />
                <Input
                  id="zscore-threshold"
                  type="number"
                  step="0.1"
                  min="0"
                  placeholder="e.g. 3.0"
                  {...register('zscoreThreshold')}
                  required
                />
              </div>
              <div className="grid gap-2">
                <LabelWithInfo label="Baseline Period (Days)" info="The length of the rolling historical baseline period to compute standard deviation and mean over." />
                <Select value={baselinePeriodDays} onValueChange={v => v && setValue('baselinePeriodDays', v)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="7">7 Days</SelectItem>
                    <SelectItem value="14">14 Days</SelectItem>
                    <SelectItem value="30">30 Days</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          ) : (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <div className="grid gap-2">
                <LabelWithInfo label="Operator" info="The mathematical condition to trigger the alert." />
                <Select value={operator} onValueChange={(v) => { const op = v ? oneOf(OPERATORS, v) : undefined; if (op) setValue('operator', op) }}>
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
                <LabelWithInfo htmlFor="threshold" label={`Threshold ${evalType !== 'absolute' || metric.endsWith('_rate') ? '(%)' : ''}`} info="The numeric value to breach. For rate/relative metrics, this is a percentage." />
                <Input
                  id="threshold"
                  type="number"
                  step="any"
                  placeholder={evalType !== 'absolute' ? "e.g. 50 (for 50% increase)" : "e.g. 100"}
                  {...register('threshold')}
                  aria-invalid={thresholdError !== null}
                  aria-describedby={thresholdError ? 'threshold-error' : undefined}
                  required
                />
                {thresholdError && (
                  <p id="threshold-error" role="alert" className="text-[11px] text-destructive">
                    {thresholdError}
                  </p>
                )}
              </div>
            </div>
          )}

          <div className="grid gap-2">
            <LabelWithInfo htmlFor="window" label="Evaluation Window" info="The length of time to aggregate data over before evaluating the threshold. A longer window prevents flapping on brief spikes." />
            <Select value={windowMin} onValueChange={(v) => v && setValue('windowMin', v)}>
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

          <div className="grid gap-4 border-t pt-4">
            <LabelWithInfo label="Alert Notification Channels" info="Configure one or more alert delivery channels (e.g. Slack, PagerDuty, generic Webhooks) for this alert." />

            {channels.length > 0 ? (
              <div className="space-y-2 max-h-[150px] overflow-y-auto pr-1">
                {keyByOccurrence(channels, (c) => `${c.type}|${c.url}`).map(({ key, item: chan }) => (
                  <div key={key} className="flex items-center justify-between p-2 bg-muted/50 rounded-md border border-border/50 text-xs">
                    <div className="flex items-center gap-2 overflow-hidden">
                      <span className="font-semibold capitalize px-1.5 py-0.5 rounded bg-accent/60 text-[10px]">
                        {chan.type}
                      </span>
                      <span className="truncate text-muted-foreground max-w-[200px]" title={chan.url}>
                        {chan.url}
                      </span>
                    </div>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="h-6 w-6 p-0 text-destructive hover:bg-destructive/10"
                      onClick={() => handleRemoveChannel(chan)}
                    >
                      ×
                    </Button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-[10px] text-muted-foreground italic">No channels configured. Legacy Webhook URL (if defined below) will be used.</p>
            )}

            <div className="flex gap-2">
              <Select value={newChannelType} onValueChange={(v) => { const ct = v ? oneOf(CHANNEL_TYPES, v) : undefined; if (ct) setNewChannelType(ct) }}>
                <SelectTrigger className="w-[110px] text-xs h-8">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="slack">Slack</SelectItem>
                  <SelectItem value="pagerduty">PagerDuty</SelectItem>
                  <SelectItem value="webhook">Webhook</SelectItem>
                </SelectContent>
              </Select>
              <Input
                placeholder={
                  newChannelType === 'slack' ? 'Slack webhook URL...' :
                  newChannelType === 'pagerduty' ? 'PagerDuty Integration URL...' :
                  'Generic webhook URL...'
                }
                value={newChannelUrl}
                onChange={e => setNewChannelUrl(e.target.value)}
                className="h-8 text-xs flex-1"
              />
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-8 text-xs"
                onClick={handleAddChannel}
                disabled={!newChannelUrl.trim()}
              >
                Add
              </Button>
            </div>
          </div>

          <div className="grid gap-2 border-t pt-4">
            <LabelWithInfo htmlFor="webhook" label="Legacy Webhook URL (Optional / Prefer Channels)" info="Legacy single-destination webhook URL. Use Alert Notification Channels above instead for flexible multi-destination routing." />
            <Input
              id="webhook"
              placeholder="https://hooks.slack.com/services/..."
              {...register('webhookUrl')}
            />
            <p className="text-[10px] text-muted-foreground italic">
              Legacy fallback endpoint. We recommend using the dedicated notification channels configured above.
            </p>
          </div>
        </div>

        {/* Right Column: Live Chart Preview */}
        <AlertPreview
          previewData={previewData}
          isPreviewLoading={isPreviewLoading}
          lookbackHours={lookbackHours}
          setLookbackHours={setLookbackHours}
          metric={metric}
          evalType={evalType}
          threshold={evalType === 'anomaly_zscore' ? zscoreThreshold : threshold}
        />
      </div>

      <DialogFooter className="pt-4 mt-auto border-t">
        <Button type="button" variant="outline" onClick={onSuccess}>Cancel</Button>
        <Button
          type="submit"
          disabled={isSaving || !isValid}
          title={!isValid ? 'Fill in Name and Threshold to enable submit.' : undefined}
        >
          {isSaving ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
          {initialAlert ? 'Save Changes' : 'Create Alert'}
        </Button>
      </DialogFooter>
    </form>
  )
}
