'use client'

import React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { Loader2 } from 'lucide-react'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import { useDebounce } from '@/hooks/useDashboardCards'
import { useServiceStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { DialogFooter } from '@/components/ui/dialog'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { showToast, oneOf } from '@/lib/utils'
import { AlertPreview } from './AlertPreview'
import {
  type Alert,
  type AlertChannel,
  type AlertPreviewData,
  type AlertFormValues,
  type ChannelType,
  CATEGORIES,
  EVAL_TYPES,
  EVAL_SCOPES,
  OPERATORS,
  METRICS_BY_CATEGORY,
  ALL_METRICS,
  CATEGORY_OPTIONS,
  SCOPE_OPTIONS,
  EVAL_TYPE_OPTIONS,
  COMP_PERIOD_OPTIONS,
  BASELINE_DAYS_OPTIONS,
  OPERATOR_OPTIONS,
  WINDOW_OPTIONS,
  alertFormSchema,
  LabeledSelect,
  parseStatusCodes,
  AlertChannelsSection,
} from './CreateAlertFormFields'

export function CreateAlertForm({
  initialAlert,
  onSuccess,
}: {
  initialAlert?: Alert | null
  onSuccess: () => void
}) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()

  const {
    register,
    handleSubmit,
    setValue,
    control,
    formState: { errors, isValid },
  } = useForm<AlertFormValues>({
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
    name,
    category,
    metric,
    evalType,
    evalScope,
    operator,
    threshold,
    windowMin,
    compPeriodMin,
    statusCodesStr,
    webhookUrl,
    zscoreThreshold,
    baselinePeriodDays,
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
  const handleRemoveChannel = (channel: AlertChannel) =>
    setChannels(channels.filter((c) => c !== channel))

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
      const isZ = p.evalType === 'anomaly_zscore'
      const data = await unwrapClient(
        client.POST('/api/alerts/preview', {
          params: { query: { lookback_hours: p.lookbackHours } },
          body: {
            service_id: p.activeServiceId,
            name: 'Preview',
            category: p.category,
            metric: p.metric,
            evaluation_type: p.evalType,
            evaluation_scope: p.evalScope,
            operator: isZ ? '>' : p.operator,
            threshold: isZ
              ? parseFloat(p.zscoreThreshold) || 3.0
              : parseFloat(p.threshold) || 0,
            window_min: parseFloat(p.windowMin),
            comparison_period_min:
              p.evalType !== 'absolute' && !isZ ? parseFloat(p.compPeriodMin) : undefined,
            status_codes: parseStatusCodes(p.metric, p.statusCodesStr),
            enabled: true,
            zscore_threshold: isZ ? parseFloat(p.zscoreThreshold) || 3.0 : undefined,
            baseline_period_days: isZ ? parseInt(p.baselinePeriodDays) || 7 : undefined,
          },
        }),
      )
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
        await unwrapClient(
          client.PUT('/api/alerts/{alert_id}', {
            params: { path: { alert_id: initialAlert.id } },
            body: payload,
          }),
        )
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
      id: initialAlert?.id,
      service_id: activeServiceId,
      name,
      category,
      metric,
      evaluation_type: evalType,
      evaluation_scope: evalScope,
      operator: isZ ? '>' : operator,
      threshold: isZ ? parseFloat(zscoreThreshold) : parseFloat(threshold),
      window_min: parseFloat(windowMin),
      comparison_period_min:
        evalType !== 'absolute' && !isZ ? parseFloat(compPeriodMin) : undefined,
      status_codes: parseStatusCodes(metric, statusCodesStr),
      webhook_url: webhookUrl || undefined,
      channels,
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
            <LabelWithInfo
              htmlFor="alert-name"
              label="Alert Name"
              info="A descriptive name for your alert, which will appear in notifications and the dashboard."
            />
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
            <LabeledSelect
              label="Category"
              info="Groups alerts logically. Does not affect evaluation logic."
              value={category}
              onValueChange={handleCategoryChange}
              options={CATEGORY_OPTIONS}
            />
            <LabeledSelect
              label="Metric"
              info="The specific data point to measure. Rate metrics represent a percentage of total traffic."
              value={metric}
              onValueChange={(v) => {
                const m = v ? oneOf(ALL_METRICS, v) : undefined
                if (m) setValue('metric', m)
              }}
              options={METRICS_BY_CATEGORY[category]}
            />
          </div>

          {(metric === 'specific_status' || metric === 'specific_status_rate') && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabelWithInfo
                htmlFor="status-codes"
                label="HTTP Status Codes"
                info="Enter one or more HTTP status codes (e.g., 503, 504) to match exactly against the log status field."
              />
              <Input
                id="status-codes"
                placeholder="e.g. 503, 504"
                {...register('statusCodesStr')}
                required
              />
              <p className="text-[10px] text-muted-foreground">
                Comma-separated list of HTTP status codes to track.
              </p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <LabeledSelect
              label="Evaluation Scope"
              info="Restricts the alert to a specific traffic scope. 'Edge Only' filters for edge responses. 'Origin Only' filters for requests that went to your origin."
              value={evalScope}
              onValueChange={(v) => {
                const sc = v ? oneOf(EVAL_SCOPES, v) : undefined
                if (sc) setValue('evalScope', sc)
              }}
              options={SCOPE_OPTIONS}
            />
            <LabeledSelect
              label="Evaluation Type"
              info={
                <>
                  <b>Absolute</b> triggers if the value crosses a hard limit.
                  <br />
                  <br />
                  <b>Relative</b> compares the current window to the <i>exact same duration</i>{' '}
                  in the past (the baseline).
                </>
              }
              value={evalType}
              onValueChange={handleEvalTypeChange}
              options={EVAL_TYPE_OPTIONS}
            />
          </div>

          {evalType !== 'absolute' && evalType !== 'anomaly_zscore' && (
            <div className="grid gap-2 p-3 bg-muted/30 rounded-md border border-border/50">
              <LabeledSelect
                label="Baseline Comparison Period"
                info="How far back to look for the baseline. If comparing the last 5m to 1 hour ago, it measures against the 5-minute window that ended 60 minutes ago."
                value={compPeriodMin}
                onValueChange={(v) => v && setValue('compPeriodMin', v)}
                options={COMP_PERIOD_OPTIONS}
              />
              <p className="text-[10px] text-muted-foreground">
                Alert will compare the current window to the exact same window this duration ago.
              </p>
            </div>
          )}

          {evalType === 'anomaly_zscore' ? (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <div className="grid gap-2">
                <LabelWithInfo
                  htmlFor="zscore-threshold"
                  label="Z-Score Threshold"
                  info="How many standard deviations above the rolling hourly baseline the current window must be to trigger. Standard value is 3.0."
                />
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
              <LabeledSelect
                label="Baseline Period (Days)"
                info="The length of the rolling historical baseline period to compute standard deviation and mean over."
                value={baselinePeriodDays}
                onValueChange={(v) => v && setValue('baselinePeriodDays', v)}
                options={BASELINE_DAYS_OPTIONS}
              />
            </div>
          ) : (
            <div className="grid grid-cols-2 gap-4 border-t pt-4">
              <LabeledSelect
                label="Operator"
                info="The mathematical condition to trigger the alert."
                value={operator}
                onValueChange={(v) => {
                  const op = v ? oneOf(OPERATORS, v) : undefined
                  if (op) setValue('operator', op)
                }}
                options={OPERATOR_OPTIONS}
              />
              <div className="grid gap-2">
                <LabelWithInfo
                  htmlFor="threshold"
                  label={`Threshold ${evalType !== 'absolute' || metric.endsWith('_rate') ? '(%)' : ''}`}
                  info="The numeric value to breach. For rate/relative metrics, this is a percentage."
                />
                <Input
                  id="threshold"
                  type="number"
                  step="any"
                  placeholder={
                    evalType !== 'absolute' ? 'e.g. 50 (for 50% increase)' : 'e.g. 100'
                  }
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

          <LabeledSelect
            htmlFor="window"
            label="Evaluation Window"
            info="The length of time to aggregate data over before evaluating the threshold. A longer window prevents flapping on brief spikes."
            value={windowMin}
            onValueChange={(v) => v && setValue('windowMin', v)}
            options={WINDOW_OPTIONS}
          />

          <AlertChannelsSection
            channels={channels}
            newChannelType={newChannelType}
            setNewChannelType={setNewChannelType}
            newChannelUrl={newChannelUrl}
            setNewChannelUrl={setNewChannelUrl}
            handleAddChannel={handleAddChannel}
            handleRemoveChannel={handleRemoveChannel}
          />

          <div className="grid gap-2 border-t pt-4">
            <LabelWithInfo
              htmlFor="webhook"
              label="Legacy Webhook URL (Optional / Prefer Channels)"
              info="Legacy single-destination webhook URL. Use Alert Notification Channels above instead for flexible multi-destination routing."
            />
            <Input
              id="webhook"
              placeholder="https://hooks.slack.com/services/..."
              {...register('webhookUrl')}
            />
            <p className="text-[10px] text-muted-foreground italic">
              Legacy fallback endpoint. We recommend using the dedicated notification channels
              configured above.
            </p>
          </div>
        </div>

        <AlertPreview
          previewData={previewQuery.data ?? null}
          isPreviewLoading={previewQuery.isFetching}
          lookbackHours={lookbackHours}
          setLookbackHours={setLookbackHours}
          metric={metric}
          evalType={evalType}
          threshold={evalType === 'anomaly_zscore' ? zscoreThreshold : threshold}
        />
      </div>

      <DialogFooter className="pt-4 mt-auto border-t">
        <Button type="button" variant="outline" onClick={onSuccess}>
          Cancel
        </Button>
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
