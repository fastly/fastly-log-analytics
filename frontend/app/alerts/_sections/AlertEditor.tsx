'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { showToast } from '@/lib/toast'
import { useServiceStore } from '@/stores/serviceStore'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { Button } from '@/components/ui/button'
import { Info, Loader2 } from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { DialogFooter } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import type { components } from '@/types/api.generated'
import { AlertPreview } from './AlertPreview'

type Alert = components["schemas"]["Alert"]

export function CreateAlertForm({ initialAlert, onSuccess }: { initialAlert?: Alert | null, onSuccess: () => void }) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { data: bootstrapData } = useBootstrap()
  // Defensive guard: AlertsPage already hides the dialog trigger for
  // analysts, but if the form mounts via any other path (deep-link,
  // older code path) the POST /api/alerts/preview call would silently
  // 403 — skip it entirely. Backend gates the same endpoint.
  const isAnalyst = useIsAnalyst()
  const queryClient = useQueryClient()

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

  // Per-field "touched" state so required-field errors only appear after
  // the user has interacted with the field (don't yell at them on first
  // render). Submit also flips all touched flags so a click on Create with
  // empty required fields surfaces the inline errors instead of silently
  // early-returning.
  const [touchedName, setTouchedName] = React.useState(false)
  const [touchedThreshold, setTouchedThreshold] = React.useState(false)
  const nameTrimmed = name.trim()
  const thresholdTrimmed = threshold.trim()
  const nameError = touchedName && nameTrimmed === '' ? 'Name is required.' : null
  const thresholdError = touchedThreshold && thresholdTrimmed === '' ? 'Threshold is required.' : null
  const formInvalid = nameTrimmed === '' || thresholdTrimmed === ''

  // Fetch preview data on change
  React.useEffect(() => {
    if (!activeServiceId) return
    if (isAnalyst) return

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
  }, [activeServiceId, isAnalyst, metric, category, evalType, evalScope, windowMin, compPeriodMin, statusCodesStr, threshold, lookbackHours])

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
    // Surface inline errors instead of silently early-returning. Flipping
    // touched flags on submit means a user who clicks Create without ever
    // focusing the required fields still sees the red error text.
    if (!activeServiceId || formInvalid) {
      setTouchedName(true)
      setTouchedThreshold(true)
      return
    }

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
      // U-4 (audit): POST mutations bypass the middleware's generic mutation
      // toast (POST is reserved for analytics queries there); surface failure
      // explicitly so the modal closing isn't the only signal the analyst sees.
      console.error('Failed to create alert', error)
      showToast(extractApiError(error) || 'Failed to save alert', 'error')
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
              onBlur={() => setTouchedName(true)}
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
                onBlur={() => setTouchedThreshold(true)}
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
        <AlertPreview
          previewData={previewData}
          isPreviewLoading={isPreviewLoading}
          lookbackHours={lookbackHours}
          setLookbackHours={setLookbackHours}
          metric={metric}
          evalType={evalType}
          threshold={threshold}
        />
      </div>

      <DialogFooter className="pt-4 mt-auto border-t">
        <Button type="button" variant="outline" onClick={onSuccess}>Cancel</Button>
        <Button
          type="submit"
          disabled={isSaving || formInvalid}
          title={formInvalid ? 'Fill in Name and Threshold to enable submit.' : undefined}
        >
          {isSaving ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
          {initialAlert ? 'Save Changes' : 'Create Alert'}
        </Button>
      </DialogFooter>
    </form>
  )
}
