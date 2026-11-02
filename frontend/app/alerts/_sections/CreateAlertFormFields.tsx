'use client'

import React from 'react'
import { z } from 'zod'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { keyByOccurrence, oneOf } from '@/lib/utils'
import type { components } from '@/types/api.generated'

export type Alert = components['schemas']['Alert']
export type AlertChannel = components['schemas']['AlertChannel']
export type AlertPreviewData = components['schemas']['AlertPreviewData']
export type Category = Alert['category']
export type Metric = Alert['metric']
export type EvalType = Alert['evaluation_type']
export type EvalScope = Alert['evaluation_scope']
export type Operator = Alert['operator']
export type ChannelType = AlertChannel['type']

export const CATEGORIES: readonly Category[] = ['reliability', 'performance', 'traffic', 'caching']
export const EVAL_TYPES: readonly EvalType[] = [
  'absolute',
  'relative_increase',
  'relative_decrease',
  'anomaly_zscore',
]
export const EVAL_SCOPES: readonly EvalScope[] = ['all', 'edge', 'origin']
export const OPERATORS: readonly Operator[] = ['>', '<', '>=', '<=']
export const CHANNEL_TYPES: readonly ChannelType[] = ['slack', 'pagerduty', 'webhook']

export const METRICS_BY_CATEGORY: Record<Category, { value: Metric; label: string }[]> = {
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

export const ALL_METRICS: readonly Metric[] = Object.values(METRICS_BY_CATEGORY).flatMap((ms) =>
  ms.map((m) => m.value),
)

export const alertFormSchema = z
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
    const thresholdField = v.evalType === 'anomaly_zscore' ? 'zscoreThreshold' : 'threshold'
    if (v[thresholdField].trim() === '') {
      ctx.addIssue({
        code: 'custom',
        path: [thresholdField],
        message: 'Threshold is required.',
      })
    }
  })

export type AlertFormValues = z.infer<typeof alertFormSchema>

export function LabeledSelect({
  htmlFor,
  label,
  info,
  value,
  onValueChange,
  options,
}: {
  htmlFor?: string
  label: string
  info: React.ReactNode
  value: string
  onValueChange: (val: string | null) => void
  options: readonly { value: string; label: string }[]
}) {
  return (
    <div className="grid gap-2">
      <LabelWithInfo htmlFor={htmlFor} label={label} info={info} />
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger id={htmlFor}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((o) => (
            <SelectItem key={o.value} value={o.value}>
              {o.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  )
}

export const CATEGORY_OPTIONS = [
  { value: 'reliability', label: 'Reliability (Errors)' },
  { value: 'traffic', label: 'Traffic (Requests/BW)' },
  { value: 'performance', label: 'Performance (Latency)' },
  { value: 'caching', label: 'Caching' },
] as const

export const SCOPE_OPTIONS = [
  { value: 'all', label: 'All Requests' },
  { value: 'edge', label: 'Edge Only' },
  { value: 'origin', label: 'Origin Only' },
] as const

export const EVAL_TYPE_OPTIONS = [
  { value: 'absolute', label: 'Absolute Threshold' },
  { value: 'relative_increase', label: 'Relative Increase (%)' },
  { value: 'relative_decrease', label: 'Relative Decrease (%)' },
  { value: 'anomaly_zscore', label: 'Anomaly Detection (Z-Score rolling baseline)' },
] as const

export const COMP_PERIOD_OPTIONS = [
  { value: '10', label: '10 minutes ago' },
  { value: '60', label: '1 hour ago' },
  { value: '1440', label: '1 day ago' },
  { value: '10080', label: '1 week ago' },
] as const

export const BASELINE_DAYS_OPTIONS = [
  { value: '7', label: '7 Days' },
  { value: '14', label: '14 Days' },
  { value: '30', label: '30 Days' },
] as const

export const OPERATOR_OPTIONS = OPERATORS.map((op) => ({ value: op, label: op }))

export const WINDOW_OPTIONS = [
  { value: '0.5', label: 'Last 30 seconds' },
  { value: '1', label: 'Last 1 minute' },
  { value: '5', label: 'Last 5 minutes' },
  { value: '15', label: 'Last 15 minutes' },
  { value: '60', label: 'Last 1 hour' },
] as const

export function parseStatusCodes(metric: string, raw: string): number[] | undefined {
  if ((metric === 'specific_status' || metric === 'specific_status_rate') && raw) {
    return raw
      .split(',')
      .map((s) => parseInt(s.trim()))
      .filter((n) => !isNaN(n))
  }
  return undefined
}

export function AlertChannelsSection({
  channels,
  newChannelType,
  setNewChannelType,
  newChannelUrl,
  setNewChannelUrl,
  handleAddChannel,
  handleRemoveChannel,
}: {
  channels: AlertChannel[]
  newChannelType: ChannelType
  setNewChannelType: (val: ChannelType) => void
  newChannelUrl: string
  setNewChannelUrl: (val: string) => void
  handleAddChannel: () => void
  handleRemoveChannel: (chan: AlertChannel) => void
}) {
  return (
    <div className="grid gap-4 border-t pt-4">
      <LabelWithInfo
        label="Alert Notification Channels"
        info="Configure one or more alert delivery channels (e.g. Slack, PagerDuty, generic Webhooks) for this alert."
      />
      {channels.length > 0 ? (
        <div className="space-y-2 max-h-[150px] overflow-y-auto pr-1">
          {keyByOccurrence(channels, (c) => `${c.type}|${c.url}`).map(
            ({ key, item: chan }) => (
              <div
                key={key}
                className="flex items-center justify-between p-2 bg-muted/50 rounded-md border border-border/50 text-xs"
              >
                <div className="flex items-center gap-2 overflow-hidden">
                  <span className="font-semibold capitalize px-1.5 py-0.5 rounded bg-accent/60 text-[10px]">
                    {chan.type}
                  </span>
                  <span
                    className="truncate text-muted-foreground max-w-[200px]"
                    title={chan.url}
                  >
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
            ),
          )}
        </div>
      ) : (
        <p className="text-[10px] text-muted-foreground italic">
          No channels configured. Legacy Webhook URL (if defined below) will be used.
        </p>
      )}

      <div className="flex gap-2">
        <Select
          value={newChannelType}
          onValueChange={(v) => {
            const ct = v ? oneOf(CHANNEL_TYPES, v) : undefined
            if (ct) setNewChannelType(ct)
          }}
        >
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
            newChannelType === 'slack'
              ? 'Slack webhook URL...'
              : newChannelType === 'pagerduty'
                ? 'PagerDuty Integration URL...'
                : 'Generic webhook URL...'
          }
          value={newChannelUrl}
          onChange={(e) => setNewChannelUrl(e.target.value)}
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
  )
}
