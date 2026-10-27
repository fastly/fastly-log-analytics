'use client'

import React from 'react'
import {
  AlertCircle,
  AlertTriangle,
  CheckCircle,
  Info,
  Loader2,
  Minus,
  TrendingDown,
  TrendingUp,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { cn, formatCount } from '@/lib/utils'

export interface DeltaIndicatorProps {
  current: number
  baseline: number | null | undefined
}

export function DeltaIndicator({ current, baseline }: DeltaIndicatorProps) {
  if (baseline == null || baseline === 0) return null
  const pct = ((current - baseline) / Math.abs(baseline)) * 100
  const abs = current - baseline
  const absStr =
    abs > 0
      ? `+${formatCount(abs, { maximumFractionDigits: 1 })}`
      : formatCount(abs, { maximumFractionDigits: 1 })

  if (Math.abs(pct) < 1) {
    return <Minus className="h-3 w-3 text-muted-foreground" />
  }

  if (pct > 0) {
    return (
      <span
        className="flex items-center gap-0.5 text-red-600 dark:text-red-400 text-[11px] sm:text-[10px] font-semibold"
        title={`${absStr} from baseline`}
      >
        <TrendingUp className="h-3 w-3" />
        {absStr} (+{Math.round(pct)}%)
      </span>
    )
  }

  return (
    <span
      className="flex items-center gap-0.5 text-green-700 dark:text-green-400 text-[11px] sm:text-[10px] font-semibold"
      title={`${absStr} from baseline`}
    >
      <TrendingDown className="h-3 w-3" />
      {absStr} ({Math.round(pct)}%)
    </span>
  )
}

export interface InsightStatCardProps {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

export function InsightStatCard({
  icon,
  label,
  value,
  valueClass,
  hint,
}: InsightStatCardProps) {
  return (
    <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
        {icon} {label}
      </div>
      <div className={cn('mt-1.5 font-mono text-xl font-bold', valueClass ?? 'text-foreground')}>
        {value}
      </div>
      {hint && <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>}
    </div>
  )
}

export function buildInvestigateHref(
  investigateUrl: string | null | undefined,
  activeServiceId: string | null | undefined,
): string | null {
  if (!investigateUrl) return null
  const url = new URL(investigateUrl, 'http://localhost')
  if (!url.searchParams.has('service') && activeServiceId) {
    url.searchParams.set('service', activeServiceId)
  }
  return url.pathname + url.search
}

export interface InsightCardSkeletonProps {
  title: string
  description?: string
  loadingMessage?: string
}

export function InsightCardSkeleton({
  title,
  description,
  loadingMessage = 'Analyzing log patterns…',
}: InsightCardSkeletonProps) {
  return (
    <Card className="h-full flex flex-col min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto] opacity-90">
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <Loader2 className="h-5 w-5 shrink-0 text-muted-foreground animate-spin" />
            <CardTitle className="text-base leading-tight">{title}</CardTitle>
          </div>
        </div>
        {description && (
          <CardDescription className="text-xs mt-1">{description}</CardDescription>
        )}
      </CardHeader>

      <CardContent className="flex-1 flex flex-col items-center justify-center pt-0 pb-6 text-xs text-muted-foreground gap-1.5">
        <span>{loadingMessage}</span>
      </CardContent>
    </Card>
  )
}

export const SEVERITY_ICON = {
  clean: CheckCircle,
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  error: AlertCircle,
}

// Icon colors use the -600 ramp on yellow/blue to clear WCAG 1.4.3
// 3:1 non-text contrast on the white card background (yellow-500 ~2.6:1,
// blue-500 ~3.5:1 marginal; -600 lands at ~4.5:1 and ~5:1 respectively).
export const SEVERITY_ICON_COLOR = {
  clean: 'text-green-500',
  info: 'text-blue-600',
  warning: 'text-yellow-600',
  critical: 'text-red-500',
  error: 'text-red-600',
}

// Exported so the /insights section headers can reuse the exact same severity
// palette for their per-section rollup chips (keeps card + chip colours in one
// place). Keys match the backend severity ramp.
export const SEVERITY_BADGE_CLASS = {
  clean: 'bg-green-50 text-green-700 border-green-200 dark:bg-green-950/30 dark:text-green-400 dark:border-green-800',
  info: 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400 dark:border-blue-800',
  warning: 'bg-yellow-50 text-yellow-700 border-yellow-200 dark:bg-yellow-950/30 dark:text-yellow-400 dark:border-yellow-800',
  critical: 'bg-red-50 text-red-700 border-red-200 dark:bg-red-950/30 dark:text-red-400 dark:border-red-800',
  error: 'bg-red-100 text-red-800 border-red-300 dark:bg-red-950/50 dark:text-red-400 dark:border-red-800',
}
