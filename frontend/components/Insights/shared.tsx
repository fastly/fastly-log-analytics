'use client'

import React from 'react'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'
import { formatCount } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { InsightCardData } from '@/types/api'

type CardOf<Id extends InsightCardData['id']> = Extract<InsightCardData, { id: Id }>

export type ImpossibleDistanceItem = CardOf<'impossible_distance'>['items'][number]
export type RepeatedPatternsItem = CardOf<'repeated_patterns'>['items'][number]
export type RepeatedPatternsFpItem = CardOf<'repeated_patterns_fp'>['items'][number]

export type BotFindingModalData =
  | {
      insightId: 'crawler_disparity'
      item: CardOf<'crawler_disparity'>['items'][number]
    }
  | {
      insightId: 'stale_browser_version'
      item: CardOf<'stale_browser_version'>['items'][number]
    }
  | {
      insightId: 'orphaned_deep_crawl'
      item: CardOf<'orphaned_deep_crawl'>['items'][number]
    }
  | {
      insightId: 'residential_fingerprint_dispersion'
      item: CardOf<'residential_fingerprint_dispersion'>['items'][number]
    }

export interface ImpossibleDistanceData {
  label: string
  client_lat: number
  client_lon: number
  pop_lat: number
  pop_lon: number
  pop: string
  tcp_rtt: number
  distance_km: number
  max_km: number
  country?: string
  city?: string
}

export interface ScriptedTrafficData {
  label: string
  score: number
  cv: number
  modal_frac: number
  mean_interval_s: number
  stddev_s: number
  mode_gap_s: number | null
  n_gaps: number
  n_events: number
  span_s: number
  rps: number
  distinct_ua: number
  distinct_ip?: number
}

export interface InsightHelpModalProps {
  insightId: string
  isOpen: boolean
  onOpenChange: (open: boolean) => void
}

export interface InsightContent {
  title: string
  icon: React.ReactNode
  description: React.ReactNode
  diagram?: React.ReactNode
  fields: string[]
}

interface DeltaIndicatorProps {
  current: number
  baseline: number | null | undefined
}

export function DeltaIndicator({ current, baseline }: DeltaIndicatorProps) {
  if (baseline == null || baseline === 0) return null
  const pct = ((current - baseline) / Math.abs(baseline)) * 100
  const abs = current - baseline
  const absStr = abs > 0 ? `+${formatCount(abs, { maximumFractionDigits: 1 })}` : formatCount(abs, { maximumFractionDigits: 1 })

  if (Math.abs(pct) < 1) return <Minus className="h-3 w-3 text-muted-foreground" />

  if (pct > 0) return (
    <span className="flex items-center gap-0.5 text-red-600 dark:text-red-400 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingUp className="h-3 w-3" />
      {absStr} (+{Math.round(pct)}%)
    </span>
  )

  return (
    <span className="flex items-center gap-0.5 text-green-700 dark:text-green-400 text-[11px] sm:text-[10px] font-semibold" title={`${absStr} from baseline`}>
      <TrendingDown className="h-3 w-3" />
      {absStr} ({Math.round(pct)}%)
    </span>
  )
}

interface InsightStatCardProps {
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
