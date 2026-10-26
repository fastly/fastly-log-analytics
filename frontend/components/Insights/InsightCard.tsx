'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
import Link from 'next/link'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Badge } from '@/components/ui/badge'
import { InsightCardData } from '@/types/api'
import { useServiceStore } from '@/stores/filterStore'
import { cn, keyByOccurrence, formatCount, formatCountMaybe, formatDuration } from "@/lib/utils";
import { AlertTriangle, Info, CheckCircle, AlertCircle, HelpCircle, Loader2, ScanSearch, Gauge, Timer, Activity, Repeat, Hash, Users, Clock, TrendingUp, TrendingDown, Minus, ArrowUpRight, MapIcon, LineChart } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { CacheCollapseModal, BotFindingModal } from './InsightDetailModal'

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

const StatCard = InsightStatCard

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

interface InsightItemRowProps {
  /** The owning card; its `id` fixes the type of `items[index].meta`. */
  insight: InsightCardData
  index: number
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
  onBotFindingClick?: (data: BotFindingModalData) => void
}

export function InsightItemRow({ insight, index, onMapClick, onCacheCollapseClick, onScriptedTrafficClick, onBotFindingClick }: InsightItemRowProps) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const item = insight.items[index]

  const investigateHref = React.useMemo(
    () => buildInvestigateHref(item.investigate_url, activeServiceId),
    [item.investigate_url, activeServiceId],
  )

  return (
    <div data-testid="insight-item-row" className="flex items-center justify-between text-xs p-2 rounded-md bg-muted/50 gap-2">
      <div className="flex flex-col min-w-0 flex-1">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="font-medium truncate">{item.label}</span>
          {insight.id === 'impossible_distance' && onMapClick && (
            <MapAction item={insight.items[index]} onMapClick={onMapClick} />
          )}
          {(insight.id === 'cache_collapse' || insight.id === 'cacheability_regression') && onCacheCollapseClick && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Analyze ${item.label} cache behavior`}
              className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
              onClick={() => {
                onCacheCollapseClick(item.label)
              }}
              title="View Cache Analysis"
            >
              <LineChart className="h-3 w-3" aria-hidden="true" />
            </Button>
          )}
          {insight.id === 'repeated_patterns' && onScriptedTrafficClick && (
            <ScriptedAction
              item={insight.items[index]}
              distinct={{ distinct_ua: insight.items[index].meta.distinct_ua }}
              onScriptedTrafficClick={onScriptedTrafficClick}
            />
          )}
          {insight.id === 'repeated_patterns_fp' && onScriptedTrafficClick && (
            <ScriptedAction
              item={insight.items[index]}
              distinct={{ distinct_ua: 0, distinct_ip: insight.items[index].meta.distinct_ip }}
              onScriptedTrafficClick={onScriptedTrafficClick}
            />
          )}
          {(insight.id === 'crawler_disparity' ||
            insight.id === 'stale_browser_version' ||
            insight.id === 'orphaned_deep_crawl' ||
            insight.id === 'residential_fingerprint_dispersion') &&
            onBotFindingClick && (
              <Button
                variant="ghost"
                size="icon"
                aria-label={`View finding details for ${item.label}`}
                className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
                onClick={() => {
                  switch (insight.id) {
                    case 'crawler_disparity':
                      onBotFindingClick({ insightId: 'crawler_disparity', item: insight.items[index] })
                      break
                    case 'stale_browser_version':
                      onBotFindingClick({ insightId: 'stale_browser_version', item: insight.items[index] })
                      break
                    case 'orphaned_deep_crawl':
                      onBotFindingClick({ insightId: 'orphaned_deep_crawl', item: insight.items[index] })
                      break
                    case 'residential_fingerprint_dispersion':
                      onBotFindingClick({ insightId: 'residential_fingerprint_dispersion', item: insight.items[index] })
                      break
                  }
                }}
                title="View Finding Details"
              >
                <ScanSearch className="h-3 w-3" aria-hidden="true" />
              </Button>
            )}
        </div>
        {item.tooltip && (
          <span className="text-[11px] sm:text-[10px] text-muted-foreground truncate">{item.tooltip}</span>
        )}
      </div>
      <div className="flex flex-col items-end gap-0.5 shrink-0">
        <div className="flex items-center gap-1.5">
          <DeltaIndicator current={item.current_val ?? 0} baseline={item.baseline_val} />
          <span className="font-bold tabular-nums">
            {formatCountMaybe(item.current_val, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        </div>
        {item.baseline_val != null && (
          <span className="text-[11px] sm:text-[10px] text-muted-foreground tabular-nums">
            {item.baseline_label || 'baseline'}: {formatCount(item.baseline_val, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        )}
        {investigateHref && (
          <Link
            href={investigateHref}
            className="text-primary hover:underline flex items-center gap-0.5 text-[11px] sm:text-[10px] mt-0.5"
            target="_blank"
            rel="noopener noreferrer"
          >
            Investigate <ArrowUpRight className="h-2.5 w-2.5" aria-hidden="true" />
          </Link>
        )}
      </div>
    </div>
  )
}

function MapAction({ item, onMapClick }: { item: ImpossibleDistanceItem; onMapClick: (data: ImpossibleDistanceData) => void }) {
  const { client_lat, client_lon, pop_lat, pop_lon, pop, tcp_rtt, city, country } = item.meta
  if (!Number.isFinite(client_lat) || client_lat === null || client_lon === null || tcp_rtt === null) return null
  if (!Number.isFinite(pop_lat) || !Number.isFinite(pop_lon)) return null
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`Show ${item.label} on map`}
      className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
      onClick={() =>
        onMapClick({
          label: item.label,
          client_lat,
          client_lon,
          pop_lat,
          pop_lon,
          pop: pop ?? '',
          tcp_rtt,
          distance_km: item.current_val || 0,
          max_km: item.baseline_val || 0,
          city: city ?? undefined,
          country: country ?? undefined,
        })
      }
      title="View on Map"
    >
      <MapIcon className="h-3 w-3" aria-hidden="true" />
    </Button>
  )
}

function ScriptedAction({
  item,
  distinct,
  onScriptedTrafficClick,
}: {
  item: RepeatedPatternsItem | RepeatedPatternsFpItem
  distinct: Pick<ScriptedTrafficData, 'distinct_ua' | 'distinct_ip'>
  onScriptedTrafficClick: (data: ScriptedTrafficData) => void
}) {
  const m = item.meta
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`Why ${item.label} was flagged`}
      className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
      onClick={() =>
        onScriptedTrafficClick({
          label: item.label,
          score: m.score,
          cv: m.cv,
          modal_frac: m.modal_frac,
          mean_interval_s: m.mean_interval_s,
          stddev_s: m.stddev_s,
          mode_gap_s: m.mode_gap_s,
          n_gaps: m.n_gaps,
          n_events: m.n_events,
          span_s: m.span_s,
          rps: m.rps,
          ...distinct,
        })
      }
      title="Why we flagged this"
    >
      <ScanSearch className="h-3 w-3" aria-hidden="true" />
    </Button>
  )
}

interface ScriptedTrafficModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  data: ScriptedTrafficData | null
}

// Inter-arrival gap: keep sub-90s readable in seconds, humanize longer cadences.
function formatInterval(sec: number | null): string {
  if (sec == null) return '—'
  if (sec < 90) return `${formatCount(sec, { maximumFractionDigits: 1 })}s`
  return formatDuration(Math.max(0, sec))
}

export function ScriptedTrafficModal({ isOpen, onOpenChange, data }: ScriptedTrafficModalProps) {
  if (!data) return null

  const modalPct = Math.round(data.modal_frac * 100)
  const span = formatDuration(Math.max(0, data.span_s))
  // Score colour tracks the backend severity bands (≥90 critical, ≥70 warning).
  const scoreColor =
    data.score >= 90 ? 'text-red-500' : data.score >= 70 ? 'text-amber-500' : 'text-blue-500'

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            <ScanSearch className="h-5 w-5 text-primary shrink-0" />
            <span className="truncate max-w-[90%]">Why we flagged this: {data.label}</span>
          </DialogTitle>
          <DialogDescription>
            Evidence that this client&apos;s request timing is machine-regular rather than human.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">
          {/* Plain-language verdict assembled from the evidence below. */}
          <div className="p-4 rounded-lg bg-muted/30 border border-border">
            <p className="text-sm leading-relaxed">
              <span className="font-semibold">{data.label}</span> made{' '}
              <span className="font-semibold">{formatCount(data.n_events)}</span> requests over{' '}
              <span className="font-semibold">{span}</span> on a near-constant{' '}
              <span className="font-semibold">~{formatInterval(data.mean_interval_s)}</span> cadence
              {' '}— <span className="font-semibold">{modalPct}%</span> of the gaps between requests
              were identical, with only <span className="font-semibold">{formatInterval(data.stddev_s)}</span>{' '}
              of jitter (σ). A regularity score of{' '}
              <span className={cn('font-semibold', scoreColor)}>{data.score}/100</span> is far more
              consistent with an automated script — a scraper, poller, or cron-scheduled job — than
              with human browsing.
            </p>
          </div>

          {/* Measured signals. */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Regularity Score"
              value={`${data.score}/100`}
              valueClass={scoreColor}
              hint="higher = more machine-like"
            />
            <StatCard
              icon={<Timer className="h-3 w-3" />}
              label="Mean Interval"
              value={formatInterval(data.mean_interval_s)}
              hint="avg. gap between requests"
            />
            <StatCard
              icon={<Activity className="h-3 w-3" />}
              label="Jitter (σ)"
              value={formatInterval(data.stddev_s)}
              hint="std. dev. of gaps"
            />
            <StatCard
              icon={<Repeat className="h-3 w-3" />}
              label="Modal Gap"
              value={formatInterval(data.mode_gap_s)}
              hint={`${modalPct}% of gaps match`}
            />
            <StatCard
              icon={<Hash className="h-3 w-3" />}
              label="Total Requests"
              value={formatCount(data.n_events)}
              hint={`${formatCount(data.n_gaps)} gaps measured`}
            />
            <StatCard
              icon={<Clock className="h-3 w-3" />}
              label="Observation Span"
              value={span}
            />
            <StatCard
              icon={<Gauge className="h-3 w-3" />}
              label="Requests / sec"
              value={formatCount(data.rps, { maximumFractionDigits: 3 })}
            />
            <StatCard
              icon={<Users className="h-3 w-3" />}
              label={data.distinct_ip != null ? 'Distinct IPs' : 'Distinct UAs'}
              value={formatCount(data.distinct_ip ?? data.distinct_ua)}
            />
          </div>

          {/* Why each signal matters — keep the score from being a black box. */}
          <div className="rounded-lg bg-muted/10 border border-border p-4">
            <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
              <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
            </h4>
            <ul className="space-y-2.5 text-xs leading-relaxed">
              <li>
                <span className="font-semibold">Cadence regularity (CV {data.cv.toFixed(2)}).</span>{' '}
                A low coefficient of variation means the time between requests barely changes — human
                traffic is bursty and irregular, scripts are not.
              </li>
              <li>
                <span className="font-semibold">Modal dominance ({modalPct}%).</span>{' '}
                The share of inter-arrival gaps that are exactly the modal interval
                {data.mode_gap_s != null ? <> (<span className="font-mono">{formatInterval(data.mode_gap_s)}</span>)</> : null}.
                A high share points to fixed-interval scheduling, like a cron job or polling loop.
              </li>
              <li>
                <span className="font-semibold">Volume &amp; span.</span>{' '}
                {formatCount(data.n_events)} requests over {span} ({formatCount(data.rps, { maximumFractionDigits: 3 })} req/s),
                sustained but below volumetric rate-limit thresholds — the pattern this insight is
                designed to catch.
              </li>
              <li>
                {data.distinct_ip != null ? (
                  <>
                    <span className="font-semibold">Distinct IPs ({formatCount(data.distinct_ip)}).</span>{' '}
                    This fingerprint appeared across multiple source addresses, which is why
                    IP-based detection alone would miss it. IP rotation with a stable TLS fingerprint
                    is a strong signal of coordinated scripted traffic.
                  </>
                ) : (
                  <>
                    <span className="font-semibold">Distinct user-agents ({formatCount(data.distinct_ua)}).</span>{' '}
                    Shown for context only — it is <span className="italic">not</span> a reason to dismiss
                    the flag. A high UA count combined with this regular timing usually signals a
                    UA-rotating scraper, which is <span className="font-semibold">more</span> suspicious,
                    not less.
                  </>
                )}
              </li>
            </ul>
          </div>
        </div>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}

interface InsightCardSkeletonProps {
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

interface InsightDataModalProps {
  insight: InsightCardData
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
  onBotFindingClick?: (data: BotFindingModalData) => void
}

export function InsightDataModal({
  insight,
  isOpen,
  onOpenChange,
  onMapClick,
  onCacheCollapseClick,
  onScriptedTrafficClick,
  onBotFindingClick,
}: InsightDataModalProps) {
  if (!insight.items || insight.items.length === 0) return null

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent data-testid="data-modal-open" className="max-w-2xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>All Data: {insight.title}</DialogTitle>
          <DialogDescription className="sr-only">
            Full list of records backing the “{insight.title}” insight.
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="flex-1 -mx-6 px-6">
          <div className="space-y-1.5 py-4">
            {keyByOccurrence(insight.items, (it) => it.label).map(({ key }, index) => (
              <InsightItemRow
                key={key}
                insight={insight}
                index={index}
                onMapClick={onMapClick}
                onCacheCollapseClick={onCacheCollapseClick}
                onScriptedTrafficClick={onScriptedTrafficClick}
                onBotFindingClick={onBotFindingClick}
              />
            ))}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  )
}

// Lazy-load the help modal: its index statically imports ~28KB of
// per-insight help copy (InsightHelpModal/sections/*) that otherwise ships
// in the /insights route chunk even though most users never open a help
// dialog. next/dynamic(ssr:false) defers that chunk to the first help-button
// click. The `helpEverOpened` latch below keeps the modal mounted after the
// first open (so Radix's close animation still plays) while still keeping
// the chunk off the cold-load critical path.
const InsightHelpModal = dynamic(
  () => import('./InsightHelpModal').then((m) => m.InsightHelpModal),
  { ssr: false },
)

const ImpossibleDistanceModal = dynamic(
  () => import('./InsightDetailModal').then((m) => m.ImpossibleDistanceModal),
  { ssr: false },
)

interface InsightCardProps {
  insight: InsightCardData
  windowHours: string
  baselineHours: string
}

const SEVERITY_ICON = {
  clean: CheckCircle,
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  error: AlertCircle,
}

// Icon colors use the -600 ramp on yellow/blue to clear WCAG 1.4.3
// 3:1 non-text contrast on the white card background (yellow-500 ~2.6:1,
// blue-500 ~3.5:1 marginal; -600 lands at ~4.5:1 and ~5:1 respectively).
const SEVERITY_ICON_COLOR = {
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

export function InsightCard({ insight, windowHours, baselineHours }: InsightCardProps) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)
  // Latches true on first help-button click so the lazy InsightHelpModal
  // mounts once and stays mounted (close animation), loading its chunk only
  // when a user actually asks for help.
  const [helpEverOpened, setHelpEverOpened] = useState(false)
  const [isDataModalOpen, setIsDataModalOpen] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)
  const [selectedCollapseUrl, setSelectedCollapseUrl] = useState<string | null>(null)
  const [selectedScriptedItem, setSelectedScriptedItem] = useState<ScriptedTrafficData | null>(null)
  const [selectedBotFindingItem, setSelectedBotFindingItem] = useState<BotFindingModalData | null>(null)

  const Icon = SEVERITY_ICON[insight.severity as keyof typeof SEVERITY_ICON] || AlertCircle
  const iconColor = SEVERITY_ICON_COLOR[insight.severity as keyof typeof SEVERITY_ICON_COLOR] || 'text-muted-foreground'
  const badgeClass = SEVERITY_BADGE_CLASS[insight.severity as keyof typeof SEVERITY_BADGE_CLASS] || ''

  return (
    <>
      <Card className="h-full flex flex-col min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]">
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <Icon className={cn('h-5 w-5 shrink-0', iconColor)} />
              <CardTitle className="text-base leading-tight">{insight.title}</CardTitle>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge
                variant="outline"
                className={cn('text-[11px] sm:text-[10px] uppercase tracking-wider border', badgeClass)}
              >
                {insight.severity}
              </Badge>
              <Button
                variant="ghost"
                size="icon"
                aria-label="How this insight works"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => { setHelpEverOpened(true); setIsHelpOpen(true) }}
                title="How this works"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <CardDescription className="text-xs mt-1">
            {insight.description}
          </CardDescription>
        </CardHeader>

        <CardContent className="flex-1 flex flex-col pt-0">
          <p className="text-sm font-medium my-2 leading-snug">{insight.summary}</p>

          {insight.items && insight.items.length > 0 && (
            <div className="space-y-1.5 mt-1">
              {keyByOccurrence(insight.items.slice(0, 5), (it) => it.label).map(({ key }, index) => (
                <InsightItemRow
                  key={key}
                  insight={insight}
                  index={index}
                  onMapClick={(data) => setSelectedMapItem(data)}
                  onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
                  onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
                  onBotFindingClick={(data) => setSelectedBotFindingItem(data)}
                />
              ))}
              {insight.items.length > 5 && (
                <div className="flex justify-center py-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-[11px] sm:text-[10px] h-6 px-2 text-muted-foreground hover:text-foreground"
                    onClick={() => setIsDataModalOpen(true)}
                  >
                    + Show {insight.items.length - 5} more
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {helpEverOpened && (
        <InsightHelpModal
          insightId={insight.id}
          isOpen={isHelpOpen}
          onOpenChange={setIsHelpOpen}
        />
      )}

      <InsightDataModal
        insight={insight}
        isOpen={isDataModalOpen}
        onOpenChange={setIsDataModalOpen}
        onMapClick={(data) => setSelectedMapItem(data)}
        onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
        onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
        onBotFindingClick={(data) => setSelectedBotFindingItem(data)}
      />

      <ImpossibleDistanceModal
        isOpen={!!selectedMapItem}
        onOpenChange={(open) => !open && setSelectedMapItem(null)}
        data={selectedMapItem}
      />

      <CacheCollapseModal
        isOpen={!!selectedCollapseUrl}
        onOpenChange={(open) => !open && setSelectedCollapseUrl(null)}
        url={selectedCollapseUrl}
        windowHours={windowHours}
        baselineHours={baselineHours}
      />

      <ScriptedTrafficModal
        isOpen={!!selectedScriptedItem}
        onOpenChange={(open) => !open && setSelectedScriptedItem(null)}
        data={selectedScriptedItem}
      />

      <BotFindingModal
        isOpen={!!selectedBotFindingItem}
        onOpenChange={(open) => !open && setSelectedBotFindingItem(null)}
        data={selectedBotFindingItem}
      />
    </>
  )
}
