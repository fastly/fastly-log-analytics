'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
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
import { cn, keyByOccurrence } from '@/lib/utils'
import { AlertTriangle, Info, CheckCircle, AlertCircle, HelpCircle, Loader2, ScanSearch, Gauge, Timer, Activity, Repeat, Hash, Users, Clock } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { formatCount } from '@/lib/format'
import { formatDuration } from '@/lib/date'
import { InsightItemRow } from './InsightItemRow'
import { BotFindingModalData, ImpossibleDistanceData, InsightStatCard as StatCard, ScriptedTrafficData } from './shared'
import { CacheCollapseModal } from './CacheCollapseModal'
import { BotFindingModal } from './BotFindingModal'

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
  () => import('./ImpossibleDistanceModal').then((m) => m.ImpossibleDistanceModal),
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
