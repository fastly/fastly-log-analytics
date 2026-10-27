'use client'

import React, { useState } from 'react'
import { Badge } from '@/components/ui/badge'
import {
  AlertCircle,
  ArrowUpRight,
  HelpCircle,
  LineChart,
  MapIcon,
  ScanSearch,
} from 'lucide-react'
import Link from 'next/link'
import dynamic from 'next/dynamic'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn, formatCount, formatCountMaybe, keyByOccurrence } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import type { InsightCardData } from '@/types/api'
import {
  BotFindingModal,
  CacheCollapseModal,
  ImpossibleDistanceModal,
  ScriptedTrafficModal,
} from './InsightDetailModal'
import {
  buildInvestigateHref,
  DeltaIndicator,
  InsightCardSkeleton,
  InsightStatCard,
  SEVERITY_BADGE_CLASS,
  SEVERITY_ICON,
  SEVERITY_ICON_COLOR,
} from './shared'
import type {
  BotFindingModalData,
  ImpossibleDistanceData,
  ImpossibleDistanceItem,
  RepeatedPatternsFpItem,
  RepeatedPatternsItem,
  ScriptedTrafficData,
} from './types'

export {
  BotFindingModal,
  buildInvestigateHref,
  CacheCollapseModal,
  DeltaIndicator,
  ImpossibleDistanceModal,
  InsightCardSkeleton,
  InsightStatCard,
  ScriptedTrafficModal,
  SEVERITY_BADGE_CLASS,
  SEVERITY_ICON,
  SEVERITY_ICON_COLOR,
}
export * from './types'

export interface InsightItemRowProps {
  /** The owning card; its `id` fixes the type of `items[index].meta`. */
  insight: InsightCardData
  index: number
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
  onBotFindingClick?: (data: BotFindingModalData) => void
}

export function InsightItemRow({
  insight,
  index,
  onMapClick,
  onCacheCollapseClick,
  onScriptedTrafficClick,
  onBotFindingClick,
}: InsightItemRowProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const item = insight.items[index]

  const investigateHref = React.useMemo(
    () => buildInvestigateHref(item.investigate_url, activeServiceId),
    [item.investigate_url, activeServiceId],
  )

  return (
    <div
      data-testid="insight-item-row"
      className="flex items-center justify-between text-xs p-2 rounded-md bg-muted/50 gap-2"
    >
      <div className="flex flex-col min-w-0 flex-1">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="font-medium truncate">{item.label}</span>
          {insight.id === 'impossible_distance' && onMapClick && (
            <MapAction item={insight.items[index]} onMapClick={onMapClick} />
          )}
          {(insight.id === 'cache_collapse' || insight.id === 'cacheability_regression') &&
            onCacheCollapseClick && (
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
                      onBotFindingClick({
                        insightId: 'crawler_disparity',
                        item: insight.items[index],
                      })
                      break
                    case 'stale_browser_version':
                      onBotFindingClick({
                        insightId: 'stale_browser_version',
                        item: insight.items[index],
                      })
                      break
                    case 'orphaned_deep_crawl':
                      onBotFindingClick({
                        insightId: 'orphaned_deep_crawl',
                        item: insight.items[index],
                      })
                      break
                    case 'residential_fingerprint_dispersion':
                      onBotFindingClick({
                        insightId: 'residential_fingerprint_dispersion',
                        item: insight.items[index],
                      })
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
          <span className="text-[11px] sm:text-[10px] text-muted-foreground truncate">
            {item.tooltip}
          </span>
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
            {item.baseline_label || 'baseline'}:{' '}
            {formatCount(item.baseline_val, { maximumFractionDigits: 1 })} {item.unit}
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

function MapAction({
  item,
  onMapClick,
}: {
  item: ImpossibleDistanceItem
  onMapClick: (data: ImpossibleDistanceData) => void
}) {
  const { client_lat, client_lon, pop_lat, pop_lon, pop, tcp_rtt, city, country } = item.meta
  if (
    !Number.isFinite(client_lat) ||
    client_lat === null ||
    client_lon === null ||
    tcp_rtt === null
  ) {
    return null
  }
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

export interface InsightDataModalProps {
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
      <DialogContent
        data-testid="data-modal-open"
        className="max-w-2xl max-h-[85vh] flex flex-col"
      >
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

export interface InsightCardProps {
  insight: InsightCardData
  windowHours: string
  baselineHours: string
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
  const [selectedBotFindingItem, setSelectedBotFindingItem] =
    useState<BotFindingModalData | null>(null)

  const Icon = SEVERITY_ICON[insight.severity as keyof typeof SEVERITY_ICON] || AlertCircle
  const iconColor =
    SEVERITY_ICON_COLOR[insight.severity as keyof typeof SEVERITY_ICON_COLOR] ||
    'text-muted-foreground'
  const badgeClass =
    SEVERITY_BADGE_CLASS[insight.severity as keyof typeof SEVERITY_BADGE_CLASS] || ''

  return (
    <>
      <Card
        data-testid="insight-card"
        className="h-full flex flex-col min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]"
      >
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <Icon className={cn('h-5 w-5 shrink-0', iconColor)} />
              <CardTitle className="text-base leading-tight">{insight.title}</CardTitle>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge
                variant="outline"
                className={cn(
                  'text-[11px] sm:text-[10px] uppercase tracking-wider border',
                  badgeClass,
                )}
              >
                {insight.severity}
              </Badge>
              <Button
                variant="ghost"
                size="icon"
                aria-label="How this insight works"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => {
                  setHelpEverOpened(true)
                  setIsHelpOpen(true)
                }}
                title="How this works"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <CardDescription className="text-xs mt-1">{insight.description}</CardDescription>
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
