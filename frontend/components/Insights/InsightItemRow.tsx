import React from 'react'
import Link from 'next/link'
import { ArrowUpRight, MapIcon, LineChart, ScanSearch } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { DeltaIndicator } from '@/components/DeltaIndicator'
import { useServiceStore } from '@/stores/serviceStore'
import { InsightCardData } from '@/types/api'
import type {
  BotFindingModalData,
  ImpossibleDistanceData,
  ImpossibleDistanceItem,
  RepeatedPatternsFpItem,
  RepeatedPatternsItem,
  ScriptedTrafficData,
} from './types'

import { formatCount, formatCountMaybe } from '@/lib/format'
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

  const investigateHref = React.useMemo(() => {
    if (!item.investigate_url) return null
    const url = new URL(item.investigate_url, 'http://localhost')
    if (!url.searchParams.has('service') && activeServiceId) {
      url.searchParams.set('service', activeServiceId)
    }
    return url.pathname + url.search
  }, [item.investigate_url, activeServiceId])

  return (
    <div className="flex items-center justify-between text-xs p-2 rounded-md bg-muted/50 gap-2">
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
