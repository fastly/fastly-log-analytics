import React from 'react'
import Link from 'next/link'
import { ArrowUpRight, MapIcon, LineChart, ScanSearch } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { DeltaIndicator } from '@/components/DeltaIndicator'
import { useServiceStore } from '@/stores/serviceStore'
import { InsightItem } from '@/types/api'
import { ImpossibleDistanceData, ScriptedTrafficData } from './types'

interface InsightItemRowProps {
  item: InsightItem
  insightId: string
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
  onScriptedTrafficClick?: (data: ScriptedTrafficData) => void
}

export function InsightItemRow({ item, insightId, onMapClick, onCacheCollapseClick, onScriptedTrafficClick }: InsightItemRowProps) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

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
          {insightId === 'impossible_distance'
            && Number.isFinite(item.meta?.client_lat)
            && Number.isFinite(item.meta?.pop_lat)
            && Number.isFinite(item.meta?.pop_lon)
            && onMapClick && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Show ${item.label} on map`}
              className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
              onClick={() => {
                if (!item.meta) return
                onMapClick({
                  label: item.label,
                  client_lat: item.meta.client_lat,
                  client_lon: item.meta.client_lon,
                  pop_lat: item.meta.pop_lat,
                  pop_lon: item.meta.pop_lon,
                  pop: item.meta.pop,
                  tcp_rtt: item.meta.tcp_rtt,
                  distance_km: item.current_val || 0,
                  max_km: item.baseline_val || 0,
                  city: item.meta.city,
                  country: item.meta.country,
                })
              }}
              title="View on Map"
            >
              <MapIcon className="h-3 w-3" aria-hidden="true" />
            </Button>
          )}
          {(insightId === 'cache_collapse' || insightId === 'cacheability_regression') && onCacheCollapseClick && (
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
          {(insightId === 'repeated_patterns' || insightId === 'repeated_patterns_fp') && item.meta && onScriptedTrafficClick && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Why ${item.label} was flagged`}
              className="h-6 w-6 text-primary hover:text-primary/80 shrink-0"
              onClick={() => {
                const m = item.meta
                if (!m) return
                onScriptedTrafficClick({
                  label: item.label,
                  score: m.score ?? 0,
                  cv: m.cv ?? 0,
                  modal_frac: m.modal_frac ?? 0,
                  mean_interval_s: m.mean_interval_s ?? item.current_val ?? 0,
                  stddev_s: m.stddev_s ?? item.baseline_val ?? 0,
                  mode_gap_s: m.mode_gap_s ?? null,
                  n_gaps: m.n_gaps ?? 0,
                  n_events: m.n_events ?? 0,
                  span_s: m.span_s ?? 0,
                  rps: m.rps ?? 0,
                  distinct_ua: m.distinct_ua ?? 0,
                  distinct_ip: m.distinct_ip,
                })
              }}
              title="Why we flagged this"
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
            {item.current_val?.toLocaleString(undefined, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        </div>
        {item.baseline_val != null && (
          <span className="text-[11px] sm:text-[10px] text-muted-foreground tabular-nums">
            {item.baseline_label || 'baseline'}: {item.baseline_val.toLocaleString(undefined, { maximumFractionDigits: 1 })} {item.unit}
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
