import React from 'react'
import Link from 'next/link'
import { ArrowUpRight, MapIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { DeltaIndicator } from '@/components/DeltaIndicator'
import { InsightItem } from '@/types/api'
import { ImpossibleDistanceData } from './types'

interface InsightItemRowProps {
  item: InsightItem
  insightId: string
  onMapClick?: (data: ImpossibleDistanceData) => void
}

export function InsightItemRow({ item, insightId, onMapClick }: InsightItemRowProps) {
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
              className="h-4 w-4 text-primary hover:text-primary/80 shrink-0"
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
              <MapIcon className="h-3 w-3" />
            </Button>
          )}
        </div>
        {item.tooltip && (
          <span className="text-[10px] text-muted-foreground truncate">{item.tooltip}</span>
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
          <span className="text-[10px] text-muted-foreground tabular-nums">
            {item.baseline_label || 'baseline'}: {item.baseline_val.toLocaleString(undefined, { maximumFractionDigits: 1 })} {item.unit}
          </span>
        )}
        {item.investigate_url && (
          <Link
            href={item.investigate_url}
            className="text-primary hover:underline flex items-center gap-0.5 text-[10px] mt-0.5"
            target="_blank"
            rel="noopener noreferrer"
          >
            Investigate <ArrowUpRight className="h-2.5 w-2.5" />
          </Link>
        )}
      </div>
    </div>
  )
}
