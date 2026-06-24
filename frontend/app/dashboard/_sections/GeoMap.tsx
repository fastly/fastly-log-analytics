'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { cn } from '@/lib/utils'

// ChoroplethMap pulls in d3-geo and the world-110m topojson. Static-import
// blocked the dashboard's initial JS parse/eval; dynamic-import slices it
// off the critical path so the rest of the page paints immediately.
// ssr:false because d3-geo uses canvas/SVG measurement APIs that don't
// work in the server-render pass.
const ChoroplethMap = dynamic(
  () => import('@/components/Map/ChoroplethMap').then((m) => ({ default: m.ChoroplethMap })),
  {
    ssr: false,
    loading: () => (
      <div
        data-empty-placeholder="true"
        className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded"
        aria-busy="true"
      >
        <span className="text-muted-foreground text-xs animate-pulse">Loading map…</span>
      </div>
    ),
  },
)

export interface GeoMapProps {
  isReady: boolean
  isLoadingAggs: boolean
  isFetchingAggs: boolean
  aggregates: any
  catalog: any
  onCountryClick: (countryName: string) => void
}

export function GeoMap({
  isReady,
  isLoadingAggs,
  isFetchingAggs,
  aggregates,
  catalog,
  onCountryClick,
}: GeoMapProps) {
  return (
    <div className={cn("border rounded-lg p-4 flex flex-col transition-opacity duration-100", isFetchingAggs && "opacity-40 pointer-events-none")}>
      {/* text-foreground anchors the card title to the high-contrast
          token — without it the h3 inherited a muted color from a
          parent and axe flagged it (2.7:1, below AA 4.5:1). */}
      <h3 className="text-sm font-medium mb-4 text-foreground">Requests by Country</h3>
      {(!isReady || !aggregates) || (isFetchingAggs && (!aggregates?.map_data || aggregates.map_data.length === 0)) ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/20 rounded-md">
          <span className="text-muted-foreground text-sm animate-pulse">
            {!isReady ? 'Initializing...' : 'Mapping traffic...'}
          </span>
        </div>
      ) : !aggregates?.map_data || aggregates.map_data.length === 0 ? (
        <div data-empty-placeholder="true" className="flex-1 min-h-[300px] flex items-center justify-center bg-muted/10 border border-dashed rounded-md">
          {/* text-foreground (not text-muted-foreground) + remove
              opacity-70 on the detail line — both were below AA
              contrast on bg-muted/10. */}
          <div className="flex flex-col items-center text-foreground text-center px-4">
            <span className="text-sm font-medium mb-1">No data available</span>
            <span className="text-[10px] text-muted-foreground">
              {(() => {
                const countryField = (catalog?.fields as any[])?.find(f => f.id === 'country')
                const groupId = countryField?.group
                if (groupId) {
                  const groupMeta = (catalog?.groups as any[])?.find(g => g.id === groupId)
                  if (groupMeta) {
                    return `Requires ${groupMeta.label} fields to be enabled in Fastly logging.`
                  }
                }
                return "Requires Geolocation fields to be enabled in Fastly logging."
              })()}
            </span>
          </div>
        </div>
      ) : (
        <ChoroplethMap
          data={aggregates?.map_data || []}
          className="flex-1 min-h-[300px]"
          onCountryClick={onCountryClick}
        />
      )}
    </div>
  )
}
