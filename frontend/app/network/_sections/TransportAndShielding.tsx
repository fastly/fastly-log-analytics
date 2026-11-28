'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import type { components } from '@/types/api'
import type { useColumnVisibility } from '@/hooks/useReportPageState'
import { downloadAsCsv, cn, formatCount } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import { AnalyticsCard, LazyMount, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Button } from '@/components/ui/button'
import { Shield, Download, Info } from 'lucide-react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ShieldingHelp } from './NetworkHelp'
import {
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  type ShieldingRowInput,
} from './shieldingConfig'
import { getShieldingLabels, SHIELDING_COLUMNS } from './shieldingColumns'

export {
  adjustShieldingRows,
  getShieldingLabels,
  SHIELDING_COLUMNS,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  type ShieldingRowInput,
}
export { TransportSection } from './TransportSection'

const ShieldingMap = dynamic(() => import('@/components/Map/ShieldingMap').then((mod) => mod.ShieldingMap), {
  ssr: false, loading: () => <Skeleton className="w-full h-[420px] rounded-xl" />,
})

type ShieldingAnalysisData = NonNullable<components['schemas']['NetworkHealthResponse']['shielding_analysis']>

export interface ShieldingSectionProps {
  shieldingData: ShieldingAnalysisData | null | undefined
  adjustedShieldingRows: ShieldingRowInput[]
  isShieldingLoadingInitial: boolean
  isFetching: boolean
  error: unknown
  minRequests: number
  setMinRequests: (val: number) => void
  shieldingVisibility: ReturnType<typeof useColumnVisibility>[0]
  setShieldingVisibility: ReturnType<typeof useColumnVisibility>[1]
  onShieldingVisChange: ReturnType<typeof useColumnVisibility>[2]
  isPridActive: boolean
}

export function ShieldingSection({
  shieldingData,
  adjustedShieldingRows,
  isShieldingLoadingInitial,
  isFetching,
  error,
  minRequests,
  setMinRequests,
  shieldingVisibility,
  setShieldingVisibility,
  onShieldingVisChange,
  isPridActive,
}: ShieldingSectionProps) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <AnalyticsCard
        title="Edge → Shield Transit Map" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null}
      >
        <LazyMount minHeight={420}>
          <ShieldingMap
            rows={adjustedShieldingRows} isLoading={isShieldingLoadingInitial}
            edgeOnly={Boolean(shieldingData?.edge_only) && !isPridActive}
            errored={Boolean(shieldingData?.error)} expandable
          />
        </LazyMount>
      </AnalyticsCard>

      <AnalyticsCard
        title="Shielding Analysis" contentClassName="p-0" helpTitle="Shielding Analysis" helpContent={<ShieldingHelp />}
        isFetching={isFetching} error={error as AnalyticsCardError | null}
        headerAction={
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-1.5">
              <span className="hidden sm:inline text-[11px] uppercase font-semibold tracking-wide text-muted-foreground">Min reqs</span>
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger render={<span className="relative inline-flex items-center text-muted-foreground hover:text-foreground transition-colors cursor-help" />}>
                    <Info className="w-3.5 h-3.5 opacity-60" />
                    <span className="sr-only">What does the minimum requests floor do?</span>
                  </TooltipTrigger>
                  <TooltipContent side="top" className="max-w-[260px] text-xs leading-relaxed">
                    Routes with fewer than this many edge→shield requests are greyed and never flagged as “suboptimal peering” — their transit median is too small a sample to trust. Raise it on busy services; lower it (or pick “No minimum”) to scrutinize quiet routes.
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <Select value={String(minRequests)} onValueChange={(v) => v && setMinRequests(Number(v))}>
                <SelectTrigger className="h-8 w-[124px] text-xs" aria-label="Minimum requests to flag a shielding route"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SHIELDING_MIN_REQUESTS_OPTIONS.map((o) => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <Button
              variant="outline" size="sm" className="h-8" disabled={!shieldingData?.has_data}
              onClick={() => {
                if (!adjustedShieldingRows.length) return
                downloadAsCsv(adjustedShieldingRows, ['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'distance_km', 'light_speed_rtt_ms', 'efficiency_ratio', 'anomaly_static', 'low_sample'], 'shielding_analysis.csv')
              }}
            >
              <Download className="w-3.5 h-3.5 mr-2" />Export
            </Button>
            <ColumnVisibilityDropdown
              columns={getShieldingLabels(['edge_pop', 'shield_pop', 'requests', 'p50_ms', 'p95_ms', 'p99_ms', 'light_speed_rtt_ms', 'efficiency_ratio'])}
              visibility={shieldingVisibility} onChange={onShieldingVisChange}
            />
          </div>
        }
      >
        <div className="flex flex-col h-[420px] overflow-hidden">
          {shieldingData?.has_data ? (
            <>
              <DataTable
                columns={SHIELDING_COLUMNS} data={adjustedShieldingRows} hideToolbar
                columnVisibility={shieldingVisibility} onColumnVisibilityChange={setShieldingVisibility}
                tableContainerClassName="max-h-[295px]"
              />
              {shieldingData?.truncated && (
                <p className="px-4 py-2 text-[11px] text-muted-foreground border-t bg-muted/20">
                  Showing {formatCount(shieldingData?.rows?.length ?? 0)} of {formatCount(Number(shieldingData.total_routes ?? 0))} routes — top by request volume and by transit overhead.
                </p>
              )}
            </>
          ) : (
            <div className="flex flex-col items-center justify-center py-12 h-full text-center px-4 gap-1">
              <Shield className={cn('h-8 w-8 mb-2 opacity-20', shieldingData?.error ? 'text-destructive opacity-30' : 'text-muted-foreground')} />
              {shieldingData?.error ? (
                <>
                  <p className="text-sm text-destructive font-medium">Shielding analysis unavailable</p>
                  <p className="text-xs text-muted-foreground max-w-sm">The analysis failed to compute for this window (server-side error, not an absence of data). Try again or narrow the time range.</p>
                </>
              ) : (shieldingData?.edge_only && !isPridActive) ? (
                <>
                  <p className="text-sm text-muted-foreground font-medium">Edge-only logging detected</p>
                  <p className="text-xs text-muted-foreground max-w-sm">Edge-to-shield transit analysis requires log lines from both edge and shield POPs. Your service is currently logging edge requests only — enable full (non-edge-only) logging to use this analysis.</p>
                </>
              ) : shieldingData?.requires_fields ? (
                <p className="text-sm text-muted-foreground italic">Required fields missing: {shieldingData.requires_fields.join(', ')}</p>
              ) : (
                <p className="text-sm text-muted-foreground italic">No shielding data detected in this time range.</p>
              )}
            </div>
          )}
        </div>
      </AnalyticsCard>
    </div>
  )
}
