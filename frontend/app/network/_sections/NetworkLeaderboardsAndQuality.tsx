'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { FilterValueCell, PopLabel } from '@/components/DataTable'
import type { components, PlotlyLayout, PlotlyTrace } from '@/types/api'
import { formatCount, resolveCountryName } from '@/lib/utils'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { NetworkQualityHelp } from './NetworkHelp'
import { HEATMAP_LAYOUT, NetworkKpiGrid } from './NetworkKpiGrid'
import {
  asnColumns,
  LeaderboardStatusBody,
  metroColumns,
  NetworkLeaderboardsSection,
  type AsnRow,
  type MetroRow,
} from './NetworkLeaderboardsSection'

export {
  asnColumns,
  HEATMAP_LAYOUT,
  LeaderboardStatusBody,
  metroColumns,
  NetworkKpiGrid,
  NetworkLeaderboardsSection,
  type AsnRow,
  type MetroRow,
}

const PlotlyChart = dynamic(() => import('@/components/PlotlyChart').then((mod) => mod.PlotlyChart), {
  ssr: false, loading: () => <Skeleton className="w-full h-[300px]" />,
})

export const QUALITY_SCATTER_LAYOUT: PlotlyLayout = { xaxis: { title: { text: 'TCP RTT (ms)' } }, yaxis: { title: { text: 'TTFB (ms)' } } }

export type QualityBarRow = components['schemas']['NetworkQualityBarRow']

export function RttBarList({ rows, filterColumn }: { rows: readonly QualityBarRow[]; filterColumn?: string }) {
  if (!rows?.length) return <div className="p-6 text-center text-sm text-muted-foreground">No data in this range</div>
  const max = Math.max(...rows.map((r) => r.rtt_ms), 1)
  return (
    <div className="space-y-1.5 p-3">
      {rows.map((r) => {
        const filterVal = r.value ?? r.label
        const display =
          filterColumn === 'pop' ? (
            <PopLabel code={filterVal} />
          ) : filterColumn === 'country' ? (
            resolveCountryName(r.label)
          ) : (
            r.label
          )
        return (
          <div key={filterVal} className="flex items-center gap-2 text-xs">
            <div className="w-48 truncate shrink-0" title={typeof display === 'string' ? display : r.label}>
              {filterColumn ? <FilterValueCell filters={[{ column: filterColumn, value: filterVal }]} display={display} /> : <span>{display}</span>}
            </div>
            <div className="flex-1 h-4 bg-muted/40 rounded-sm overflow-hidden">
              <div className="h-full bg-primary/70" style={{ width: `${(r.rtt_ms / max) * 100}%` }} />
            </div>
            <div className="w-16 text-right font-mono shrink-0">{r.rtt_ms.toFixed(1)}ms</div>
            <div className="w-16 text-right font-mono text-muted-foreground shrink-0">{formatCount(r.reqs ?? 0)}</div>
          </div>
        )
      })}
    </div>
  )
}

export function NetworkQualitySection({
  qualityData,
  isQualityLoadingInitial,
  isFetching,
  error,
  regionCountry,
  setRegionCountry,
  qualityScatterTrace,
}: {
  qualityData: components['schemas']['NetworkQualityResponse'] | undefined
  isQualityLoadingInitial: boolean
  isFetching: boolean
  error: unknown
  regionCountry: string
  setRegionCountry: (country: string) => void
  qualityScatterTrace: PlotlyTrace[]
}) {
  const qualityBreakdowns: Array<{
    title: string; loadingMessage: string; rows: readonly QualityBarRow[]; filterColumn: string; headerAction?: React.ReactNode
  }> = [
    { title: 'Avg RTT by Country', loadingMessage: 'Analyzing RTT by country...', rows: qualityData?.by_country ?? [], filterColumn: 'country' },
    { title: 'Avg RTT by ASN', loadingMessage: 'Analyzing RTT by ASN...', rows: qualityData?.by_asn ?? [], filterColumn: 'asn' },
    {
      title: 'Avg RTT by Region', loadingMessage: 'Analyzing RTT by region...', rows: qualityData?.by_region ?? [], filterColumn: 'region',
      headerAction: (
        <select
          aria-label="Region breakdown country" value={regionCountry}
          onChange={(e) => setRegionCountry(e.target.value)} className="h-7 rounded-md border bg-background px-2 text-xs"
        >
          {(qualityData?.countries ?? []).length === 0 && <option value={regionCountry}>{resolveCountryName(regionCountry)}</option>}
          {(qualityData?.countries ?? []).map((c) => <option key={c} value={c}>{resolveCountryName(c)}</option>)}
        </select>
      ),
    },
    { title: 'Avg RTT by POP', loadingMessage: 'Analyzing RTT by POP...', rows: qualityData?.by_pop ?? [], filterColumn: 'pop' },
  ]

  return (
    <div className="space-y-4 min-h-[400px] [contain-intrinsic-size:400px]">
      <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Network Quality</h2>
      {isQualityLoadingInitial ? (
        <SkeletonGrid count={4} height="180px" className="rounded-xl" />
      ) : (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {qualityBreakdowns.map((b) => (
              <AnalyticsCard
                key={b.title} title={b.title} contentClassName="p-0" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
                isFetching={isFetching} error={error as AnalyticsCardError | null}
                loadingMessage={b.loadingMessage} className="min-h-[240px]" headerAction={b.headerAction}
              >
                <RttBarList rows={b.rows} filterColumn={b.filterColumn} />
              </AnalyticsCard>
            ))}
          </div>
          {qualityScatterTrace.length > 0 && (
            <AnalyticsCard
              title="RTT vs TTFB" helpTitle="Network Quality" helpContent={<NetworkQualityHelp />}
              isFetching={isFetching} error={error as AnalyticsCardError | null}
              loadingMessage="Plotting RTT vs TTFB correlation..." className="min-h-[380px]"
            >
              <PlotlyChart data={qualityScatterTrace} layout={QUALITY_SCATTER_LAYOUT} height={360} />
            </AnalyticsCard>
          )}
        </>
      )}
    </div>
  )
}
