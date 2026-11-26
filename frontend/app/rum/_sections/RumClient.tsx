'use client'

import React, { useState, useEffect, useMemo } from 'react'
import dynamic from 'next/dynamic'
import { useQuery } from '@tanstack/react-query'
import { BookOpen, Radio } from 'lucide-react'
import { CardErrorState } from '@/components/AnalyticsCard'
import { DashboardSkeleton, ReportLayout } from '@/components/ReportLayout'
import { RawWindowNote, readRawWindow } from '@/components/network/rollupLabels'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useIsAnalyst, useMounted } from '@/hooks/useBootstrap'
import { useRumStatus } from '@/hooks/useDashboardCards'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useReportPageState'
import { useTimezone } from '@/hooks/useSSE'
import { adminJsonResult } from '@/lib/api'
import { arrayOrEmpty, rangeLabelToHours, recordOrNull, type RangeWireArgs } from '@/lib/utils'
import type { FiltersPayload } from '@/types/api'
import {
  RumEmptyWindowCard,
  RumFaroVersionCard,
  RumNoDataBanner,
  RumStatusPanel,
  type RumAnalytics,
} from './RumStatusSection'
import { RumVitalsTopSection } from './RumVitalsSection'
import type { RumLiveEvent } from './RumTablesSection'

export {
  RumFaroVersionCard,
  RumStatusPanel,
  RumNoDataBanner,
  RumEmptyWindowCard,
  RumInstallationDialog,
  type RumFaroVersionCardProps,
  type RumVersionsResponse,
  type RumWire,
  type RumAnalytics,
} from './RumStatusSection'
export {
  buildEnvPieData,
  DONUT_CHART_LAYOUT,
  getThresholdColors,
  RumEnvironmentAndBeaconsSection,
  RumVitalsTopSection,
  STACKED_BAR_LAYOUT,
  TREND_CHART_LAYOUT,
  type CwvColors,
  type RumCwvDistribution,
} from './RumVitalsSection'
export {
  envName,
  RumClientResourceTablesSection,
  RumLiveActivitySection,
  RumWorstPagesAndErrorsSection,
  WORST_PAGE_COLUMNS,
  type RumJsError,
  type RumLiveEvent,
  type RumRawLog,
  type RumSlowestNetworkItem,
  type RumSlowestUrlItem,
  type RumWorstPage,
} from './RumTablesSection'

const LazyRumInstallationDialog = dynamic(
  () => import('./RumStatusSection').then((m) => m.RumInstallationDialog),
  { ssr: false },
)

const LazyRumWorstPagesAndErrorsSection = dynamic(
  () => import('./RumTablesSection').then((m) => m.RumWorstPagesAndErrorsSection),
  {
    ssr: false,
    loading: () => (
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6" aria-busy="true">
        <Skeleton className="h-[320px] w-full rounded-xl" />
        <Skeleton className="h-[320px] w-full rounded-xl" />
      </div>
    ),
  },
)

const LazyRumClientResourceTablesSection = dynamic(
  () => import('./RumTablesSection').then((m) => m.RumClientResourceTablesSection),
  {
    ssr: false,
    loading: () => (
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6" aria-busy="true">
        <Skeleton className="h-[320px] w-full rounded-xl" />
        <Skeleton className="h-[320px] w-full rounded-xl" />
      </div>
    ),
  },
)

const LazyRumEnvironmentAndBeaconsSection = dynamic(
  () => import('./RumVitalsSection').then((m) => m.RumEnvironmentAndBeaconsSection),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-6" aria-busy="true">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Skeleton className="h-[300px] w-full rounded-xl" />
          <Skeleton className="h-[300px] w-full rounded-xl" />
          <Skeleton className="h-[300px] w-full rounded-xl" />
        </div>
        <Skeleton className="h-[310px] w-full rounded-xl" />
      </div>
    ),
  },
)

const LazyRumLiveActivitySection = dynamic(
  () => import('./RumTablesSection').then((m) => m.RumLiveActivitySection),
  {
    ssr: false,
    loading: () => <Skeleton className="h-[360px] w-full rounded-xl" aria-busy="true" />,
  },
)

// Columns the RUM analytics API accepts (backend/high_scale/rum.py _FILTER_COLUMNS). The shared
// FilterBar carries request-log fields such as status across pages; both backends answer 400 for
// those on RUM, so they are dropped here instead of failing the whole report.
const RUM_FILTER_COLUMNS = new Set(['browser', 'os', 'device', 'country', 'pathname', 'city', 'region', 'pop', 'tls'])

export function rumSupportedFilters(filterPayload: FiltersPayload): FiltersPayload {
  return Object.fromEntries(
    Object.entries(filterPayload ?? {}).filter(([key]) => RUM_FILTER_COLUMNS.has(key.replace(/_\d+$/, ''))),
  )
}

export interface RumClientProps extends Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> {
  serviceId: string | null
  startTime: string | null
  endTime: string | null
  filterPayload: FiltersPayload
}

export function RumClient({
  serviceId,
  startTime,
  endTime,
  filterPayload: rawFilterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: RumClientProps) {
  const timezone = useTimezone()
  const filterPayload = useMemo(() => rumSupportedFilters(rawFilterPayload), [rawFilterPayload])
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId)
  // Zustand serves its initial (default 24h) range while hydrating, so fetching before mount runs the
  // heavy RUM queries for a window the URL-selected range immediately replaces (Trap #46).
  const mounted = useMounted()
  const queriesEnabled = mounted && !!serviceId && status?.enabled !== false

  const { data: analytics, isLoading, isError, refetch } = useAnalyticsReportQuery<RumAnalytics | null>({
    namespace: ['rum-analytics'],
    serviceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    queryFn: async ({ rangeBody, signal }) => {
      if (!serviceId) return null
      const params = new URLSearchParams()
      for (const [k, v] of Object.entries(rangeBody)) if (v) params.append(k, v)
      if (filterPayload && Object.keys(filterPayload).length > 0) params.append('filters', JSON.stringify(filterPayload))
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/analytics', RumAnalytics>(
        '/api/services/{service_id}/rum/analytics',
        `/api/services/${serviceId}/rum/analytics${qs ? `?${qs}` : ''}`,
        { signal },
      )
      if (!res.ok) throw new Error(res.errorText || `RUM analytics failed (HTTP ${res.status})`)
      return recordOrNull(res.data)
    },
    queryOptions: { enabled: queriesEnabled, refetchInterval: false },
  })

  const liveRange = useMemo(() => {
    const hours = relativeRange ? rangeLabelToHours(relativeRange) : isAutoRange ? 24 : null
    if (hours !== null && anchor) {
      const endMs = Date.parse(anchor)
      if (Number.isFinite(endMs)) {
        return {
          start: new Date(endMs - hours * 3_600_000).toISOString(),
          end: anchor,
        }
      }
    }
    return { start: startTime, end: endTime }
  }, [relativeRange, isAutoRange, anchor, startTime, endTime])

  const { data: liveEvents, isLoading: isLiveEventsLoading, isError: isLiveEventsError, refetch: refetchLiveEvents } = useQuery({
    queryKey: ['rum-live-events', serviceId, liveRange.start, liveRange.end, filterPayload],
    queryFn: async ({ signal }) => {
      if (!serviceId) return []
      const params = new URLSearchParams()
      if (liveRange.start) params.append('start_time', liveRange.start)
      if (liveRange.end) params.append('end_time', liveRange.end)
      if (filterPayload && Object.keys(filterPayload).length > 0) params.append('filters', JSON.stringify(filterPayload))
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/live-events', RumLiveEvent[]>(
        '/api/services/{service_id}/rum/live-events',
        qs ? `/api/services/${serviceId}/rum/live-events?${qs}` : `/api/services/${serviceId}/rum/live-events`,
        { signal },
      )
      if (!res.ok) throw new Error(res.errorText || `RUM live events failed (HTTP ${res.status})`)
      return arrayOrEmpty(res.data)
    },
    enabled: queriesEnabled,
    refetchInterval: false,
  })

  if (isStatusLoading) return <DashboardSkeleton />
  if (!status?.enabled) return <RumStatusPanel />
  if (isError) return (
    <CardErrorState
      title="RUM analytics unavailable"
      message="Could not load analytics for this time window."
      onRetry={() => void refetch()}
    />
  )
  if (isLoading || !analytics) return <DashboardSkeleton />

  if (analytics.no_data) {
    return <RumNoDataBanner message={analytics.message} beaconCount={analytics.beacon_count} />
  }

  if (analytics.beacon_count === 0) {
    return <RumEmptyWindowCard />
  }

  return (
    <div className="space-y-6">
      <RawWindowNote window={readRawWindow(analytics)} />
      <RumVitalsTopSection
        analytics={analytics}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />

      <LazyRumWorstPagesAndErrorsSection
        worstPages={analytics.worst_pages}
        errors={analytics.errors}
      />

      <LazyRumClientResourceTablesSection
        slowestUrls={analytics.slowest_urls ?? []}
        slowestNetworks={analytics.slowest_networks ?? []}
      />

      <LazyRumEnvironmentAndBeaconsSection
        analytics={analytics}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />

      {isLiveEventsError ? <CardErrorState
        title="Live activity unavailable"
        message="Could not load RUM live events for this time window."
        onRetry={() => void refetchLiveEvents()}
      /> : <LazyRumLiveActivitySection
        liveEvents={liveEvents}
        isLiveEventsLoading={isLiveEventsLoading}
        timezone={timezone}
        serviceId={serviceId}
      />}
    </div>
  )
}

export interface RumPageClientProps {
  initialServiceId?: string
}

function ServiceSync({ serviceId, onSync }: { serviceId: string | null; onSync: (id: string | null) => void }) {
  useEffect(() => { onSync(serviceId) }, [serviceId, onSync])
  return null
}

export function RumPageClient({ initialServiceId }: RumPageClientProps) {
  const [showInstructions, setShowInstructions] = useState(false)
  const [activeServiceId, setActiveServiceId] = useState<string | null>(initialServiceId || null)
  const isAnalyst = useIsAnalyst()
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()

  return (
    <>
      <ReportLayout
        title="Real User Monitoring"
        description="Monitor real user performance metrics including Core Web Vitals, JavaScript errors, and session analytics."
        icon={Radio}
        serviceId={initialServiceId}
        headerActions={
          <div className="flex items-center gap-3">
            <RumFaroVersionCard serviceId={activeServiceId} />
            {!isAnalyst && (
              <Button variant="outline" size="sm" onClick={() => setShowInstructions(true)} className="flex items-center gap-2">
                <BookOpen className="h-4 w-4" />
                Installation Instructions
              </Button>
            )}
          </div>
        }
      >
        {({ startTime, endTime, activeServiceId: sid, filterPayload }) => (
          <>
            <ServiceSync serviceId={sid} onSync={setActiveServiceId} />
            <RumClient
              serviceId={sid}
              startTime={startTime}
              endTime={endTime}
              filterPayload={filterPayload}
              relativeRange={relativeRange}
              isAutoRange={isAutoRange}
              anchor={anchor}
            />
          </>
        )}
      </ReportLayout>

      {!isAnalyst && showInstructions && (
        <LazyRumInstallationDialog open={showInstructions} onOpenChange={setShowInstructions} />
      )}
    </>
  )
}

export default RumPageClient
