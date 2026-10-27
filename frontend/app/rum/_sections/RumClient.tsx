'use client'

import React, { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BookOpen, Radio } from 'lucide-react'
import { DashboardSkeleton, ReportLayout } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import { useRumStatus } from '@/hooks/useDashboardCards'
import { useAnalyticsReportQuery, useReportRangeAnchor } from '@/hooks/useReportPageState'
import { useTimezone } from '@/hooks/useSSE'
import { adminJsonResult } from '@/lib/api'
import { arrayOrEmpty, recordOrNull, type RangeWireArgs } from '@/lib/utils'
import type { FiltersPayload } from '@/types/api'
import {
  RumEmptyWindowCard,
  RumFaroVersionCard,
  RumInstallationDialog,
  RumNoDataBanner,
  RumStatusPanel,
  type RumAnalytics,
  type RumFaroVersionCardProps,
  type RumVersionsResponse,
  type RumWire,
} from './RumStatusSection'
import {
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
import {
  envName,
  RumLiveActivitySection,
  RumWorstPagesAndErrorsSection,
  WORST_PAGE_COLUMNS,
  type RumJsError,
  type RumLiveEvent,
  type RumRawLog,
  type RumWorstPage,
} from './RumTablesSection'

export {
  RumFaroVersionCard,
  RumStatusPanel,
  RumNoDataBanner,
  RumEmptyWindowCard,
  RumInstallationDialog,
  RumVitalsTopSection,
  RumEnvironmentAndBeaconsSection,
  RumWorstPagesAndErrorsSection,
  RumLiveActivitySection,
  WORST_PAGE_COLUMNS,
  envName,
  getThresholdColors,
  buildEnvPieData,
  TREND_CHART_LAYOUT,
  STACKED_BAR_LAYOUT,
  DONUT_CHART_LAYOUT,
  type RumFaroVersionCardProps,
  type RumVersionsResponse,
  type RumWire,
  type RumAnalytics,
  type RumCwvDistribution,
  type CwvColors,
  type RumWorstPage,
  type RumJsError,
  type RumRawLog,
  type RumLiveEvent,
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
  filterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: RumClientProps) {
  const timezone = useTimezone()
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId)

  const { data: analytics, isLoading } = useAnalyticsReportQuery<RumAnalytics | null>({
    namespace: ['rum-analytics'],
    serviceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    queryFn: async ({ rangeBody }) => {
      if (!serviceId) return null
      const params = new URLSearchParams()
      for (const [k, v] of Object.entries(rangeBody)) if (v) params.append(k, v)
      if (filterPayload && Object.keys(filterPayload).length > 0) params.append('filters', JSON.stringify(filterPayload))
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/analytics', RumAnalytics>(
        '/api/services/{service_id}/rum/analytics',
        `/api/services/${serviceId}/rum/analytics${qs ? `?${qs}` : ''}`,
      )
      if (!res.ok) throw new Error(res.errorText || `RUM analytics failed (HTTP ${res.status})`)
      return recordOrNull(res.data)
    },
    queryOptions: { enabled: !!serviceId && !!status?.enabled, refetchInterval: false },
  })

  const { data: liveEvents, isLoading: isLiveEventsLoading } = useQuery({
    queryKey: ['rum-live-events', serviceId, startTime, endTime, filterPayload],
    queryFn: async () => {
      if (!serviceId) return []
      const params = new URLSearchParams()
      if (startTime) params.append('start_time', startTime)
      if (endTime) params.append('end_time', endTime)
      if (filterPayload) params.append('filters', JSON.stringify(filterPayload))
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/live-events', RumLiveEvent[]>(
        '/api/services/{service_id}/rum/live-events',
        qs ? `/api/services/${serviceId}/rum/live-events?${qs}` : `/api/services/${serviceId}/rum/live-events`,
      )
      if (!res.ok) throw new Error(res.errorText || `RUM live events failed (HTTP ${res.status})`)
      return arrayOrEmpty(res.data)
    },
    enabled: !!serviceId && !!status?.enabled,
    refetchInterval: false,
  })

  if (isStatusLoading || isLoading) return <DashboardSkeleton />
  if (!status?.enabled) return <RumStatusPanel />
  if (!analytics) return <DashboardSkeleton />

  if (analytics.no_data) {
    return <RumNoDataBanner message={analytics.message} beaconCount={analytics.beacon_count} />
  }

  if (analytics.beacon_count === 0) {
    return <RumEmptyWindowCard />
  }

  return (
    <div className="space-y-6">
      <RumVitalsTopSection
        analytics={analytics}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />

      <RumWorstPagesAndErrorsSection
        worstPages={analytics.worst_pages}
        errors={analytics.errors}
      />

      <RumEnvironmentAndBeaconsSection
        analytics={analytics}
        startTime={startTime}
        endTime={endTime}
        timezone={timezone}
      />

      <RumLiveActivitySection
        liveEvents={liveEvents}
        isLiveEventsLoading={isLiveEventsLoading}
        timezone={timezone}
      />
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

      {!isAnalyst && (
        <RumInstallationDialog open={showInstructions} onOpenChange={setShowInstructions} />
      )}
    </>
  )
}

export default RumPageClient
