'use client'

import React from 'react'
import { useActiveService } from '@/hooks/useActiveService'
import { useEffectiveServiceId } from '@/hooks/useIsDataReady'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useTimezone } from '@/hooks/useTimezone'
import { useReportConfig, type ReportConfiguration } from '@/hooks/useReportConfig'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useViewMetricUrlSync } from '@/hooks/useViewMetricUrlSync'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { ReportShell } from '@/components/ReportShell'
import { INTERVAL_SECONDS, type ChartInterval } from '@/lib/constants'
import { ChartIntervalButtons } from '@/components/ChartIntervalButtons'
import { extractApiError } from '@/lib/api'
import { type LucideIcon } from 'lucide-react'

interface ReportLayoutProps<TData = unknown> {
  title: string
  description: string
  icon: LucideIcon
  queryKey?: string
  apiCall?: (params: {
    startTime: string | null
    endTime: string | null
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    filters: any
    bucketSeconds: number
  }) => Promise<TData | undefined>
  defaultInterval?: ChartInterval
  headerActions?: React.ReactNode
  serviceId?: string | null
  children: (props: {
    data: TData | undefined
    isLoading: boolean
    isFetching: boolean
    /** E-6 (audit): expose error/refetch so pages that own their own
     *  useQuery (e.g. sessions) can surface failures through the same
     *  banner instead of letting them disappear into an empty table. */
    isError: boolean
    error: Error | null
    refetch: () => void
    config: ReportConfiguration
    setChartInterval: (interval: ChartInterval) => void
    trend: string
    setTrend: (trend: string) => void
    intervalButtons: React.ReactNode
    bucketSeconds: number
    startTime: string | null
    endTime: string | null
    timezone: string
    activeServiceId: string | null
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    filterPayload: any
  }) => React.ReactNode
}

export function ReportLayout<TData = unknown>({
  title,
  description,
  icon,
  queryKey,
  apiCall,
  defaultInterval = '1 hour',
  headerActions,
  serviceId,
  children
}: ReportLayoutProps<TData>) {
  const { startTime, endTime } = useTimeRange()
  const effectiveServiceId = useEffectiveServiceId()
  const activeServiceId = (serviceId !== undefined ? serviceId : effectiveServiceId) ?? null
  const timezone = useTimezone()
  const { config, setChartInterval, trend, setTrend } = useReportConfig({ defaultInterval })
  // Pass `true` so the FilterBar's "Edge only" toggle injects
  // `edge=include[true]` into every report query. Without it the toggle
  // was visible in the UI but discarded before reaching the backend.
  const filterPayload = useDebouncedFilterPayload(true)

  useViewMetricUrlSync()

  const bucketSeconds = INTERVAL_SECONDS[config.effectiveInterval as keyof typeof INTERVAL_SECONDS] ?? 3600

  const query = useServiceQuery<TData | undefined>(
    [queryKey || 'report', 'aggregates', activeServiceId, startTime, endTime, filterPayload, bucketSeconds],
    () => apiCall ? apiCall({
      startTime,
      endTime,
      filters: filterPayload,
      bucketSeconds
    }) : Promise.resolve(undefined),
    { enabled: !!apiCall }
  )

  const intervalButtons = (
    <ChartIntervalButtons
      effectiveInterval={config.effectiveInterval}
      validIntervals={config.validIntervals}
      onIntervalChange={setChartInterval}
    />
  )

  // E-6 (audit): when ReportLayout owns the query (apiCall provided)
  // and it failed, surface it through ReportShell's banner. Pages that
  // manage their own useQuery (e.g. /sessions) get isError/error/refetch
  // in the children callback and can render the banner themselves via
  // the same ReportShell prop path.
  const ownsQuery = !!apiCall
  const queryError = ownsQuery && query.isError
    ? { message: extractApiError(query.error), onRetry: () => { void query.refetch() } }
    : null

  return (
    <ReportShell
      title={title}
      description={description}
      icon={icon}
      headerActions={headerActions}
      queryError={queryError}
    >
      {children({
        data: query.data,
        isLoading: query.isLoading,
        isFetching: query.isFetching,
        isError: query.isError,
        error: query.error ?? null,
        refetch: () => { void query.refetch() },
        config,
        setChartInterval,
        trend,
        setTrend,
        intervalButtons,
        bucketSeconds,
        startTime,
        endTime,
        timezone,
        activeServiceId,
        filterPayload
      })}
    </ReportShell>
  )
}
