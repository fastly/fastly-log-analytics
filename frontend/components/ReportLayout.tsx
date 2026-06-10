'use client'

import React from 'react'
import { useActiveService } from '@/hooks/useActiveService'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useTimezone } from '@/hooks/useTimezone'
import { useReportConfig, type ReportConfiguration } from '@/hooks/useReportConfig'
import { useFilterPayload } from '@/hooks/useFilterPayload'
import { useUrlFilterSync } from '@/hooks/useUrlFilterSync'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { ReportShell } from '@/components/ReportShell'
import { INTERVAL_SECONDS, type ChartInterval } from '@/lib/constants'
import { ChartIntervalButtons } from '@/components/ChartIntervalButtons'
import { type LucideIcon } from 'lucide-react'

interface ReportLayoutProps {
  title: string
  description: string
  icon: LucideIcon
  queryKey?: string
  apiCall?: (params: {
    startTime: string | null
    endTime: string | null
    filters: any
    bucketSeconds: number
  }) => Promise<any>
  defaultInterval?: ChartInterval
  headerActions?: React.ReactNode
  children: (props: {
    data: any
    isLoading: boolean
    isFetching: boolean
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
    filterPayload: any
  }) => React.ReactNode
}

export function ReportLayout({
  title,
  description,
  icon,
  queryKey,
  apiCall,
  defaultInterval = '1 hour',
  headerActions,
  children
}: ReportLayoutProps) {
  const { startTime, endTime } = useTimeRange()
  const { activeServiceId } = useActiveService()
  const timezone = useTimezone()
  const { config, setChartInterval, trend, setTrend } = useReportConfig({ defaultInterval })
  const filterPayload = useFilterPayload()

  useUrlFilterSync()

  const bucketSeconds = INTERVAL_SECONDS[config.effectiveInterval as keyof typeof INTERVAL_SECONDS] ?? 3600

  const query = useServiceQuery(
    [queryKey || 'report', 'aggregates', activeServiceId, startTime, endTime, filterPayload, bucketSeconds],
    () => apiCall ? apiCall({
      startTime,
      endTime,
      filters: filterPayload,
      bucketSeconds
    }) : Promise.resolve(null),
    { enabled: !!apiCall }
  )

  const intervalButtons = (
    <ChartIntervalButtons
      effectiveInterval={config.effectiveInterval}
      validIntervals={config.validIntervals}
      onIntervalChange={setChartInterval}
    />
  )

  return (
    <ReportShell
      title={title}
      description={description}
      icon={icon}
      headerActions={headerActions}
    >
      {children({
        data: query.data,
        isLoading: query.isLoading,
        isFetching: query.isFetching,
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
