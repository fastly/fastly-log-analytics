'use client'

import React, { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useEffectiveServiceId } from '@/hooks/useIsDataReady'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useTimezone } from '@/hooks/useTimezone'
import { useReportConfig, type ReportConfiguration } from '@/hooks/useReportConfig'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useViewMetricUrlSync } from '@/hooks/useViewMetricUrlSync'
import { ReportShell } from '@/components/ReportShell'
import { INTERVAL_SECONDS, type ChartInterval } from '@/lib/constants'
import { ChartIntervalButtons } from '@/components/ChartIntervalButtons'
import { type LucideIcon } from 'lucide-react'

interface ReportLayoutProps {
  title: string
  description: string
  icon: LucideIcon
  queryKey?: string
  defaultInterval?: ChartInterval
  headerActions?: React.ReactNode
  serviceId?: string | null
  children: (props: {
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

export function ReportLayout({
  title,
  description,
  icon,
  queryKey,
  defaultInterval = '1 hour',
  headerActions,
  serviceId,
  children
}: ReportLayoutProps) {
  const { startTime, endTime } = useTimeRange()
  const effectiveServiceId = useEffectiveServiceId()
  const activeServiceId = (serviceId !== undefined ? serviceId : effectiveServiceId) ?? null
  const timezone = useTimezone()
  const { config, setChartInterval, trend, setTrend } = useReportConfig({ defaultInterval })
  // Pass `true` so the FilterBar's "Edge only" toggle injects
  // `edge=include[true]` into every report query. Without it the toggle
  // was visible in the UI but discarded before reaching the backend.
  const filterPayload = useDebouncedFilterPayload(true)

  const queryClient = useQueryClient()
  useEffect(() => {
    return () => {
      // Cancel report queries on unmount to free pool connections when navigating
      queryClient.cancelQueries({ queryKey: [queryKey || 'report'] })
    }
  }, [queryClient, queryKey])

  useViewMetricUrlSync()

  const bucketSeconds = INTERVAL_SECONDS[config.effectiveInterval as keyof typeof INTERVAL_SECONDS] ?? 3600

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
