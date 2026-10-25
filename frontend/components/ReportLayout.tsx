'use client'

import React, { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useEffectiveServiceId } from '@/hooks/useIsDataReady'
import { useTimeRange } from '@/hooks/useReportRangeAnchor'
import { useTimezone } from '@/hooks/useDateFormat'
import { useReportConfig, type ReportConfiguration } from '@/hooks/useReportConfig'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useViewMetricUrlSync } from '@/hooks/useViewMetricUrlSync'
import { ReportShell } from '@/components/ReportShell'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { INTERVALS, INTERVAL_SECONDS, type ChartInterval } from '@/lib/constants'
import { cn } from '@/lib/utils'
import { type LucideIcon } from 'lucide-react'
import type { FiltersPayload } from '@/types/filters'

interface ChartIntervalButtonsProps {
  effectiveInterval: string
  validIntervals: Set<string>
  onIntervalChange: (val: ChartInterval) => void
}

export function ChartIntervalButtons({
  effectiveInterval,
  validIntervals,
  onIntervalChange,
}: ChartIntervalButtonsProps) {
  return (
    <ButtonGroup>
      {INTERVALS.map(i => (
        <Button
          key={i.value}
          variant={effectiveInterval === i.value ? 'default' : 'ghost'}
          size="sm"
          onClick={() => React.startTransition(() => onIntervalChange(i.value))}
          disabled={!validIntervals.has(i.value)}
          aria-pressed={effectiveInterval === i.value}
          // Distinct accessible name: the visible label ("1h", "1d", …) is
          // identical to the main time-range buttons, so screen readers and
          // automation see duplicate "1h"/"1d" controls. Scope the name to
          // the chart-bucket role.
          aria-label={`Chart bucket size: ${i.label}`}
          className={cn(
            'h-9 text-xs px-2 shadow-none transition-colors disabled:opacity-30 sm:h-7 sm:text-[11px]',
            effectiveInterval === i.value
              ? 'bg-primary text-primary-foreground hover:bg-primary/90'
              : 'hover:text-primary hover:bg-muted'
          )}
        >
          {i.label}
        </Button>
      ))}
    </ButtonGroup>
  )
}

export interface ReportLayoutRenderProps {
  config: ReportConfiguration
  metric: string
  setMetric: (metric: string) => void
  setChartInterval: (interval: ChartInterval) => void
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  bucketSeconds: number
  startTime: string | null
  endTime: string | null
  timezone: string
  activeServiceId: string | null
  filterPayload: FiltersPayload
}

interface ReportLayoutProps {
  title: string
  description: string
  icon: LucideIcon
  queryKey?: string
  defaultInterval?: ChartInterval
  headerActions?: React.ReactNode
  serviceId?: string | null
  children: (props: ReportLayoutRenderProps) => React.ReactNode
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
  const { config, metric, setMetric, setChartInterval, trend, setTrend } = useReportConfig({ defaultInterval })
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

  useViewMetricUrlSync({ setMetric })

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
        metric,
        setMetric,
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
