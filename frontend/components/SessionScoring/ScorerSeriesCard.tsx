'use client'

import { useState, type ReactNode } from 'react'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import {
  deriveScorerSeries,
  useScorerTimeseries,
  type LatencyResponse,
} from '@/components/SessionScoring/useScorerTimeseries'
import type { PlotlyLayout, PlotlyTrace } from '@/types/plotly'

type Derived = ReturnType<typeof deriveScorerSeries>

interface ScorerSeriesCardProps {
  serviceId: string
  sinceHours: number
  title: string
  /** Receives the per-minute flag so the card can say "Bucketed per minute". */
  description: (perMinute: boolean) => string
  help: ReactNode
  helpTitle: string
  a11yTitle: string
  toggle: { id: string; label: string }
  /** Traces + layout for the current toggle state. */
  build: (series: Derived & { data: LatencyResponse | undefined }, toggleOn: boolean) => {
    traces: PlotlyTrace[]
    layout: PlotlyLayout
  }
  /** Whether the toggle is offered for this data (default: whenever rows exist). */
  showToggle?: (series: Derived & { data: LatencyResponse | undefined }) => boolean
  /** Replaces the chart (e.g. a "waiting for fields" explanation). */
  placeholder?: (series: Derived & { data: LatencyResponse | undefined }) => ReactNode | null
}

/**
 * Card shell shared by the scorer latency and fail-open charts: one timeseries
 * query, one optional secondary-series toggle, one PlotlyChart. Each chart
 * supplies only its traces/layout.
 */
export function ScorerSeriesCard({
  serviceId,
  sinceHours,
  title,
  description,
  help,
  helpTitle,
  a11yTitle,
  toggle,
  build,
  showToggle,
  placeholder,
}: ScorerSeriesCardProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)
  const [toggleOn, setToggleOn] = useState(true)

  const series = { ...deriveScorerSeries(data), data }
  const { rows, perMinute } = series
  const { traces, layout } = build(series, toggleOn)
  const offerToggle = rows.length > 0 && (showToggle ? showToggle(series) : true)
  const replacement = placeholder?.(series) ?? null

  return (
    <AnalyticsCard
      title={title}
      description={description(perMinute)}
      helpContent={help}
      helpTitle={helpTitle}
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      className="min-h-[320px]"
      headerAction={
        offerToggle ? (
          <div className="flex items-center gap-2 mr-1">
            <Switch id={toggle.id} checked={toggleOn} onCheckedChange={setToggleOn} />
            <Label htmlFor={toggle.id} className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground">
              {toggle.label}
            </Label>
          </div>
        ) : undefined
      }
    >
      {replacement ?? <PlotlyChart data={traces} layout={layout} height="100%" a11yTitle={a11yTitle} />}
    </AnalyticsCard>
  )
}
