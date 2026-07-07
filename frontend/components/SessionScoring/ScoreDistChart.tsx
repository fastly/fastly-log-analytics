'use client'

import { AlertCircle } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/CardErrorState'
import { ScoreDistHelp } from '@/components/SessionScoring/help-content'

import { StackedHourlyBarChart } from './StackedHourlyBarChart'
import { useScoringQuery } from './useScoringQuery'

interface ScoreDistChartProps {
  serviceId: string
  sinceHours?: number
}

interface DistRow {
  hour: string
  bucket: string
  count: number
}

// Bucket order + color map. Higher buckets = redder. Fixed order preserved
// via categoryOrder so the stack always reads low→high bottom-to-top.
const BUCKETS = ['0-25', '25-50', '50-75', '75-100'] as const
const BUCKET_COLORS: Record<(typeof BUCKETS)[number], string> = {
  '0-25': '#94a3b8',
  '25-50': '#facc15',
  '50-75': '#fb923c',
  '75-100': '#e11d48',
}

export function ScoreDistChart({ serviceId, sinceHours = 24 }: ScoreDistChartProps) {
  const { data, isLoading, isFetching, isError, error, refetch } = useScoringQuery<{ rows: DistRow[] }>(
    ['scoring-score-dist', serviceId, sinceHours],
    serviceId,
    'score-distribution',
    { since_hours: sinceHours },
  )

  if (isError) {
    return (
      <AnalyticsCard
        title={`Score distribution — last ${sinceHours}h`}
        description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
        helpContent={<ScoreDistHelp />}
        helpTitle="About Score Distribution"
      >
        <CardErrorState
          variant="stacked"
          icon={<AlertCircle className="h-4 w-4" />}
          title="Failed to load score distribution"
          message={(error as Error)?.message ?? 'unknown error'}
          onRetry={() => refetch()}
        />
      </AnalyticsCard>
    )
  }

  return (
    <StackedHourlyBarChart<DistRow>
      title={`Score distribution — last ${sinceHours}h`}
      description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
      helpContent={<ScoreDistHelp />}
      helpTitle="About Score Distribution"
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={(data?.rows?.length ?? 0) === 0}
      rows={data?.rows ?? []}
      categoryKey="bucket"
      colors={BUCKET_COLORS}
      categoryOrder={BUCKETS}
    />
  )
}
