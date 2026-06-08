'use client'

import { useQuery } from '@tanstack/react-query'
import { AlertCircle } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ScoreDistHelp } from '@/components/SessionScoring/help-content'
import { Button } from '@/components/ui/button'
import { client } from '@/lib/api'

import { StackedHourlyBarChart } from './StackedHourlyBarChart'

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
  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['scoring-score-dist', serviceId, sinceHours],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/score-distribution' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { since_hours: sinceHours },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as { rows: DistRow[] }
    },
  })

  if (isError) {
    return (
      <AnalyticsCard
        title={`Score distribution — last ${sinceHours}h`}
        description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
        helpContent={<ScoreDistHelp />}
        helpTitle="About Score Distribution"
      >
        <div className="flex flex-col items-start gap-3 p-4 border border-destructive/20 bg-destructive/5 rounded-md">
          <div className="flex items-center gap-2 text-destructive">
            <AlertCircle className="h-4 w-4" />
            <span className="text-sm font-medium">Failed to load score distribution</span>
          </div>
          <p className="text-xs text-muted-foreground">
            {(error as Error)?.message ?? 'unknown error'}
          </p>
          <Button size="sm" variant="outline" onClick={() => refetch()}>
            Retry
          </Button>
        </div>
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
      rows={data?.rows ?? []}
      categoryKey="bucket"
      colors={BUCKET_COLORS}
      categoryOrder={BUCKETS}
    />
  )
}
