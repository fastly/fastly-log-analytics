'use client'

import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, Activity, Gauge, Users, XCircle } from 'lucide-react'

import { client } from '@/lib/api'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ScoringHealthHelp } from '@/components/SessionScoring/help-content'

interface ScoringHealthCardProps {
  serviceId: string
  sinceHours?: number
}

interface HealthResponse {
  since_hours: number
  total_edge_rows: number
  scored_rows: number
  fire_rate_pct: number
  distinct_sids: number
  avg_score: number
  p50_score: number
  p95_score: number
  max_score: number
  scorer_errors: number
  top_reasons: { reason: string; count: number }[]
  matrix_staleness?: {
    l2_evaluated: number
    l2_high_count: number
    l2_high_pct: number
    is_stale: boolean
    threshold_pct: number
  }
}

function Metric({
  icon: Icon,
  label,
  value,
  sub,
  tone = 'default',
}: {
  icon: React.ComponentType<{ className?: string }>
  label: string
  value: string | number
  sub?: string
  tone?: 'default' | 'warn' | 'good'
}) {
  const valueClass =
    tone === 'warn'
      ? 'text-destructive'
      : tone === 'good'
        ? 'text-green-600'
        : 'text-foreground'
  return (
    <div className="flex flex-col gap-1 p-3 rounded-md border bg-card">
      <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        <Icon className="h-3 w-3" />
        {label}
      </div>
      <div className={`text-lg font-mono tabular-nums ${valueClass}`}>{value}</div>
      {sub && <div className="text-[10px] text-muted-foreground">{sub}</div>}
    </div>
  )
}

export function ScoringHealthCard({ serviceId, sinceHours = 24 }: ScoringHealthCardProps) {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['scoring-health', serviceId, sinceHours],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/health' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { since_hours: sinceHours },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as HealthResponse
    },
  })

  if (isError) {
    return (
      <AnalyticsCard
        title="Scoring Health"
        description={`Operational metrics for the last ${sinceHours}h.`}
      >
        <div className="border border-destructive/20 bg-destructive/5 rounded-md p-4">
          <div className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-4 w-4" />
            <span className="text-sm font-medium">Failed to load scoring health</span>
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            {(error as any)?.message || 'Unknown error'}
          </p>
          <Button
            size="sm"
            variant="outline"
            className="mt-3"
            onClick={() => refetch()}
          >
            Retry
          </Button>
        </div>
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard
        title="Scoring Health"
        description={`Operational metrics for the last ${sinceHours}h.`}
      >
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-20 w-full" />
          ))}
        </div>
      </AnalyticsCard>
    )
  }

  const fireRateTone: 'default' | 'warn' = data.fire_rate_pct < 20 ? 'warn' : 'default'
  const errorsTone: 'default' | 'warn' | 'good' =
    data.scorer_errors === 0 ? 'good' : data.scorer_errors > 10 ? 'warn' : 'default'

  return (
    <AnalyticsCard
      title="Scoring Health"
      description={`Snapshot for the last ${sinceHours}h — fire rate, score distribution, and top reasons.`}
      helpContent={<ScoringHealthHelp />}
    >
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
        <Metric
          icon={Activity}
          label="Fire Rate"
          value={`${data.fire_rate_pct.toFixed(1)}%`}
          sub={`${data.scored_rows.toLocaleString()} of ${data.total_edge_rows.toLocaleString()} edge rows`}
          tone={fireRateTone}
        />
        <Metric
          icon={Gauge}
          label="Avg Score"
          value={data.avg_score.toFixed(1)}
          sub={`p50 ${data.p50_score.toFixed(0)} · p95 ${data.p95_score.toFixed(0)} · max ${data.max_score}`}
        />
        <Metric
          icon={Users}
          label="Sessions"
          value={data.distinct_sids.toLocaleString()}
          sub="distinct sids seen"
        />
        <Metric
          icon={XCircle}
          label="Scorer Errors"
          value={data.scorer_errors.toLocaleString()}
          sub="fail-open + auth fail rows"
          tone={errorsTone}
        />
        <Metric
          icon={AlertTriangle}
          label="Top Reason"
          value={data.top_reasons[0]?.reason ?? '—'}
          sub={
            data.top_reasons[0]
              ? `${data.top_reasons[0].count.toLocaleString()} hits`
              : 'no scored rows yet'
          }
        />
      </div>
      {data.top_reasons.length > 0 && (
        <div className="mt-4 space-y-1">
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
            Score Reason Breakdown
          </div>
          <div className="flex flex-wrap gap-2">
            {data.top_reasons.map((r) => (
              <div
                key={r.reason}
                className="flex items-center gap-1.5 px-2 py-1 rounded-md border bg-muted/40 text-xs"
              >
                <span className="font-mono">{r.reason}</span>
                <span className="text-muted-foreground tabular-nums">{r.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.matrix_staleness?.is_stale && (
        <div className="mt-4 p-3 border border-amber-300 bg-amber-50/60 rounded-md text-xs flex items-start gap-2">
          <AlertTriangle className="h-4 w-4 text-amber-600 mt-0.5 flex-none" />
          <div className="space-y-0.5">
            <div className="font-semibold text-amber-900">Matrix may be stale</div>
            <div className="text-amber-800">
              {data.matrix_staleness.l2_high_pct.toFixed(1)}% of L2-evaluated sessions
              {' '}({data.matrix_staleness.l2_high_count.toLocaleString()} of{' '}
              {data.matrix_staleness.l2_evaluated.toLocaleString()}) are scoring
              ≥50 — above the {data.matrix_staleness.threshold_pct}% threshold.
              The matrix is treating too much real traffic as rare. Click
              <strong className="px-0.5">Retrain matrix</strong> in the header
              to refresh it.
            </div>
          </div>
        </div>
      )}
    </AnalyticsCard>
  )
}
