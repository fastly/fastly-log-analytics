'use client'

import { AlertTriangle, Activity, Gauge, Users, XCircle, Clock } from 'lucide-react'

import { Skeleton } from '@/components/ui/skeleton'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/SessionScoring/CardErrorState'
import { ScoringHealthHelp } from '@/components/SessionScoring/help-content'

import { useScoringQuery } from './useScoringQuery'

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
  fail_open_rate_pct?: number | null
  top_reasons: { reason: string; count: number }[]
  latency?: {
    available: boolean
    rtt_p50_us: number | null
    rtt_p95_us: number | null
    rtt_p99_us: number | null
    rtt_max_us: number | null
    exec_p50_us: number | null
    exec_p95_us: number | null
  }
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
  const { data, isLoading, isError, error, refetch } = useScoringQuery<HealthResponse>(
    ['scoring-health', serviceId, sinceHours],
    serviceId,
    'health',
    { since_hours: sinceHours },
  )

  if (isError) {
    // M-6: the raw DuckDB error ("IO Error: No files found that match the
    // pattern 'cache/fos-<id>-logs/buffer/batch_<hash>.parquet'") used to
    // be surfaced verbatim — exposing internal cache layout and reading
    // as if the scoring system was broken. Map the common transient
    // signatures to friendly copy; fall back to a clean message for
    // anything else. The original payload is still in the network tab if
    // an operator needs to dig.
    const raw = String(error?.message || 'Unknown error')
    const friendly =
      /No files found that match the pattern/i.test(raw) || /IO Error/i.test(raw)
        ? 'Scoring data is still warming up — try again in a few minutes.'
        : /timed out|timeout/i.test(raw)
          ? 'The scoring service took too long to respond. Retry, or check the scorer Compute service is reachable.'
          : raw
    return (
      <AnalyticsCard
        title="Scoring Health"
        description={`Operational metrics for the last ${sinceHours}h.`}
      >
        <CardErrorState
          icon={<AlertTriangle className="h-4 w-4" />}
          title="Scoring health unavailable"
          message={friendly}
          onRetry={() => refetch()}
        />
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard
        title="Scoring Health"
        description={`Operational metrics for the last ${sinceHours}h.`}
      >
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].map((k) => (
            <Skeleton key={k} className="h-20 w-full" />
          ))}
        </div>
      </AnalyticsCard>
    )
  }

  const fireRateTone: 'default' | 'warn' = data.fire_rate_pct < 20 ? 'warn' : 'default'
  // SRE-15: tone on the traffic-normalized fail-open RATE, not the absolute
  // count. The count scales with request volume (scorer is instance-per-
  // request → fail-opens track traffic), so a fixed count threshold cries
  // wolf under load and stays silent on a low-traffic spike. Steady-state is
  // ~1.6% (scorer-instance-per-request-coldstart); warn above 2.5%. Fall back
  // to the legacy count tone if the rate field is absent (older backend).
  const failRate = data.fail_open_rate_pct ?? null
  const errorsTone: 'default' | 'warn' | 'good' =
    data.scorer_errors === 0
      ? 'good'
      : failRate != null
        ? failRate > 2.5 ? 'warn' : 'default'
        : data.scorer_errors > 10 ? 'warn' : 'default'

  // Scorer latency tile. rtt is the edge round-trip (compared against the
  // ~100ms backend timeout); exec is the scorer's own Wasm time (~µs).
  const lat = data.latency
  const rttP95Us = lat?.rtt_p95_us ?? null
  const fmtMs = (us: number | null | undefined) =>
    us == null ? '—' : `${(us / 1000).toFixed(us < 10_000 ? 1 : 0)}ms`
  const fmtUs = (us: number | null | undefined) =>
    us == null ? '—' : us >= 1_000 ? `${(us / 1000).toFixed(1)}ms` : `${us}µs`
  // Warn as p95 round-trip approaches the timeout budget — that's when
  // fail-opens start. 80ms ≈ 80% of the 100ms ceiling.
  const latencyTone: 'default' | 'warn' =
    rttP95Us != null && rttP95Us / 1000 > 80 ? 'warn' : 'default'

  return (
    <AnalyticsCard
      title="Scoring Health"
      description={`Snapshot for the last ${sinceHours}h — fire rate, score distribution, and top reasons.`}
      helpContent={<ScoringHealthHelp />}
    >
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
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
          sub={failRate != null ? `${failRate.toFixed(2)}% of edge rows` : 'fail-open + auth fail rows'}
          tone={errorsTone}
        />
        <Metric
          icon={Clock}
          label="Scorer Latency"
          value={rttP95Us != null ? fmtMs(rttP95Us) : '—'}
          sub={
            rttP95Us != null
              ? `p95 rtt · p50 ${fmtMs(lat?.rtt_p50_us)} · exec ${fmtUs(lat?.exec_p95_us)}`
              : 'awaiting re-provision'
          }
          tone={latencyTone}
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
