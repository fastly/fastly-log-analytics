'use client'

import { AlertTriangle, CheckCircle2, Clock, KeyRound, Lock, ServerCrash, XCircle } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/CardErrorState'
import { ScorerFailOpenHelp } from '@/components/SessionScoring/help-content'
import { Skeleton } from '@/components/ui/skeleton'
import type { components } from '@/types/api.generated'

import { useScoringQuery } from './useScoringQuery'

// Generated from the /scoring/health response_model — single source of truth.
type HealthResponse = components['schemas']['ScoringHealthResponse']
type FailOpenBucket = NonNullable<components['schemas']['ScoringReasonCount']>

interface ScorerFailOpenBreakdownCardProps {
  serviceId: string
  sinceHours?: number
}

/**
 * Fail-open breakdown by exact reason/status. The scorer fails OPEN on any
 * error (timeout, auth, KV miss) — requests flow through unscored — and the
 * VCL deliver snippet records WHY in the ``edge_score_reason`` field as
 * ``compute-unavailable-<status>`` (or the Rust scorer's bare
 * ``internal-error-keys``). ScorerErrorsChart shows WHEN fail-opens happen;
 * this card shows WHAT KIND, so a 503 spike (scorer timeout → raise the
 * timeout / investigate cold starts) is distinguishable from a 401 (auth
 * secret drift → redeploy) or internal-error-keys (KV/key store
 * misconfiguration).
 *
 * Shares ScoringHealthCard's query key so the ``/health`` payload is fetched
 * once and read by both cards.
 */
export function ScorerFailOpenBreakdownCard({
  serviceId,
  sinceHours = 24,
}: ScorerFailOpenBreakdownCardProps) {
  const { data, isLoading, isError, error, refetch } = useScoringQuery<HealthResponse>(
    ['scoring-health', serviceId, sinceHours],
    serviceId,
    'health',
    { since_hours: sinceHours },
  )

  const buckets = data?.fail_open_breakdown ?? []
  const total = buckets.reduce((acc, b) => acc + (b.count ?? 0), 0)
  const max = buckets.reduce((acc, b) => Math.max(acc, b.count ?? 0), 0)

  return (
    <AnalyticsCard
      title={`Fail-open breakdown — last ${sinceHours}h`}
      description="Fail-opens grouped by reason/status — why the scorer let requests through unscored."
      helpContent={<ScorerFailOpenHelp />}
      helpTitle="About Fail-open Breakdown"
    >
      {isError ? (
        <CardErrorState
          icon={<AlertTriangle className="h-4 w-4" />}
          title="Fail-open breakdown unavailable"
          message={(error as Error)?.message || 'Unknown error'}
          onRetry={() => refetch()}
        />
      ) : isLoading || !data ? (
        <div className="space-y-2">
          {['f1', 'f2', 'f3'].map((k) => (
            <Skeleton key={k} className="h-10 w-full" />
          ))}
        </div>
      ) : buckets.length === 0 ? (
        <div className="flex items-center gap-2 rounded-md border border-emerald-200 bg-emerald-50/60 px-3 py-3 text-sm text-emerald-800">
          <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
          <span>
            No fail-opens in the last {sinceHours}h — every routed request was scored.
          </span>
        </div>
      ) : (
        <div className="space-y-3">
          <div className="text-xs text-muted-foreground">
            <span className="font-mono tabular-nums text-foreground">{total.toLocaleString()}</span>{' '}
            fail-open {total === 1 ? 'row' : 'rows'} across{' '}
            <span className="font-mono tabular-nums text-foreground">{buckets.length}</span>{' '}
            {buckets.length === 1 ? 'class' : 'classes'}.
          </div>
          <div className="space-y-1.5">
            {buckets.map((b) => (
              <FailOpenRow key={b.reason ?? ''} bucket={b} max={max} />
            ))}
          </div>
        </div>
      )}
    </AnalyticsCard>
  )
}

function FailOpenRow({ bucket, max }: { bucket: FailOpenBucket; max: number }) {
  const { label, status, Icon } = describeFailOpen(bucket.reason ?? '')
  const widthPct = max > 0 ? Math.max(4, Math.round(((bucket.count ?? 0) / max) * 100)) : 0
  return (
    <div className="flex items-center gap-3 rounded-md border bg-card px-3 py-2 text-xs">
      <Icon className="h-4 w-4 shrink-0 text-destructive/80" />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-medium text-foreground">{label}</span>
          {status != null && (
            <span className="rounded bg-destructive/10 px-1.5 py-0.5 font-mono text-[10px] font-semibold text-destructive">
              {status}
            </span>
          )}
        </div>
        <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-border/50">
          <div
            className="h-full rounded-full bg-rose-500/50"
            style={{ width: `${widthPct}%` }}
          />
        </div>
        <code className="mt-1 block truncate text-[10px] text-muted-foreground">{bucket.reason}</code>
      </div>
      <span className="shrink-0 font-mono tabular-nums text-foreground">
        {(bucket.count ?? 0).toLocaleString()}
      </span>
    </div>
  )
}

/**
 * Map a raw fail-open reason to a human label + (when present) the HTTP
 * status the scorer sub-fetch returned. The VCL writes
 * ``compute-unavailable-<status>``; the Rust scorer emits bare
 * ``internal-error-keys`` / ``unauthorized``.
 */
function describeFailOpen(reason: string): {
  label: string
  status: string | null
  Icon: React.ComponentType<{ className?: string }>
} {
  const m = /^compute-unavailable-(\d+|.+)$/.exec(reason)
  if (m) {
    const status = m[1]
    if (status === '503') return { label: 'Scorer timeout / unavailable', status, Icon: Clock }
    if (status === '500') return { label: 'Scorer error', status, Icon: ServerCrash }
    if (status === '401') return { label: 'Auth rejected', status, Icon: Lock }
    return { label: 'Scorer unavailable', status, Icon: ServerCrash }
  }
  if (reason === 'internal-error-keys') return { label: 'Key / KV store load failure', status: null, Icon: KeyRound }
  if (reason.startsWith('internal-error')) return { label: 'Internal scorer error', status: null, Icon: ServerCrash }
  if (reason === 'unauthorized') return { label: 'Unauthorized', status: null, Icon: Lock }
  return { label: reason, status: null, Icon: XCircle }
}
