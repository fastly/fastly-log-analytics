'use client'

import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, ShieldCheck } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { useMounted } from '@/hooks/useBootstrap'
import { adminFetch } from '@/lib/api'
import { formatCount } from '@/lib/format'
import type { queryCloudScoring } from '@/lib/cloud-engine/scoring-reads'
import { useServiceStore } from '@/stores/filterStore'
import { ComplianceChart, ScoreDistChart, ScorerLatencyChart } from './ScoringOverviewTab'
import { TopFlaggedTable } from './TopFlaggedTable'

type Analytics = Extract<Awaited<ReturnType<typeof queryCloudScoring>>, { top_flagged: unknown }>
type Unavailable = { available: false; reason: string }

async function read<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await adminFetch(path, { signal })
  if (!response.ok) throw new Error(`Session scoring read failed (${response.status})`)
  return response.json()
}

function measured(value: unknown): string {
  if (value == null) return '—'
  return typeof value === 'number' ? formatCount(value, { maximumFractionDigits: 2 }) : String(value)
}

export function CloudScoringOverview() {
  const mounted = useMounted()
  const activeServiceId = useServiceStore((state) => state.activeServiceId)
  const [sinceHours, setSinceHours] = useState(24)
  const qc = useQueryClient()
  const services = useQuery({
    queryKey: ['cloud-scoring-services'],
    queryFn: ({ signal }) => read<{ services: { service_id: string }[] }>('/api/services', signal),
    enabled: mounted,
    staleTime: 60_000,
  })
  const serviceId = mounted
    ? (activeServiceId && activeServiceId !== 'all' ? activeServiceId : services.data?.services[0]?.service_id)
    : undefined
  const analytics = useQuery({
    queryKey: ['cloud-scoring-analytics', serviceId, sinceHours],
    enabled: !!serviceId,
    staleTime: 60_000,
    queryFn: async ({ signal }) => {
      const data = await read<Analytics | Unavailable>(
        `/api/services/${encodeURIComponent(serviceId!)}/scoring/analytics?since_hours=${sinceHours}`, signal,
      )
      if ('top_flagged' in data) {
        qc.setQueryData(['scoring-top-flagged', serviceId, sinceHours], data.top_flagged)
        qc.setQueryData(['scoring-score-dist', serviceId, sinceHours], data.score_distribution)
        if ('rows' in data.compliance_breakdown) {
          qc.setQueryData(['scoring-compliance', serviceId, sinceHours], data.compliance_breakdown)
        }
        if ('rows' in data.latency_timeseries) {
          qc.setQueryData(['scoring-latency-timeseries', serviceId, sinceHours], data.latency_timeseries)
        }
      }
      return data
    },
  })
  const data = analytics.data && 'top_flagged' in analytics.data ? analytics.data : null
  const error = services.error ?? analytics.error
  return (
    <div className="space-y-6" data-testid="admin-session-scoring-root">
      <PageHeader title="Session Scoring" description="Historical edge scoring from retained request Parquet." icon={ShieldCheck}>
        <label className="text-sm">
          Time range{' '}
          <select aria-label="Scoring time range" className="rounded border bg-background p-1" value={sinceHours}
            onChange={(event) => setSinceHours(Number(event.target.value))}>
            {[1, 6, 24, 72, 168].map((hours) => <option key={hours} value={hours}>Last {hours}h</option>)}
          </select>
        </label>
        <Button variant="outline" size="sm" disabled={!serviceId || analytics.isFetching}
          onClick={() => analytics.refetch()}>
          <RefreshCw className="mr-1 h-4 w-4" />Refresh
        </Button>
        <BackToAdminLink />
      </PageHeader>
      <Alert>
        <AlertDescription>
          Cloud does not provision or manage the edge scorer. Labels, trained matrices, evaluation curves,
          thresholds, enforcement controls and scoring audit history are unavailable.
          Existing scoring fields in request logs can still be analyzed below.
        </AlertDescription>
      </Alert>
      {error ? (
        <Alert variant="destructive"><AlertDescription>{error.message}</AlertDescription></Alert>
      ) : !mounted || services.isPending || (serviceId && analytics.isPending) ? (
        <Skeleton className="h-48 w-full" aria-label="Loading scoring data" />
      ) : !serviceId ? (
        <Alert><AlertDescription>No active service selected.</AlertDescription></Alert>
      ) : !data ? (
        <Alert><AlertDescription>
          Session scoring is not configured in the retained logs for this range. No scoring metrics are available.
        </AlertDescription></Alert>
      ) : (
        <>
          <AnalyticsCard title="Recorded scoring health">
            <dl className="grid grid-cols-2 gap-4 md:grid-cols-4">
              {[
                ['Scored requests', data.health.scored_rows],
                ['Distinct sessions', data.health.distinct_sids],
                ['Average score', data.health.avg_score],
                ['p95 score', data.health.p95_score],
              ].map(([label, value]) => (
                <div key={String(label)}><dt className="text-sm text-muted-foreground">{label}</dt>
                  <dd className="text-xl font-semibold">{measured(value)}</dd></div>
              ))}
            </dl>
          </AnalyticsCard>
          <TopFlaggedTable serviceId={serviceId} sinceHours={sinceHours} />
          <ScoreDistChart serviceId={serviceId} sinceHours={sinceHours} />
          {'rows' in data.compliance_breakdown ? <ComplianceChart serviceId={serviceId} sinceHours={sinceHours} /> : (
            <Alert><AlertDescription>Cookie compliance was not recorded in these logs.</AlertDescription></Alert>
          )}
          {'rows' in data.latency_timeseries ? <ScorerLatencyChart serviceId={serviceId} sinceHours={sinceHours} /> : (
            <Alert><AlertDescription>Scorer execution and round-trip latency were not recorded in these logs.</AlertDescription></Alert>
          )}
        </>
      )}
    </div>
  )
}
