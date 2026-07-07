'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { ListChecks } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/CardErrorState'
import { PerReasonAucHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import { Info } from 'lucide-react'
import type { components } from '@/types/api.generated'

type PerReasonResponse = components['schemas']['ScoringPerReasonResponse']

interface PerReasonAucCardProps {
  serviceId: string
}

/**
 * Per-rule AUC breakdown. Shows the matrix's separation power for each
 * L1/L2 atom (cookie-missing, impossibly-fast, etc.) so the operator
 * can see which rule is doing the work and which is noise.
 *
 * Sub-min-samples bucket shows a CTA pushing the operator to label more
 * sessions with that reason instead of a noisy AUC. When the overall
 * label population is under-min, renders one empty state instead of
 * five empty buckets.
 */
export function PerReasonAucCard({ serviceId }: PerReasonAucCardProps) {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['scoring-evaluation-per-reason', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/evaluation/per-reason',
        { params: { path: { service_id: serviceId } } },
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as PerReasonResponse
    },
    staleTime: 30_000,
  })

  return (
    <AnalyticsCard
      title="AUC by rule"
      description="Which scoring rule contributes most to matrix-vs-labels separation. Each bucket shows AUC against sessions where that rule fired."
      icon={<ListChecks className="h-4 w-4" />}
      helpContent={<PerReasonAucHelp />}
      helpTitle="About AUC by Rule"
    >
      {isError ? (
        <CardErrorState
          icon={<Info className="h-4 w-4" />}
          title="Failed to load per-rule AUC"
          message={(error as Error | null)?.message || 'Unknown error'}
          onRetry={() => refetch()}
        />
      ) : isLoading || !data ? (
        <div className="space-y-2">
          {['r1', 'r2', 'r3', 'r4', 'r5'].map((k) => (
            <Skeleton key={k} className="h-10 w-full" />
          ))}
        </div>
      ) : !data.has_min_samples_overall ? (
        <div className="p-4 border border-dashed rounded-md text-center text-sm text-muted-foreground">
          Per-rule breakdown unlocks once headline AUC is computable —
          <span className="ml-1 text-foreground font-mono">
            need {data.min_per_class}+ good / {data.min_per_class}+ bad labels (have{' '}
            {data.n_good}/{data.n_bad})
          </span>
        </div>
      ) : (
        <div className="space-y-1.5">
          {data.buckets.map((b) => (
            <BucketRow key={b.reason} bucket={b} />
          ))}
        </div>
      )}
    </AnalyticsCard>
  )
}

function BucketRow({ bucket }: { bucket: components['schemas']['ScoringPerReasonBucket'] }) {
  if (!bucket.has_min_samples) {
    return (
      <div className="flex items-center justify-between gap-3 p-2 rounded-md border bg-muted/20 text-xs">
        <span className="font-mono">{bucket.reason}</span>
        <span className="text-muted-foreground">
          need {bucket.min_per_class}+ good / {bucket.min_per_class}+ bad with this reason
          <span className="ml-1 text-foreground tabular-nums">
            (have {bucket.n_good}/{bucket.n_bad})
          </span>
        </span>
      </div>
    )
  }

  const passed = !!bucket.passed
  const aucClass = passed ? 'text-emerald-600' : 'text-amber-600'
  return (
    <div className="flex items-center justify-between gap-3 p-2 rounded-md border text-xs">
      <span className="font-mono">{bucket.reason}</span>
      <div className="flex items-center gap-2">
        <span className={`font-mono font-semibold tabular-nums ${aucClass}`}>
          {(bucket.auc ?? 0).toFixed(3)}
        </span>
        <Badge variant={passed ? 'success' : 'secondary'} className="text-[10px]">
          {passed ? 'PASS' : 'BELOW'}
        </Badge>
        <span className="text-muted-foreground text-[10px] tabular-nums">
          n={bucket.n_good}g / {bucket.n_bad}b
        </span>
      </div>
    </div>
  )
}
