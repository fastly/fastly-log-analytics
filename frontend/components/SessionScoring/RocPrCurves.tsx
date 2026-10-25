'use client'

import * as React from 'react'
import { Info, LineChart, ListChecks } from 'lucide-react'
import { useScoringQuery } from './useScorerTimeseries'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/AnalyticsCard'
import { PerReasonAucHelp, RocPrCurvesHelp } from '@/components/SessionScoring/help-content'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { CHART_COLORS } from '@/lib/chart-helpers'

// Generated from the /scoring/curves response_model — single source of truth.
type CurvePoint = NonNullable<components['schemas']['ScoringCurvePoint']>

interface RocPrCurvesProps {
  serviceId: string
  /** sinceHours is accepted for API consistency but unused — curves
   * evaluate against ALL labels regardless of window (labels are tagged
   * to specific sids; their score history is per-session, not windowed). */
  sinceHours?: number
}

/**
 * ROC + Precision-Recall curves against the operator's labels.
 *
 * Pinned visual sanity-checks for any threshold choice the operator
 * might make in ThresholdSlider:
 *   ROC: curve hugging the top-left corner = great separation; the
 *        diagonal = random guessing.
 *   PR: curve hugging the top-right = high precision AT high recall;
 *       dropping off fast = matrix can either be precise OR thorough
 *       but not both.
 *
 * Sub-min-samples state renders a CTA pushing the operator to label
 * more sessions — sub-3 curves are spike-shaped noise.
 */
export function RocPrCurves({ serviceId }: RocPrCurvesProps) {
  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-curves', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/curves', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  return (
    <AnalyticsCard
      title="ROC + Precision-Recall curves"
      description="Two views of the trade-off between catching bad sessions (true positives) and over-flagging good ones (false positives). Computed against ALL of your accumulated labels."
      icon={<LineChart className="h-4 w-4" />}
      helpContent={<RocPrCurvesHelp />}
      helpTitle="About ROC & PR Curves"
    >
      {isError ? (
        <CardErrorState
          icon={<Info className="h-4 w-4" />}
          title="Failed to load ROC + PR curves"
          message={error?.message || 'Unknown error'}
          onRetry={() => refetch()}
        />
      ) : isLoading || !data ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          <Skeleton className="h-64 w-full" />
          <Skeleton className="h-64 w-full" />
        </div>
      ) : !data.has_min_samples ? (
        <div className="p-6 border border-dashed rounded-md text-center text-sm text-muted-foreground">
          Need {data.min_per_class}+ good and {data.min_per_class}+ bad labels to plot
          meaningful curves
          <span className="ml-1 text-foreground font-mono tabular-nums">
            (have {data.n_good}/{data.n_bad})
          </span>
          {data.note && (
            <div className="mt-2 text-xs">
              <span className="text-amber-600">{data.note}</span>
            </div>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <RocChart points={data.roc ?? []} auc={data.auc ?? 0} />
            <PrChart points={data.pr ?? []} ap={data.average_precision ?? 0} />
          </div>
          <p className="text-[11px] text-muted-foreground italic text-center">
            n={data.n_good} good / {data.n_bad} bad · AUC{' '}
            <span className="font-mono tabular-nums font-semibold">{(data.auc ?? 0).toFixed(3)}</span>{' '}
            · Average precision{' '}
            <span className="font-mono tabular-nums font-semibold">
              {(data.average_precision ?? 0).toFixed(3)}
            </span>
          </p>
        </div>
      )}
    </AnalyticsCard>
  )
}

function RocChart({ points, auc }: { points: CurvePoint[]; ap?: number; auc: number }) {
  // Plotly trace: curve from (0,0) to (1,1). The diagonal reference line
  // represents random guessing — anything ABOVE it has signal.
  const traces = [
    {
      x: points.map((p) => p.fpr ?? 0),
      y: points.map((p) => p.tpr ?? 0),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'ROC',
      line: { color: '#0ea5e9', width: 2 },
      marker: { size: 4 },
      hovertemplate: 'threshold=%{customdata}<br>FPR=%{x:.3f}<br>TPR=%{y:.3f}<extra></extra>',
      customdata: points.map((p) => p.threshold),
    },
    {
      x: [0, 1],
      y: [0, 1],
      type: 'scatter',
      mode: 'lines',
      name: 'random',
      line: { color: CHART_COLORS.neutral, width: 1, dash: 'dash' },
      hoverinfo: 'skip',
    },
  ]
  return (
    <div className="border rounded-md p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground px-2 pb-1">
        ROC — AUC {auc.toFixed(3)}
      </div>
      <PlotlyChart
        data={traces}
        height={240}
        layout={{
          xaxis: { title: 'False positive rate', range: [0, 1] },
          yaxis: { title: 'True positive rate', range: [0, 1] },
          showlegend: false,
          margin: { l: 50, r: 10, t: 10, b: 40 },
        }}
      />
    </div>
  )
}

function PrChart({ points, ap }: { points: CurvePoint[]; ap: number }) {
  const traces = [
    {
      x: points.map((p) => p.recall ?? 0),
      y: points.map((p) => p.precision ?? 0),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'PR',
      line: { color: CHART_COLORS.success, width: 2 },
      marker: { size: 4 },
      hovertemplate: 'threshold=%{customdata}<br>recall=%{x:.3f}<br>precision=%{y:.3f}<extra></extra>',
      customdata: points.map((p) => p.threshold),
    },
  ]
  return (
    <div className="border rounded-md p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground px-2 pb-1">
        Precision-Recall — AP {ap.toFixed(3)}
      </div>
      <PlotlyChart
        data={traces}
        height={240}
        layout={{
          xaxis: { title: 'Recall', range: [0, 1] },
          yaxis: { title: 'Precision', range: [0, 1] },
          showlegend: false,
          margin: { l: 50, r: 10, t: 10, b: 40 },
        }}
      />
    </div>
  )
}

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
  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-evaluation-per-reason', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/evaluation/per-reason', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

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
