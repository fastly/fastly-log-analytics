'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Info, LineChart } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/SessionScoring/CardErrorState'
import { RocPrCurvesHelp } from '@/components/SessionScoring/help-content'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

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
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['scoring-curves', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/curves',
        { params: { path: { service_id: serviceId } } },
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data
    },
    staleTime: 30_000,
  })

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
          message={(error as any)?.message || 'Unknown error'}
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
      line: { color: '#94a3b8', width: 1, dash: 'dash' },
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
      line: { color: '#10b981', width: 2 },
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
