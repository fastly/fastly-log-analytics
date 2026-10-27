'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Info,
  LineChart,
  ListChecks,
  ShieldAlert,
  ShieldCheck,
  TrendingUp,
} from 'lucide-react'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { client, unwrapClient } from '@/lib/api'
import { CHART_COLORS, cn, formatPctValue } from '@/lib/utils'
import type { components } from '@/types/api'
import { PerReasonAucHelp, RocPrCurvesHelp } from './help-content'
import {
  scoringAnalyticsCompositeKey,
  scoringConfigCompositeKey,
  useScoringQuery,
} from '@/components/SessionScoring/ScoringAdminPanels'

type CurvePoint = NonNullable<components['schemas']['ScoringCurvePoint']>

export interface L2EnforcementCardProps {
  serviceId: string
  sinceHours?: number
}

const L2_DESCRIPTION =
  'Decide when edge Layer-2 (route-transition anomaly) joins the enforced score. L2 is always logged; this only gates blocking.'
const fmtDaysFixed = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(1))

export function L2EnforcementCard({ serviceId, sinceHours = 24 }: L2EnforcementCardProps) {
  const queryClient = useQueryClient()
  const [pendingEnable, setPendingEnable] = React.useState<boolean | null>(null)

  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-l2-enforce', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/l2-enforce', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  const { data: health } = useScoringQuery(
    ['scoring-health', serviceId, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/health', {
        params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      }),
    { staleTime: 30_000 },
  )

  const mutation = useMutation({
    mutationFn: async (enabled: boolean) =>
      unwrapClient(
        client.PUT('/api/services/{service_id}/scoring/l2-enforce', {
          params: { path: { service_id: serviceId }, query: { confirm: true } },
          body: { enabled },
        }),
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-l2-enforce', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringAnalyticsCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
    },
  })

  if (isError) {
    const raw = String(error?.message || 'Unknown error')
    const friendly =
      /status 400|http 400|config store missing|scoring not enabled/i.test(raw) ||
      (error as { status?: number } | null)?.status === 400
        ? 'Enable session scoring for this service to manage L2 enforcement.'
        : raw
    return (
      <AnalyticsCard title="L2 enforcement" description={L2_DESCRIPTION}>
        <CardErrorState
          icon={<ShieldAlert className="h-4 w-4" />}
          title="L2 enforcement unavailable"
          message={friendly}
          onRetry={() => refetch()}
        />
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard title="L2 enforcement" description={L2_DESCRIPTION}>
        <div className="space-y-3" aria-busy="true">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-12 w-full" />
        </div>
      </AnalyticsCard>
    )
  }

  const { enabled, ready } = data
  const ageDays = data.deployment_age_days
  const readinessDays = data.readiness_days || 7
  const rampDays = data.ramp_days || 3
  const l2HighPct = health?.matrix_staleness?.l2_high_pct ?? null
  const rampPct = Math.round((data.ramp_progress ?? 0) * 100)

  const isGreenAlert = enabled || ready
  const AlertIcon = enabled ? TrendingUp : ready ? ShieldCheck : ShieldAlert
  const alertTitle = enabled
    ? 'L2 contributes to enforcement'
    : ready
      ? 'Ready to enable'
      : 'Not yet recommended'
  const alertDesc = enabled ? (
    data.fully_ramped ? (
      <>L2 is fully active in the enforced score.</>
    ) : (
      <>
        Fading in — {rampPct}% of full weight
        {data.warmup_days_remaining != null && (
          <> (~{fmtDaysFixed(data.warmup_days_remaining)} days to full)</>
        )}
        . The ramp opens at 0% at opt-in, so there&apos;s no instant block.
      </>
    )
  ) : ready ? (
    <>
      {fmtDaysFixed(ageDays)} days of observed L2 data (≥{readinessDays} recommended).
      {l2HighPct != null && (
        <> About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.</>
      )}
    </>
  ) : (
    <>
      Only {fmtDaysFixed(ageDays)} days of observed L2 data so far (≥{readinessDays} recommended).
      Enabling blocking now is risky — you haven&apos;t seen enough of how L2 scores real traffic.
      {l2HighPct != null && (
        <> It would currently flag ~{formatPctValue(l2HighPct, 1)} of sessions.</>
      )}
    </>
  )

  return (
    <AnalyticsCard
      title="L2 enforcement"
      description={L2_DESCRIPTION}
      icon={<ShieldAlert className="h-4 w-4" />}
    >
      <div className="space-y-4">
        <Alert
          className={
            isGreenAlert
              ? 'border-green-300 bg-green-50/60 dark:bg-green-950/20'
              : 'border-amber-300 bg-amber-50/60 dark:bg-amber-950/20'
          }
        >
          <AlertIcon className={cn('h-4 w-4', isGreenAlert ? 'text-green-600' : 'text-amber-600')} />
          <AlertTitle
            className={
              isGreenAlert
                ? 'text-green-900 dark:text-green-300'
                : 'text-amber-900 dark:text-amber-300'
            }
          >
            {alertTitle}
          </AlertTitle>
          <AlertDescription
            className={
              isGreenAlert
                ? 'text-green-800 dark:text-green-400'
                : 'text-amber-800 dark:text-amber-400'
            }
          >
            {alertDesc}
          </AlertDescription>
        </Alert>

        <div className="flex items-center justify-between gap-4 rounded-md border bg-card p-3">
          <div className="space-y-0.5">
            <div id="l2-enforce-label" className="text-sm font-medium">
              Enforce L2 (live edge blocking)
            </div>
            <div className="text-xs text-muted-foreground">
              {enabled
                ? 'On — L2 contributes to the enforced score.'
                : 'Off — L2 is observe-only (computed and logged, never blocks).'}
            </div>
          </div>
          <Switch
            checked={enabled}
            aria-labelledby="l2-enforce-label"
            disabled={mutation.isPending}
            onCheckedChange={(next) => setPendingEnable(next)}
          />
        </div>
      </div>

      <ConfirmDialog
        open={pendingEnable !== null}
        onOpenChange={(open) => {
          if (!open && !mutation.isPending) setPendingEnable(null)
        }}
        isPending={mutation.isPending}
        isDangerous={pendingEnable === true}
        title={pendingEnable ? 'Enable L2 enforcement (LIVE)' : 'Disable L2 enforcement'}
        description={
          pendingEnable ? (
            !ready ? (
              <>
                <strong>Heads up:</strong> only {fmtDaysFixed(ageDays)} days of observed L2 data so
                far (≥{readinessDays} recommended). Enabling now means L2 starts contributing to
                live edge blocking before you&apos;ve seen how it scores real traffic. It fades in
                over {rampDays} days from now (starting at 0, so no instant block), but consider
                waiting until the readiness gauge is green. Enable anyway?
              </>
            ) : (
              <>
                L2 will join the enforced score, fading in over {rampDays} days from now (starting
                at 0% weight, so no instant block).
                {l2HighPct != null && (
                  <>
                    {' '}
                    About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.
                  </>
                )}{' '}
                Effective at the edge within seconds.
              </>
            )
          ) : (
            <>
              Disable L2 enforcement. L2 returns to observe-only — still computed and logged, but
              it stops contributing to the enforced score (no edge blocking from L2). Effective
              within seconds. Re-enabling later restarts the {rampDays}-day fade-in.
            </>
          )
        }
        confirmLabel={pendingEnable ? 'Enable' : 'Disable'}
        onConfirm={() => {
          if (pendingEnable !== null) {
            mutation.mutate(pendingEnable, { onSettled: () => setPendingEnable(null) })
          }
        }}
      />
    </AnalyticsCard>
  )
}

export interface RocPrCurvesProps {
  serviceId: string
  sinceHours?: number
}

const makeCurveTrace = (
  pts: CurvePoint[] | null | undefined,
  xK: keyof CurvePoint,
  yK: keyof CurvePoint,
  name: string,
  color: string,
  xLbl: string,
  yLbl: string,
) => ({
  x: (pts ?? []).map((p) => (p[xK] as number | undefined) ?? 0),
  y: (pts ?? []).map((p) => (p[yK] as number | undefined) ?? 0),
  type: 'scatter',
  mode: 'lines+markers',
  name,
  line: { color, width: 2 },
  marker: { size: 4 },
  hovertemplate: `threshold=%{customdata}<br>${xLbl}=%{x:.3f}<br>${yLbl}=%{y:.3f}<extra></extra>`,
  customdata: (pts ?? []).map((p) => p.threshold),
})

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
      icon={<LineChart className="h-4 w-4" />}
      helpContent={<RocPrCurvesHelp />}
      helpTitle="About ROC & PR Curves"
      description="Two views of the trade-off between catching bad sessions (true positives) and over-flagging good ones (false positives). Computed against ALL of your accumulated labels."
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
          Need {data.min_per_class}+ good and {data.min_per_class}+ bad labels to plot meaningful
          curves
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
            <CurveChart
              header={`ROC — AUC ${(data.auc ?? 0).toFixed(3)}`}
              xTitle="False positive rate"
              yTitle="True positive rate"
              traces={[
                makeCurveTrace(data.roc, 'fpr', 'tpr', 'ROC', '#0ea5e9', 'FPR', 'TPR'),
                {
                  x: [0, 1],
                  y: [0, 1],
                  type: 'scatter',
                  mode: 'lines',
                  name: 'random',
                  line: { color: CHART_COLORS.neutral, width: 1, dash: 'dash' },
                  hoverinfo: 'skip',
                },
              ]}
            />
            <CurveChart
              header={`Precision-Recall — AP ${(data.average_precision ?? 0).toFixed(3)}`}
              xTitle="Recall"
              yTitle="Precision"
              traces={[
                makeCurveTrace(
                  data.pr,
                  'recall',
                  'precision',
                  'PR',
                  CHART_COLORS.success,
                  'recall',
                  'precision',
                ),
              ]}
            />
          </div>
          <p className="text-[11px] text-muted-foreground italic text-center">
            n={data.n_good} good / {data.n_bad} bad · AUC{' '}
            <span className="font-mono tabular-nums font-semibold">
              {(data.auc ?? 0).toFixed(3)}
            </span>{' '}
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

function CurveChart({
  header,
  xTitle,
  yTitle,
  traces,
}: {
  header: string
  xTitle: string
  yTitle: string
  traces: Record<string, unknown>[]
}) {
  return (
    <div className="border rounded-md p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground px-2 pb-1">
        {header}
      </div>
      <PlotlyChart
        data={traces}
        height={240}
        layout={{
          xaxis: { title: xTitle, range: [0, 1] },
          yaxis: { title: yTitle, range: [0, 1] },
          showlegend: false,
          margin: { l: 50, r: 10, t: 10, b: 40 },
        }}
      />
    </div>
  )
}

export function PerReasonAucCard({ serviceId }: { serviceId: string }) {
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
      icon={<ListChecks className="h-4 w-4" />}
      helpContent={<PerReasonAucHelp />}
      helpTitle="About AUC by Rule"
      description="Which scoring rule contributes most to matrix-vs-labels separation. Each bucket shows AUC against sessions where that rule fired."
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
            need {data.min_per_class}+ good / {data.min_per_class}+ bad labels (have {data.n_good}/
            {data.n_bad})
          </span>
        </div>
      ) : (
        <div className="space-y-1.5">
          {data.buckets.map((b: components['schemas']['ScoringPerReasonBucket']) => (
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
  return (
    <div className="flex items-center justify-between gap-3 p-2 rounded-md border text-xs">
      <span className="font-mono">{bucket.reason}</span>
      <div className="flex items-center gap-2">
        <span
          className={`font-mono font-semibold tabular-nums ${passed ? 'text-emerald-600' : 'text-amber-600'}`}
        >
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

export function ScoringEvaluationPanel({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  return (
    <div className="space-y-6">
      <L2EnforcementCard serviceId={serviceId} sinceHours={sinceHours} />
      <RocPrCurves serviceId={serviceId} sinceHours={sinceHours} />
      <PerReasonAucCard serviceId={serviceId} />
    </div>
  )
}
