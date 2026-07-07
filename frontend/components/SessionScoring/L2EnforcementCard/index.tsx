'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { ShieldAlert, ShieldCheck, TrendingUp } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/CardErrorState'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { client } from '@/lib/api'

import { useScoringQuery } from '../useScoringQuery'

/**
 * Operator control for whether edge Layer-2 contributes to the *enforced*
 * combined session score. L2's sub-score is always computed + logged; this card
 * gates only its contribution to blocking.
 *
 * Two decoupled signals:
 *  - Readiness GAUGE (advisory): deployment age ≥ 7 days = "enough observed L2
 *    data to consider enabling". Never an actuator.
 *  - Opt-in (actuator): an explicit Switch (behind a ConfirmDialog) that flips
 *    ``l2_enforce_enabled``. On enable, L2 fades in over ~3 days from the moment
 *    of consent — no instant step-change block.
 *
 * SOFT gate: the Switch is always usable; the ConfirmDialog copy escalates when
 * the readiness gauge is not yet green.
 */

interface L2EnforceState {
  available: boolean
  enabled: boolean
  l2_enabled_at: number | null
  days_since_optin: number | null
  ramp_progress: number
  fully_ramped: boolean
  warmup_days_remaining: number | null
  scoring_enabled_at: number | null
  deployment_age_days: number | null
  ready: boolean
  ramp_days: number
  readiness_days: number
}

interface HealthForL2 {
  matrix_staleness?: { l2_high_pct: number; l2_evaluated: number }
}

interface L2EnforcementCardProps {
  serviceId: string
  sinceHours?: number
}

const DESCRIPTION =
  'Decide when edge Layer-2 (route-transition anomaly) joins the enforced score. L2 is always logged; this only gates blocking.'

const fmtDays = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(1))
const fmtPct = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(1)}%`)

export function L2EnforcementCard({ serviceId, sinceHours = 24 }: L2EnforcementCardProps) {
  const queryClient = useQueryClient()
  // null = dialog closed; true/false = pending target enabled-state under confirm.
  const [pendingEnable, setPendingEnable] = React.useState<boolean | null>(null)

  const { data, isLoading, isError, error, refetch } = useScoringQuery<L2EnforceState>(
    ['scoring-l2-enforce', serviceId],
    serviceId,
    'l2-enforce',
    {},
    { staleTime: 30_000 },
  )

  // "What L2 would flag" (l2_high_pct) rides the health query the page already
  // fetches + seeds — read the same key so we don't fire a second request.
  const { data: health } = useScoringQuery<HealthForL2>(
    ['scoring-health', serviceId, sinceHours],
    serviceId,
    'health',
    { since_hours: sinceHours },
    { staleTime: 30_000 },
  )

  const mutation = useMutation({
    mutationFn: async (enabled: boolean) => {
      const { data, response } = await client.PUT('/api/services/{service_id}/scoring/l2-enforce', {
        params: { path: { service_id: serviceId }, query: { confirm: true } },
        body: { enabled },
      })
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-l2-enforce', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-analytics-composite', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-config-composite', serviceId] })
    },
  })

  if (isError) {
    // A 400 here means scoring isn't enabled yet (no config store) — that's not a
    // failure, just a precondition. Map it to friendly copy; surface anything
    // else as a real error.
    const raw = String(error?.message || 'Unknown error')
    // A 400 here means scoring isn't enabled yet / the config store isn't
    // resolvable. The openapi client may surface either "status 400" or the raw
    // backend detail ("Scoring not enabled or config store missing"), so match
    // both rather than leaking the raw string. A freshly-enabled service that
    // still shows this is just a pre-enable fetch the StatusPanel re-invalidates.
    const friendly = /status 400|config store missing|scoring not enabled/i.test(raw)
      ? 'Enable session scoring for this service to manage L2 enforcement.'
      : raw
    return (
      <AnalyticsCard title="L2 enforcement" description={DESCRIPTION}>
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
      <AnalyticsCard title="L2 enforcement" description={DESCRIPTION}>
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

  return (
    <AnalyticsCard
      title="L2 enforcement"
      description={DESCRIPTION}
      icon={<ShieldAlert className="h-4 w-4" />}
    >
      <div className="space-y-4">
        {/* Readiness / status banner. */}
        {enabled ? (
          <Alert className="border-green-300 bg-green-50/60 dark:bg-green-950/20">
            <TrendingUp className="h-4 w-4 text-green-600" />
            <AlertTitle className="text-green-900 dark:text-green-300">
              L2 contributes to enforcement
            </AlertTitle>
            <AlertDescription className="text-green-800 dark:text-green-400">
              {data.fully_ramped ? (
                <>L2 is fully active in the enforced score.</>
              ) : (
                <>
                  Fading in — {rampPct}% of full weight
                  {data.warmup_days_remaining != null && (
                    <> (~{fmtDays(data.warmup_days_remaining)} days to full)</>
                  )}
                  . The ramp opens at 0% at opt-in, so there&apos;s no instant block.
                </>
              )}
            </AlertDescription>
          </Alert>
        ) : ready ? (
          <Alert className="border-green-300 bg-green-50/60 dark:bg-green-950/20">
            <ShieldCheck className="h-4 w-4 text-green-600" />
            <AlertTitle className="text-green-900 dark:text-green-300">Ready to enable</AlertTitle>
            <AlertDescription className="text-green-800 dark:text-green-400">
              {fmtDays(ageDays)} days of observed L2 data (≥{readinessDays} recommended).
              {l2HighPct != null && <> About {fmtPct(l2HighPct)} of recent sessions would be flagged by L2.</>}
            </AlertDescription>
          </Alert>
        ) : (
          <Alert className="border-amber-300 bg-amber-50/60 dark:bg-amber-950/20">
            <ShieldAlert className="h-4 w-4 text-amber-600" />
            <AlertTitle className="text-amber-900 dark:text-amber-300">Not yet recommended</AlertTitle>
            <AlertDescription className="text-amber-800 dark:text-amber-400">
              Only {fmtDays(ageDays)} days of observed L2 data so far (≥{readinessDays} recommended).
              Enabling blocking now is risky — you haven&apos;t seen enough of how L2 scores real
              traffic.
              {l2HighPct != null && <> It would currently flag ~{fmtPct(l2HighPct)} of sessions.</>}
            </AlertDescription>
          </Alert>
        )}

        {/* The opt-in switch (soft gate — always usable, gated by a confirm). */}
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
                <strong>Heads up:</strong> only {fmtDays(ageDays)} days of observed L2 data so far
                (≥{readinessDays} recommended). Enabling now means L2 starts contributing to live
                edge blocking before you&apos;ve seen how it scores real traffic. It fades in over{' '}
                {rampDays} days from now (starting at 0, so no instant block), but consider waiting
                until the readiness gauge is green. Enable anyway?
              </>
            ) : (
              <>
                L2 will join the enforced score, fading in over {rampDays} days from now (starting at
                0% weight, so no instant block).
                {l2HighPct != null && <> About {fmtPct(l2HighPct)} of recent sessions would be flagged by L2.</>}{' '}
                Effective at the edge within seconds.
              </>
            )
          ) : (
            <>
              Disable L2 enforcement. L2 returns to observe-only — still computed and logged, but it
              stops contributing to the enforced score (no edge blocking from L2). Effective within
              seconds. Re-enabling later restarts the {rampDays}-day fade-in.
            </>
          )
        }
        confirmLabel={pendingEnable ? 'Enable' : 'Disable'}
        onConfirm={() => {
          if (pendingEnable === null) return
          mutation.mutate(pendingEnable, { onSettled: () => setPendingEnable(null) })
        }}
      />
    </AnalyticsCard>
  )
}
