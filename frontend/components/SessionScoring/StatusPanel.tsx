'use client'

import React, { useState, type ReactNode } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  ExternalLink,
  Info,
  LineChart,
  ListChecks,
  Power,
  PowerOff,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  ShieldOff,
  TrendingUp,
} from 'lucide-react'
import { AnalyticsCard, CardErrorState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { SSEModal } from '@/components/SSEModal'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog, CopyButton } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { client, unwrapClient } from '@/lib/api'
import {
  CHART_COLORS,
  cn,
  denseBarTrace,
  denseTimeGrid,
  formatLocalDateTime,
  formatPctValue,
  lineTrace,
  withSecondaryAxis,
} from '@/lib/utils'
import type { components, PlotlyLayout, PlotlyTrace } from '@/types/api'
import {
  ComplianceHelp,
  PerReasonAucHelp,
  RocPrCurvesHelp,
  ScoreDistHelp,
  ScorerErrorsHelp,
  ScorerLatencyHelp,
  StatusPanelHelp,
} from './help-content'
import {
  deriveScorerSeries,
  invalidateServiceScoringQueries,
  scorerHourlyLayout,
  scoringAnalyticsCompositeKey,
  scoringConfigCompositeKey,
  useScorerTimeseries,
  useScoringQuery,
  usToMs,
  type LatencyResponse,
  type LatencyRow,
} from '@/components/SessionScoring/ScoringAdminPanels'

export { StorageCalendar, StorageStatus, MetadataStorageCard } from './StoragePanels'

export interface StatusPanelProps {
  serviceId: string
}

type ScoringEvaluation = components['schemas']['ScoringEvaluationResponse']
type CurvePoint = NonNullable<components['schemas']['ScoringCurvePoint']>

const formatActivated = (raw: string): string => {
  const d = new Date(raw)
  return isNaN(d.getTime()) ? raw : formatLocalDateTime(d)
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-muted-foreground text-xs w-32 shrink-0">{label}:</span>
      <span className="text-xs">{value}</span>
    </div>
  )
}

function AucField({ evalData }: { evalData: ScoringEvaluation }) {
  if (!evalData.has_min_samples) {
    return (
      <Field
        label="Matrix Quality"
        value={
          <span className="text-xs text-muted-foreground">
            Need {evalData.min_per_class}+ good / {evalData.min_per_class}+ bad labels
            <span className="ml-1 text-foreground tabular-nums">
              (have {evalData.n_good}/{evalData.n_bad})
            </span>
          </span>
        }
      />
    )
  }
  if (evalData.error) {
    return (
      <Field
        label="Matrix Quality"
        value={<span className="text-xs text-amber-600">{evalData.error}</span>}
      />
    )
  }
  const auc = evalData.auc ?? 0
  const passed = !!evalData.passed
  const colorClass = passed ? 'text-emerald-600' : 'text-amber-600'
  return (
    <Field
      label="Matrix Quality (AUC)"
      value={
        <span className="text-xs">
          <span className={`font-mono font-semibold tabular-nums ${colorClass}`}>
            {auc.toFixed(3)}
          </span>
          <span className={`ml-1 font-semibold ${colorClass}`}>
            — {passed ? 'PASS' : 'BELOW THRESHOLD'}
          </span>
          <span className="ml-1 text-muted-foreground">
            (n={evalData.n_good} good / {evalData.n_bad} bad
            {evalData.threshold ? `, threshold ${evalData.threshold.toFixed(2)}` : ''})
          </span>
        </span>
      }
    />
  )
}

const REDEPLOY_STEPS = [
  'Checks the live edge first: re-uploads the Wasm only if the build changed',
  'Re-activates the logging VCL only if the scoring snippets/backend changed',
  'Skips no-op version bumps when nothing changed — no scoring gap',
]

const DISABLE_STEPS = [
  'Removes the 6 VCL snippets (recv / pass / fetch / deliver / miss / enforce) that route requests through the scorer',
  'Removes the 6 scoring custom_fields from the log format',
  'Clones + activates a new VCL version on the customer service',
  'Deletes the scorer Compute service + its ConfigStores and matrix KV Store (a future Enable re-provisions them from scratch)',
]

function StepListDescription({
  intro,
  serviceId,
  steps,
  header,
  footer,
}: {
  intro: string
  serviceId: string
  steps: React.ReactNode[]
  header?: React.ReactNode
  footer?: React.ReactNode
}) {
  return (
    <div className="space-y-2">
      {header}
      <p>
        {intro} <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:
      </p>
      <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
        {steps.map((s) => (
          <li key={typeof s === 'string' ? s : String(steps.indexOf(s))}>{s}</li>
        ))}
      </ul>
      {footer}
      <p className="text-xs text-muted-foreground italic">
        Click <strong>Start</strong> to proceed.
      </p>
    </div>
  )
}

function CalloutBanner({
  tone = 'amber',
  children,
  testId,
}: {
  tone?: 'amber' | 'sky' | 'muted'
  children: React.ReactNode
  testId?: string
}) {
  const Icon = tone === 'amber' ? AlertTriangle : Info
  const cls =
    tone === 'amber'
      ? 'border-amber-300 bg-amber-50 text-amber-900'
      : tone === 'sky'
        ? 'border-sky-300 bg-sky-50 text-sky-900'
        : 'bg-muted/40 text-muted-foreground'
  return (
    <div className={`flex items-start gap-2 rounded border px-2.5 py-1.5 ${cls}`} data-testid={testId}>
      <Icon className="h-3.5 w-3.5 mt-0.5 shrink-0" />
      <div className="text-xs">{children}</div>
    </div>
  )
}

export function StatusPanel({ serviceId }: StatusPanelProps) {
  const queryClient = useQueryClient()
  const { data, isLoading, isFetching, error, refetch } = useScoringQuery(
    ['scoring-status', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/status', {
        params: { path: { service_id: serviceId } },
      }),
  )

  const enabled = !!data?.enabled
  const pragmasMissing = !enabled && data?.preflight_pragmas_ready === false
  const [pragmaModalOpen, setPragmaModalOpen] = React.useState(false)
  if (data?.preflight_pragmas_ready === true && pragmaModalOpen) setPragmaModalOpen(false)

  const accountTeamMessage = `Hi Fastly team — please enable the preflight request body preservation service pragmas on Fastly service ${serviceId} (preserving unsent request bodies across VCL restarts and omitting request bodies when backend requests are rewritten to GET or HEAD) so we can enable our Session Scoring Compute preflight pass.`

  const refreshScoring = React.useCallback(() => {
    refetch()
    invalidateServiceScoringQueries(queryClient, serviceId)
  }, [queryClient, refetch, serviceId])

  const { data: evalData } = useScoringQuery(
    ['scoring-evaluation', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/evaluation', {
        params: { path: { service_id: serviceId } },
      }),
    { enabled, staleTime: 30_000 },
  )

  const extLinkCls =
    'text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity'

  return (
    <div data-testid="admin-session-scoring-status-card">
      <AnalyticsCard
        title="Session Scoring"
        icon={enabled ? <ShieldCheck className="h-4 w-4" /> : <ShieldOff className="h-4 w-4" />}
        description="Edge scorer that classifies sessions in real-time using the cookie-bound state machine and matrix-based L2 evaluation."
        isLoading={isLoading}
        isFetching={isFetching}
        loadingMessage="Loading session scoring status..."
        error={error as (Error & { status?: number }) | null}
        helpContent={<StatusPanelHelp />}
        helpTitle="About Session Scoring"
        className="min-h-[220px]"
        headerAction={
          enabled ? (
            <div className="flex items-center gap-2">
              <SSEModal
                title="Redeploy Scorer"
                endpoint={`/api/services/${serviceId}/scoring/enable`}
                body={{}}
                onClose={refreshScoring}
                description={
                  <StepListDescription
                    intro="Push the latest scorer build to the edge for"
                    serviceId={serviceId}
                    steps={REDEPLOY_STEPS}
                  />
                }
                trigger={
                  <Button
                    variant={data?.scorer_drift ? 'default' : 'outline'}
                    size="sm"
                    data-testid="admin-session-scoring-redeploy-btn"
                  >
                    <RefreshCw className="h-3.5 w-3.5 mr-1" /> Redeploy
                  </Button>
                }
              />
              <SSEModal
                title="Disable Session Scoring"
                endpoint={`/api/services/${serviceId}/scoring/disable`}
                body={{}}
                onClose={refreshScoring}
                description={
                  <StepListDescription
                    intro="This will disable session scoring on"
                    serviceId={serviceId}
                    steps={DISABLE_STEPS}
                  />
                }
                trigger={
                  <Button
                    variant="outline"
                    size="sm"
                    className="text-rose-700 border-rose-300 hover:bg-rose-50"
                    data-testid="admin-session-scoring-disable-btn"
                  >
                    <PowerOff className="h-3.5 w-3.5 mr-1" /> Disable
                  </Button>
                }
              />
            </div>
          ) : pragmasMissing ? (
            <>
              <Button
                variant="default"
                size="sm"
                data-testid="admin-session-scoring-enable-btn"
                onClick={() => setPragmaModalOpen(true)}
              >
                <Power className="h-3.5 w-3.5 mr-1" /> Enable
              </Button>
              <Dialog open={pragmaModalOpen} onOpenChange={setPragmaModalOpen}>
                <DialogContent className="sm:max-w-lg" data-testid="admin-session-scoring-pragma-modal">
                  <DialogHeader>
                    <DialogTitle>Preflight Request Body Preservation Required</DialogTitle>
                    <DialogDescription>
                      Before enabling the Session Scoring Compute scorer on{' '}
                      <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>, your
                      Fastly account team must enable preflight request body preservation on this
                      service.
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-3 py-2 text-sm">
                    <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Why is this required?</div>
                        <div>
                          Session Scoring routes each request through a lightweight Compute
                          preflight sub-request (rewritten to <code>GET</code>) and then restarts
                          VCL to forward the original request to your origin. Without service-level
                          preflight body preservation enabled, <code>POST</code>/<code>PUT</code>/
                          <code>PATCH</code> request bodies can be drained or stall when forwarding
                          to origin after the restart.
                        </div>
                      </div>
                    </div>
                    <div className="space-y-1.5">
                      <div className="text-xs font-medium text-muted-foreground">
                        Send this request to your Fastly account team:
                      </div>
                      <pre
                        className="rounded border bg-muted/60 p-3 text-xs whitespace-pre-wrap break-words font-mono"
                        data-testid="admin-session-scoring-pragma-message"
                      >
                        {accountTeamMessage}
                      </pre>
                    </div>
                  </div>
                  <DialogFooter className="gap-2 sm:justify-between">
                    <CopyButton
                      text={accountTeamMessage}
                      label="Copy Message"
                      copiedLabel="Copied"
                      variant="outline"
                      size="sm"
                      className=""
                      iconClassName="h-3.5 w-3.5"
                      data-testid="admin-session-scoring-pragma-copy-btn"
                    />
                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => refetch()}
                        disabled={isFetching}
                        data-testid="admin-session-scoring-pragma-recheck-btn"
                      >
                        <RefreshCw
                          className={`h-3.5 w-3.5 mr-1 ${isFetching ? 'animate-spin' : ''}`}
                        />{' '}
                        Check Again
                      </Button>
                      <Button
                        type="button"
                        variant="default"
                        size="sm"
                        onClick={() => setPragmaModalOpen(false)}
                      >
                        Close
                      </Button>
                    </div>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </>
          ) : (
            <SSEModal
              title="Enable Session Scoring"
              endpoint={`/api/services/${serviceId}/scoring/enable`}
              body={{}}
              onClose={refreshScoring}
              description={
                <StepListDescription
                  intro="This will enable session scoring on"
                  serviceId={serviceId}
                  header={
                    <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Experimental feature</div>
                        <div>
                          Session scoring has not been validated against production traffic at
                          scale. Run it on a non-critical service first, watch the scorer Compute
                          service&apos;s error rate, and keep the disable path handy.
                        </div>
                      </div>
                    </div>
                  }
                  steps={[
                    "Provisions the scorer Compute service if it doesn't exist, deploys the trained Wasm scorer with the latest matrix",
                    <>
                      Adds the <code>session_scorer</code> backend, 6 VCL snippets, and 9 score
                      fields (<code>edge_score</code>, session id, reason, L1/L2, latency, …) to
                      the customer service
                    </>,
                    'Clones + activates a new VCL version',
                    'Every request will be routed through the scorer for an L1+L2 score on the response cookie',
                  ]}
                  footer={
                    <div className="flex items-start gap-2 rounded border border-sky-300 bg-sky-50 px-3 py-2 text-sky-900">
                      <Info className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Log fields</div>
                        <div>
                          Scoring also needs the standard request fields for context —{' '}
                          <strong>URL, method, user agent, and geo</strong>. Enabling scoring
                          ensures those stay logged (added automatically if this service was set up
                          with a minimal log-field set), so scored sessions show <em>which</em>{' '}
                          requests were flagged, not just the score.
                        </div>
                      </div>
                    </div>
                  }
                />
              }
              trigger={
                <Button variant="default" size="sm" data-testid="admin-session-scoring-enable-btn">
                  <Power className="h-3.5 w-3.5 mr-1" /> Enable
                </Button>
              }
            />
          )
        }
      >
        <div className="space-y-3 text-sm">
          <div className="flex items-center gap-2" data-testid="admin-session-scoring-status-line">
            <span className="text-muted-foreground">Status:</span>
            {enabled ? (
              <Badge
                variant="default"
                className="bg-emerald-600"
                data-testid="admin-session-scoring-status-badge"
              >
                Enabled
              </Badge>
            ) : (
              <Badge variant="secondary" data-testid="admin-session-scoring-status-badge">
                Disabled
              </Badge>
            )}
          </div>

          {enabled && (
            <>
              {data?.scorer_drift && (
                <CalloutBanner>
                  <span className="font-semibold">Redeploy needed.</span> The edge is running an
                  older scorer build{data.drift_detail ? ` (${data.drift_detail})` : ''} than the
                  one shipped to the backend. Click <strong>Redeploy</strong> above to push the
                  latest.
                </CalloutBanner>
              )}
              {!data?.scorer_drift && data?.drift_known === false && (
                <CalloutBanner tone="muted">
                  <span className="font-semibold text-foreground">
                    Drift detection not baselined.
                  </span>{' '}
                  This service was enabled before edge-drift tracking shipped, so we can&apos;t tell
                  whether the edge is up to date. Click <strong>Redeploy</strong> once to baseline
                  it — after that a banner appears here whenever the edge falls behind.
                </CalloutBanner>
              )}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-2">
                {data?.scoring_service_id && (
                  <Field
                    label="Scoring Service ID"
                    value={
                      <div className="flex items-center gap-1.5">
                        <code className="text-xs">{data.scoring_service_id}</code>
                        <a
                          href={`https://manage.fastly.com/configure/services/${data.scoring_service_id}`}
                          target="_blank"
                          rel="noreferrer"
                          className={extLinkCls}
                          title="View Service in Fastly"
                        >
                          <ExternalLink className="h-3 w-3" />
                        </a>
                      </div>
                    }
                  />
                )}
                {typeof data?.scoring_active_version === 'number' && (
                  <Field
                    label="Active Version"
                    value={
                      <div className="flex items-center gap-1.5">
                        <code className="text-xs">v{data.scoring_active_version}</code>
                        {data?.scoring_service_id && (
                          <a
                            href={`https://manage.fastly.com/configure/services/${data.scoring_service_id}/versions`}
                            target="_blank"
                            rel="noreferrer"
                            className={extLinkCls}
                            title="View versions in Fastly"
                          >
                            <ExternalLink className="h-3 w-3" />
                          </a>
                        )}
                      </div>
                    }
                  />
                )}
                {data?.scoring_activated_at && (
                  <Field
                    label="Activated"
                    value={
                      <span className="text-xs text-muted-foreground" title={data.scoring_activated_at}>
                        {formatActivated(data.scoring_activated_at)}
                      </span>
                    }
                  />
                )}
                {data?.scoring_service_name && (
                  <Field label="Service Name" value={data.scoring_service_name} />
                )}
                {data?.scoring_domain && (
                  <Field
                    label="Domain"
                    value={
                      <a
                        href={`https://${data.scoring_domain}/_status`}
                        target="_blank"
                        rel="noreferrer"
                        className="text-primary hover:underline inline-flex items-center gap-1"
                      >
                        {data.scoring_domain} <ExternalLink className="h-3 w-3" />
                      </a>
                    }
                  />
                )}
                {data?.matrix_version && (
                  <Field
                    label="Matrix Version"
                    value={<code className="text-xs">{data.matrix_version}</code>}
                  />
                )}
                {data?.enabled_at && (
                  <Field
                    label="Enabled At"
                    value={<span className="text-xs text-muted-foreground">{data.enabled_at}</span>}
                  />
                )}
                {evalData && <AucField evalData={evalData} />}
              </div>
            </>
          )}

          {!enabled && !isLoading && (
            <>
              {pragmasMissing && (
                <CalloutBanner testId="admin-session-scoring-pragma-warning">
                  <span className="font-semibold">Prerequisite required.</span> Preflight request
                  body preservation is not yet enabled on this service. Click{' '}
                  <strong>Enable</strong> for instructions to share with your Fastly account team.
                </CalloutBanner>
              )}
              <CalloutBanner>
                <span className="font-semibold">Experimental.</span> Not yet validated against
                production traffic at scale — run on a non-critical service first.
              </CalloutBanner>
              <p className="text-xs text-muted-foreground">
                Session scoring is off. Click <strong>Enable</strong> to provision the scorer
                Compute service and start scoring every request at the edge.
              </p>
            </>
          )}
        </div>
      </AnalyticsCard>
    </div>
  )
}

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

interface HourlyRow {
  hour: string
  count: number
}

interface StackedHourlyBarChartProps<R extends HourlyRow> {
  title: string
  description: string
  isLoading?: boolean
  isFetching?: boolean
  isEmpty?: boolean
  error?: AnalyticsCardError | null
  rows: R[]
  categoryKey: keyof R & string
  colors: Record<string, string>
  categoryOrder?: readonly string[]
  helpContent?: React.ReactNode
  helpTitle?: string
}

export function StackedHourlyBarChart<R extends HourlyRow>({
  title,
  description,
  isLoading,
  isFetching,
  isEmpty,
  error,
  rows,
  categoryKey,
  colors,
  categoryOrder,
  helpContent,
  helpTitle,
}: StackedHourlyBarChartProps<R>) {
  const traces = React.useMemo((): PlotlyTrace[] => {
    const presentHours = Array.from(new Set(rows.map((r) => r.hour))).sort()
    const xGrid = denseTimeGrid(presentHours, 3600) ?? presentHours
    const cats = categoryOrder ?? Array.from(new Set(rows.map((r) => String(r[categoryKey]))))
    return cats.map((cat) =>
      denseBarTrace({
        name: cat,
        rows: rows.filter((r) => String(r[categoryKey]) === cat),
        getTime: (r) => r.hour,
        getValue: (r) => r.count,
        intervalSeconds: 3600,
        color: colors[cat] ?? '#64748b',
        grid: xGrid,
      }),
    )
  }, [rows, categoryKey, colors, categoryOrder])

  return (
    <AnalyticsCard
      title={title}
      description={description}
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={isEmpty}
      error={error}
      className="min-h-[320px]"
      helpContent={helpContent}
      helpTitle={helpTitle}
    >
      <PlotlyChart
        data={traces}
        height="100%"
        layout={{
          barmode: 'stack',
          showlegend: true,
          margin: { l: 50, r: 20, t: 10, b: 40 },
          xaxis: { title: '', type: 'date' },
          yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' },
        }}
      />
    </AnalyticsCard>
  )
}

type CompRow = components['schemas']['ScoringComplianceRow']
const COMPLIANCE_COLORS: Record<string, string> = {
  ok: CHART_COLORS.success,
  missing: CHART_COLORS.neutral,
  tampered: '#e11d48',
  expired: CHART_COLORS.warning,
  unknown: '#7c3aed',
}

export function ComplianceChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(
    ['scoring-compliance', serviceId, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/compliance-breakdown', {
        params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      }),
  )
  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`}
      helpContent={<ComplianceHelp />}
      helpTitle="About Cookie Compliance"
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      isLoading={isLoading}
      isFetching={isFetching}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="compliance"
      colors={COMPLIANCE_COLORS}
    />
  )
}

type DistRow = components['schemas']['ScoringDistributionRow']
const BUCKETS = ['0-25', '25-50', '50-75', '75-100'] as const
const BUCKET_COLORS: Record<(typeof BUCKETS)[number], string> = {
  '0-25': CHART_COLORS.neutral,
  '25-50': '#facc15',
  '50-75': '#fb923c',
  '75-100': '#e11d48',
}

export function ScoreDistChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(
    ['scoring-score-dist', serviceId, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/score-distribution', {
        params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
      }),
  )
  return (
    <StackedHourlyBarChart<DistRow>
      title={`Score distribution — last ${sinceHours}h`}
      helpContent={<ScoreDistHelp />}
      helpTitle="About Score Distribution"
      description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={(data?.rows?.length ?? 0) === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []}
      categoryKey="bucket"
      colors={BUCKET_COLORS}
      categoryOrder={BUCKETS}
    />
  )
}

type Derived = ReturnType<typeof deriveScorerSeries>

interface ScorerSeriesCardProps {
  serviceId: string
  sinceHours: number
  title: string
  description: (perMinute: boolean) => string
  help: ReactNode
  helpTitle: string
  a11yTitle: string
  toggle: { id: string; label: string }
  build: (
    series: Derived & { data: LatencyResponse | undefined },
    toggleOn: boolean,
  ) => { traces: PlotlyTrace[]; layout: PlotlyLayout }
  showToggle?: (series: Derived & { data: LatencyResponse | undefined }) => boolean
  placeholder?: (series: Derived & { data: LatencyResponse | undefined }) => ReactNode | null
}

export function ScorerSeriesCard({
  serviceId,
  sinceHours,
  title,
  description,
  help,
  helpTitle,
  a11yTitle,
  toggle,
  build,
  showToggle,
  placeholder,
}: ScorerSeriesCardProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)
  const [toggleOn, setToggleOn] = useState(true)

  const series = { ...deriveScorerSeries(data), data }
  const { rows, perMinute } = series
  const { traces, layout } = build(series, toggleOn)
  const offerToggle = rows.length > 0 && (showToggle ? showToggle(series) : true)
  const replacement = placeholder?.(series) ?? null

  return (
    <AnalyticsCard
      title={title}
      description={description(perMinute)}
      helpContent={help}
      helpTitle={helpTitle}
      isLoading={isLoading}
      isFetching={isFetching}
      isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null}
      className="min-h-[320px]"
      headerAction={
        offerToggle ? (
          <div className="flex items-center gap-2 mr-1">
            <Switch id={toggle.id} checked={toggleOn} onCheckedChange={setToggleOn} />
            <Label
              htmlFor={toggle.id}
              className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground"
            >
              {toggle.label}
            </Label>
          </div>
        ) : undefined
      }
    >
      {replacement ?? <PlotlyChart data={traces} layout={layout} height="100%" a11yTitle={a11yTitle} />}
    </AnalyticsCard>
  )
}

const roundToTwo = (num: number): number => Math.round((num + Number.EPSILON) * 100) / 100

export function ScorerErrorsChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer errors — last ${sinceHours}h`}
      description={(perMinute) =>
        `Fail-open errors — requests that timed out or failed auth and were let through unscored.${perMinute ? ' Bucketed per minute.' : ''}`
      }
      help={<ScorerErrorsHelp />}
      helpTitle="About Scorer Errors"
      a11yTitle={`Scorer fail-open errors over the last ${sinceHours} hours`}
      toggle={{ id: 'show-rate', label: 'Show rate (%)' }}
      build={({ rows, hours, perMinute }, showRate) => {
        const errorTrace = denseBarTrace({
          name: 'Fail-opens',
          rows,
          getTime: (r) => r.hour,
          getValue: (r) => r.fail_open_count,
          intervalSeconds: perMinute ? 60 : 3600,
          color: 'rgba(225,29,72,0.45)',
        })
        const rateTrace = lineTrace({
          name: 'Error rate (%)',
          x: hours,
          y: rows.map((r) => {
            const total = r.total_count ?? 0
            return total > 0 ? roundToTwo(((r.fail_open_count ?? 0) / total) * 100) : 0
          }),
          color: '#e11d48',
          yaxis: 'y2',
        })
        const base = scorerHourlyLayout(perMinute, 'Fail-opens', showRate)
        return {
          traces: [errorTrace, ...(showRate ? [rateTrace] : [])],
          layout: showRate ? withSecondaryAxis(base, 'Error rate (%)', { ticksuffix: '%' }) : base,
        }
      }}
    />
  )
}

const LATENCY_SERIES: {
  name: string
  get: (r: LatencyRow) => number | null | undefined
  color: string
  dash?: 'dot' | 'dash'
}[] = [
  { name: 'p50 rtt', get: (r) => r.rtt_p50_us, color: '#60a5fa' },
  { name: 'p95 rtt', get: (r) => r.rtt_p95_us, color: CHART_COLORS.info },
  { name: 'p99 rtt', get: (r) => r.rtt_p99_us, color: '#1d4ed8' },
  { name: 'p50 exec', get: (r) => r.exec_p50_us, color: '#34d399', dash: 'dot' },
  { name: 'p95 exec', get: (r) => r.exec_p95_us, color: CHART_COLORS.success, dash: 'dot' },
]

export function ScorerLatencyChart({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const hasLatency = ({ data, rows }: { data?: { has_latency?: boolean }; rows: LatencyRow[] }) =>
    (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  return (
    <ScorerSeriesCard
      serviceId={serviceId}
      sinceHours={sinceHours}
      title={`Scorer latency — last ${sinceHours}h`}
      description={(perMinute) =>
        `Edge round-trip (p50/p95/p99) vs Wasm exec (dashed) for the Compute scorer leg.${perMinute ? ' Bucketed per minute.' : ''}`
      }
      help={<ScorerLatencyHelp />}
      helpTitle="About Scorer Latency"
      a11yTitle={`Scorer latency percentiles over the last ${sinceHours} hours`}
      toggle={{ id: 'show-volume', label: 'Show volume' }}
      showToggle={hasLatency}
      placeholder={(series) =>
        series.rows.length > 0 && !hasLatency(series) ? (
          <div className="flex h-full items-center justify-center p-4 text-center">
            <p className="max-w-xs text-xs text-muted-foreground">
              Waiting for traffic logged with the scorer latency fields (
              <code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>). These are added to
              the log format on enable and fill in once scored requests flow through and are
              ingested.
            </p>
          </div>
        ) : null
      }
      build={({ rows, hours, perMinute }, showVolume) => {
        const volumeTrace = denseBarTrace({
          name: 'Inspected requests',
          rows,
          getTime: (r) => r.hour,
          getValue: (r) => r.total_count,
          intervalSeconds: perMinute ? 60 : 3600,
          color: 'rgba(148, 163, 184, 0.12)',
          yaxis: 'y2',
          showlegend: true,
        })
        const latencyTraces = LATENCY_SERIES.map((s) =>
          lineTrace({
            name: s.name,
            x: hours,
            y: rows.map((r) => usToMs(s.get(r))),
            color: s.color,
            dash: s.dash,
            connectgaps: true,
          }),
        )
        const base = scorerHourlyLayout(perMinute, 'Round-trip / exec (ms)', true)
        return {
          traces: [...(showVolume ? [volumeTrace] : []), ...latencyTraces],
          layout: showVolume ? withSecondaryAxis(base, 'Inspected requests') : base,
        }
      }}
    />
  )
}
