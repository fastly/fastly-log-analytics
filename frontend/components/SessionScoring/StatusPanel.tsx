'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ExternalLink, Info, LineChart, ListChecks, Power, PowerOff, RefreshCw, ShieldAlert, ShieldCheck, ShieldOff, TrendingUp } from 'lucide-react'
import {
  invalidateServiceScoringQueries,
  PerReasonAucHelp,
  RocPrCurvesHelp,
  scoringAnalyticsCompositeKey,
  scoringConfigCompositeKey,
  StatusPanelHelp,
  useScoringQuery,
} from '@/components/SessionScoring/help-content'

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
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { SSEModal } from '@/components/SSEModal'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { CHART_COLORS } from '@/lib/chart-helpers'
import { formatLocalDateTime, formatPctValue } from '@/lib/format'
import { showToast } from '@/lib/utils'

interface StatusPanelProps {
  serviceId: string
}

// Generated from the /scoring/evaluation response_model — single source of truth.
type ScoringEvaluation = components['schemas']['ScoringEvaluationResponse']

export function StatusPanel({ serviceId }: StatusPanelProps) {
  const queryClient = useQueryClient()
  // No refetchInterval: polling 4 endpoints every 30-60s caused
  // constant .duckdb-wal churn that ate ~1.5GB of mds_stores + VS Code
  // extension-host RAM. Refresh happens after enable/disable via refetch().
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

  if (data?.preflight_pragmas_ready === true && pragmaModalOpen) {
    setPragmaModalOpen(false)
  }

  const accountTeamMessage = `Hi Fastly team — please enable the preflight request body preservation service pragmas on Fastly service ${serviceId} (preserving unsent request bodies across VCL restarts and omitting request bodies when backend requests are rewritten to GET or HEAD) so we can enable our Session Scoring Compute preflight pass.`

  // After an enable / disable / redeploy SSE stream closes, the scoring config
  // and analytics have all changed (or the stores were just created), so the
  // L2 card, latency chart, etc. would otherwise show stale pre-op data until a
  // manual refresh. refetch() only covers scoring-status, so also invalidate
  // every scoring-* query for THIS service. Predicate-match so the
  // sinceHours-suffixed analytics keys (['scoring-health', id, hours], …) match.
  const refreshScoring = React.useCallback(() => {
    refetch()
    invalidateServiceScoringQueries(queryClient, serviceId)
  }, [queryClient, refetch, serviceId])

  // Matrix-quality (ROC-AUC) — only fire when scoring is actually on.
  // Cache invalidation handled server-side via _bust_analytics_cache on
  // label POST/PATCH/DELETE; client-side staleTime keeps the StatusPanel
  // from re-fetching on every focus change.
  const { data: evalData } = useScoringQuery(
    ['scoring-evaluation', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/evaluation', {
        params: { path: { service_id: serviceId } },
      }),
    { enabled, staleTime: 30_000 },
  )

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
                description={
                  <div className="space-y-2">
                    <p>
                      Push the latest scorer build to the edge for{' '}
                      <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:
                    </p>
                    <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
                      <li>Checks the live edge first: re-uploads the Wasm only if the build changed</li>
                      <li>Re-activates the logging VCL only if the scoring snippets/backend changed</li>
                      <li>Skips no-op version bumps when nothing changed — no scoring gap</li>
                    </ul>
                    <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
                  </div>
                }
                endpoint={`/api/services/${serviceId}/scoring/enable`}
                body={{}}
                onClose={refreshScoring}
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
                description={
                  <div className="space-y-2">
                    <p>This will disable session scoring on <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:</p>
                    <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
                      <li>Removes the 6 VCL snippets (recv / pass / fetch / deliver / miss / enforce) that route requests through the scorer</li>
                      <li>Removes the 6 scoring custom_fields from the log format</li>
                      <li>Clones + activates a new VCL version on the customer service</li>
                      <li>Deletes the scorer Compute service + its ConfigStores and matrix KV Store (a future Enable re-provisions them from scratch)</li>
                    </ul>
                    <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
                  </div>
                }
                endpoint={`/api/services/${serviceId}/scoring/disable`}
                body={{}}
                onClose={refreshScoring}
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
                      <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>, your Fastly account team must
                      enable preflight request body preservation on this service.
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-3 py-2 text-sm">
                    <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Why is this required?</div>
                        <div>
                          Session Scoring routes each request through a lightweight Compute preflight sub-request
                          (rewritten to <code>GET</code>) and then restarts VCL to forward the original request to your
                          origin. Without service-level preflight body preservation enabled,{' '}
                          <code>POST</code>/<code>PUT</code>/<code>PATCH</code> request bodies can be drained or stall
                          when forwarding to origin after the restart.
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
                        <RefreshCw className={`h-3.5 w-3.5 mr-1 ${isFetching ? 'animate-spin' : ''}`} />
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
              description={
                <div className="space-y-2">
                  <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                    <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                    <div className="text-xs space-y-1">
                      <div className="font-semibold">Experimental feature</div>
                      <div>
                        Session scoring has not been validated against production traffic at scale.
                        Run it on a non-critical service first, watch the scorer Compute service&apos;s
                        error rate, and keep the disable path handy.
                      </div>
                    </div>
                  </div>
                  <p>This will enable session scoring on <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:</p>
                  <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
                    <li>Provisions the scorer Compute service if it doesn&apos;t exist, deploys the trained Wasm scorer with the latest matrix</li>
                    <li>Adds the <code>session_scorer</code> backend, 6 VCL snippets, and 9 score fields (<code>edge_score</code>, session id, reason, L1/L2, latency, …) to the customer service</li>
                    <li>Clones + activates a new VCL version</li>
                    <li>Every request will be routed through the scorer for an L1+L2 score on the response cookie</li>
                  </ul>
                  <div className="flex items-start gap-2 rounded border border-sky-300 bg-sky-50 px-3 py-2 text-sky-900">
                    <Info className="h-4 w-4 mt-0.5 shrink-0" />
                    <div className="text-xs space-y-1">
                      <div className="font-semibold">Log fields</div>
                      <div>
                        Scoring also needs the standard request fields for context — <strong>URL, method, user agent, and geo</strong>.
                        Enabling scoring ensures those stay logged (added automatically if this service was set up with a minimal
                        log-field set), so scored sessions show <em>which</em> requests were flagged, not just the score.
                      </div>
                    </div>
                  </div>
                  <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
                </div>
              }
              endpoint={`/api/services/${serviceId}/scoring/enable`}
              body={{}}
              onClose={refreshScoring}
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
              <Badge variant="default" className="bg-emerald-600" data-testid="admin-session-scoring-status-badge">
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
                <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-2.5 py-1.5 text-amber-900">
                  <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                  <div className="text-xs">
                    <span className="font-semibold">Redeploy needed.</span> The edge is running an older
                    scorer build{data.drift_detail ? ` (${data.drift_detail})` : ''} than the one shipped to
                    the backend. Click <strong>Redeploy</strong> above to push the latest.
                  </div>
                </div>
              )}
              {!data?.scorer_drift && data?.drift_known === false && (
                <div className="flex items-start gap-2 rounded border bg-muted/40 px-2.5 py-1.5 text-muted-foreground">
                  <Info className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                  <div className="text-xs">
                    <span className="font-semibold text-foreground">Drift detection not baselined.</span>{' '}
                    This service was enabled before edge-drift tracking shipped, so we can&apos;t tell
                    whether the edge is up to date. Click <strong>Redeploy</strong> once to baseline it —
                    after that a banner appears here whenever the edge falls behind.
                  </div>
                </div>
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
                          className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
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
                            className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
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
                        {data.scoring_domain}
                        <ExternalLink className="h-3 w-3" />
                      </a>
                    }
                  />
                )}
                {data?.matrix_version && (
                  <Field label="Matrix Version" value={<code className="text-xs">{data.matrix_version}</code>} />
                )}
                {data?.enabled_at && (
                  <Field label="Enabled At" value={<span className="text-xs text-muted-foreground">{data.enabled_at}</span>} />
                )}
                {evalData && <AucField evalData={evalData} />}
              </div>
            </>
          )}

          {!enabled && !isLoading && (
            <>
              {pragmasMissing && (
                <div
                  className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-2.5 py-1.5 text-amber-900"
                  data-testid="admin-session-scoring-pragma-warning"
                >
                  <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                  <div className="text-xs">
                    <span className="font-semibold">Prerequisite required.</span> Preflight request body
                    preservation is not yet enabled on this service. Click <strong>Enable</strong> for instructions
                    to share with your Fastly account team.
                  </div>
                </div>
              )}
              <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-2.5 py-1.5 text-amber-900">
                <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                <div className="text-xs">
                  <span className="font-semibold">Experimental.</span> Not yet validated against
                  production traffic at scale — run on a non-critical service first.
                </div>
              </div>
              <p className="text-xs text-muted-foreground">
                Session scoring is off. Click <strong>Enable</strong> to provision the scorer Compute service and start
                scoring every request at the edge.
              </p>
            </>
          )}
        </div>
      </AnalyticsCard>
    </div>
  )
}

// Render a Fastly version timestamp (ISO-ish) as a readable local datetime;
// fall back to the raw string if it doesn't parse. The raw value is kept in
// the title attribute by the caller.
function formatActivated(raw: string): string {
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
  // Sub-min-samples: render a self-explaining CTA instead of a misleading
  // 0.5 AUC. The numbers in parens make the gap concrete (e.g. "have
  // 2/0" tells the operator they need 1 more good + 3 bad).
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
  // has_min_samples + no error → AUC is real.
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

interface L2EnforcementCardProps {
  serviceId: string
  sinceHours?: number
}

const DESCRIPTION =
  'Decide when edge Layer-2 (route-transition anomaly) joins the enforced score. L2 is always logged; this only gates blocking.'

const fmtDays = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(1))

export function L2EnforcementCard({ serviceId, sinceHours = 24 }: L2EnforcementCardProps) {
  const queryClient = useQueryClient()
  // null = dialog closed; true/false = pending target enabled-state under confirm.
  const [pendingEnable, setPendingEnable] = React.useState<boolean | null>(null)

  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-l2-enforce', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/l2-enforce', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  // "What L2 would flag" (l2_high_pct) rides the health query the page already
  // fetches + seeds — read the same key so we don't fire a second request.
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
    // A 400 here means scoring isn't enabled yet (no config store) — that's not a
    // failure, just a precondition. Map it to friendly copy; surface anything
    // else as a real error.
    const raw = String(error?.message || 'Unknown error')
    // A 400 here means scoring isn't enabled yet / the config store isn't
    // resolvable. The openapi client may surface either "status 400" or the raw
    // backend detail ("Scoring not enabled or config store missing"), so match
    // both rather than leaking the raw string. A freshly-enabled service that
    // still shows this is just a pre-enable fetch the StatusPanel re-invalidates.
    const friendly =
      /status 400|http 400|config store missing|scoring not enabled/i.test(raw) ||
      (error as { status?: number } | null)?.status === 400
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
              {l2HighPct != null && <> About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.</>}
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
              {l2HighPct != null && <> It would currently flag ~{formatPctValue(l2HighPct, 1)} of sessions.</>}
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
                {l2HighPct != null && <> About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.</>}{' '}
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

type CurvePoint = NonNullable<components['schemas']['ScoringCurvePoint']>

interface RocPrCurvesProps {
  serviceId: string
  sinceHours?: number
}

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
