'use client'

import * as React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ExternalLink, Info, Power, PowerOff, RefreshCw, ShieldCheck, ShieldOff } from 'lucide-react'
import { invalidateServiceScoringQueries, useScoringQuery } from './useScorerTimeseries'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { StatusPanelHelp } from '@/components/SessionScoring/help-content'
import { SSEModal } from '@/components/SSEModal'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { formatLocalDateTime } from '@/lib/date'

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
