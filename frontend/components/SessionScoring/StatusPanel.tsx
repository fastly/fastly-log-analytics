'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  ExternalLink,
  Info,
  Power,
  PowerOff,
  RefreshCw,
  ShieldCheck,
  ShieldOff,
} from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { SSEModal } from '@/components/SSEModal'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { client } from '@/lib/api'
import { formatLocalDateTime } from '@/lib/utils'
import type { components } from '@/types/api'
import { StatusPanelHelp } from './help-content'
import {
  invalidateServiceScoringQueries,
  useScoringQuery,
} from '@/components/SessionScoring/ScoringAdminPanels'
import {
  CalloutBanner,
  DISABLE_STEPS,
  ENABLE_STEPS,
  PragmaMissingDialog,
  REDEPLOY_STEPS,
  StepListDescription,
} from './ScoringVclTab'

export { StorageCalendar, StorageStatus, MetadataStorageCard } from './StoragePanels'
export {
  L2EnforcementCard,
  type L2EnforcementCardProps,
  RocPrCurves,
  type RocPrCurvesProps,
  PerReasonAucCard,
  ScoringEvaluationPanel,
} from './ScoringEvaluationPanel'
export {
  StackedHourlyBarChart,
  type StackedHourlyBarChartProps,
  type HourlyRow,
  ComplianceChart,
  ScoreDistChart,
  ScorerSeriesCard,
  type ScorerSeriesCardProps,
  ScorerErrorsChart,
  ScorerLatencyChart,
  ScoringOverviewTab,
} from './ScoringOverviewTab'
export {
  REDEPLOY_STEPS,
  DISABLE_STEPS,
  ENABLE_STEPS,
  StepListDescription,
  CalloutBanner,
  PragmaMissingDialog,
  ScoringVclTab,
} from './ScoringVclTab'
export {
  ScoringRulesDialog,
  type ScoringRulesDialogProps,
} from './ScoringRulesDialog'

export interface StatusPanelProps {
  serviceId: string
}

type ScoringEvaluation = components['schemas']['ScoringEvaluationResponse']

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
              <PragmaMissingDialog
                serviceId={serviceId}
                open={pragmaModalOpen}
                onOpenChange={setPragmaModalOpen}
                isFetching={isFetching}
                onRecheck={() => refetch()}
              />
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
                  steps={ENABLE_STEPS}
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
