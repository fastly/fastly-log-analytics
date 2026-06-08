'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, ExternalLink, Power, PowerOff, ShieldCheck, ShieldOff } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { StatusPanelHelp } from '@/components/SessionScoring/help-content'
import { SSEModal } from '@/components/SSEModal'
import { client } from '@/lib/api'

interface StatusPanelProps {
  serviceId: string
}

interface ScoringStatus {
  enabled: boolean
  scoring_service_id?: string
  scoring_service_name?: string
  scoring_domain?: string
  scoring_backend_name?: string
  matrix_version?: string
  enabled_at?: string
}

interface ScoringEvaluation {
  has_min_samples: boolean
  min_per_class: number
  n_good: number
  n_bad: number
  n_neutral: number
  n_reconstructed?: number
  n_labels_total?: number
  auc?: number
  passed?: boolean
  threshold?: number
  default_min_auc?: number
  matrix_version?: string
  error?: string
}

export function StatusPanel({ serviceId }: StatusPanelProps) {
  const { data, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['scoring-status', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/status' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ScoringStatus
    },
    // No refetchInterval: polling 4 endpoints every 30-60s caused
    // constant .duckdb-wal churn that ate ~1.5GB of mds_stores + VS Code
    // extension-host RAM. Refresh happens after enable/disable via refetch().
  })

  const enabled = !!data?.enabled

  // Matrix-quality (ROC-AUC) — only fire when scoring is actually on.
  // Cache invalidation handled server-side via _bust_analytics_cache on
  // label POST/PATCH/DELETE; client-side staleTime keeps the StatusPanel
  // from re-fetching on every focus change.
  const { data: evalData } = useQuery({
    queryKey: ['scoring-evaluation', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/evaluation' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ScoringEvaluation
    },
    enabled,
    staleTime: 30_000,
  })

  return (
    <AnalyticsCard
      title="Session Scoring"
      icon={enabled ? <ShieldCheck className="h-4 w-4" /> : <ShieldOff className="h-4 w-4" />}
      description="Edge scorer that classifies sessions in real-time using the cookie-bound state machine and matrix-based L2 evaluation."
      isLoading={isLoading}
      isFetching={isFetching}
      helpContent={<StatusPanelHelp />}
      helpTitle="About Session Scoring"
      headerAction={
        enabled ? (
          <SSEModal
            title="Disable Session Scoring"
            description={
              <div className="space-y-2">
                <p>This will disable session scoring on <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:</p>
                <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
                  <li>Removes the 3 VCL snippets (recv / fetch / deliver) that route requests through the scorer</li>
                  <li>Removes the 6 scoring custom_fields from the log format</li>
                  <li>Clones + activates a new VCL version on the customer service</li>
                  <li>Leaves the scorer Compute service in place — re-enable later without rebuilding</li>
                </ul>
                <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
              </div>
            }
            endpoint={`/api/services/${serviceId}/scoring/disable`}
            body={{}}
            onClose={() => refetch()}
            trigger={
              <Button variant="outline" size="sm" className="text-rose-700 border-rose-300 hover:bg-rose-50">
                <PowerOff className="h-3.5 w-3.5 mr-1" /> Disable
              </Button>
            }
          />
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
                  <li>Adds the <code>session_scorer</code> backend, 3 VCL snippets, and 6 custom_fields to the customer service</li>
                  <li>Clones + activates a new VCL version</li>
                  <li>Every request will be routed through the scorer for an L1+L2 score on the response cookie</li>
                </ul>
                <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
              </div>
            }
            endpoint={`/api/services/${serviceId}/scoring/enable`}
            body={{}}
            onClose={() => refetch()}
            trigger={
              <Button variant="default" size="sm">
                <Power className="h-3.5 w-3.5 mr-1" /> Enable
              </Button>
            }
          />
        )
      }
    >
      <div className="space-y-3 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground">Status:</span>
          {enabled ? (
            <Badge variant="default" className="bg-emerald-600">Enabled</Badge>
          ) : (
            <Badge variant="secondary">Disabled</Badge>
          )}
        </div>

        {enabled && (
          <>
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
            {data?.scoring_backend_name && (
              <Field label="VCL Backend" value={<code className="text-xs">{data.scoring_backend_name}</code>} />
            )}
            {data?.matrix_version && (
              <Field label="Matrix Version" value={<code className="text-xs">{data.matrix_version}</code>} />
            )}
            {data?.enabled_at && (
              <Field label="Enabled At" value={<span className="text-xs text-muted-foreground">{data.enabled_at}</span>} />
            )}
            {evalData && <AucField evalData={evalData} />}
          </>
        )}

        {!enabled && !isLoading && (
          <>
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
  )
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
