'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, ShieldCheck } from 'lucide-react'

import { client } from '@/lib/api'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ComplianceChart } from '@/components/SessionScoring/ComplianceChart'
import { ExcludeRegexCard } from '@/components/SessionScoring/ExcludeRegexCard'
import { MatrixVersionsCard } from '@/components/SessionScoring/MatrixVersionsCard'
import { PerReasonAucCard } from '@/components/SessionScoring/PerReasonAucCard'
import { RetrainButton } from '@/components/SessionScoring/RetrainButton'
import { RocPrCurves } from '@/components/SessionScoring/RocPrCurves'
import { RotateKeyButton } from '@/components/SessionScoring/RotateKeyButton'
import { ScoreDistChart } from '@/components/SessionScoring/ScoreDistChart'
import { ScorerLatencyChart } from '@/components/SessionScoring/ScorerLatencyChart'
import { ScorerErrorsChart } from '@/components/SessionScoring/ScorerErrorsChart'
import { ScorerFailOpenBreakdownCard } from '@/components/SessionScoring/ScorerFailOpenBreakdownCard'
import { ScoringHealthCard } from '@/components/SessionScoring/ScoringHealthCard'
import { SinceHoursPicker } from '@/components/SessionScoring/SinceHoursPicker'
import { StatusPanel } from '@/components/SessionScoring/StatusPanel'
import { ThresholdSlider } from '@/components/SessionScoring/ThresholdSlider'
import { L2EnforcementCard } from '@/components/SessionScoring/L2EnforcementCard'
import { TopFlaggedTable } from '@/components/SessionScoring/TopFlaggedTable'

// LabelsTab is gated behind the "Labels" tab — most page loads never open
// it. Lazy-load to keep it out of the overview bundle (~200 LOC plus its
// Popover/Dialog/Table dependencies). `ssr: false` because the tab uses
// client-only React Query hooks and there's no SEO value to pre-rendering.
const LabelsTab = dynamic(
  () => import('@/components/SessionScoring/LabelsTab').then((m) => ({ default: m.LabelsTab })),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-3" aria-busy="true">
        <Skeleton className="h-5 w-48" />
        <Skeleton className="h-32 w-full" />
      </div>
    ),
  },
)
const AuditLogTab = dynamic(
  () => import('@/components/SessionScoring/AuditLogTab').then((m) => ({ default: m.AuditLogTab })),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-3" aria-busy="true">
        <Skeleton className="h-5 w-48" />
        <Skeleton className="h-32 w-full" />
      </div>
    ),
  },
)
import { useServiceStore } from '@/stores/serviceStore'

export default function SessionScoringPage() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const [tab, setTab] = React.useState('overview')
  // Shared time window across all overview cards. Defaults to 24h —
  // matches the prior per-component hard-coded value. Picker lives in
  // the PageHeader action slot so it's always visible.
  const [sinceHours, setSinceHours] = React.useState(24)
  const qc = useQueryClient()

  // ── Composite queries: collapse 10+ individual requests into 2 ──
  // Analytics composite: health, top-flagged, score-dist, compliance,
  // evaluation, evaluation-per-reason. Config composite: status,
  // threshold, exclude-regex, enforce-status-code.
  // Individual component queries stay intact — they find pre-populated
  // cache entries and skip their network requests.
  const analyticsComposite = useQuery({
    queryKey: ['scoring-analytics-composite', activeServiceId, sinceHours],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/analytics',
        {
          params: {
            path: { service_id: activeServiceId ?? '' },
            query: { since_hours: sinceHours },
          },
          signal,
        },
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as unknown as Record<string, unknown>
    },
    enabled: !!activeServiceId,
  })

  const configComposite = useQuery({
    queryKey: ['scoring-config-composite', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/config',
        {
          params: { path: { service_id: activeServiceId ?? '' } },
          signal,
        },
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as unknown as Record<string, unknown>
    },
    enabled: !!activeServiceId,
  })

  // Seed individual component cache keys from composite responses.
  // Ref-guarded by dataUpdatedAt so seeding runs once per fresh fetch.
  const analyticsSeededAt = React.useRef(0)
  const configSeededAt = React.useRef(0)

  // Seeding mutates refs + the query cache — both are side effects, so they
  // run in effects (not during render). Each effect is keyed on its
  // composite's dataUpdatedAt so it re-seeds once per fresh fetch; the ref
  // guard preserves the refreshAll() reset path (which zeroes the refs to
  // force a re-seed after invalidation).
  React.useEffect(() => {
    if (analyticsComposite.data && analyticsComposite.dataUpdatedAt > analyticsSeededAt.current) {
      analyticsSeededAt.current = analyticsComposite.dataUpdatedAt
      const d = analyticsComposite.data
      if (d.health) qc.setQueryData(['scoring-health', activeServiceId, sinceHours], d.health)
      if (d.top_flagged) qc.setQueryData(['scoring-top-flagged', activeServiceId, sinceHours], d.top_flagged)
      if (d.score_distribution) qc.setQueryData(['scoring-score-dist', activeServiceId, sinceHours], d.score_distribution)
      if (d.latency_timeseries) qc.setQueryData(['scoring-latency-timeseries', activeServiceId, sinceHours], d.latency_timeseries)
      if (d.compliance_breakdown) qc.setQueryData(['scoring-compliance', activeServiceId, sinceHours], d.compliance_breakdown)
      if (d.evaluation_per_reason) qc.setQueryData(['scoring-evaluation-per-reason', activeServiceId], d.evaluation_per_reason)
      if (d.evaluation) qc.setQueryData(['scoring-evaluation', activeServiceId], d.evaluation)
    }
  }, [analyticsComposite.data, analyticsComposite.dataUpdatedAt, activeServiceId, sinceHours, qc])

  React.useEffect(() => {
    if (configComposite.data && configComposite.dataUpdatedAt > configSeededAt.current) {
      configSeededAt.current = configComposite.dataUpdatedAt
      const d = configComposite.data
      if (d.status) qc.setQueryData(['scoring-status', activeServiceId], d.status)
      if (d.threshold) qc.setQueryData(['scoring-threshold-committed', activeServiceId], d.threshold)
      if (d.exclude_regex) qc.setQueryData(['scoring-exclude-regex', activeServiceId], d.exclude_regex)
      if (d.enforce_status_code) qc.setQueryData(['scoring-enforce-status-code', activeServiceId], d.enforce_status_code)
    }
  }, [configComposite.data, configComposite.dataUpdatedAt, activeServiceId, qc])

  // Gate each region on ONLY the composite that feeds it, not on both.
  // The StatusPanel (above the fold, the LCP element) is driven by the
  // fast config composite; the overview cards by the slow 7-query
  // analytics composite. Coupling them made the fast panel wait on the
  // slow query — a self-inflicted waterfall that pushed LCP to ~6s.
  const statusLoading = configComposite.isLoading
  const overviewLoading = analyticsComposite.isLoading

  // Refresh invalidates composite keys (re-seeding individual caches on
  // resolve) plus any queries not covered by composites.
  const refreshAll = () => {
    analyticsSeededAt.current = 0
    configSeededAt.current = 0
    qc.invalidateQueries({ queryKey: ['scoring-analytics-composite', activeServiceId] })
    qc.invalidateQueries({ queryKey: ['scoring-config-composite', activeServiceId] })
    qc.invalidateQueries({
      predicate: (q) =>
        Array.isArray(q.queryKey) &&
        typeof q.queryKey[0] === 'string' &&
        ['scoring-curves', 'scoring-enforce-threshold', 'scoring-threshold-preview',
         'scoring-labels', 'scoring-labels-counts'].includes(q.queryKey[0] as string) &&
        q.queryKey[1] === activeServiceId,
    })
  }

  if (!activeServiceId) {
    return (
      <div className="space-y-6">
        <PageHeader
          title="Session Scoring"
          description="Enable scoring, view distributions, and label sessions for matrix evaluation."
          icon={ShieldCheck}
        >
          <BackToAdminLink />
        </PageHeader>
        <Alert>
          <AlertDescription>
            No active service selected. Pick a service from the sidebar to manage session scoring.
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Session Scoring"
        description="Real-time edge scoring of every request via the scorer Compute service. Toggle scoring on/off, watch the score distribution, and label sessions to evaluate matrix quality (ROC-AUC)."
        icon={ShieldCheck}
      >
        <SinceHoursPicker value={sinceHours} onChange={setSinceHours} />
        <RetrainButton serviceId={activeServiceId} />
        <RotateKeyButton serviceId={activeServiceId} />
        <Button variant="outline" size="sm" onClick={refreshAll}>
          <RefreshCw className="h-4 w-4 mr-1" />
          Refresh
        </Button>
        <BackToAdminLink />
      </PageHeader>

      {statusLoading ? (
        <div className="space-y-3" aria-busy="true">
          <Skeleton className="h-48 w-full" />
        </div>
      ) : (
        <StatusPanel serviceId={activeServiceId} />
      )}

      <Tabs value={tab} onValueChange={setTab} className="w-full">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="labels">Labels</TabsTrigger>
          <TabsTrigger value="matrix">Matrix history</TabsTrigger>
          <TabsTrigger value="audit">Audit</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="pt-4 space-y-6">
          {overviewLoading ? (
            // Skeleton mirrors the real overview layout below (health card,
            // 2-up chart grid, breakdown, threshold, curves, table, 2-up
            // grid) so the loading→loaded swap shifts minimally — a faithful
            // placeholder is the cheapest CLS win on a data-heavy page.
            <div className="space-y-6" aria-busy="true">
              <Skeleton className="h-40 w-full" />
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <Skeleton className="h-80 w-full" />
                <Skeleton className="h-80 w-full" />
              </div>
              <Skeleton className="h-48 w-full" />
              <Skeleton className="h-64 w-full" />
              <Skeleton className="h-72 w-full" />
              <Skeleton className="h-64 w-full" />
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <Skeleton className="h-80 w-full" />
                <Skeleton className="h-80 w-full" />
              </div>
            </div>
          ) : (
            <>
              <ScoringHealthCard serviceId={activeServiceId} sinceHours={sinceHours} />
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <ScorerLatencyChart serviceId={activeServiceId} sinceHours={sinceHours} />
                <ScorerErrorsChart serviceId={activeServiceId} sinceHours={sinceHours} />
              </div>
              <ScorerFailOpenBreakdownCard serviceId={activeServiceId} sinceHours={sinceHours} />
              <ThresholdSlider serviceId={activeServiceId} sinceHours={sinceHours} />
              <L2EnforcementCard serviceId={activeServiceId} sinceHours={sinceHours} />
              <ExcludeRegexCard serviceId={activeServiceId} />
              <RocPrCurves serviceId={activeServiceId} sinceHours={sinceHours} />
              <PerReasonAucCard serviceId={activeServiceId} />
              <TopFlaggedTable serviceId={activeServiceId} sinceHours={sinceHours} />
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <ScoreDistChart serviceId={activeServiceId} sinceHours={sinceHours} />
                <ComplianceChart serviceId={activeServiceId} sinceHours={sinceHours} />
              </div>
            </>
          )}
        </TabsContent>

        <TabsContent value="labels" className="pt-4">
          <LabelsTab serviceId={activeServiceId} />
        </TabsContent>

        <TabsContent value="matrix" className="pt-4">
          <MatrixVersionsCard serviceId={activeServiceId} />
        </TabsContent>

        <TabsContent value="audit" className="pt-4">
          <AuditLogTab serviceId={activeServiceId} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
