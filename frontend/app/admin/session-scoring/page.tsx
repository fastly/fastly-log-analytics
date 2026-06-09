'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import Link from 'next/link'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, RefreshCw, ShieldCheck } from 'lucide-react'

import { client } from '@/lib/api'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button, buttonVariants } from '@/components/ui/button'
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
import { ScoringHealthCard } from '@/components/SessionScoring/ScoringHealthCard'
import { SinceHoursPicker } from '@/components/SessionScoring/SinceHoursPicker'
import { StatusPanel } from '@/components/SessionScoring/StatusPanel'
import { ThresholdSlider } from '@/components/SessionScoring/ThresholdSlider'
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
  const { activeServiceId } = useServiceStore()
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
        '/api/services/{service_id}/scoring/analytics' as any,
        {
          params: {
            path: { service_id: activeServiceId },
            query: { since_hours: sinceHours },
          },
          signal,
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as Record<string, any>
    },
    enabled: !!activeServiceId,
  })

  const configComposite = useQuery({
    queryKey: ['scoring-config-composite', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/config' as any,
        {
          params: { path: { service_id: activeServiceId }, signal } as any,
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as Record<string, any>
    },
    enabled: !!activeServiceId,
  })

  // Seed individual component cache keys from composite responses.
  // Ref-guarded by dataUpdatedAt so seeding runs once per fresh fetch.
  const analyticsSeededAt = React.useRef(0)
  const configSeededAt = React.useRef(0)

  if (analyticsComposite.data && analyticsComposite.dataUpdatedAt > analyticsSeededAt.current) {
    analyticsSeededAt.current = analyticsComposite.dataUpdatedAt
    const d = analyticsComposite.data
    if (d.health) qc.setQueryData(['scoring-health', activeServiceId, sinceHours], d.health)
    if (d.top_flagged) qc.setQueryData(['scoring-top-flagged', activeServiceId, sinceHours], d.top_flagged)
    if (d.score_distribution) qc.setQueryData(['scoring-score-dist', activeServiceId, sinceHours], d.score_distribution)
    if (d.compliance_breakdown) qc.setQueryData(['scoring-compliance', activeServiceId, sinceHours], d.compliance_breakdown)
    if (d.evaluation_per_reason) qc.setQueryData(['scoring-evaluation-per-reason', activeServiceId], d.evaluation_per_reason)
    if (d.evaluation) qc.setQueryData(['scoring-evaluation', activeServiceId], d.evaluation)
  }

  if (configComposite.data && configComposite.dataUpdatedAt > configSeededAt.current) {
    configSeededAt.current = configComposite.dataUpdatedAt
    const d = configComposite.data
    if (d.status) qc.setQueryData(['scoring-status', activeServiceId], d.status)
    if (d.threshold) qc.setQueryData(['scoring-threshold-committed', activeServiceId], d.threshold)
    if (d.exclude_regex) qc.setQueryData(['scoring-exclude-regex', activeServiceId], d.exclude_regex)
    if (d.enforce_status_code) qc.setQueryData(['scoring-enforce-status-code', activeServiceId], d.enforce_status_code)
  }

  const compositesLoading = analyticsComposite.isLoading || configComposite.isLoading

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
          <Link href="/admin" prefetch={true} className={buttonVariants({ variant: 'outline', size: 'sm' })}>
            <ArrowLeft className="h-4 w-4 mr-1" />
            Back to Admin
          </Link>
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
        <Link href="/admin" prefetch={true} className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to Admin
        </Link>
      </PageHeader>

      {compositesLoading ? (
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
          {compositesLoading ? (
            <div className="space-y-6" aria-busy="true">
              <Skeleton className="h-48 w-full" />
              <Skeleton className="h-64 w-full" />
              <Skeleton className="h-32 w-full" />
            </div>
          ) : (
            <>
              <ScoringHealthCard serviceId={activeServiceId} sinceHours={sinceHours} />
              <ThresholdSlider serviceId={activeServiceId} sinceHours={sinceHours} />
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
