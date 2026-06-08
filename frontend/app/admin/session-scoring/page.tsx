'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, RefreshCw, ShieldCheck } from 'lucide-react'

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

  // Manual refresh replaces the per-component refetchInterval polling we
  // removed after the 2026-06-01 mds_stores + VS Code RAM crash. Predicate
  // invalidation matches every ['scoring-*', activeServiceId, ...] key,
  // so new scoring queries (e.g. ['scoring-labels-counts', sid]) get
  // refreshed without having to add a new invalidate line here.
  const refreshAll = () => {
    qc.invalidateQueries({
      predicate: (q) =>
        Array.isArray(q.queryKey) &&
        typeof q.queryKey[0] === 'string' &&
        (q.queryKey[0] as string).startsWith('scoring-') &&
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

      <StatusPanel serviceId={activeServiceId} />

      <Tabs value={tab} onValueChange={setTab} className="w-full">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="labels">Labels</TabsTrigger>
          <TabsTrigger value="matrix">Matrix history</TabsTrigger>
          <TabsTrigger value="audit">Audit</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="pt-4 space-y-6">
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
