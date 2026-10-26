'use client'

import * as React from 'react'
import dynamic from 'next/dynamic'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Clock, RefreshCw, ShieldCheck } from 'lucide-react'

import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { cn } from '@/lib/utils'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/confirm-dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  ComplianceChart,
  ScoreDistChart,
  ScorerLatencyChart,
  ScorerErrorsChart,
} from '@/components/SessionScoring/ScorerSeriesCard'
import {
  AuditLogTab,
  ExcludeRegexCard,
  MatrixVersionsCard,
  RetrainButton,
  RotateKeyButton,
  ScorerFailOpenBreakdownCard,
  ScoringHealthCard,
} from '@/components/SessionScoring/ScoringAdminPanels'
import { StatusPanel, L2EnforcementCard, PerReasonAucCard, RocPrCurves } from '@/components/SessionScoring/StatusPanel'
import { ThresholdSlider } from '@/components/SessionScoring/ThresholdSlider'
import { TopFlaggedTable } from '@/components/SessionScoring/TopFlaggedTable'
import {
  invalidateServiceScoringQueries,
  scoringAnalyticsCompositeOptions,
  scoringConfigCompositeOptions,
} from '@/components/SessionScoring/help-content'
import { useServiceStore } from '@/stores/filterStore'
import Loading from './loading'

const PRESETS = [
  { value: 1, label: 'Last 1h', shortLabel: '1h' },
  { value: 6, label: 'Last 6h', shortLabel: '6h' },
  { value: 24, label: 'Last 24h', shortLabel: '24h' },
  { value: 72, label: 'Last 3d', shortLabel: '3d' },
  { value: 168, label: 'Last 7d', shortLabel: '7d' },
] as const

interface SinceHoursPickerProps {
  value: number
  onChange: (sinceHours: number) => void
  compact?: boolean
}

/**
 * Time-range picker for the session-scoring page. Drives every card
 * that takes a `sinceHours` prop (ScoringHealthCard, ThresholdSlider,
 * TopFlaggedTable, ScoreDistChart, ComplianceChart, RocPrCurves).
 *
 * Capped at 168h (7d) because the backend's threshold-preview /
 * health endpoints already enforce `le=168` — going wider needs a
 * separate "deep history" endpoint that paginates the DuckDB scan.
 */
export function SinceHoursPicker({ value, onChange, compact = false }: SinceHoursPickerProps) {
  return (
    <Select value={String(value)} onValueChange={(v) => v && onChange(Number(v) || 24)}>
      <SelectTrigger className={compact ? 'h-7 text-xs w-20' : 'h-8 w-[120px] text-xs'}>
        {!compact && <Clock className="h-3.5 w-3.5 mr-1 text-muted-foreground" />}
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {PRESETS.map((p) => (
          <SelectItem key={p.value} value={String(p.value)} className="text-xs">
            {compact ? p.shortLabel : p.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

// LabelsTab is gated behind the "Labels" tab — most page loads never open
// it. Lazy-load to keep it out of the overview bundle (~200 LOC plus its
// Popover/Dialog/Table dependencies). `ssr: false` because the tab uses
// client-only React Query hooks and there's no SEO value to pre-rendering.
const LabelsTab = dynamic(
  () => import('@/components/SessionScoring/ScoringAdminPanels').then((m) => ({ default: m.LabelsTab })),
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

function SessionScoringPageContent() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const [tab, setTab] = React.useState('overview')
  // Shared time window across all overview cards. Defaults to 24h —
  // matches the prior per-component hard-coded value. Picker lives in
  // the PageHeader action slot so it's always visible.
  const [sinceHours, setSinceHours] = React.useState(24)
  const qc = useQueryClient()

  // Fallback `/api/services` query when navigating directly to `/admin/session-scoring`
  // before the global ServiceInitializer has populated `activeServiceId`.
  const { data: servicesData, isLoading: servicesLoading } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async () => unwrapClient(client.GET('/api/services')),
    enabled: !activeServiceId,
  })

  const effectiveServiceId =
    activeServiceId ||
    servicesData?.services?.[0]?.service_id ||
    ''

  React.useEffect(() => {
    if (!activeServiceId && effectiveServiceId) {
      setActiveServiceId(effectiveServiceId)
    }
  }, [activeServiceId, effectiveServiceId, setActiveServiceId])

  const baseAnalyticsOpts = scoringAnalyticsCompositeOptions(effectiveServiceId, sinceHours)
  const baseConfigOpts = scoringConfigCompositeOptions(effectiveServiceId)

  const analyticsSeededAt = React.useRef(0)
  const configSeededAt = React.useRef(0)

  const seedAnalyticsSubQueries = React.useCallback(
    (d: NonNullable<typeof analyticsComposite.data>) => {
      if (d.health) qc.setQueryData(['scoring-health', effectiveServiceId, sinceHours], d.health)
      if (d.top_flagged) qc.setQueryData(['scoring-top-flagged', effectiveServiceId, sinceHours], d.top_flagged)
      if (d.score_distribution) qc.setQueryData(['scoring-score-dist', effectiveServiceId, sinceHours], d.score_distribution)
      if (d.latency_timeseries) qc.setQueryData(['scoring-latency-timeseries', effectiveServiceId, sinceHours], d.latency_timeseries)
      if (d.compliance_breakdown) qc.setQueryData(['scoring-compliance', effectiveServiceId, sinceHours], d.compliance_breakdown)
      if (d.evaluation_per_reason) qc.setQueryData(['scoring-evaluation-per-reason', effectiveServiceId], d.evaluation_per_reason)
      if (d.evaluation) qc.setQueryData(['scoring-evaluation', effectiveServiceId], d.evaluation)
    },
    [qc, effectiveServiceId, sinceHours],
  )

  const seedConfigSubQueries = React.useCallback(
    (d: NonNullable<typeof configComposite.data>) => {
      if (d.status) qc.setQueryData(['scoring-status', effectiveServiceId], d.status)
      if (d.threshold) qc.setQueryData(['scoring-threshold-committed', effectiveServiceId], d.threshold)
      if (d.exclude_regex) qc.setQueryData(['scoring-exclude-regex', effectiveServiceId], d.exclude_regex)
      if (d.enforce_status_code) qc.setQueryData(['scoring-enforce-status-code', effectiveServiceId], d.enforce_status_code)
    },
    [qc, effectiveServiceId],
  )

  // ── Composite queries: collapse 10+ individual requests into 2 ──
  // Seed individual component cache keys synchronously inside queryFn BEFORE
  // returning so that when isLoading flips false and child cards mount, their
  // per-card query keys are already populated on the very first render frame
  // (preventing a 2-frame loading→loaded layout shift and duplicate fetches).
  const analyticsComposite = useQuery({
    ...baseAnalyticsOpts,
    queryFn: async (ctx) => {
      const d = await baseAnalyticsOpts.queryFn!(ctx)
      if (d) {
        seedAnalyticsSubQueries(d)
        analyticsSeededAt.current = Date.now() + 1
      }
      return d
    },
    enabled: !!effectiveServiceId,
  })

  const configComposite = useQuery({
    ...baseConfigOpts,
    queryFn: async (ctx) => {
      const d = await baseConfigOpts.queryFn!(ctx)
      if (d) {
        seedConfigSubQueries(d)
        configSeededAt.current = Date.now() + 1
      }
      return d
    },
    enabled: !!effectiveServiceId,
  })

  // Also keep effect-based seeding in case the composite was pre-warmed in cache
  // by AdminPrefetchLinks before mount.
  React.useEffect(() => {
    if (analyticsComposite.data && analyticsComposite.dataUpdatedAt > analyticsSeededAt.current) {
      analyticsSeededAt.current = analyticsComposite.dataUpdatedAt
      seedAnalyticsSubQueries(analyticsComposite.data)
    }
  }, [analyticsComposite.data, analyticsComposite.dataUpdatedAt, seedAnalyticsSubQueries])

  React.useEffect(() => {
    if (configComposite.data && configComposite.dataUpdatedAt > configSeededAt.current) {
      configSeededAt.current = configComposite.dataUpdatedAt
      seedConfigSubQueries(configComposite.data)
    }
  }, [configComposite.data, configComposite.dataUpdatedAt, seedConfigSubQueries])

  const statusLoading = !effectiveServiceId ? servicesLoading : configComposite.isLoading
  const overviewLoading = !effectiveServiceId ? servicesLoading : analyticsComposite.isLoading
  const isStatusRefreshing = configComposite.isFetching && !statusLoading
  const isOverviewRefreshing = analyticsComposite.isFetching && !overviewLoading
  const isRefreshing = isStatusRefreshing || isOverviewRefreshing

  // Refresh invalidates composite keys (re-seeding individual caches on
  // resolve) plus any queries not covered by composites.
  const refreshAll = () => {
    if (!effectiveServiceId) return
    analyticsSeededAt.current = 0
    configSeededAt.current = 0
    invalidateServiceScoringQueries(qc, effectiveServiceId)
  }

  return (
    <div className="space-y-6" data-testid="admin-session-scoring-root">
      <PageHeader
        title="Session Scoring"
        description="Real-time edge scoring of every request via the scorer Compute service. Toggle scoring on/off, watch the score distribution, and label sessions to evaluate matrix quality (ROC-AUC)."
        icon={ShieldCheck}
      >
        <SinceHoursPicker value={sinceHours} onChange={setSinceHours} />
        {effectiveServiceId ? (
          <>
            <RetrainButton serviceId={effectiveServiceId} />
            <RotateKeyButton serviceId={effectiveServiceId} />
          </>
        ) : (
          <>
            <Skeleton className="h-8 w-28 rounded-md" />
            <Skeleton className="h-8 w-24 rounded-md" />
          </>
        )}
        <Button
          variant="outline"
          size="sm"
          onClick={refreshAll}
          disabled={isRefreshing || !effectiveServiceId}
          data-testid="admin-session-scoring-refresh-btn"
        >
          <RefreshCw className={cn('h-4 w-4 mr-1', isRefreshing && 'animate-spin')} />
          Refresh
        </Button>
        <BackToAdminLink />
      </PageHeader>

      <div
        className={cn(
          'min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto] transition-opacity',
          isStatusRefreshing && 'opacity-40 pointer-events-none',
        )}
        data-testid="admin-session-scoring-status-reservation"
      >
        {statusLoading ? (
          <div className="space-y-3" aria-busy="true">
            <Skeleton className="h-[220px] w-full rounded-xl" />
          </div>
        ) : !effectiveServiceId ? (
          <Alert data-testid="admin-session-scoring-no-service-alert">
            <AlertDescription>
              No active service selected. Pick a service from the sidebar to manage session scoring.
            </AlertDescription>
          </Alert>
        ) : (
          <StatusPanel serviceId={effectiveServiceId} />
        )}
      </div>

      {effectiveServiceId || servicesLoading ? (
        <Tabs
          value={tab}
          onValueChange={setTab}
          className="w-full"
          data-testid="admin-session-scoring-tabs"
        >
          <TabsList>
            <TabsTrigger value="overview" data-testid="admin-session-scoring-tab-overview">
              Overview
            </TabsTrigger>
            <TabsTrigger value="labels" data-testid="admin-session-scoring-tab-labels">
              Labels
            </TabsTrigger>
            <TabsTrigger value="matrix" data-testid="admin-session-scoring-tab-matrix">
              Matrix history
            </TabsTrigger>
            <TabsTrigger value="audit" data-testid="admin-session-scoring-tab-audit">
              Audit
            </TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="pt-4 space-y-6">
            <div
              className={cn(
                'space-y-6 min-h-[900px] [contain-intrinsic-size:auto_900px] [content-visibility:auto] transition-opacity',
                isOverviewRefreshing && 'opacity-40 pointer-events-none',
              )}
              data-testid="admin-session-scoring-overview-reservation"
            >
              {overviewLoading || !effectiveServiceId ? (
                <div className="space-y-6" aria-busy="true">
                  <Skeleton className="h-[240px] w-full rounded-xl" />
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <Skeleton className="h-80 w-full rounded-xl" />
                    <Skeleton className="h-80 w-full rounded-xl" />
                  </div>
                  <Skeleton className="h-48 w-full rounded-xl" />
                  <Skeleton className="h-64 w-full rounded-xl" />
                  <Skeleton className="h-72 w-full rounded-xl" />
                  <Skeleton className="h-64 w-full rounded-xl" />
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <Skeleton className="h-80 w-full rounded-xl" />
                    <Skeleton className="h-80 w-full rounded-xl" />
                  </div>
                </div>
              ) : (
                <>
                  <ScoringHealthCard serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <ScorerLatencyChart serviceId={effectiveServiceId} sinceHours={sinceHours} />
                    <ScorerErrorsChart serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  </div>
                  <ScorerFailOpenBreakdownCard serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <ThresholdSlider serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <L2EnforcementCard serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <ExcludeRegexCard serviceId={effectiveServiceId} />
                  <RocPrCurves serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <PerReasonAucCard serviceId={effectiveServiceId} />
                  <TopFlaggedTable serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <ScoreDistChart serviceId={effectiveServiceId} sinceHours={sinceHours} />
                    <ComplianceChart serviceId={effectiveServiceId} sinceHours={sinceHours} />
                  </div>
                </>
              )}
            </div>
          </TabsContent>

          <TabsContent value="labels" className="pt-4">
            {effectiveServiceId && <LabelsTab serviceId={effectiveServiceId} />}
          </TabsContent>

          <TabsContent value="matrix" className="pt-4">
            {effectiveServiceId && <MatrixVersionsCard serviceId={effectiveServiceId} />}
          </TabsContent>

          <TabsContent value="audit" className="pt-4">
            {effectiveServiceId && <AuditLogTab serviceId={effectiveServiceId} />}
          </TabsContent>
        </Tabs>
      ) : null}
    </div>
  )
}

export default function SessionScoringPage() {
  return (
    <React.Suspense fallback={<Loading />}>
      <SessionScoringPageContent />
    </React.Suspense>
  )
}
