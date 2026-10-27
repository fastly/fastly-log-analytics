'use client'
import React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import Link from 'next/link'
import { Activity, AlertTriangle, Database, Eye, Layers, ShieldCheck, TrendingUp, UserPlus } from 'lucide-react'
import type { components } from '@/types/api.generated'
import { adminJson, client, unwrapClient, queryKeys } from '@/lib/api'
import { buildServiceHref, formatCount } from '@/lib/utils'
import { useBootstrap, useMounted } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/filterStore'
import { scoringAnalyticsCompositeOptions, scoringConfigCompositeOptions } from '@/components/SessionScoring/ScoringAdminPanels'
import { Badge } from '@/components/ui/badge'
import { buttonVariants } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'

const ADMIN_PREFETCH_LINKS = [
  { href: '/admin/share', testId: 'open-share-dialog', label: 'Share Dashboard', icon: UserPlus, key: 'share' },
  { href: '/admin/session-scoring', label: 'Session Scoring', icon: ShieldCheck, key: 'scoring' },
  { href: '/admin/queries', label: 'Live Queries', icon: Activity, key: 'queries' },
  { href: '/admin/trends', label: 'Trends', icon: TrendingUp, key: 'trends' },
  { href: '/admin/queue', label: 'Queue', icon: Layers, key: 'queue' },
  { href: '/admin/rum', label: 'RUM', icon: Eye, key: 'rum' },
  { href: '/admin/clickhouse', label: 'ClickHouse', icon: Database, key: 'clickhouse', highScaleOnly: true },
] as const

export function AdminPrefetchLinks({ isHighScale }: { isHighScale?: boolean } = {}) {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const services = useServiceStore(s => s.services)
  const activeServiceId = mounted ? storeServiceId : null
  const showClickHouse = isHighScale ?? services.some(s => Boolean(s.is_high_scale))
  const handlePrefetch = (key: (typeof ADMIN_PREFETCH_LINKS)[number]['key']) => {
    if (key === 'share') queryClient.prefetchQuery({ queryKey: queryKeys.admin.shareStatus(), queryFn: async ({ signal }) => unwrapClient(client.GET('/api/admin/share/status', { signal })) })
    else if (key === 'scoring' && activeServiceId) { queryClient.prefetchQuery(scoringAnalyticsCompositeOptions(activeServiceId, 24)); queryClient.prefetchQuery(scoringConfigCompositeOptions(activeServiceId)) }
    else if (key === 'trends') queryClient.prefetchQuery({ queryKey: queryKeys.admin.metricHistoryBatch('1h'), queryFn: async ({ signal }) => unwrapClient(client.GET('/api/admin/metric-history/batch', { params: { query: { since: '1h' } }, signal })) })
    else if (key === 'queue') queryClient.prefetchQuery({ queryKey: queryKeys.admin.celeryStatus(), queryFn: async ({ signal }) => unwrapClient(client.GET('/api/admin/celery/status', { signal })) })
    else if (key === 'clickhouse' && activeServiceId) queryClient.prefetchQuery({
      queryKey: queryKeys.admin.clickhouseStatus(activeServiceId),
      queryFn: async ({ signal }) => {
        const { data, response } = await client.GET('/api/admin/clickhouse/status', { params: { query: { service_id: activeServiceId } }, signal })
        if (!response.ok && response.status !== 503) throw new Error(`status ${response.status}`)
        return data
      },
    })
  }
  return (
    <>
      {ADMIN_PREFETCH_LINKS.filter(item => !('highScaleOnly' in item && item.highScaleOnly) || showClickHouse).map(item => (
        <Link key={item.key} href={activeServiceId ? `${item.href}?service=${activeServiceId}` : item.href} prefetch={false} onMouseEnter={() => handlePrefetch(item.key)} data-testid={'testId' in item ? item.testId : undefined} className={buttonVariants({ variant: 'secondary', size: 'sm' })}>
          <item.icon className="h-4 w-4 mr-1" /> {item.label}
        </Link>
      ))}
    </>
  )
}

const POLL_MS = 10_000
type SummaryResponse = components['schemas']['SummaryResponse']
type LogAccountingResponse = components['schemas']['LogAccountingResponse']
type OpsOverviewSeed = { queries_summary?: SummaryResponse; log_accounting?: LogAccountingResponse; slow_queries_count?: { count: number; since_hours: number; threshold_ms: number } }

function useOpsOverviewSeed(): { seed: OpsOverviewSeed; seedServiceId: string | null } {
  const { data } = useBootstrap()
  return {
    seed: (data as { ops_overview?: OpsOverviewSeed } | undefined)?.ops_overview ?? {},
    seedServiceId: (data as { active_service_id?: string | null } | undefined)?.active_service_id ?? null,
  }
}

export function OperationsOverview() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-3 min-h-[108px] [contain-intrinsic-size:auto_108px]" data-testid="admin-operations-overview">
      <LiveQueriesCard /><IngestHealthCard /><SlowQueriesTeaser /><CeleryStatusCard />
    </div>
  )
}

function LiveQueriesCard() {
  const mounted = useMounted()
  const seed = useOpsOverviewSeed().seed.queries_summary
  const { data: queryData, isLoading: queryIsLoading, isFetching, isError } = useQuery<SummaryResponse>({
    queryKey: queryKeys.admin.overviewQueriesSummary(),
    queryFn: async ({ signal }) => adminJson('/api/admin/queries/summary', '/api/admin/queries/summary', { signal }),
    initialData: seed, retry: false, staleTime: POLL_MS, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  const data = mounted ? queryData : seed
  const isLoading = !mounted ? !seed : queryIsLoading
  const active = data?.active_total ?? 0
  return (
    <OverviewCard
      href="/admin/queries" icon={<Activity className="h-4 w-4" />} title="Live Queries"
      primary={isLoading && !data ? '—' : String(active)} primaryTone={active > 0 ? 'default' : 'muted'}
      secondary={isLoading && !data ? 'Inspecting background scheduler state...' : data?.by_db_type ? Object.entries(data.by_db_type).filter(([, n]) => n > 0).map(([db, n]) => `${n} ${db}`).join(' · ') || 'idle' : '—'}
      isStale={mounted && isError && !!data} isRefreshing={mounted && isFetching && !isLoading}
    />
  )
}

function IngestHealthCard() {
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.log_accounting : undefined
  const { data, isFetching, isLoading, isError } = useQuery<LogAccountingResponse>({
    queryKey: queryKeys.admin.overviewLogAccounting(activeServiceId),
    queryFn: async ({ signal }) => adminJson('/api/admin/log-accounting', '/api/admin/log-accounting?hours=24', { signal }),
    initialData: seed, enabled: !!activeServiceId, retry: false, staleTime: 30_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  if (!activeServiceId) return <OverviewCard href="/admin/usage-log" icon={<Database className="h-4 w-4" />} title="Ingest Gap" primary="—" primaryTone="muted" secondary="no service yet" />
  const gapPct = data?.totals?.gap_pct ?? 0
  const sustained = data?.sustained_loss
  const tone: CardTone = isError ? 'muted' : sustained ? 'critical' : gapPct >= 0.1 ? 'warning' : gapPct >= 0.02 ? 'attention' : 'default'
  const primary = data?.totals ? `${(gapPct * 100).toFixed(gapPct === 0 ? 0 : 1)}%` : '—'
  const secondary = isError ? 'unconfigured or credentials missing' : sustained ? `sustained: ${sustained.n_buckets} bucket(s), ${formatCount(sustained.total_lost_lines)} lost` : gapPct >= 0.02 ? 'recent loss — check ingest accounting' : 'healthy · 24h'
  return (
    <OverviewCard
      href="/admin/usage-log" icon={tone === 'critical' || tone === 'warning' ? <AlertTriangle className="h-4 w-4" /> : <Database className="h-4 w-4" />}
      title="Ingest Gap" primary={primary} primaryTone={tone} secondary={secondary} isStale={isError && !!data} isRefreshing={isFetching && !isLoading}
    />
  )
}

function SlowQueriesTeaser() {
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.slow_queries_count : undefined
  const { data, isFetching, isLoading, isError } = useQuery<{ count: number }>({
    queryKey: queryKeys.admin.overviewSlowQueriesCount(activeServiceId),
    queryFn: async ({ signal }) => adminJson('/api/admin/slow-queries/count', '/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000', { signal }),
    initialData: seed, enabled: !!activeServiceId, retry: false, staleTime: POLL_MS, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  if (!activeServiceId) return <OverviewCard href="/admin/queries?view=past&slow=1000" icon={<Activity className="h-4 w-4" />} title="Notable Slow Queries" primary="—" primaryTone="muted" secondary="no service yet" />
  const slowCount = data?.count ?? 0
  return (
    <OverviewCard
      href="/admin/queries?view=past&slow=1000" icon={<Activity className="h-4 w-4" />} title="Notable Slow Queries"
      primary={String(slowCount)} primaryTone={slowCount > 0 ? 'attention' : 'muted'}
      secondary={slowCount === 0 ? 'none ≥ 1s in last 24h' : '≥ 1s in last 24h'}
      isStale={isError && !!data} isRefreshing={isFetching && !isLoading}
    />
  )
}

type CardTone = 'default' | 'muted' | 'attention' | 'warning' | 'critical'
const TONE_CLASSES: Record<CardTone, string> = {
  critical: 'text-red-600 dark:text-red-400', warning: 'text-orange-600 dark:text-orange-400',
  attention: 'text-amber-600 dark:text-amber-400', muted: 'text-muted-foreground', default: 'text-foreground',
}

function OverviewCard({ href, icon, title, primary, primaryTone, secondary, isStale = false, isRefreshing = false }: {
  href: string; icon: React.ReactNode; title: string; primary: string; primaryTone: CardTone; secondary: string; isStale?: boolean; isRefreshing?: boolean
}) {
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  return (
    <Link href={buildServiceHref(href, activeServiceId)} prefetch={false} className="block group">
      <Card className={`transition-all group-hover:bg-muted/40 cursor-pointer ${isRefreshing ? 'opacity-40 pointer-events-none' : ''}`}>
        <CardContent className="p-4">
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-2">
            <span className="flex items-center gap-1.5">{icon}{title}</span>
            <div className="flex items-center gap-1.5">
              {isStale && <Badge variant="outline" className="text-[10px] border-amber-500/40 text-amber-700 dark:text-amber-400" title="Refetch failed — showing last known value">stale</Badge>}
              <Badge variant="outline" className="text-[10px]">view →</Badge>
            </div>
          </div>
          <div className={`text-3xl font-semibold tabular-nums ${TONE_CLASSES[primaryTone]}`}>{primary}</div>
          <div className="text-xs text-muted-foreground mt-1 truncate">{secondary}</div>
        </CardContent>
      </Card>
    </Link>
  )
}

function CeleryStatusCard() {
  const mounted = useMounted()
  const { data: queryData, isFetching, isLoading, isError } = useQuery({
    queryKey: queryKeys.admin.celeryStatus(),
    queryFn: async ({ signal }) => adminJson<'/api/admin/celery/status'>('/api/admin/celery/status', '/api/admin/celery/status', { signal }),
    retry: false, staleTime: POLL_MS, refetchInterval: POLL_MS,
  })
  const data = mounted ? queryData : undefined
  const isStandardMode = data?.deployment_mode === 'standard' && !data?.broker_reachable
  const workers = data?.workers?.count ?? 0
  const ingestQ = data?.queues?.['q.ingest'] ?? 0
  const controlQ = data?.queues?.['q.control'] ?? 0
  const deadLetters = data?.ledger?.dead_letter ?? 0
  const quarantined = (data?.ledger?.quarantined ?? 0) + (data?.standard_ingest?.quarantined_items ?? 0)
  const schedCount = data?.inprocess_schedules?.length ?? 0
  const activeRuns = data?.inprocess_active_runs?.length ?? 0
  const ingestedFiles = data?.standard_ingest?.ingested_files ?? 0
  let primaryText = isStandardMode ? `${schedCount} scheduled` : `${workers} workers`
  let primaryTone: CardTone = isStandardMode ? 'default' : workers > 0 ? 'default' : 'warning'
  let secondaryText = isStandardMode ? `${activeRuns} active · ${formatCount(ingestedFiles)} ingested · in-process` : `ingest: ${ingestQ} · control: ${controlQ}`
  if (deadLetters > 0) {
    primaryText = `${deadLetters} dead letters`; primaryTone = 'critical'
    secondaryText = isStandardMode ? `${schedCount} scheduled · in-process` : `${workers} workers · ingest: ${ingestQ}`
  } else if (quarantined > 0) {
    primaryText = `${quarantined} quarantined`; primaryTone = 'attention'
    secondaryText = isStandardMode ? `${schedCount} scheduled · in-process` : `${workers} workers · ingest: ${ingestQ}`
  }
  return <OverviewCard href="/admin/queue" icon={<Activity className="h-4 w-4" />} title="Workers & Queues" primary={primaryText} primaryTone={primaryTone} secondary={secondaryText} isStale={isError && !!data} isRefreshing={isFetching && !isLoading} />
}
