'use client'

/**
 * Operations Overview — at-a-glance status row at the top of /admin so
 * operators see ingest health + live query activity without having to
 * navigate into the sub-pages. Cards are entirely clickable; each links
 * to the page that owns the full UI for that subsystem.
 *
 * The 2026-06-12 incident-debug session that motivated this: the
 * sustained_loss alert from log-accounting was buried in a sub-page
 * that nobody routinely visited. A 47% ingest gap sat undetected for
 * ~12 days. Surfacing the gap on /admin (the page operators DO open
 * regularly) closes that visibility gap.
 *
 * Polls every 10s (not every 1s like the dashboard) because the values
 * here change on a minutes-to-hours cadence; sub-second freshness costs
 * RSS + DB pressure for no benefit.
 */

import * as React from 'react'
import { formatCount } from '@/lib/format'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import Link from 'next/link'
import { Activity, Database, AlertTriangle, UserPlus, ShieldCheck, TrendingUp, Layers, Eye, RefreshCw } from 'lucide-react'

import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Sparkline } from '@/components/Sparkline'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useMounted } from '@/hooks/useBootstrap'
import type { components } from '@/types/api.generated'
import { adminJson, client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { scoringAnalyticsCompositeOptions, scoringConfigCompositeOptions } from '@/components/SessionScoring/useScorerTimeseries'
import { useServiceStore } from '@/stores/serviceStore'
import { buildServiceHref, cn } from '@/lib/utils'
import { queryKeys } from '@/lib/query-keys'

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
    if (key === 'share') {
      queryClient.prefetchQuery({
        queryKey: queryKeys.admin.shareStatus(),
        queryFn: async ({ signal }) =>
          unwrapClient(client.GET('/api/admin/share/status', { signal })),
      })
    } else if (key === 'scoring' && activeServiceId) {
      queryClient.prefetchQuery(scoringAnalyticsCompositeOptions(activeServiceId, 24))
      queryClient.prefetchQuery(scoringConfigCompositeOptions(activeServiceId))
    } else if (key === 'trends') {
      queryClient.prefetchQuery({
        queryKey: queryKeys.admin.metricHistoryBatch('1h'),
        queryFn: async ({ signal }) =>
          unwrapClient(
            client.GET('/api/admin/metric-history/batch', {
              params: { query: { since: '1h' } },
              signal,
            }),
          ),
      })
    } else if (key === 'queue') {
      queryClient.prefetchQuery({
        queryKey: queryKeys.admin.celeryStatus(),
        queryFn: async ({ signal }) =>
          unwrapClient(client.GET('/api/admin/celery/status', { signal })),
      })
    } else if (key === 'clickhouse' && activeServiceId) {
      queryClient.prefetchQuery({
        queryKey: queryKeys.admin.clickhouseStatus(activeServiceId),
        queryFn: async ({ signal }) => {
          const { data, response } = await client.GET('/api/admin/clickhouse/status', {
            params: { query: { service_id: activeServiceId } },
            signal,
          })
          if (!response.ok && response.status !== 503) throw new Error(`status ${response.status}`)
          return data
        },
      })
    }
  }

  return (
    <>
      {ADMIN_PREFETCH_LINKS.filter(item => !('highScaleOnly' in item && item.highScaleOnly) || showClickHouse).map(item => {
        const Icon = item.icon
        return (
          <Link
            key={item.key}
            href={activeServiceId ? `${item.href}?service=${activeServiceId}` : item.href}
            prefetch={false}
            onMouseEnter={() => handlePrefetch(item.key)}
            data-testid={'testId' in item ? item.testId : undefined}
            className={buttonVariants({ variant: 'secondary', size: 'sm' })}
          >
            <Icon className="h-4 w-4 mr-1" /> {item.label}
          </Link>
        )
      })}
    </>
  )
}

const POLL_MS = 10_000

type SummaryResponse = components['schemas']['SummaryResponse']
type LogAccountingResponse = components['schemas']['LogAccountingResponse']

type OpsOverviewSeed = {
  queries_summary?: SummaryResponse
  log_accounting?: LogAccountingResponse
  slow_queries_count?: { count: number; since_hours: number; threshold_ms: number }
}

function useOpsOverviewSeed(): { seed: OpsOverviewSeed; seedServiceId: string | null } {
  const { data } = useBootstrap()
  // The seeds in ``ops_overview`` are computed for the bootstrap's active
  // service (``active_service_id``). Hand that id back alongside the seed so
  // the service-scoped cards (ingest gap, slow queries) only apply it as
  // ``initialData`` when the *currently* active service matches — otherwise
  // switching services would flash the previous service's numbers before the
  // re-keyed refetch lands.
  return {
    seed: (data as { ops_overview?: OpsOverviewSeed } | undefined)?.ops_overview ?? {},
    seedServiceId:
      (data as { active_service_id?: string | null } | undefined)?.active_service_id ?? null,
  }
}

export function OperationsOverview() {
  return (
    <div
      className="grid grid-cols-1 md:grid-cols-4 gap-3 min-h-[108px] [contain-intrinsic-size:auto_108px]"
      data-testid="admin-operations-overview"
    >
      <LiveQueriesCard />
      <IngestHealthCard />
      <SlowQueriesTeaser />
      <CeleryStatusCard />
    </div>
  )
}

// ── Card 1: live query activity ───────────────────────────────────────────

function LiveQueriesCard() {
  // /queries/summary is the GLOBAL live-query registry (process-wide, not
  // service-scoped), so the key intentionally omits the service id — the
  // number is the same regardless of which service is active.
  // SSR-safe: gate live SSE/query cache reads behind useMounted() so the first
  // client hydration pass matches the server's seed/initial render even if
  // useAdminEventStream (mounted in the layout chunk) pushes before this page
  // chunk hydrates.
  const mounted = useMounted()
  const seed = useOpsOverviewSeed().seed.queries_summary
  // Freshness via useAdminEventStream (mounted in SyncStatusBadge).
  // 5-min refetchInterval is a pure safety net.
  const { data: queryData, isLoading: queryIsLoading, isFetching, isError } = useQuery<SummaryResponse>({
    queryKey: queryKeys.admin.overviewQueriesSummary(),
    queryFn: async ({ signal }) => {
      return adminJson('/api/admin/queries/summary', '/api/admin/queries/summary', { signal })
    },
    initialData: seed,
    retry: false,
    staleTime: POLL_MS,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  const data = mounted ? queryData : seed
  const isLoading = !mounted ? !seed : queryIsLoading
  const active = data?.active_total ?? 0
  return (
    <OverviewCard
      href="/admin/queries"
      icon={<Activity className="h-4 w-4" />}
      title="Live Queries"
      primary={isLoading && !data ? '—' : String(active)}
      primaryTone={active > 0 ? 'default' : 'muted'}
      secondary={
        isLoading && !data
          ? 'Inspecting background scheduler state...'
          : data?.by_db_type
            ? Object.entries(data.by_db_type)
                .filter(([, n]) => n > 0)
                .map(([db, n]) => `${n} ${db}`)
                .join(' · ') || 'idle'
            : '—'
      }
      isStale={mounted && isError && !!data}
      isRefreshing={mounted && isFetching && !isLoading}
    />
  )
}

// ── Card 2: ingest gap ───────────────────────────────────────────────────

function IngestHealthCard() {
  // log-accounting is service-scoped (per-service ingest gap); the backend
  // 400s without a service. On a fresh install there is none yet, so gate
  // the fetch off — firing it just spams the console with 400s and the gap
  // is meaningless with nothing ingested.
  // SSR-safe: StoreHydrator / useBootstrap live in RootLayout and can flip
  // activeServiceId before this page chunk hydrates; gate on useMounted() so
  // the first client render matches the server's activeServiceId=null state.
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  // Only honour the seed when it belongs to the active service (it's
  // currently never seeded — see bootstrap.py — but stay correct if that
  // changes). The gap is per-service, so the key MUST include the service id;
  // without it, switching services serves the previous service's cached gap.
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.log_accounting : undefined
  const { data, isFetching, isLoading, isError } = useQuery<LogAccountingResponse>({
    queryKey: queryKeys.admin.overviewLogAccounting(activeServiceId),
    queryFn: async ({ signal }) => {
      return adminJson('/api/admin/log-accounting', '/api/admin/log-accounting?hours=24', { signal })
    },
    initialData: seed,
    enabled: !!activeServiceId,
    retry: false,
    // Freshness via useAdminEventStream. 5-min poll is the safety
    // net; the DuckDB COUNT(*) only runs on the safety tick now.
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  if (!activeServiceId) {
    return (
      <OverviewCard
        href="/admin/usage-log"
        icon={<Database className="h-4 w-4" />}
        title="Ingest Gap"
        primary="—"
        primaryTone="muted"
        secondary="no service yet"
      />
    )
  }
  const gapPct = data?.totals?.gap_pct ?? 0
  const sustained = data?.sustained_loss
  // Gap = Fastly requests − our ingested rows. `requests` sits 1:1 with our
  // rows on every service, so the gap is directly comparable (no endpoint
  // caveat). gap_pct can be negative (we have more rows than Fastly — usually
  // in-flight bucket noise); only POSITIVE gaps mean real loss, which is what
  // the tone reflects.
  const tone: CardTone = isError
    ? 'muted'
    : sustained
      ? 'critical'
      : gapPct >= 0.1
        ? 'warning'
        : gapPct >= 0.02
          ? 'attention'
          : 'default'
  const primary = data?.totals
    ? `${(gapPct * 100).toFixed(gapPct === 0 ? 0 : 1)}%`
    : '—'
  const secondary = isError
    ? 'unconfigured or credentials missing'
    : sustained
      ? `sustained: ${sustained.n_buckets} bucket(s), ${formatCount(sustained.total_lost_lines)} lost`
      : gapPct >= 0.02
        ? 'recent loss — check ingest accounting'
        : 'healthy · 24h'
  return (
    <OverviewCard
      href="/admin/usage-log"
      icon={
        tone === 'critical' || tone === 'warning' ? (
          <AlertTriangle className="h-4 w-4" />
        ) : (
          <Database className="h-4 w-4" />
        )
      }
      title="Ingest Gap"
      primary={primary}
      primaryTone={tone}
      secondary={secondary}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading}
    />
  )
}

// ── Card 3: slow queries teaser ──────────────────────────────────────────

function SlowQueriesTeaser() {
  // Use the persistent ``slow_queries`` SQLite via the dedicated count
  // endpoint instead of filtering the in-memory snapshot client-side.
  // Three wins: (1) the count is time-bounded ("in last 24 h") instead
  // of size-bounded ("in last 2000-query window"), which is what an
  // operator actually wants; (2) survives restarts; (3) single indexed
  // COUNT(*) instead of shipping a 2000-row JSON payload.
  // slow-queries/count is service-scoped (the backend 400s `service_id_required`
  // without one). Gate it off on a fresh install so we don't spam 400s before
  // any service — and any query exists — to count.
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  // slow-queries/count is service-scoped, so the key must include the service
  // id; gate the seed to the matching service to avoid flashing the previous
  // service's count on switch.
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.slow_queries_count : undefined
  const { data, isFetching, isLoading, isError } = useQuery<{ count: number }>({
    queryKey: queryKeys.admin.overviewSlowQueriesCount(activeServiceId),
    queryFn: async ({ signal }) => {
      return adminJson(
        '/api/admin/slow-queries/count',
        '/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000',
        { signal },
      )
    },
    initialData: seed,
    enabled: !!activeServiceId,
    retry: false,
    // Freshness via useAdminEventStream. Safety-net poll only.
    staleTime: POLL_MS,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })
  if (!activeServiceId) {
    return (
      <OverviewCard
        href="/admin/queries?view=past&slow=1000"
        icon={<Activity className="h-4 w-4" />}
        title="Notable Slow Queries"
        primary="—"
        primaryTone="muted"
        secondary="no service yet"
      />
    )
  }
  const slowCount = data?.count ?? 0
  return (
    <OverviewCard
      href="/admin/queries?view=past&slow=1000"
      icon={<Activity className="h-4 w-4" />}
      title="Notable Slow Queries"
      primary={String(slowCount)}
      primaryTone={slowCount > 0 ? 'attention' : 'muted'}
      secondary={slowCount === 0 ? 'none ≥ 1s in last 24h' : '≥ 1s in last 24h'}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading}
    />
  )
}

// ── Shared primitive ─────────────────────────────────────────────────────

type CardTone = 'default' | 'muted' | 'attention' | 'warning' | 'critical'

function OverviewCard({
  href,
  icon,
  title,
  primary,
  primaryTone,
  secondary,
  isStale = false,
  isRefreshing = false,
}: {
  href: string
  icon: React.ReactNode
  title: string
  primary: string
  primaryTone: CardTone
  secondary: string
  /** Refetch failed but seed/last-good data is still rendered. Surface a
   *  small "stale" badge so operators don't read the seeded value as live.
   *  Without this, a backend hiccup silently freezes the number with no
   *  signal — the exact failure mode the post-2026-06-12 OperationsOverview
   *  was built to prevent. */
  isStale?: boolean
  isRefreshing?: boolean
}) {
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  // Tone-driven classes for the primary metric. Keep these as literal
  // strings so Tailwind's content-scanner can see them — building class
  // names dynamically (e.g. `text-${tone}-600`) would be invisible to
  // the build and silently render as default colour.
  const primaryClass =
    primaryTone === 'critical'
      ? 'text-red-600 dark:text-red-400'
      : primaryTone === 'warning'
        ? 'text-orange-600 dark:text-orange-400'
        : primaryTone === 'attention'
          ? 'text-amber-600 dark:text-amber-400'
          : primaryTone === 'muted'
            ? 'text-muted-foreground'
            : 'text-foreground'
  return (
    <Link href={buildServiceHref(href, activeServiceId)} prefetch={false} className="block group">
      <Card
        className={`transition-all group-hover:bg-muted/40 cursor-pointer ${
          isRefreshing ? 'opacity-40 pointer-events-none' : ''
        }`}
      >
        <CardContent className="p-4">
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-2">
            <span className="flex items-center gap-1.5">
              {icon}
              {title}
            </span>
            <div className="flex items-center gap-1.5">
              {isStale && (
                <Badge
                  variant="outline"
                  className="text-[10px] border-amber-500/40 text-amber-700 dark:text-amber-400"
                  title="Refetch failed — showing last known value"
                >
                  stale
                </Badge>
              )}
              <Badge variant="outline" className="text-[10px]">view →</Badge>
            </div>
          </div>
          <div className={`text-3xl font-semibold tabular-nums ${primaryClass}`}>
            {primary}
          </div>
          <div className="text-xs text-muted-foreground mt-1 truncate">{secondary}</div>
        </CardContent>
      </Card>
    </Link>
  )
}


// ── Card 4: Celery Status ───────────────────────────────────────────

function CeleryStatusCard() {
  const mounted = useMounted()
  const { data: queryData, isFetching, isLoading, isError } = useQuery({
    queryKey: queryKeys.admin.celeryStatus(),
    queryFn: async ({ signal }) => {
      return adminJson<'/api/admin/celery/status'>('/api/admin/celery/status', '/api/admin/celery/status', { signal })
    },
    retry: false,
    staleTime: POLL_MS,
    refetchInterval: POLL_MS,
  })
  const data = mounted ? queryData : undefined

  const isStandardMode = data?.deployment_mode === 'standard' && !data?.broker_reachable
  const workers = data?.workers?.count ?? 0
  const ingestQ = data?.queues?.['q.ingest'] ?? 0
  const controlQ = data?.queues?.['q.control'] ?? 0
  const deadLetters = data?.ledger?.dead_letter ?? 0
  const quarantined =
    (data?.ledger?.quarantined ?? 0) + (data?.standard_ingest?.quarantined_items ?? 0)
  const schedCount = data?.inprocess_schedules?.length ?? 0
  const activeRuns = data?.inprocess_active_runs?.length ?? 0
  const ingestedFiles = data?.standard_ingest?.ingested_files ?? 0

  let primaryText = isStandardMode ? `${schedCount} scheduled` : `${workers} workers`
  let primaryTone: CardTone = isStandardMode
    ? 'default'
    : workers > 0
      ? 'default'
      : 'warning'
  let secondaryText = isStandardMode
    ? `${activeRuns} active · ${formatCount(ingestedFiles)} ingested · in-process`
    : `ingest: ${ingestQ} · control: ${controlQ}`

  if (deadLetters > 0) {
    primaryText = `${deadLetters} dead letters`
    primaryTone = 'critical'
    secondaryText = isStandardMode
      ? `${schedCount} scheduled · in-process`
      : `${workers} workers · ingest: ${ingestQ}`
  } else if (quarantined > 0) {
    primaryText = `${quarantined} quarantined`
    primaryTone = 'attention'
    secondaryText = isStandardMode
      ? `${schedCount} scheduled · in-process`
      : `${workers} workers · ingest: ${ingestQ}`
  }

  return (
    <OverviewCard
      href="/admin/queue"
      icon={<Activity className="h-4 w-4" />}
      title="Workers & Queues"
      primary={primaryText}
      primaryTone={primaryTone}
      secondary={secondaryText}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading}
    />
  )
}

export type TrendWindow = '1h' | '6h' | '24h' | '7d'

export const WINDOWS: { label: string; value: TrendWindow }[] = [
  { label: 'Last hour', value: '1h' },
  { label: 'Last 6h', value: '6h' },
  { label: 'Last 24h', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
]

export const SERIES_LAYOUT: {
  title: string
  description: string
  metric: string
  unit: string
  yDomain?: [number | 'auto', number | 'auto']
  aggregateBy?: 'max-per-ts'
}[] = [
  { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m', unit: '' },
  { title: 'Memory used', description: 'Physical memory consumption.', metric: 'mem_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Data disk used', description: 'Disk consumption on the data mount.', metric: 'disk_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Boot disk used', description: 'Disk consumption on the root mount.', metric: 'disk_used_pct_root', unit: '%', yDomain: [0, 100] },
  { title: 'Active DuckDB queries', description: 'Concurrent in-flight query count from the registry.', metric: 'active_query_count', unit: '' },
  { title: 'Pool wait p95', description: 'Worst-case DuckDB pool checkout wait (ms) — max across services per timestamp.', metric: 'pool_wait_p95_ms', unit: 'ms', aggregateBy: 'max-per-ts' },
  { title: 'Ingest lag', description: 'Seconds since the most recent ingest per service — max across services per timestamp.', metric: 'ingest_lag_s', unit: 's', aggregateBy: 'max-per-ts' },
]

type TrendPoint = components['schemas']['MetricHistoryPoint']
type TrendBatch = components['schemas']['MetricHistoryBatchResponse']

function maxAcrossServicesPerTs(series: Record<string, TrendPoint[]>, metric: string): TrendPoint[] {
  const keys = Object.keys(series).filter((k) => k.startsWith(`${metric}|`))
  if (!keys.length) return []
  const byTs = new Map<string, number>()
  for (const k of keys) {
    for (const p of series[k] ?? []) {
      const prev = byTs.get(p.ts) ?? 0
      if (p.value > prev) byTs.set(p.ts, p.value)
    }
  }
  return Array.from(byTs.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([ts, value]) => ({ ts, value }))
}

function formatTrendValue(v: number, unit: string): string {
  if (unit === '%') return `${v.toFixed(1)}%`
  if (unit === 'ms') return `${v.toFixed(1)}ms`
  if (unit === 's') return `${v.toFixed(0)}s`
  return v.toFixed(2)
}

export function AdminTrendsClient() {
  const [window, setWindow] = React.useState<TrendWindow>('1h')

  const { data: trends, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: queryKeys.admin.metricHistoryBatch(window),
    queryFn: async ({ signal }) => {
      return (await unwrapClient(
        client.GET('/api/admin/metric-history/batch', {
          params: { query: { since: window } },
          signal,
        }),
      )) as TrendBatch
    },
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    staleTime: 60_000,
  })

  const series = trends?.series ?? {}
  const isRefreshing = isFetching && !isLoading

  return (
    <div data-testid="admin-trends-root" className="container mx-auto p-4 space-y-4">
      <PageHeader
        title="Operational Trends"
        description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots."
        icon={TrendingUp}
      >
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            data-testid="admin-trends-refresh-btn"
            onClick={() => void refetch()}
            disabled={isFetching}
          >
            <RefreshCw className={cn('mr-1.5 h-3.5 w-3.5', isRefreshing && 'animate-spin')} />
            Refresh
          </Button>
          <BackToAdminLink variant="ghost" prefetch={false} />
        </div>
      </PageHeader>

      <div
        data-testid="admin-trends-toolbar"
        className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        {WINDOWS.map((w) => (
          <Button
            key={w.value}
            type="button"
            data-testid={`admin-trends-window-${w.value}`}
            aria-pressed={window === w.value}
            variant={window === w.value ? 'default' : 'outline'}
            size="sm"
            onClick={() => setWindow(w.value)}
          >
            {w.label}
          </Button>
        ))}
      </div>

      <div
        data-testid="admin-trends-grid-reservation"
        className={cn(
          'grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]',
          isRefreshing && 'opacity-40 pointer-events-none transition-opacity',
        )}
      >
        {SERIES_LAYOUT.map((cfg) => {
          const points =
            cfg.aggregateBy === 'max-per-ts'
              ? maxAcrossServicesPerTs(series, cfg.metric)
              : series[cfg.metric] ?? []
          const latest = points.length ? points[points.length - 1].value : null
          return (
            <div key={cfg.metric} data-testid={`admin-trends-card-${cfg.metric}`}>
              <AnalyticsCard
                title={cfg.title}
                description={cfg.description}
                isLoading={isLoading}
                isFetching={isFetching}
                loadingMessage="Loading system vital trends..."
                error={error as AnalyticsCardError | null}
                isEmpty={!isLoading && !error && points.length === 0}
              >
                <div className="flex items-baseline justify-between mb-2">
                  <div
                    data-testid={`admin-trends-latest-${cfg.metric}`}
                    className="text-2xl font-semibold tabular-nums"
                  >
                    {latest === null ? '–' : formatTrendValue(latest, cfg.unit)}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {points.length > 0 ? `${points.length} samples` : isLoading ? 'loading…' : 'no samples yet'}
                  </div>
                </div>
                <div className={cn('text-foreground/70', latest === null && 'opacity-40')}>
                  <Sparkline
                    points={points}
                    yDomain={cfg.yDomain}
                    height={120}
                    label={cfg.title}
                    formatValue={(v) => formatTrendValue(v, cfg.unit)}
                  />
                </div>
              </AnalyticsCard>
            </div>
          )
        })}
      </div>
    </div>
  )
}
export default AdminTrendsClient
