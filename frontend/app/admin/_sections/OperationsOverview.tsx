'use client'
import React, { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Activity, AlertCircle, AlertTriangle, Bot, CheckCircle2, ChevronDown, CloudDownload, Database, DollarSign, Eraser, ExternalLink, Eye, KeyRound, Layers, Loader2, MapPin, Pencil, Play, Plus, RefreshCw, Save, Settings, Settings2, ShieldCheck, Trash2, TrendingUp, UserPlus, } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import type { ServiceConfig } from '@/types/api'
import { adminFetch, adminJson, client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { buildServiceHref, cn, downloadBlob, formatBytes, formatCount, panelDialogContent, panelDialogFooter, panelDialogHeaderMuted, panelDialogHeaderSolid, } from '@/lib/utils'
import { useBootstrap, useMounted } from '@/hooks/useBootstrap'
import { cronCacheBust, useDerivedState } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { BackToAdminLink } from '@/components/ReportLayout'
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { DeleteDataDialog, TeardownDialog } from '@/components/TeardownDialog'
import { CronSettingsModal } from '@/components/CronSettingsModal/CronSettingsModal'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { InviteAnalystDialog } from '@/components/share-dashboard/InvitationsPanel'
import { Sparkline } from '@/app/dashboard/_sections/DashboardClient'
import { BotSourcesPanel } from '@/app/control-room/_sections/ControlRoomClient'
import { scoringAnalyticsCompositeOptions, scoringConfigCompositeOptions } from '@/components/SessionScoring/ScoringAdminPanels'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
// ── Admin Header Prefetch Links & Operations Overview ──────────────────────
const ADMIN_PREFETCH_LINKS = [ { href: '/admin/share', testId: 'open-share-dialog', label: 'Share Dashboard', icon: UserPlus, key: 'share' },
  { href: '/admin/session-scoring', label: 'Session Scoring', icon: ShieldCheck, key: 'scoring' },
  { href: '/admin/queries', label: 'Live Queries', icon: Activity, key: 'queries' }, { href: '/admin/trends', label: 'Trends', icon: TrendingUp, key: 'trends' },
  { href: '/admin/queue', label: 'Queue', icon: Layers, key: 'queue' }, { href: '/admin/rum', label: 'RUM', icon: Eye, key: 'rum' },
  { href: '/admin/clickhouse', label: 'ClickHouse', icon: Database, key: 'clickhouse', highScaleOnly: true }, ] as const
export function AdminPrefetchLinks({ isHighScale }: { isHighScale?: boolean } = {}) { const queryClient = useQueryClient()
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const services = useServiceStore(s => s.services)
  const activeServiceId = mounted ? storeServiceId : null
  const showClickHouse = isHighScale ?? services.some(s => Boolean(s.is_high_scale))
  const handlePrefetch = (key: (typeof ADMIN_PREFETCH_LINKS)[number]['key']) => { if (key === 'share') { queryClient.prefetchQuery({
        queryKey: queryKeys.admin.shareStatus(), queryFn: async ({ signal }) => unwrapClient(client.GET('/api/admin/share/status', { signal })), })
    } else if (key === 'scoring' && activeServiceId) { queryClient.prefetchQuery(scoringAnalyticsCompositeOptions(activeServiceId, 24))
      queryClient.prefetchQuery(scoringConfigCompositeOptions(activeServiceId))
    } else if (key === 'trends') { queryClient.prefetchQuery({ queryKey: queryKeys.admin.metricHistoryBatch('1h'), queryFn: async ({ signal }) =>
          unwrapClient(client.GET('/api/admin/metric-history/batch', { params: { query: { since: '1h' } }, signal })), })
    } else if (key === 'queue') { queryClient.prefetchQuery({ queryKey: queryKeys.admin.celeryStatus(),
        queryFn: async ({ signal }) => unwrapClient(client.GET('/api/admin/celery/status', { signal })), })
    } else if (key === 'clickhouse' && activeServiceId) { queryClient.prefetchQuery({ queryKey: queryKeys.admin.clickhouseStatus(activeServiceId),
        queryFn: async ({ signal }) => { const { data, response } = await client.GET('/api/admin/clickhouse/status', {
            params: { query: { service_id: activeServiceId } }, signal, })
          if (!response.ok && response.status !== 503) throw new Error(`status ${response.status}`)
          return data }, })
    }
  }
  return ( <>
      {ADMIN_PREFETCH_LINKS.filter(item => !('highScaleOnly' in item && item.highScaleOnly) || showClickHouse).map(item => { const Icon = item.icon
        return ( <Link
            key={item.key}
            href={activeServiceId ? `${item.href}?service=${activeServiceId}` : item.href}
            prefetch={false}
            onMouseEnter={() => handlePrefetch(item.key)}
            data-testid={'testId' in item ? item.testId : undefined}
            className={buttonVariants({ variant: 'secondary', size: 'sm' })} >
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
type OpsOverviewSeed = { queries_summary?: SummaryResponse
  log_accounting?: LogAccountingResponse
  slow_queries_count?: { count: number; since_hours: number; threshold_ms: number }
}
function useOpsOverviewSeed(): { seed: OpsOverviewSeed; seedServiceId: string | null } { const { data } = useBootstrap()
  return { seed: (data as { ops_overview?: OpsOverviewSeed } | undefined)?.ops_overview ?? {},
    seedServiceId: (data as { active_service_id?: string | null } | undefined)?.active_service_id ?? null, }
}
export function OperationsOverview() { return ( <div
      className="grid grid-cols-1 md:grid-cols-4 gap-3 min-h-[108px] [contain-intrinsic-size:auto_108px]"
      data-testid="admin-operations-overview" >
      <LiveQueriesCard />
      <IngestHealthCard />
      <SlowQueriesTeaser />
      <CeleryStatusCard />
    </div>
  )
}
function LiveQueriesCard() { const mounted = useMounted()
  const seed = useOpsOverviewSeed().seed.queries_summary
  const { data: queryData, isLoading: queryIsLoading, isFetching, isError } = useQuery<SummaryResponse>({ queryKey: queryKeys.admin.overviewQueriesSummary(),
    queryFn: async ({ signal }) => adminJson('/api/admin/queries/summary', '/api/admin/queries/summary', { signal }), initialData: seed, retry: false,
    staleTime: POLL_MS, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, })
  const data = mounted ? queryData : seed
  const isLoading = !mounted ? !seed : queryIsLoading
  const active = data?.active_total ?? 0
  return ( <OverviewCard
      href="/admin/queries"
      icon={<Activity className="h-4 w-4" />}
      title="Live Queries"
      primary={isLoading && !data ? '—' : String(active)}
      primaryTone={active > 0 ? 'default' : 'muted'}
      secondary={ isLoading && !data
          ? 'Inspecting background scheduler state...'
          : data?.by_db_type
            ? Object.entries(data.by_db_type)
                .filter(([, n]) => n > 0)
                .map(([db, n]) => `${n} ${db}`)
                .join(' · ') || 'idle'
            : '—'
      }
      isStale={mounted && isError && !!data}
      isRefreshing={mounted && isFetching && !isLoading} />
  )
}
function IngestHealthCard() { const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.log_accounting : undefined
  const { data, isFetching, isLoading, isError } = useQuery<LogAccountingResponse>({ queryKey: queryKeys.admin.overviewLogAccounting(activeServiceId),
    queryFn: async ({ signal }) => adminJson('/api/admin/log-accounting', '/api/admin/log-accounting?hours=24', { signal }), initialData: seed,
    enabled: !!activeServiceId, retry: false, staleTime: 30_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, })
  if (!activeServiceId) { return ( <OverviewCard
        href="/admin/usage-log"
        icon={<Database className="h-4 w-4" />}
        title="Ingest Gap"
        primary="—"
        primaryTone="muted"
        secondary="no service yet" />
    )
  }
  const gapPct = data?.totals?.gap_pct ?? 0
  const sustained = data?.sustained_loss
  const tone: CardTone = isError
    ? 'muted'
    : sustained
      ? 'critical'
      : gapPct >= 0.1
        ? 'warning'
        : gapPct >= 0.02
          ? 'attention'
          : 'default'
  const primary = data?.totals ? `${(gapPct * 100).toFixed(gapPct === 0 ? 0 : 1)}%` : '—'
  const secondary = isError
    ? 'unconfigured or credentials missing'
    : sustained
      ? `sustained: ${sustained.n_buckets} bucket(s), ${formatCount(sustained.total_lost_lines)} lost`
      : gapPct >= 0.02
        ? 'recent loss — check ingest accounting'
        : 'healthy · 24h'
  return ( <OverviewCard
      href="/admin/usage-log"
      icon={tone === 'critical' || tone === 'warning' ? <AlertTriangle className="h-4 w-4" /> : <Database className="h-4 w-4" />}
      title="Ingest Gap"
      primary={primary}
      primaryTone={tone}
      secondary={secondary}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading} />
  )
}
function SlowQueriesTeaser() { const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const { seed: opsSeed, seedServiceId } = useOpsOverviewSeed()
  const seed = activeServiceId && activeServiceId === seedServiceId ? opsSeed.slow_queries_count : undefined
  const { data, isFetching, isLoading, isError } = useQuery<{ count: number }>({ queryKey: queryKeys.admin.overviewSlowQueriesCount(activeServiceId),
    queryFn: async ({ signal }) => adminJson('/api/admin/slow-queries/count', '/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000', { signal }),
    initialData: seed, enabled: !!activeServiceId, retry: false, staleTime: POLL_MS, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, })
  if (!activeServiceId) { return ( <OverviewCard
        href="/admin/queries?view=past&slow=1000"
        icon={<Activity className="h-4 w-4" />}
        title="Notable Slow Queries"
        primary="—"
        primaryTone="muted"
        secondary="no service yet" />
    )
  }
  const slowCount = data?.count ?? 0
  return ( <OverviewCard
      href="/admin/queries?view=past&slow=1000"
      icon={<Activity className="h-4 w-4" />}
      title="Notable Slow Queries"
      primary={String(slowCount)}
      primaryTone={slowCount > 0 ? 'attention' : 'muted'}
      secondary={slowCount === 0 ? 'none ≥ 1s in last 24h' : '≥ 1s in last 24h'}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading} />
  )
}
type CardTone = 'default' | 'muted' | 'attention' | 'warning' | 'critical'
function OverviewCard({ href, icon, title, primary, primaryTone, secondary, isStale = false, isRefreshing = false, }: { href: string
  icon: React.ReactNode
  title: string
  primary: string
  primaryTone: CardTone
  secondary: string
  isStale?: boolean
  isRefreshing?: boolean
}) { const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const primaryClass = primaryTone === 'critical'
      ? 'text-red-600 dark:text-red-400'
      : primaryTone === 'warning'
        ? 'text-orange-600 dark:text-orange-400'
        : primaryTone === 'attention'
          ? 'text-amber-600 dark:text-amber-400'
          : primaryTone === 'muted'
            ? 'text-muted-foreground'
            : 'text-foreground'
  return ( <Link href={buildServiceHref(href, activeServiceId)} prefetch={false} className="block group">
      <Card className={`transition-all group-hover:bg-muted/40 cursor-pointer ${isRefreshing ? 'opacity-40 pointer-events-none' : ''}`}>
        <CardContent className="p-4">
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-2">
            <span className="flex items-center gap-1.5">{icon}{title}</span>
            <div className="flex items-center gap-1.5">
              {isStale && ( <Badge
                  variant="outline"
                  className="text-[10px] border-amber-500/40 text-amber-700 dark:text-amber-400"
                  title="Refetch failed — showing last known value" >
                  stale
                </Badge>
              )}
              <Badge variant="outline" className="text-[10px]">view →</Badge>
            </div>
          </div>
          <div className={`text-3xl font-semibold tabular-nums ${primaryClass}`}>{primary}</div>
          <div className="text-xs text-muted-foreground mt-1 truncate">{secondary}</div>
        </CardContent>
      </Card>
    </Link>
  )
}
function CeleryStatusCard() { const mounted = useMounted()
  const { data: queryData, isFetching, isLoading, isError } = useQuery({ queryKey: queryKeys.admin.celeryStatus(),
    queryFn: async ({ signal }) => adminJson<'/api/admin/celery/status'>('/api/admin/celery/status', '/api/admin/celery/status', { signal }), retry: false,
    staleTime: POLL_MS, refetchInterval: POLL_MS, })
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
  let secondaryText = isStandardMode
    ? `${activeRuns} active · ${formatCount(ingestedFiles)} ingested · in-process`
    : `ingest: ${ingestQ} · control: ${controlQ}`
  if (deadLetters > 0) {
    primaryText = `${deadLetters} dead letters`
    primaryTone = 'critical'
    secondaryText = isStandardMode ? `${schedCount} scheduled · in-process` : `${workers} workers · ingest: ${ingestQ}`
  } else if (quarantined > 0) {
    primaryText = `${quarantined} quarantined`
    primaryTone = 'attention'
    secondaryText = isStandardMode ? `${schedCount} scheduled · in-process` : `${workers} workers · ingest: ${ingestQ}`
  }
  return ( <OverviewCard
      href="/admin/queue"
      icon={<Activity className="h-4 w-4" />}
      title="Workers & Queues"
      primary={primaryText}
      primaryTone={primaryTone}
      secondary={secondaryText}
      isStale={isError && !!data}
      isRefreshing={isFetching && !isLoading} />
  )
}
// ── Operational Trends (/admin/trends) ─────────────────────────────────────
export type TrendWindow = '1h' | '6h' | '24h' | '7d'
export const WINDOWS: { label: string; value: TrendWindow }[] = [ { label: 'Last hour', value: '1h' }, { label: 'Last 6h', value: '6h' },
  { label: 'Last 24h', value: '24h' }, { label: 'Last 7 days', value: '7d' }, ]
export const SERIES_LAYOUT: { title: string
  description: string
  metric: string
  unit: string
  yDomain?: [number | 'auto', number | 'auto']
  aggregateBy?: 'max-per-ts'
}[] = [ { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m', unit: '' },
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
  for (const k of keys) { for (const p of series[k] ?? []) { const prev = byTs.get(p.ts) ?? 0
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
export function AdminTrendsClient() { const [window, setWindow] = React.useState<TrendWindow>('1h')
  const { data: trends, isLoading, isFetching, error, refetch } = useQuery({ queryKey: queryKeys.admin.metricHistoryBatch(window), queryFn: async ({ signal }) =>
      (await unwrapClient( client.GET('/api/admin/metric-history/batch', { params: { query: { since: window } }, signal }), )) as TrendBatch,
    refetchInterval: 60_000, refetchIntervalInBackground: false, staleTime: 60_000, })
  const series = trends?.series ?? {}
  const isRefreshing = isFetching && !isLoading
  return ( <div data-testid="admin-trends-root" className="container mx-auto p-4 space-y-4">
      <PageHeader title="Operational Trends" description="Sampled every 60 s. Retained 30 days. Backed by PostgreSQL metric_snapshots." icon={TrendingUp} >
        <div className="flex items-center gap-2">
          <Button type="button" variant="outline" size="sm" data-testid="admin-trends-refresh-btn" onClick={() => void refetch()} disabled={isFetching} >
            <RefreshCw className={cn('mr-1.5 h-3.5 w-3.5', isRefreshing && 'animate-spin')} />
            Refresh
          </Button>
          <BackToAdminLink variant="ghost" prefetch={false} />
        </div>
      </PageHeader>
      <div data-testid="admin-trends-toolbar" className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        {WINDOWS.map((w) => ( <Button
            key={w.value}
            type="button"
            data-testid={`admin-trends-window-${w.value}`}
            aria-pressed={window === w.value}
            variant={window === w.value ? 'default' : 'outline'}
            size="sm"
            onClick={() => setWindow(w.value)} >
            {w.label}
          </Button>
        ))}
      </div>
      <div data-testid="admin-trends-grid-reservation" className={cn(
          'grid grid-cols-1 lg:grid-cols-2 gap-4 min-h-[680px] [contain-intrinsic-size:auto_680px] [content-visibility:auto]',
          isRefreshing && 'opacity-40 pointer-events-none transition-opacity', )} >
        {SERIES_LAYOUT.map((cfg) => { const points = cfg.aggregateBy === 'max-per-ts' ? maxAcrossServicesPerTs(series, cfg.metric) : series[cfg.metric] ?? []
          const latest = points.length ? points[points.length - 1].value : null
          return (
            <div key={cfg.metric} data-testid={`admin-trends-card-${cfg.metric}`}>
              <AnalyticsCard title={cfg.title} description={cfg.description} isLoading={isLoading} isFetching={isFetching}
                loadingMessage="Loading system vital trends..."
                error={error as AnalyticsCardError | null}
                isEmpty={!isLoading && !error && points.length === 0} >
                <div className="flex items-baseline justify-between mb-2">
                  <div data-testid={`admin-trends-latest-${cfg.metric}`} className="text-2xl font-semibold tabular-nums">
                    {latest === null ? '–' : formatTrendValue(latest, cfg.unit)}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {points.length > 0 ? `${points.length} samples` : isLoading ? 'loading…' : 'no samples yet'}
                  </div>
                </div>
                <div className={cn('text-foreground/70', latest === null && 'opacity-40')}>
                  <Sparkline points={points} yDomain={cfg.yDomain} height={120} label={cfg.title} formatValue={(v) => formatTrendValue(v, cfg.unit)} />
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
// ── Global Settings, Diagnostics & Pricing ─────────────────────────────────
type PopLocation = components['schemas']['PopLocation']
function popCoord(pop: PopLocation, axis: 'latitude' | 'longitude'): number | null { const v = pop[axis] ?? pop.coordinates?.[axis] ?? pop.attributes?.[axis]
  return typeof v === 'number' ? v : null
}
function PopLocationsModal({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) { const [apiKey, setApiKey] = useState('')
  const queryClient = useQueryClient()
  const { data, isLoading } = useQuery({ queryKey: ['pop-locations'], queryFn: async () => unwrapClient(client.GET('/api/admin/pop-locations')), enabled: open, })
  const mutation = useMutation({ mutationFn: async () => unwrapClient(client.POST('/api/admin/pop-locations/refresh', { body: { token: apiKey } })),
    onSuccess: (result) => { queryClient.setQueryData(['pop-locations'], { pops: result?.pops || [] })
      setApiKey('') }, })
  const pops = data?.pops || []
  const withCoords = pops.filter(p => popCoord(p, 'latitude') != null)
  return ( <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn('sm:max-w-2xl', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <DialogTitle className="flex items-center gap-2">
            <MapPin className="h-4 w-4" />
            POP Location Data
          </DialogTitle>
          <p className="text-sm text-muted-foreground mt-1">
            POP coordinates power the Impossible Distance insight — they detect when a client&apos;s
            claimed geo location is physically impossible given their TCP RTT. Refresh when
            Fastly adds new POPs.
          </p>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">Cached POPs</span>
            <div className="flex items-center gap-2">
              <Badge variant={withCoords.length > 0 ? 'success' : 'secondary'} className="text-[10px] px-1.5">
                {withCoords.length} with coordinates
              </Badge>
              {pops.length > 0 && <Badge variant="outline" className="text-[10px] px-1.5">{pops.length} total</Badge>}
            </div>
          </div>
          {isLoading ? ( <div className="h-48 flex items-center justify-center text-muted-foreground text-sm animate-pulse border rounded-lg">
              Loading...
            </div>
          ) : pops.length === 0 ? ( <div className="h-48 flex flex-col items-center justify-center gap-2 border rounded-lg bg-muted/20">
              <AlertCircle className="h-5 w-5 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No POP data cached yet.</p>
              <p className="text-xs text-muted-foreground">Enter your Fastly API key below to fetch it.</p>
            </div>
          ) : ( <ScrollArea className="h-52 border rounded-lg">
              <div className="divide-y">
                {pops.map((pop) => { const code = pop.code || '?'
                  const lat = popCoord(pop, 'latitude')
                  const lon = popCoord(pop, 'longitude')
                  const hasGeo = lat != null && lon != null
                  return ( <div key={code} className="flex items-center gap-3 px-3 py-1.5 hover:bg-muted/30">
                      <span className="font-mono text-xs font-bold w-10 shrink-0 text-foreground">{code}</span>
                      <span className="text-xs text-muted-foreground flex-1 truncate">{pop.name}</span>
                      {hasGeo ? ( <span className="text-[10px] font-mono text-muted-foreground/70 shrink-0 tabular-nums">
                          {lat?.toFixed(2)}, {lon?.toFixed(2)}
                        </span>
                      ) : ( <span className="text-[10px] text-destructive/70 shrink-0">no coords</span>
                      )}
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
          )}
          <div className="space-y-2 pt-2 border-t">
            <Label htmlFor="pop-api-key" className="text-sm font-medium">Update from Fastly API</Label>
            <p className="text-xs text-muted-foreground">
              A read-only Fastly API token is sufficient. The key is used once to call{' '}
              <code className="font-mono">/datacenters</code> and is not stored.
            </p>
            <div className="flex gap-2">
              <input id="pop-api-key" type="password" placeholder="Fastly API key..." value={apiKey} onChange={e => setApiKey(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && apiKey.trim() && !mutation.isPending && mutation.mutate()}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
              />
              <Button onClick={() => mutation.mutate()} disabled={!apiKey.trim() || mutation.isPending} className="shrink-0">
                <RefreshCw className={`h-4 w-4 mr-2 ${mutation.isPending ? 'animate-spin' : ''}`} />
                {mutation.isPending ? 'Fetching...' : 'Update'}
              </Button>
            </div>
            {mutation.isSuccess && ( <div className="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
                <CheckCircle2 className="h-4 w-4" />
                Updated — {withCoords.length} POPs with coordinates cached.
              </div>
            )}
            {mutation.isError && ( <div className="flex items-center gap-2 text-sm text-destructive">
                <AlertCircle className="h-4 w-4" />
                {mutation.error?.message || 'Update failed. Check your API key.'}
              </div>
            )}
          </div>
        </div>
        <DialogFooter className={panelDialogFooter} />
      </DialogContent>
    </Dialog>
  )
}
export function DiagnosticsPanel() { const queryClient = useQueryClient()
  const { data: settings, isLoading: isSettingsLoading } = useQuery({ queryKey: queryKeys.admin.debugSettings(), queryFn: async () => {
      const { data, error } = await client.GET('/api/admin/debug-settings')
      if (error) throw new Error(extractApiError(error) || 'Failed to fetch debug settings')
      return data }, })
  const updateSettingsMutation = useMutation({ mutationFn: async (body: { query_debug_visibility?: string; api_call_debug_visibility?: string }) => {
      const { data, error } = await client.PATCH('/api/admin/debug-settings', { body })
      if (error) throw new Error(extractApiError(error) || 'Failed to update debug settings')
      return data }, onSuccess: () => { queryClient.invalidateQueries({ queryKey: queryKeys.admin.debugSettings() })
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() }) }, })
  const queryVisibility = settings?.query_debug_visibility ?? 'disabled'
  const apiCallVisibility = settings?.api_call_debug_visibility ?? 'disabled'
  const isUpdating = updateSettingsMutation.isPending
  const { data: bootstrapData } = useBootstrap()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const services = useServiceStore((s) => s.services)
  const activeService = services.find((s) => s.id === activeServiceId) ?? services[0]
  const queryEngineLabel = activeService?.is_high_scale ? 'ClickHouse' : 'DuckDB'
  const debugState = (bootstrapData as { debug_state?: { debug_responses_enabled?: boolean } } | undefined)?.debug_state
  const debugBackendOn = debugState?.debug_responses_enabled !== false
  const debugDisabledTooltip = !debugBackendOn
    ? 'Backend debug responses are disabled — set DEBUG_RESPONSES=true in the server env (or .env file) and restart to see data here.'
    : undefined
  const panels = [ { id: 'diag-query-debug-label', title: 'Query debugging panel',
      desc: `Bottom-of-screen panel with ${queryEngineLabel} SQL queries and execution times.`,
      value: queryVisibility, onChange: (val: string | null) => val && updateSettingsMutation.mutate({ query_debug_visibility: val }), }, {
      id: 'diag-api-panel-label', title: 'API call panel', desc: 'Bottom-of-screen panel with all Fastly API calls and FOS operations per request.',
      value: apiCallVisibility, onChange: (val: string | null) => val && updateSettingsMutation.mutate({ api_call_debug_visibility: val }), }, ]
  return ( <>
      {panels.map((p) => (
        <div key={p.id} className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
          <div className="min-w-0 space-y-0.5">
            <Label id={p.id} className="text-sm font-medium">{p.title}</Label>
            <p className="text-xs text-muted-foreground">{p.desc}</p>
            {!debugBackendOn && ( <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>
                Disabled — backend ``DEBUG_RESPONSES`` env is off.
              </p>
            )}
          </div>
          <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
            <Select value={p.value} onValueChange={p.onChange} disabled={!debugBackendOn || isSettingsLoading || isUpdating} >
              <SelectTrigger className="h-8 text-xs w-[180px]">
                <SelectValue placeholder="Select visibility..." />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="disabled" className="text-xs">Off / Disabled</SelectItem>
                <SelectItem value="admins" className="text-xs">Admins Only</SelectItem>
                <SelectItem value="analysts" className="text-xs">Analysts Only</SelectItem>
                <SelectItem value="both" className="text-xs">Both Admins & Analysts</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      ))}
    </>
  )
}
function UsageLogRetentionInput({ initial, onSave }: { initial: number; onSave: (days: number) => void }) {
  const [value, setValue] = useDerivedState(String(initial))
  return ( <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span>Keep for</span>
      <Input type="number" min={1} aria-label="Usage-log retention in days" className="h-7 w-14 text-xs text-right" value={value}
        onChange={(e) => setValue(e.target.value)}
        onBlur={() => { const n = parseInt(value)
          if (Number.isFinite(n) && n >= 1) onSave(n)
          else setValue(String(initial))
        }}
        onKeyDown={(e) => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur() }} />
      <span>days</span>
    </div>
  )
}
const _PRICING_DEFAULTS = { class_a_rate_per_1k: 0.005, class_b_rate_per_10k: 0.01, cdn_egress_rate_per_gb: 0.12, storage_rate_per_gb_month: 0.02,
  min_billed_days: 30, }
export const PricingSettings = () => { const queryClient = useQueryClient()
  const mounted = useMounted()
  const [editing, setEditing] = useState(false)
  const { data: settings, isLoading } = useQuery({ queryKey: ['usage-logging-settings'],
    queryFn: async () => (await unwrapClient(client.GET('/api/admin/usage-logging'))) ?? null, })
  const serverForm = useMemo( () => ({ rateA: String(settings?.class_a_rate_per_1k ?? _PRICING_DEFAULTS.class_a_rate_per_1k),
      rateB: String(settings?.class_b_rate_per_10k ?? _PRICING_DEFAULTS.class_b_rate_per_10k),
      rateCdn: String(settings?.cdn_egress_rate_per_gb ?? _PRICING_DEFAULTS.cdn_egress_rate_per_gb),
      rateStorage: String(settings?.storage_rate_per_gb_month ?? _PRICING_DEFAULTS.storage_rate_per_gb_month),
      minBilledDays: String(settings?.min_billed_days ?? _PRICING_DEFAULTS.min_billed_days), }), [settings], )
  const [form, setForm] = useDerivedState(serverForm)
  const { rateA, rateB, rateCdn, rateStorage, minBilledDays } = form
  const fieldSetter = (field: keyof typeof form) => (value: string) => setForm((f) => ({ ...f, [field]: value }))
  const saveMutation = useMutation({ mutationFn: async () => { await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: {
          class_a_rate_per_1k: parseFloat(rateA), class_b_rate_per_10k: parseFloat(rateB), cdn_egress_rate_per_gb: parseFloat(rateCdn),
          storage_rate_per_gb_month: parseFloat(rateStorage), min_billed_days: parseInt(minBilledDays), }, })) }, onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
      queryClient.invalidateQueries({ queryKey: ['usage'] })
      queryClient.invalidateQueries({ queryKey: ['usage-log'] })
      setEditing(false) }, })
  const saving = saveMutation.isPending
  if (!mounted || isLoading) return <AnalyticsCard title="FOS Pricing Defaults" isLoading>{null}</AnalyticsCard>
  const fields = [ { label: 'Class A Ops ($/1k)', value: rateA, setValue: fieldSetter('rateA') },
    { label: 'Class B Ops ($/10k)', value: rateB, setValue: fieldSetter('rateB') }, { label: 'CDN Egress ($/GB)', value: rateCdn, setValue: fieldSetter('rateCdn') },
    { label: 'Storage ($/GB/mo)', value: rateStorage, setValue: fieldSetter('rateStorage') },
    { label: 'Min. Days Billed/Object', value: minBilledDays, setValue: fieldSetter('minBilledDays') }, ]
  return ( <AnalyticsCard
      title="Pricing & Retention Defaults"
      description="Global rates used for cost estimation across all services. Changes apply to all historical views."
      icon={<DollarSign className="h-4 w-4" />}
      headerAction={ !editing ? ( <Button size="sm" variant="outline" onClick={() => setEditing(true)} className="h-8 font-bold uppercase tracking-tight">
            <Pencil className="h-3 w-3 mr-1.5" />
            Edit
          </Button>
        ) : null
      } >
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {fields.map((f) => {
          const inputId = `gs-${f.label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
          return ( <div key={f.label} className="space-y-1.5">
              <Label htmlFor={inputId} className="text-[10px] uppercase font-bold text-muted-foreground">{f.label}</Label>
              {editing ? ( <Input id={inputId} className="h-8 font-mono text-xs" value={f.value} onChange={(e) => f.setValue(e.target.value)} />
              ) : ( <div className="h-8 flex items-center font-mono text-xs px-3 rounded-md bg-muted/40 border border-transparent">
                  {f.value}
                </div>
              )}
            </div>
          )
        })}
      </div>
      {editing && ( <div className="flex justify-end gap-2 mt-4">
          <Button size="sm" variant="outline" onClick={() => { if (settings) setForm(serverForm); setEditing(false) }} disabled={saving}
            className="h-8 font-bold uppercase tracking-tight" >
            Cancel
          </Button>
          <Button size="sm" onClick={() => saveMutation.mutate()} disabled={saving} className="h-8 font-bold uppercase tracking-tight">
            {saving ? <Loader2 className="h-3 w-3 mr-1.5 animate-spin" /> : <Save className="h-3 w-3 mr-1.5" />}
            Save Changes
          </Button>
        </div>
      )}
    </AnalyticsCard>
  )
}
export function GlobalSettings() { const queryClient = useQueryClient()
  const router = useRouter()
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const [popLocationsOpen, setPopLocationsOpen] = useState(false)
  const { data: queryUsageLoggingSettings } = useQuery({ queryKey: ['usage-logging-settings'],
    queryFn: async () => unwrapClient(client.GET('/api/admin/usage-logging')), staleTime: 60_000, })
  const usageLoggingSettings = mounted ? queryUsageLoggingSettings : undefined
  const usageLoggingEnabled = usageLoggingSettings?.enabled ?? false
  const usageLogRetention = usageLoggingSettings?.retention_days ?? 30
  const retentionMutation = useMutation({ mutationFn: async (days: number) => {
      await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { retention_days: days } })) }, onMutate: (days) => {
      queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, retention_days: days })) }, onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] }) }, })
  const toggleMutation = useMutation({ mutationFn: async (enabled: boolean) => { await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { enabled } }))
    }, onMutate: (enabled) => { queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, enabled })) },
    onSettled: () => { queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] }) }, })
  return ( <>
      <AnalyticsCard title="Overall Settings" description="Global preferences for the application.">
        <div className="flex flex-col gap-3">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <DiagnosticsPanel />
            <div className="flex flex-col p-3 border rounded-lg gap-3">
              <div className="min-w-0 space-y-0.5">
                <Label id="gs-usage-log-label" className="text-sm font-medium">Log FOS / CDN usage</Label>
                <p className="text-xs text-muted-foreground">
                  Records every Class A/B operation and CDN download with function + process context for cost analysis.
                </p>
              </div>
              <div className="flex items-center justify-end gap-2 flex-wrap mt-auto">
                {usageLoggingEnabled && ( <>
                    <UsageLogRetentionInput initial={usageLogRetention} onSave={(days) => {
                        if (Number.isFinite(days) && days >= 1 && days !== usageLogRetention) retentionMutation.mutate(days)
                      }} />
                    <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => router.push(buildServiceHref('/admin/usage-log', activeServiceId))} >
                      View Usage Log
                    </Button>
                  </>
                )}
                <Switch aria-labelledby="gs-usage-log-label" checked={usageLoggingEnabled} onCheckedChange={(enabled) => toggleMutation.mutate(enabled)}
                  disabled={toggleMutation.isPending} />
              </div>
            </div>
            <div className="flex flex-col p-3 border rounded-lg gap-3">
              <div className="min-w-0 space-y-0.5">
                <Label className="text-sm font-medium">POP location data</Label>
                <p className="text-xs text-muted-foreground">
                  Fastly PoP coordinates used by the Impossible Distance insight for geo/RTT spoofing detection.
                </p>
              </div>
              <div className="flex items-center justify-end mt-auto">
                <Button variant="outline" size="sm" onClick={() => setPopLocationsOpen(true)}>
                  <MapPin className="h-3.5 w-3.5 mr-1.5" /> Update POP Info
                </Button>
              </div>
            </div>
          </div>
          <BotSourcesPanel />
        </div>
      </AnalyticsCard>
      <PopLocationsModal open={popLocationsOpen} onOpenChange={setPopLocationsOpen} />
    </>
  )
}
// ── Service Management Table & Dialogs ─────────────────────────────────────
interface ServiceColumnDeps { activeServiceId: string | null
  switchToService: (id: string) => void
  servicesLength: number
  setCronService: (s: ServiceConfig) => void
  setSettingsService: (s: ServiceConfig) => void
  setDeleteDataService: (s: ServiceConfig) => void
  setTeardownService: (s: ServiceConfig) => void
  setInviteService: (s: ServiceConfig) => void
  openNgwaf: (s: ServiceConfig) => void
  openRum?: (s: ServiceConfig) => void
  openCredentials: (s: ServiceConfig) => void
}
function MonoExternalCell({ text, href, title }: { text: string; href: string; title: string }) { return ( <div className="flex items-center gap-1.5">
      <span className="font-mono text-xs text-muted-foreground">{text}</span>
      <a href={href} target="_blank" rel="noreferrer" className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
        title={title} >
        <ExternalLink className="h-3 w-3" />
      </a>
    </div>
  )
}
function buildServiceColumns(deps: ServiceColumnDeps): ColumnDef<ServiceConfig>[] { const { activeServiceId, switchToService, servicesLength, setCronService,
    setSettingsService, setDeleteDataService, setTeardownService, setInviteService, openNgwaf, openRum, openCredentials, } = deps
  return [ { accessorKey: 'name', header: 'Service Name', size: 200, cell: ({ row }) => ( <div className="flex items-center gap-2 font-medium">
          {row.original.service_id === activeServiceId && (
            <Badge variant="default" className="h-5 px-1.5 text-[10px] uppercase font-bold bg-blue-600 hover:bg-blue-700 shadow-none border-none">Active</Badge>
          )}
          {row.getValue('name')}
        </div>
      ), }, { accessorKey: 'service_id', header: 'ID', size: 160,
      cell: ({ row }) => {
        const id = row.getValue('service_id') as string
        return <MonoExternalCell text={id} href={`https://manage.fastly.com/configure/services/${id}`} title="View Service in Fastly" />
      },
    },
    {
      accessorKey: 'fos_bucket',
      header: 'FOS Bucket',
      size: 180,
      cell: ({ row }) => (
        <MonoExternalCell
          text={row.getValue('fos_bucket') as string}
          href="https://manage.fastly.com/resources/object-storage/buckets"
          title="View Object Storage in Fastly"
        />
      ),
    },
    {
      id: 'local_cache',
      accessorFn: (row) => row.duckdb_size_bytes,
      header: 'Local Cache',
      size: 180,
      cell: ({ row }) => {
        const size = row.original.duckdb_size_bytes
        const files = row.original.cache_file_count || 0
        const rows = row.original.log_row_count || 0
        if (!size) return <span className="text-xs text-muted-foreground italic">No cache</span>
        return (
          <div className="flex flex-col gap-0.5">
            <span className="font-mono text-xs tabular-nums text-muted-foreground">{formatBytes(size)}</span>
            <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
              <span className="flex items-center gap-1">
                {formatCount(files)} file{files === 1 ? '' : 's'}
                {files > 0 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label="Download local cache as ZIP"
                    className="h-4 w-4 p-0 ml-0.5 hover:bg-transparent text-muted-foreground hover:text-primary transition-colors"
                    onClick={(e) => {
                      e.stopPropagation()
                      const sid = row.original.service_id
                      void adminFetch(`/api/download-all?service_id=${encodeURIComponent(sid)}&include=local`).then(
                        async (res) => { if (res.ok) downloadBlob(await res.blob(), `${sid}_local_cache.zip`) },
                      )
                    }}
                    title="Download local cache as ZIP"
                  >
                    <CloudDownload className="h-3 w-3" />
                  </Button>
                )}
              </span>
              <span>•</span>
              <span>{formatCount(rows)} rows</span>
            </div>
          </div>
        )
      },
    },
    {
      id: 'status',
      accessorFn: (row) => (row.cron_sync?.enabled ? 1 : 0),
      header: 'Cron Sync',
      size: 140,
      cell: ({ row }) => {
        const service = row.original
        return (
          <div className="flex items-center gap-2">
            {service.cron_sync?.enabled ? (
              <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Enabled</Badge>
            ) : (
              <Badge variant="secondary" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold">Disabled</Badge>
            )}
            <Button
              variant="ghost"
              size="icon"
              aria-label="Cron sync settings"
              className="h-6 w-6 hover:bg-muted cursor-pointer"
              onClick={() => setCronService(service)}
              title="Cron Sync Settings"
            >
              <Settings className="h-3 w-3 text-muted-foreground" />
            </Button>
          </div>
        )
      },
    },
    {
      id: 'actions',
      header: 'Actions',
      size: servicesLength > 0 ? 220 : 120,
      cell: ({ row }) => {
        const service = row.original
        const isActive = service.service_id === activeServiceId
        return (
          <div className="flex items-center gap-2">
            <DropdownMenu>
              <DropdownMenuTrigger render={
                <Button variant="outline" size="sm" className="h-8 gap-1.5 px-3 font-bold uppercase text-[10px] tracking-wider">
                  Manage <ChevronDown className="h-3.5 w-3.5" />
                </Button>
              } />
              <DropdownMenuContent align="end" className="w-52">
                {isActive && (
                  <DropdownMenuItem disabled>
                    <Play className="mr-2 h-4 w-4" /> Active Service
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem onClick={() => setSettingsService(service)}>
                  <Settings2 className="mr-2 h-4 w-4" /> Log Settings
                </DropdownMenuItem>
                {openRum && (
                  <DropdownMenuItem onClick={() => openRum(service)}>
                    <Eye className="mr-2 h-4 w-4" /> RUM Configuration
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem onClick={() => openNgwaf(service)}>
                  <Bot className="mr-2 h-4 w-4" /> NGWAF Config
                </DropdownMenuItem>
                {service.access_level === 'read_write' && service.analyst_path_a_supported && (
                  <DropdownMenuItem onClick={() => setInviteService(service)}>
                    <UserPlus className="mr-2 h-4 w-4" /> Invite User
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem onClick={() => openCredentials(service)}>
                  <KeyRound className="mr-2 h-4 w-4" /> Rotate Key
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                {service.access_level === 'read_write' && (
                  <DropdownMenuItem
                    className="text-destructive focus:text-destructive focus:bg-destructive/10"
                    onClick={() => setDeleteDataService(service)}
                  >
                    <Eraser className="mr-2 h-4 w-4" /> Delete Data
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem
                  className="text-destructive focus:text-destructive focus:bg-destructive/10"
                  onClick={() => setTeardownService(service)}
                >
                  <Trash2 className="mr-2 h-4 w-4" /> Teardown Service
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            {!isActive && (
              <Button
                variant="default"
                size="sm"
                className="h-8 px-3 text-[11px] font-bold uppercase tracking-tight bg-primary hover:bg-primary/90"
                onClick={() => switchToService(service.service_id)}
              >
                <Play className="h-3 w-3 mr-1.5 fill-current" /> Switch to
              </Button>
            )}
          </div>
        )
      },
    },
  ]
}
interface CredentialsDialogProps {
  service: ServiceConfig | null
  onClose: () => void
  initialMode: 'token' | 'manual'
}
function CredentialsDialog(props: CredentialsDialogProps) {
  return <CredentialsDialogBody key={`${props.service?.service_id ?? ''}|${props.initialMode}`} {...props} />
}
function CredentialsDialogBody({ service, onClose, initialMode }: CredentialsDialogProps) {
  const [credMode, setCredMode] = useState<'token' | 'manual'>(initialMode)
  const [credApiToken, setCredApiToken] = useState('')
  const [credAccessKey, setCredAccessKey] = useState('')
  const [credSecretKey, setCredSecretKey] = useState('')
  const credentialsMutation = useMutation({
    mutationFn: async ({ service_id, payload }: { service_id: string; payload: { api_token: string } | { access_key: string; secret_key: string } }) =>
      unwrapClient(client.PATCH('/api/services/{service_id}/credentials', {
        params: { path: { service_id } },
        body: { api_token: '', access_key: '', secret_key: '', ...payload },
      })),
    onSuccess: () => onClose(),
  })
  function handleClose() {
    onClose()
    credentialsMutation.reset()
  }
  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) handleClose() }}>
      <DialogContent className={cn('sm:max-w-md', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle>Rotate FOS Credentials</DialogTitle>
          <DialogDescription>
            Replace the Fastly Object Storage access key for <strong>{service?.name}</strong>.
            {service?.access_level === 'read_write'
              ? ' Use your Fastly API token to auto-generate a new key, or enter one manually.'
              : ' Enter the new key credentials manually.'}
          </DialogDescription>
        </DialogHeader>
        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {service?.access_level === 'read_write' && (
            <div className="flex rounded-md border overflow-hidden text-xs font-semibold">
              {(['token', 'manual'] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  className={`flex-1 py-1.5 transition-colors ${credMode === m ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`}
                  onClick={() => { setCredMode(m); credentialsMutation.reset() }}
                >
                  {m === 'token' ? 'Auto (API Token)' : 'Manual'}
                </button>
              ))}
            </div>
          )}
          {credMode === 'token' && service?.access_level === 'read_write' && (
            <div className="space-y-1.5">
              <Label htmlFor="cred-api-token" className="text-sm">Fastly API Token</Label>
              <p className="text-xs text-muted-foreground">
                A new <code>read-write-objects</code> FOS key will be created for this bucket. The old key will be deleted automatically.
              </p>
              <Input
                id="cred-api-token"
                type="password"
                placeholder="Fastly API token"
                value={credApiToken}
                onChange={(e) => setCredApiToken(e.target.value)}
                className="font-mono text-sm"
              />
            </div>
          )}
          {(credMode === 'manual' || service?.access_level !== 'read_write') && (
            <>
              <div className="space-y-1.5">
                <Label htmlFor="cred-access-key" className="text-sm">Access Key ID</Label>
                <Input
                  id="cred-access-key"
                  placeholder="FOS access key ID"
                  value={credAccessKey}
                  onChange={(e) => setCredAccessKey(e.target.value)}
                  className="font-mono text-sm"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="cred-secret-key" className="text-sm">Secret Access Key</Label>
                <Input
                  id="cred-secret-key"
                  type="password"
                  placeholder="FOS secret access key"
                  value={credSecretKey}
                  onChange={(e) => setCredSecretKey(e.target.value)}
                  className="font-mono text-sm"
                />
              </div>
            </>
          )}
          {credentialsMutation.isError && (
            <p className="text-sm text-destructive">{credentialsMutation.error?.message ?? 'Failed to update credentials.'}</p>
          )}
        </div>
        <DialogFooter className={panelDialogFooter}>
          <Button variant="outline" onClick={handleClose}>Cancel</Button>
          <Button
            disabled={credentialsMutation.isPending || (credMode === 'token' ? !credApiToken : !credAccessKey || !credSecretKey)}
            onClick={() => {
              if (!service) return
              const payload = credMode === 'token' ? { api_token: credApiToken } : { access_key: credAccessKey, secret_key: credSecretKey }
              credentialsMutation.mutate({ service_id: service.service_id, payload })
            }}
          >
            {credentialsMutation.isPending
              ? (credMode === 'token' ? 'Creating key…' : 'Validating…')
              : (credMode === 'token' ? 'Rotate Key' : 'Save Credentials')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
interface NgwafDialogProps {
  service: ServiceConfig | null
  onClose: () => void
}
function NgwafDialog(props: NgwafDialogProps) {
  return <NgwafDialogBody key={props.service?.service_id ?? ''} {...props} />
}
function NgwafDialogBody({ service, onClose }: NgwafDialogProps) {
  const queryClient = useQueryClient()
  const [ngwafWorkspaceId, setNgwafWorkspaceId] = useState(service?.ngwaf_workspace_id || '')
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetchError, setNgwafFetchError] = useState('')
  const [ngwafSaved, setNgwafSaved] = useState(false)
  const [ngwafApiToken, setNgwafApiToken] = useState('')
  const fetchWorkspacesMutation = useMutation({
    mutationFn: async ({ serviceId, token }: { serviceId: string; token: string }) => {
      const data = await unwrapClient(client.GET('/api/provision/ngwaf-workspaces', {
        params: { query: { service_id: serviceId } },
        headers: { Authorization: `Bearer ${token}` },
      }))
      return (data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : []))
    },
    onMutate: () => {
      setNgwafWorkspaces([])
      setNgwafFetchError('')
    },
    onSuccess: (workspaces) => setNgwafWorkspaces(workspaces),
    onError: (e) => setNgwafFetchError(extractApiError(e) || 'Could not load workspaces'),
  })
  const saveWorkspaceMutation = useMutation({
    mutationFn: async ({ serviceId, token, workspaceId }: { serviceId: string; token: string; workspaceId: string | null }) =>
      unwrapClient(client.PATCH('/api/provision/services/{service_id}/ngwaf-workspace', {
        params: { path: { service_id: serviceId } },
        headers: { Authorization: `Bearer ${token}` },
        body: { ngwaf_workspace_id: workspaceId },
      })),
    onSuccess: () => {
      setNgwafSaved(true)
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    },
    onError: (e) => setNgwafFetchError(extractApiError(e) || 'Failed to save'),
  })
  const ngwafFetching = fetchWorkspacesMutation.isPending
  const ngwafSaving = saveWorkspaceMutation.isPending
  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) onClose() }}>
      <DialogContent className={cn('sm:max-w-md', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2">
            <Bot className="h-5 w-5 text-primary" />
            NGWAF Bot Enrichment
          </DialogTitle>
          <DialogDescription>
            Set the NGWAF workspace for <strong>{service?.name}</strong>. When configured, the bot sync cron will enrich log data with specific bot names from Fastly NGWAF.
          </DialogDescription>
        </DialogHeader>
        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {service && !ngwafSaved && (
            <div className="space-y-1">
              <Label htmlFor="ngwaf-api-token" className="text-xs font-semibold">Fastly API token</Label>
              <p className="text-[10px] text-muted-foreground">
                Required to list AND save NGWAF workspace bindings (security /).
              </p>
              <div className="flex gap-2">
                <Input
                  id="ngwaf-api-token"
                  type="password"
                  placeholder="Fastly API token"
                  value={ngwafApiToken}
                  onChange={(e) => setNgwafApiToken(e.target.value)}
                  className="h-8 font-mono text-xs flex-1"
                  autoComplete="off"
                />
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!ngwafApiToken.trim() || ngwafFetching}
                  onClick={() => {
                    if (service) fetchWorkspacesMutation.mutate({ serviceId: service.service_id, token: ngwafApiToken })
                  }}
                  className="h-8 text-xs"
                >
                  {ngwafFetching ? 'Loading…' : 'Load'}
                </Button>
              </div>
            </div>
          )}
          {ngwafFetching ? (
            <p className="text-xs text-muted-foreground animate-pulse">Loading workspaces…</p>
          ) : ngwafWorkspaces.length > 0 ? (
            <div className="space-y-1">
              <Label className="text-xs font-semibold">Select workspace</Label>
              <Select value={ngwafWorkspaceId} onValueChange={(v) => setNgwafWorkspaceId(v ?? '')}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="Choose a workspace…" />
                </SelectTrigger>
                <SelectContent>
                  {ngwafWorkspaces.map((w) => (
                    <SelectItem key={w.id} value={w.id} className="text-xs">{w.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          ) : ngwafFetchError ? (
            <p className="text-xs text-destructive">{ngwafFetchError}</p>
          ) : null}
          {ngwafSaved && (
            <p className="text-xs text-green-600 font-medium">Workspace saved. The NGWAF sync cron will start on the next scheduler tick.</p>
          )}
        </div>
        <DialogFooter className={panelDialogFooter}>
          {ngwafSaved ? (
            <Button size="sm" onClick={onClose}>Close</Button>
          ) : (
            <>
              <Button variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button
                size="sm"
                disabled={ngwafSaving || !ngwafApiToken.trim()}
                title={!ngwafApiToken.trim() ? 'Enter your Fastly API token to save' : undefined}
                onClick={() => {
                  if (!service) return
                  saveWorkspaceMutation.mutate({
                    serviceId: service.service_id,
                    token: ngwafApiToken,
                    workspaceId: ngwafWorkspaceId.trim() || null,
                  })
                }}
              >
                {ngwafSaving ? 'Saving…' : 'Save'}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
export function ServicesTable() {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const router = useRouter()
  const [cronService, setCronService] = useState<ServiceConfig | null>(null)
  const [settingsService, setSettingsService] = useState<ServiceConfig | null>(null)
  const [teardownService, setTeardownService] = useState<ServiceConfig | null>(null)
  const [deleteDataService, setDeleteDataService] = useState<ServiceConfig | null>(null)
  const [inviteService, setInviteService] = useState<ServiceConfig | null>(null)
  const [credentialsService, setCredentialsService] = useState<ServiceConfig | null>(null)
  const [credInitialMode, setCredInitialMode] = useState<'token' | 'manual'>('token')
  const [wizardOpen, setWizardOpen] = useState(false)
  const [ngwafService, setNgwafService] = useState<ServiceConfig | null>(null)
  const openCredentials = React.useCallback((service: ServiceConfig) => {
    setCredInitialMode(service.access_level === 'read_write' ? 'token' : 'manual')
    setCredentialsService(service)
  }, [])
  const switchToService = React.useCallback(
    (id: string) => {
      setActiveServiceId(id)
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      router.push(`/dashboard?service=${id}`)
    },
    [setActiveServiceId, queryClient, router],
  )
  const openRum = React.useCallback(
    (s: ServiceConfig) => {
      setActiveServiceId(s.service_id)
      router.push(`/admin/rum?service=${s.service_id}`)
    },
    [setActiveServiceId, router],
  )
  const { data: services, isLoading, isFetching, isError, error } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async ({ signal }) => unwrapClient(client.GET('/api/services', { signal })),
  })
  const isRefreshing = isFetching && !isLoading
  const handleRefreshAll = React.useCallback(() => {
    for (const queryKey of [
      queryKeys.services(),
      queryKeys.admin.overviewAll(),
      queryKeys.admin.healthSnapshot(),
      queryKeys.syncStatus(activeServiceId),
    ]) {
      void queryClient.invalidateQueries({ queryKey })
    }
  }, [queryClient, activeServiceId])
  const columns = React.useMemo(
    () => buildServiceColumns({
      activeServiceId,
      switchToService,
      servicesLength: services?.services?.length || 0,
      setCronService,
      setSettingsService,
      setDeleteDataService,
      setTeardownService,
      setInviteService,
      openNgwaf: setNgwafService,
      openRum,
      openCredentials,
    }),
    [activeServiceId, switchToService, services?.services?.length, openRum, openCredentials],
  )
  return (
    <>
      <div className="space-y-4" data-testid="admin-services-section">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <h2 className="text-xl font-semibold tracking-tight">Service Management</h2>
            <Button size="sm" onClick={() => setWizardOpen(true)}>
              <Plus className="h-4 w-4 mr-1" /> Add Service
            </Button>
          </div>
          <Button
            size="sm"
            variant="outline"
            data-testid="admin-overview-refresh-btn"
            onClick={handleRefreshAll}
            disabled={isFetching}
          >
            Refresh Status
          </Button>
        </div>
        {isLoading && (
          <div className="text-xs text-muted-foreground" role="status" aria-live="polite" data-testid="services-loading-status">
            Loading service configs...
          </div>
        )}
        {isError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Couldn&apos;t load services</AlertTitle>
            <AlertDescription className="text-xs">
              {error instanceof Error ? error.message : 'The backend returned an error. The list below may be incomplete — investigate before provisioning new services.'}
            </AlertDescription>
          </Alert>
        )}
        <div
          data-testid="services-table-container"
          className={`border rounded-lg bg-card shadow-sm overflow-hidden transition-opacity ${
            isRefreshing ? 'opacity-40 pointer-events-none' : ''
          }`}
        >
          <DataTable
            columns={columns}
            data={services?.services || []}
            isLoading={isLoading}
            searchKey="name"
            emptyMessage="No services yet"
            emptyHint="Add a service to get started."
          />
        </div>
      </div>
      <ProvisionWizard open={wizardOpen} onOpenChange={setWizardOpen} />
      {cronService && (
        <CronSettingsModal service={cronService} open={!!cronService} onOpenChange={(open) => !open && setCronService(null)} />
      )}
      {settingsService && (
        <LogSettingsModal service={settingsService} open={!!settingsService} onOpenChange={(open) => !open && setSettingsService(null)} />
      )}
      <InviteAnalystDialog service={inviteService} open={!!inviteService} onOpenChange={(open) => !open && setInviteService(null)} />
      <TeardownDialog
        service={teardownService}
        open={!!teardownService}
        onOpenChange={(open) => !open && setTeardownService(null)}
        onComplete={() => {
          queryClient.invalidateQueries({ queryKey: queryKeys.services() })
          queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
          setTeardownService(null)
        }}
      />
      <DeleteDataDialog
        service={deleteDataService}
        open={!!deleteDataService}
        onOpenChange={(open) => !open && setDeleteDataService(null)}
        onComplete={() => {
          cronCacheBust(queryClient, deleteDataService?.service_id)
          queryClient.invalidateQueries({ queryKey: queryKeys.services() })
          setDeleteDataService(null)
        }}
      />
      <CredentialsDialog service={credentialsService} initialMode={credInitialMode} onClose={() => setCredentialsService(null)} />
      <NgwafDialog service={ngwafService} onClose={() => setNgwafService(null)} />
    </>
  )
}
