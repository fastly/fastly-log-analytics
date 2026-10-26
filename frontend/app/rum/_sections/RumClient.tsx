'use client'

import React, { useState, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import {
  Activity,
  AlertTriangle,
  BookOpen,
  Cpu,
  Eye,
  Monitor,
  PieChart,
  Radio,
  RefreshCw,
  ShieldAlert,
  Terminal,
  TrendingUp,
  TriangleAlert,
} from 'lucide-react'
import { AnalyticsCard, TimeSeriesChart } from '@/components/AnalyticsCard'
import { DataTable, DataTableReadonly } from '@/components/DataTable'
import { PlotlyChart } from '@/components/PlotlyChart'
import { DashboardSkeleton, ReportLayout } from '@/components/ReportLayout'
import { DisableRumDialog, EnableRumDialog, RumSettingsDialog } from '@/components/Rum/RumSettingsDialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { useIsAnalyst } from '@/hooks/useBootstrap'
import { useAnalyticsReportQuery, useReportRangeAnchor, useRumStatus } from '@/hooks/useDashboardCards'
import { useTimezone } from '@/hooks/useSSE'
import { adminJsonResult, client, unwrapClient } from '@/lib/api'
import { CHART_COLORS } from '@/lib/chart-helpers'
import { formatCount, formatDate, formatLocalDateTime } from '@/lib/format'
import { queryKeys } from '@/lib/query-keys'
import { arrayOrEmpty, cn, recordOrNull, type RangeWireArgs } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import type { FiltersPayload, StripIndex, WithRequired, components } from '@/types/api'

type RumVersionsResponse = components['schemas']['RumVersionsResponse']
type RumWire = components['schemas']['RumAnalyticsResponse']
type RumWorstPage = components['schemas']['RumWorstPage']
type RumJsError = components['schemas']['RumJsError']
type RumCwvDistribution = components['schemas']['RumCwvDistribution']
type RumAnalytics = WithRequired<RumWire, 'beacon_count' | 'vitals' | 'worst_pages' | 'errors' | 'trends' | 'environments'>

interface RumFaroVersionCardProps {
  serviceId: string | null
}

export function RumFaroVersionCard({ serviceId }: RumFaroVersionCardProps) {
  const queryClient = useQueryClient()
  const [isUpgradeOpen, setIsUpgradeOpen] = useState(false)
  const isAnalyst = useIsAnalyst()
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId)
  const rumEnabled = !!status?.enabled

  const { data, isLoading: isVersionsLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: queryKeys.rumVersions(serviceId),
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET('/api/services/{service_id}/rum/versions', {
          params: { path: { service_id: serviceId as string } },
          signal,
        }),
      ),
    enabled: !!serviceId && rumEnabled && !isAnalyst,
    retry: false,
  })

  if (isAnalyst) return null
  if (!serviceId || isStatusLoading || (rumEnabled && isVersionsLoading)) {
    return (
      <div className="flex items-center gap-2 shrink-0">
        <Skeleton className="h-8 w-32" />
        <Skeleton className="h-8 w-24" />
      </div>
    )
  }
  if (!rumEnabled) return null

  if (isError) {
    return (
      <div className="flex items-center gap-2 text-xs text-destructive bg-destructive/10 px-2.5 py-1 rounded-md border border-destructive/20 shrink-0">
        <TriangleAlert className="h-3.5 w-3.5 shrink-0" />
        <span>Versions offline</span>
        <Button
          variant="ghost"
          size="icon"
          onClick={() => refetch()}
          disabled={isFetching}
          aria-label="Retry"
          className="h-5 w-5 hover:bg-destructive/20 hover:text-destructive shrink-0"
        >
          <RefreshCw className={cn('h-2.5 w-2.5', isFetching && 'animate-spin')} />
        </Button>
      </div>
    )
  }

  const available = data?.available ?? []
  const current = data?.current ?? null
  const latest = data?.latest ?? null
  const updateAvailable = data?.update_available ?? false

  const handleComplete = () => {
    queryClient.invalidateQueries({ queryKey: queryKeys.rumVersions(serviceId) })
    queryClient.invalidateQueries({ queryKey: queryKeys.rumStatus(serviceId) })
  }

  return (
    <>
      <div className="flex items-center gap-2 shrink-0" data-testid="admin-rum-faro-version-card">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground mr-1">
          <span className="font-semibold uppercase tracking-wider text-[10px]">Faro SDK:</span>
          <span
            className="font-mono font-medium text-foreground bg-muted/60 px-1.5 py-0.5 rounded border border-muted-foreground/10"
            data-testid="admin-rum-faro-version"
          >
            {current ?? 'Not pinned'}
          </span>
          {updateAvailable && (
            <Badge variant="warning" className="text-[9px] py-0 px-1.5 h-4.5 font-medium ml-1 shrink-0">
              Update available
            </Badge>
          )}
        </div>

        <Button
          onClick={() => setIsUpgradeOpen(true)}
          variant={updateAvailable ? 'default' : 'outline'}
          size="sm"
          className="h-8 text-xs font-semibold px-3"
          data-testid="admin-rum-settings-btn"
        >
          RUM Settings
        </Button>
      </div>

      <RumSettingsDialog
        serviceId={serviceId}
        open={isUpgradeOpen}
        onOpenChange={setIsUpgradeOpen}
        availableVersions={available}
        currentVersion={current}
        latestVersion={latest}
        onComplete={handleComplete}
      />
    </>
  )
}

export function RumStatusPanel() {
  const searchParams = useSearchParams()
  const queryClient = useQueryClient()
  const queryServiceId = searchParams?.get('service')
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)

  useEffect(() => {
    if (queryServiceId && queryServiceId !== storeServiceId) {
      setActiveServiceId(queryServiceId)
    }
  }, [queryServiceId, storeServiceId, setActiveServiceId])

  const { data: services, isLoading: isServicesLoading, refetch: refetchServices } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async () => {
      const res = await adminJsonResult('/api/services', '/api/services')
      return res.ok ? res.data : null
    },
  })

  const serviceId = queryServiceId || storeServiceId || services?.services?.[0]?.service_id || null
  const isAnalyst = useIsAnalyst()
  const [isEnableOpen, setIsEnableOpen] = useState(false)
  const [isDisableOpen, setIsDisableOpen] = useState(false)

  const {
    data: status,
    isLoading: isStatusLoading,
    isFetching: isStatusFetching,
    refetch: refetchStatus,
  } = useRumStatus(serviceId, { refetchInterval: 30_000 })

  const currentService =
    services?.services?.find((s: components['schemas']['ServiceConfig']) => s.service_id === serviceId) || null

  const isInitialLoading = (!serviceId && isServicesLoading) || (!!serviceId && isStatusLoading && !status)
  const isRefreshing = isStatusFetching && !isInitialLoading

  const handleManualRefresh = async () => {
    await Promise.all([
      refetchStatus(),
      refetchServices(),
      serviceId ? queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] }) : Promise.resolve(),
    ])
  }

  if (isAnalyst && (status?.unauthorized || (!isInitialLoading && !status?.enabled))) {
    const denied = !!status?.unauthorized
    return (
      <AnalyticsCard title="RUM Status">
        <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
          <AlertTriangle className={cn('h-12 w-12', denied ? 'text-destructive' : 'text-muted-foreground')} />
          <div>
            <h3 className={cn('text-lg font-medium', denied && 'text-destructive')}>
              {denied ? 'Access Denied' : 'RUM not enabled'}
            </h3>
            <p className="text-sm text-muted-foreground mt-1">
              {denied
                ? 'You are not authorized to view RUM data for this service. Please contact your administrator to grant access.'
                : 'Real User Monitoring is not enabled for this service. Ask an administrator to enable it.'}
            </p>
          </div>
        </div>
      </AnalyticsCard>
    )
  }

  const enabledCollectors = status
    ? [
        status.capture_vitals !== false && 'Vitals',
        status.capture_performance !== false && 'Performance',
        status.capture_errors !== false && 'Errors',
        status.capture_events !== false && 'Events',
      ].filter(Boolean)
    : []

  return (
    <div className="space-y-4" data-testid="admin-rum-panel">
      <div
        className="flex items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 h-[56px] min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
        data-testid="admin-rum-toolbar"
      >
        <div className="flex items-center gap-3 min-w-0">
          <Badge
            variant={isInitialLoading ? 'secondary' : status?.enabled ? 'default' : 'outline'}
            className="min-w-[152px] justify-center shrink-0"
            data-testid="admin-rum-status-badge"
          >
            {isInitialLoading ? 'Checking...' : status?.enabled ? 'Beacon Pipeline Active' : 'Beacon Pipeline Inactive'}
          </Badge>
          <span className="text-xs text-muted-foreground truncate" data-testid="admin-rum-service-label">
            Service: <strong className="text-foreground">{currentService?.name ?? serviceId ?? 'Loading...'}</strong>
          </span>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <RumFaroVersionCard serviceId={serviceId} />
          <Button
            variant="outline"
            size="sm"
            onClick={handleManualRefresh}
            disabled={isStatusFetching || !serviceId}
            data-testid="admin-rum-refresh-btn"
            className="h-8 text-xs shrink-0"
          >
            <RefreshCw className={cn('mr-2 h-3.5 w-3.5', isStatusFetching && 'animate-spin')} />
            Refresh
          </Button>
        </div>
      </div>

      <div
        className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]"
        data-testid="admin-rum-status-reservation"
      >
        <AnalyticsCard
          title="RUM Status"
          className={cn('space-y-4 min-h-[260px]', isRefreshing && 'opacity-40 pointer-events-none transition-opacity')}
        >
          <div data-testid="admin-rum-status-card" className="space-y-4">
            {isInitialLoading ? (
              <div className="space-y-4" data-testid="admin-rum-status-skeleton">
                <div className="space-y-2 min-h-[112px]">
                  <p className="text-xs text-muted-foreground">Inspecting RUM beacon pipeline...</p>
                  <Skeleton className="h-4 w-48" />
                  <Skeleton className="h-4 w-64" />
                  <Skeleton className="h-4 w-40" />
                </div>
                <div className="flex gap-2">
                  <Skeleton className="h-9 w-28" />
                  <Skeleton className="h-9 w-28" />
                </div>
              </div>
            ) : (
              <>
                <div className="space-y-2 min-h-[112px]">
                  <p className="text-sm" data-testid="admin-rum-status-line">
                    <strong>Status:</strong> {status?.enabled ? '✅ Enabled' : '❌ Disabled'}
                  </p>
                  {status?.enabled && (
                    <>
                      <p className="text-sm" data-testid="admin-rum-enabled-at">
                        <strong>Enabled at:</strong>{' '}
                        {status.enabled_at ? formatLocalDateTime(status.enabled_at) : 'Unknown'}
                      </p>
                      <p className="text-sm" data-testid="admin-rum-vcl-drift">
                        <strong>VCL Drift:</strong> {status.vcl_drift ? '⚠️ Yes' : '✅ No'}
                      </p>
                      {!isAnalyst && (
                        <p className="text-sm" data-testid="admin-rum-capture-summary">
                          <strong>Collectors:</strong>{' '}
                          {enabledCollectors.length > 0 ? enabledCollectors.join(', ') : 'None'}
                          {status.custom_condition ? ` · VCL Condition: ${status.custom_condition}` : ''}
                        </p>
                      )}
                    </>
                  )}
                </div>

                <div className="flex gap-2">
                  <Button
                    onClick={() => setIsEnableOpen(true)}
                    disabled={status?.enabled}
                    variant={status?.enabled ? 'secondary' : 'default'}
                    data-testid="admin-rum-enable-btn"
                  >
                    Enable RUM
                  </Button>
                  <Button
                    onClick={() => setIsDisableOpen(true)}
                    disabled={!status?.enabled}
                    variant="destructive"
                    data-testid="admin-rum-disable-btn"
                  >
                    Disable RUM
                  </Button>
                </div>
              </>
            )}
          </div>
        </AnalyticsCard>
      </div>

      <EnableRumDialog
        service={currentService}
        open={isEnableOpen}
        onOpenChange={setIsEnableOpen}
        onComplete={async () => { await refetchStatus() }}
      />

      <DisableRumDialog
        service={currentService}
        open={isDisableOpen}
        onOpenChange={setIsDisableOpen}
        onComplete={async () => { await refetchStatus() }}
      />
    </div>
  )
}

interface RumClientProps extends Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> {
  serviceId: string | null
  startTime: string | null
  endTime: string | null
  filterPayload: FiltersPayload
}

interface CwvColors {
  text: string
  bg: string
  label: string
}

interface RumRawLog {
  city?: string
  region?: string
  country?: string
  pop?: string
  tls?: string
  ttfb?: number | string | null
  faro_payload?: unknown
  meta?: unknown
  measurements?: unknown
  events?: unknown
  [key: string]: unknown
}

type RumLiveEvent = Omit<StripIndex<components['schemas']['RumLiveEventItem']>, 'browser' | 'os' | 'raw_log'> & {
  browser: components['schemas']['RumLiveEventItem']['browser'] | { name?: string }
  os: components['schemas']['RumLiveEventItem']['os'] | { name?: string }
  raw_log?: RumRawLog
}

function worstPageCol(
  id: string,
  header: string,
  size: number,
  accessorFn: (p: RumWorstPage) => string | number,
  render: (p: RumWorstPage) => React.ReactNode,
): ColumnDef<RumWorstPage> {
  return { id, header, size, accessorFn, cell: ({ row }) => render(row.original) }
}

const WORST_PAGE_COLUMNS: ColumnDef<RumWorstPage>[] = [
  worstPageCol('path', 'Path', 260, (p) => p.path, (p) => <span className="font-semibold font-mono text-xs">{p.path}</span>),
  worstPageCol('views', 'Pageviews', 110, (p) => p.views, (p) => <span className="block text-right text-sm">{p.views}</span>),
  worstPageCol('avg_load_time', 'Avg Load (s)', 120, (p) => p.avg_load_time ?? -1, (p) => (
    <span className="block text-right text-sm">{p.avg_load_time != null ? `${p.avg_load_time.toFixed(1)}s` : '—'}</span>
  )),
  worstPageCol('lcp_p75', 'LCP p75', 100, (p) => p.lcp_p75 ?? -1, (p) => (
    <span className="block text-right text-sm text-amber-500">{p.lcp_p75 != null ? `${p.lcp_p75.toFixed(2)}s` : '—'}</span>
  )),
  worstPageCol('cls_p75', 'CLS p75', 100, (p) => p.cls_p75 ?? -1, (p) => (
    <span className="block text-right text-sm">{p.cls_p75 != null ? p.cls_p75.toFixed(2) : '—'}</span>
  )),
]

function envName(v: string | { name?: string } | null | undefined): string {
  return (typeof v === 'object' && v ? v.name : v) || 'Unknown'
}

function getThresholdColors(p75: number | null | undefined, goodMax: number, poorMin: number): CwvColors {
  if (p75 == null) return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' }
  if (p75 <= goodMax) return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' }
  if (p75 > poorMin) return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' }
  return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' }
}

function buildEnvPieData(envMap: Record<string, number> | undefined) {
  return [{
    labels: Object.keys(envMap || {}),
    values: Object.values(envMap || {}),
    type: 'pie' as const,
    hole: 0.4,
    textinfo: 'label+percent' as const,
  }]
}

const TREND_CHART_LAYOUT = {
  margin: { t: 30, b: 40, l: 30, r: 10 },
  legend: { orientation: 'h' as const, y: 1.08, x: 0.5, xanchor: 'center' as const, yanchor: 'bottom' as const },
}
const STACKED_BAR_LAYOUT = { ...TREND_CHART_LAYOUT, barmode: 'stack' as const }
const DONUT_CHART_LAYOUT = { height: 250, margin: { t: 10, b: 10, l: 10, r: 10 }, showlegend: false }

export function RumClient({
  serviceId, startTime, endTime, filterPayload, relativeRange, isAutoRange, anchor,
}: RumClientProps) {
  const timezone = useTimezone()
  const [selectedEvent, setSelectedEvent] = useState<RumLiveEvent | null>(null)

  const columns = React.useMemo<ColumnDef<RumLiveEvent>[]>(() => [
    {
      accessorKey: 'type', id: 'type', meta: { label: 'Status' }, header: 'Status',
      cell: ({ row }) => {
        const isErr = row.original.type === 'error'
        return (
          <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-[10px] font-bold ${
            isErr ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'
          }`}>
            <span className={`h-1.5 w-1.5 rounded-full ${isErr ? 'bg-rose-500' : 'bg-emerald-500'}`} />
            {isErr ? 'Error' : 'Success'}
          </span>
        )
      },
    },
    {
      accessorKey: 'time', id: 'time', meta: { label: 'Time' }, header: 'Time',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">
          {formatDate(row.original.time, timezone, 'HH:mm:ss')}
        </span>
      ),
    },
    {
      accessorKey: 'path', id: 'path', meta: { label: 'Page Path' }, header: 'Page Path',
      cell: ({ row }) => (
        <span className="font-semibold font-mono text-xs text-zinc-300 block max-w-[200px] truncate" title={row.original.path}>
          {row.original.path}
        </span>
      ),
    },
    {
      accessorKey: 'desc', id: 'desc', meta: { label: 'Description' }, header: 'Description',
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs block max-w-[250px] truncate" title={row.original.desc}>
          {row.original.desc}
        </span>
      ),
    },
    {
      id: 'environment',
      accessorFn: (row) => `${envName(row.browser)} on ${envName(row.os)}`,
      meta: { label: 'Environment' }, header: 'Environment',
      cell: ({ row }) => {
        const label = `${envName(row.original.browser)} on ${envName(row.original.os)}`
        return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={label}>{label}</span>
      },
    },
    {
      id: 'geo',
      accessorFn: (row) => `${row.raw_log?.city || ''}, ${row.raw_log?.country || ''}`,
      meta: { label: 'Location' }, header: 'Location',
      cell: ({ row }) => {
        const raw = row.original.raw_log || {}
        const location = [raw.city, raw.region, raw.country].filter(Boolean).join(', ')
        return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={location}>{location || '—'}</span>
      },
    },
    {
      id: 'pop',
      accessorFn: (row) => row.raw_log?.pop || '',
      meta: { label: 'Edge POP' }, header: 'Edge POP',
      cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground uppercase">{row.original.raw_log?.pop || '—'}</span>,
    },
    {
      id: 'tls',
      accessorFn: (row) => row.raw_log?.tls || '',
      meta: { label: 'TLS' }, header: 'TLS',
      cell: ({ row }) => {
        const tls = row.original.raw_log?.tls
        return <span className="font-mono text-xs text-muted-foreground">{tls ? `TLS ${tls}` : '—'}</span>
      },
    },
    {
      id: 'ttfb',
      accessorFn: (row) => (row.raw_log?.ttfb != null ? parseFloat(String(row.raw_log.ttfb)) : null),
      meta: { label: 'TTFB (Edge)' }, header: 'TTFB (Edge)',
      cell: ({ row }) => {
        const ttfb = row.original.raw_log?.ttfb
        return <span className="font-mono text-xs text-muted-foreground">{ttfb != null ? `${ttfb}ms` : '—'}</span>
      },
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      cell: ({ row }) => (
        <div className="text-right">
          <Button
            variant="outline" size="sm" className="h-8 text-xs font-medium"
            onClick={(e) => { e.stopPropagation(); setSelectedEvent(row.original) }}
          >
            Details
          </Button>
        </div>
      ),
    },
  ], [timezone])

  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId)

  const { data: analytics, isLoading } = useAnalyticsReportQuery<RumAnalytics | null>({
    namespace: ['rum-analytics'],
    serviceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    queryFn: async ({ rangeBody }) => {
      if (!serviceId) return null
      const params = new URLSearchParams()
      for (const [k, v] of Object.entries(rangeBody)) if (v) params.append(k, v)
      if (filterPayload && Object.keys(filterPayload).length > 0) {
        params.append('filters', JSON.stringify(filterPayload))
      }
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/analytics', RumAnalytics>(
        '/api/services/{service_id}/rum/analytics',
        `/api/services/${serviceId}/rum/analytics${qs ? `?${qs}` : ''}`,
      )
      if (!res.ok) throw new Error(res.errorText || `RUM analytics failed (HTTP ${res.status})`)
      return recordOrNull(res.data)
    },
    queryOptions: { enabled: !!serviceId && !!status?.enabled, refetchInterval: false },
  })

  const formattedTimestamps = React.useMemo(
    () => (analytics?.trends?.timestamps || []).map((t: string) => formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')),
    [analytics?.trends?.timestamps, timezone],
  )

  const webVitalsTrendData = React.useMemo(
    () => [
      {
        x: formattedTimestamps, y: analytics?.trends?.lcp || [], name: 'LCP (s)',
        type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: CHART_COLORS.success },
      },
      {
        x: formattedTimestamps, y: analytics?.trends?.cls || [], name: 'CLS',
        type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: CHART_COLORS.info },
      },
    ],
    [formattedTimestamps, analytics?.trends?.lcp, analytics?.trends?.cls],
  )

  const errorRateTrendData = React.useMemo(
    () => [{
      x: formattedTimestamps, y: analytics?.trends?.error_rate || [], name: 'Error Rate (%)',
      type: 'scatter' as const, mode: 'lines+markers' as const, line: { color: '#f43f5e' }, fill: 'tozeroy' as const,
    }],
    [formattedTimestamps, analytics?.trends?.error_rate],
  )

  const browserPieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.browsers), [analytics?.environments?.browsers])
  const osPieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.os), [analytics?.environments?.os])
  const devicePieData = React.useMemo(() => buildEnvPieData(analytics?.environments?.devices), [analytics?.environments?.devices])

  const beaconsOverTimeData = React.useMemo(
    () => [
      { x: formattedTimestamps, y: analytics?.trends?.pageviews || [], name: 'Pageviews', type: 'bar' as const, marker: { color: CHART_COLORS.info } },
      { x: formattedTimestamps, y: analytics?.trends?.interactions || [], name: 'Interactions', type: 'bar' as const, marker: { color: CHART_COLORS.success } },
      { x: formattedTimestamps, y: analytics?.trends?.errors || [], name: 'Errors', type: 'bar' as const, marker: { color: '#f43f5e' } },
    ],
    [formattedTimestamps, analytics?.trends?.pageviews, analytics?.trends?.interactions, analytics?.trends?.errors],
  )

  const { data: liveEvents, isLoading: isLiveEventsLoading } = useQuery({
    queryKey: ['rum-live-events', serviceId, startTime, endTime, filterPayload],
    queryFn: async () => {
      if (!serviceId) return []
      const params = new URLSearchParams()
      if (startTime) params.append('start_time', startTime)
      if (endTime) params.append('end_time', endTime)
      if (filterPayload) params.append('filters', JSON.stringify(filterPayload))
      const qs = params.toString()
      const res = await adminJsonResult<'/api/services/{service_id}/rum/live-events', RumLiveEvent[]>(
        '/api/services/{service_id}/rum/live-events',
        qs ? `/api/services/${serviceId}/rum/live-events?${qs}` : `/api/services/${serviceId}/rum/live-events`,
      )
      if (!res.ok) throw new Error(res.errorText || `RUM live events failed (HTTP ${res.status})`)
      return arrayOrEmpty(res.data)
    },
    enabled: !!serviceId && !!status?.enabled,
    refetchInterval: false,
  })

  if (isStatusLoading || isLoading) return <DashboardSkeleton />
  if (!status?.enabled) return <RumStatusPanel />
  if (!analytics) return <DashboardSkeleton />

  if (analytics.no_data) {
    return (
      <div className="space-y-6">
        <div className="flex justify-between items-center bg-emerald-500/10 p-3 rounded-lg border border-emerald-500/20 text-emerald-900 dark:text-emerald-300 shadow-sm animate-in fade-in-50 duration-300">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-500" aria-hidden="true" />
            <span className="text-xs font-bold uppercase tracking-wider">Live RUM Stream Active</span>
          </div>
          <span className="text-xs opacity-85 font-medium">Waiting for real-time beacons…</span>
        </div>

        <AnalyticsCard title="Real User Monitoring Status">
          <div className="space-y-6 text-center py-12">
            <div>
              <p className="text-lg font-semibold text-muted-foreground mb-4">{analytics.message}</p>
              <div className="flex justify-center mb-6">
                <div className="w-48 h-2 bg-secondary rounded-full overflow-hidden">
                  <div style={{ width: `${(analytics.beacon_count / 10) * 100}%` }} className="h-full bg-emerald-500 transition-all" />
                </div>
              </div>
              <p className="text-sm text-muted-foreground">
                Make sure the RUM script is installed on your website and users are visiting your site to generate beacons.
              </p>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    )
  }

  if (analytics.beacon_count === 0) {
    return (
      <div className="space-y-6">
        <AnalyticsCard title="Real User Monitoring Dashboard">
          <div className="space-y-6 text-center py-12">
            <div>
              <div className="mx-auto w-16 h-16 bg-muted/20 rounded-full flex items-center justify-center mb-4 border border-border">
                <Activity className="h-8 w-8 text-muted-foreground/50" />
              </div>
              <p className="text-lg font-semibold text-foreground">No data for this time period</p>
              <p className="text-sm text-muted-foreground mt-2 max-w-sm mx-auto">
                No real-time beacons were received during the selected time window. Try expanding your time range or check back later.
              </p>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    )
  }

  const lcpColors = getThresholdColors(analytics.vitals.lcp.p75, 2.5, 4.0)
  const clsColors = getThresholdColors(analytics.vitals.cls.p75, 0.1, 0.25)
  const inpColors = getThresholdColors(analytics.vitals.inp.p75, 200, 500)

  const renderDistributionBar = (distribution: RumCwvDistribution | null) => {
    const dist = distribution ?? { good: 0, needs_improvement: 0, poor: 0 }
    return (
      <div className="w-full mt-2">
        <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-secondary">
          <div style={{ width: `${dist.good}%` }} className="bg-emerald-500" title={`Good: ${dist.good}%`} />
          <div style={{ width: `${dist.needs_improvement}%` }} className="bg-amber-400" title={`Needs Improvement: ${dist.needs_improvement}%`} />
          <div style={{ width: `${dist.poor}%` }} className="bg-rose-500" title={`Poor: ${dist.poor}%`} />
        </div>
        <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
          <span>{dist.good}% Good</span>
          <span>{dist.needs_improvement}% Needs Imp.</span>
          <span>{dist.poor}% Poor</span>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Beacons', value: analytics.beacon_count ?? 0, textClass: 'text-blue-500', bgClass: 'bg-blue-500/10', Icon: Activity },
          { label: 'Pageviews', value: analytics.pageview_count ?? 0, textClass: 'text-emerald-500', bgClass: 'bg-emerald-500/10', Icon: Eye },
          { label: 'Interactions', value: analytics.interaction_count ?? 0, textClass: 'text-amber-500', bgClass: 'bg-amber-500/10', Icon: Cpu },
          { label: 'JavaScript Errors', value: analytics.error_count ?? 0, textClass: 'text-rose-500', bgClass: 'bg-rose-500/10', Icon: ShieldAlert },
        ].map(({ label, value, textClass, bgClass, Icon }) => (
          <div key={label} className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
            <div className="space-y-1">
              <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">{label}</p>
              <p className={`text-2xl font-extrabold ${textClass}`}>{formatCount(value)}</p>
            </div>
            <div className={`p-2.5 ${bgClass} rounded-lg`}>
              <Icon className={`h-5 w-5 ${textClass}`} />
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          {
            title: 'Largest Contentful Paint (LCP)',
            Icon: Activity,
            colors: lcpColors,
            formatted: analytics.vitals.lcp.p75 != null ? `${analytics.vitals.lcp.p75.toFixed(2)}s` : '—',
            distribution: analytics.vitals.lcp.distribution,
          },
          {
            title: 'Cumulative Layout Shift (CLS)',
            Icon: Cpu,
            colors: clsColors,
            formatted: analytics.vitals.cls.p75 != null ? analytics.vitals.cls.p75.toFixed(2) : '—',
            distribution: analytics.vitals.cls.distribution,
          },
          {
            title: 'Interaction to Next Paint (INP)',
            Icon: Monitor,
            colors: inpColors,
            formatted: analytics.vitals.inp.p75 != null ? `${analytics.vitals.inp.p75.toFixed(2)}ms` : '—',
            distribution: analytics.vitals.inp.distribution,
          },
        ].map(({ title, Icon, colors, formatted, distribution }) => (
          <AnalyticsCard
            key={title}
            title={<span className={colors.text}>{title}</span>}
            icon={<Icon className={colors.text} />}
            headerAction={
              <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${colors.bg} ${colors.text}`}>
                {colors.label}
              </span>
            }
          >
            <div className="space-y-2">
              <p className={`text-3xl font-extrabold ${colors.text}`}>{formatted}</p>
              <p className={`text-xs ${colors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
              {renderDistributionBar(distribution)}
            </div>
          </AnalyticsCard>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Web Vitals Trend" icon={<TrendingUp />}>
          <TimeSeriesChart data={webVitalsTrendData} layout={TREND_CHART_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>

        <AnalyticsCard title="JS Error Rate Trend" icon={<TrendingUp className="text-rose-500" />}>
          <TimeSeriesChart data={errorRateTrendData} layout={TREND_CHART_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Worst Performing Pages" icon={<Monitor />}>
          <DataTableReadonly
            columns={WORST_PAGE_COLUMNS} data={analytics.worst_pages}
            hideToolbar showPagination={false} tableCaption="Worst performing pages" emptyMessage="No page data in this window"
          />
        </AnalyticsCard>

        <AnalyticsCard title="JavaScript Runtime Exceptions" icon={<ShieldAlert className="text-rose-500" />}>
          <div className="space-y-4">
            {analytics.errors.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4 text-center">No JS errors captured in this window</p>
            ) : (
              analytics.errors.map((e: RumJsError) => (
                <div key={`${e.message}-${e.file}-${e.line}-${e.col}`} className="p-3 bg-rose-500/5 rounded-lg border border-rose-500/10 space-y-1.5">
                  <div className="flex justify-between items-start gap-4">
                    <p className="text-xs font-semibold text-rose-600 font-mono break-all">{e.message}</p>
                    <span className="bg-rose-500/10 text-rose-600 px-2 py-0.5 rounded text-[10px] font-bold">{e.count}x</span>
                  </div>
                  <p className="text-[10px] text-muted-foreground font-mono">File: {e.file}:{e.line}:{e.col}</p>
                </div>
              ))
            )}
          </div>
        </AnalyticsCard>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          { title: 'Browser Distribution', data: browserPieData, a11yTitle: 'Browser Distribution Donut Chart' },
          { title: 'OS Distribution', data: osPieData, a11yTitle: 'OS Distribution Donut Chart' },
          { title: 'Device Distribution', data: devicePieData, a11yTitle: 'Device Distribution Donut Chart' },
        ].map(({ title, data, a11yTitle }) => (
          <AnalyticsCard key={title} title={title} icon={<PieChart />}>
            <PlotlyChart data={data} layout={DONUT_CHART_LAYOUT} a11yTitle={a11yTitle} />
          </AnalyticsCard>
        ))}
      </div>

      <div className="mb-6">
        <AnalyticsCard title="Beacons Over Time" icon={<Activity className="text-primary" />}>
          <TimeSeriesChart data={beaconsOverTimeData} layout={STACKED_BAR_LAYOUT} startTime={startTime} endTime={endTime} timezone={timezone} height={250} />
        </AnalyticsCard>
      </div>

      <AnalyticsCard title="Live Activity Monitor" icon={<Terminal className="h-4 w-4" />}>
        <div className="w-full">
          <DataTable
            columns={columns} data={liveEvents || []} searchKey="path" isLoading={isLiveEventsLoading}
            showPagination={false} emptyMessage="Waiting for real-time RUM user events..."
            onRowClick={(row) => setSelectedEvent(row)}
          />
        </div>
      </AnalyticsCard>

      <Dialog open={selectedEvent !== null} onOpenChange={(open) => { if (!open) setSelectedEvent(null) }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="text-lg font-bold flex items-center gap-2">
              <Activity className={selectedEvent?.type === 'error' ? 'text-rose-500 h-5 w-5' : 'text-emerald-500 h-5 w-5'} />
              RUM Beacon Details
            </DialogTitle>
            <DialogDescription>
              Extracted client characteristics and complete raw payload captured at the Fastly edge.
            </DialogDescription>
          </DialogHeader>

          {selectedEvent && (() => {
            let faroPayload: unknown = null
            let edgeMetadata: Record<string, unknown> | null = null
            const rawLog = selectedEvent.raw_log
            if (rawLog) {
              if (rawLog.faro_payload) {
                faroPayload = rawLog.faro_payload
                edgeMetadata = { ...rawLog }
                delete edgeMetadata.faro_payload
              } else if (rawLog.meta && (rawLog.measurements || rawLog.events)) {
                faroPayload = rawLog
                edgeMetadata = {
                  time: selectedEvent.time, type: selectedEvent.type, path: selectedEvent.path,
                  desc: selectedEvent.desc, browser: selectedEvent.browser, os: selectedEvent.os,
                }
              } else {
                edgeMetadata = rawLog
              }
            }

            return (
              <div className="space-y-4 my-2 text-xs">
                <div className="grid grid-cols-2 gap-4 bg-muted/20 p-4 rounded-lg border border-border">
                  <div>
                    <span className="text-muted-foreground block mb-1">Timestamp</span>
                    <span className="font-mono">{formatDate(selectedEvent.time, timezone, 'yyyy-MM-dd HH:mm:ss (zzz)')}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Event Type</span>
                    <span className={`capitalize font-semibold ${selectedEvent.type === 'error' ? 'text-rose-500' : 'text-emerald-500'}`}>
                      {selectedEvent.type}
                    </span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Url Path</span>
                    <span className="font-mono font-semibold">{selectedEvent.path}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Client Platform</span>
                    <span>{envName(selectedEvent.browser)} on {envName(selectedEvent.os)}</span>
                  </div>
                  <div className="col-span-2">
                    <span className="text-muted-foreground block mb-1">Description</span>
                    <span className={`font-mono block p-2 rounded ${selectedEvent.type === 'error' ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-muted/40 text-foreground'}`}>
                      {selectedEvent.desc}
                    </span>
                  </div>
                </div>

                {faroPayload != null && (
                  <div className="space-y-2">
                    <span className="text-muted-foreground font-semibold block">Client Faro JSON Payload (Full)</span>
                    <div className="bg-emerald-950/5 dark:bg-emerald-950/15 border border-emerald-500/15 rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                      <pre>{JSON.stringify(faroPayload, null, 2)}</pre>
                    </div>
                  </div>
                )}

                <div className="space-y-2">
                  <span className="text-muted-foreground font-semibold block">
                    {faroPayload ? 'Edge Log Representation (Extracted Row Metadata)' : 'Raw Beacon Log JSON'}
                  </span>
                  <div className="bg-muted/30 border border-border rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                    <pre>{JSON.stringify(edgeMetadata || rawLog || {}, null, 2)}</pre>
                  </div>
                </div>
              </div>
            )
          })()}
        </DialogContent>
      </Dialog>
    </div>
  )
}

interface RumPageClientProps {
  initialServiceId?: string
}

function ServiceSync({ serviceId, onSync }: { serviceId: string | null; onSync: (id: string | null) => void }) {
  useEffect(() => { onSync(serviceId) }, [serviceId, onSync])
  return null
}

export function RumPageClient({ initialServiceId }: RumPageClientProps) {
  const [showInstructions, setShowInstructions] = useState(false)
  const [activeServiceId, setActiveServiceId] = useState<string | null>(initialServiceId || null)
  const isAnalyst = useIsAnalyst()
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()
  const scriptTag = '<script src="/js/rum.js"></script>'

  return (
    <>
      <ReportLayout
        title="Real User Monitoring"
        description="Monitor real user performance metrics including Core Web Vitals, JavaScript errors, and session analytics."
        icon={Radio}
        serviceId={initialServiceId}
        headerActions={
          <div className="flex items-center gap-3">
            <RumFaroVersionCard serviceId={activeServiceId} />
            {!isAnalyst && (
              <Button variant="outline" size="sm" onClick={() => setShowInstructions(true)} className="flex items-center gap-2">
                <BookOpen className="h-4 w-4" />
                Installation Instructions
              </Button>
            )}
          </div>
        }
      >
        {({ startTime, endTime, activeServiceId: sid, filterPayload }) => (
          <>
            <ServiceSync serviceId={sid} onSync={setActiveServiceId} />
            <RumClient
              serviceId={sid} startTime={startTime} endTime={endTime} filterPayload={filterPayload}
              relativeRange={relativeRange} isAutoRange={isAutoRange} anchor={anchor}
            />
          </>
        )}
      </ReportLayout>

      {!isAnalyst && (
        <Dialog open={showInstructions} onOpenChange={setShowInstructions}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>Installation Instructions</DialogTitle>
              <DialogDescription>Add Real User Monitoring to your website to start collecting performance data.</DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 1: Add the RUM Script</h4>
                <p className="text-sm text-muted-foreground">
                  Add this single line to the `&lt;head&gt;` section of your HTML or any template that renders across your entire site. The version hash automatically updates when we release new RUM features:
                </p>
                <div className="bg-muted rounded-md overflow-hidden border">
                  <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/50">
                    <span className="text-[10px] font-mono text-muted-foreground">installation code</span>
                    <CopyButton
                      text={scriptTag}
                      ariaLabel="Copy script tag"
                      copiedAriaLabel="Copied script tag"
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                    />
                  </div>
                  <div className="p-3 font-mono text-xs overflow-x-auto">
                    <code>{scriptTag}</code>
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 2: Works Everywhere</h4>
                <p className="text-sm text-muted-foreground">This script works on any website regardless of framework:</p>
                <ul className="text-sm text-muted-foreground list-disc list-inside space-y-1">
                  <li>Plain HTML, PHP, Django, Rails, etc.</li>
                  <li>React, Vue, Svelte, Angular, Next.js</li>
                  <li>Static sites, dynamic apps, and everything in between</li>
                </ul>
              </div>

              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 3: Start Collecting Data</h4>
                <p className="text-sm text-muted-foreground">
                  Once deployed, real user beacons will automatically be sent to your analytics dashboard when users visit your site. After collecting 1+ beacons, you&apos;ll see real performance data appear on this page.
                </p>
              </div>

              <div className="bg-blue-50 dark:bg-blue-950 p-3 rounded-md space-y-2">
                <p className="text-sm font-semibold text-blue-900 dark:text-blue-100">💡 Tips:</p>
                <ul className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
                  <li>• Beacons capture Web Vitals, JavaScript errors, and session info automatically</li>
                  <li>• The script is lightweight and asynchronous — no performance impact</li>
                  <li>• Data is sent securely to your Fastly-backed analytics backend</li>
                  <li>• The version hash automatically updates when we release RUM improvements — just copy the script tag again from these instructions</li>
                </ul>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      )}
    </>
  )
}

export default RumPageClient
