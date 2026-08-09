'use client'

import React, { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { StatCard } from '@/components/ui/stat-card'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { Skeleton } from '@/components/ui/skeleton'
import { SkeletonGrid } from '@/components/ui/skeleton-grid'
import { PlotlyChart } from '@/components/PlotlyChart/PlotlyChart'
import { CostCalculator } from '@/components/CostCalculator/CostCalculator'
import { Database, HardDrive, Zap, DollarSign, Activity as ActivityIcon } from 'lucide-react'
import { useTheme } from 'next-themes'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils';
import { formatBytes } from '@/lib/format'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ReportLayout } from '@/components/ReportLayout'
function fmtN(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString()
}


const FosOperationsHelp = ({ note }: { note?: string }) => (
  <div className="space-y-4">
    <p>Fastly Object Storage (FOS) bills operations in two classes:</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Zap className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span><strong>Class A Operations:</strong> Includes state-changing operations like uploads (writes), list bucket requests, and deletes. Fastly&apos;s edge writes your raw logs here, and the backend lists the bucket to find them.</span>
      </li>
      <li className="flex gap-3">
        <Database className="h-5 w-5 shrink-0 text-green-500 mt-0.5" />
        <span><strong>Class B Operations:</strong> Includes reads (downloads) and metadata checks. The backend downloads the raw log files to process them into Parquet format.</span>
      </li>
    </ul>
    {note && (
      <div className="mt-4 p-3 bg-muted/50 border rounded-md text-xs">
        {note}
      </div>
    )}
  </div>
)

import { useRouter } from 'next/navigation'
import { useServiceStore } from '@/stores/serviceStore'
import { buildServiceHref } from '@/lib/navigation'

export default function UsagePage() {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  const router = useRouter()

  return (
    <ReportLayout
      title="System Usage"
      description="Estimate Fastly Object Storage and CDN logging costs."
      icon={ActivityIcon}
      defaultInterval="1 day"
    >
      {({
        startTime,
        endTime,
        activeServiceId,
        config,
        setChartInterval,
        intervalButtons,
      }) => {
        const services = useServiceStore(s => s.services);
        const isAnalyst = services.find((s: any) => s.id === activeServiceId)?.accessLevel === 'read_only'
        const isReady = useIsDataReady()
        const activityBy = config.effectiveInterval.split(' ')[1] || 'hour' // fallback

        // FOS Operations, Bandwidth, and Log Activity use Fastly's /stats/* APIs
        // which only support hour/day granularity. Render a custom button group
        // with just those two so the user sees no misleading 1s/1m buttons.
        const restictedIntervalButtons = (
          <ButtonGroup>
            {[
              { label: '1h', value: '1 hour' as const },
              { label: '1d', value: '1 day' as const },
            ].map(i => {
              const isActive = config.effectiveInterval === i.value
              return (
                <Button
                  key={i.value}
                  variant={isActive ? 'default' : 'ghost'}
                  size="sm"
                  onClick={() => React.startTransition(() => setChartInterval(i.value))}
                  className={cn(
                    'h-9 text-xs px-2 shadow-none transition-colors sm:h-7 sm:text-[11px]',
                    isActive
                      ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                      : 'hover:text-primary hover:bg-muted',
                  )}
                >
                  {i.label}
                </Button>
              )
            })}
          </ButtonGroup>
        )

        const fosOpsIntervalButtons = restictedIntervalButtons

        const { data: storage, isLoading: loadingStorage, isFetching: fetchingStorage, error: errorStorage } = useQuery({
    queryKey: ['usage', 'storage', activeServiceId, startTime, endTime],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/current-storage", { signal,
        params: { query: { start: startTime ?? undefined, end: endTime ?? undefined } }
      })
      return data
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const { data: ops, isLoading: loadingOps, isFetching: fetchingOps, error: errorOps } = useQuery({
    queryKey: ['usage', 'operations', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/operations", { signal,
        params: { query: { start: startTime ?? undefined, end: endTime ?? undefined, by: activityBy } }
      })
      return data
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const { data: bw, isLoading: loadingBw, isFetching: fetchingBw, error: errorBw } = useQuery({
    queryKey: ['usage', 'bandwidth', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/bandwidth", { signal,
        params: { query: { start: startTime ?? undefined, end: endTime ?? undefined, by: activityBy } }
      })
      return data
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const { data: logActivity, isLoading: loadingActivity, isFetching: fetchingActivity, error: errorActivity } = useQuery({
    queryKey: ['usage', 'log-activity', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/log-activity", { signal,
        params: { query: { start: startTime ?? undefined, end: endTime ?? undefined, by: activityBy } }
      })
      return data
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const { data: rumBreakdown, isLoading: loadingRumBreakdown, isFetching: fetchingRumBreakdown, error: errorRumBreakdown } = useQuery({
    queryKey: ['usage', 'rum-breakdown', activeServiceId, startTime, endTime],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/rum-breakdown", { signal,
        params: { query: { start: startTime ?? undefined, end: endTime ?? undefined, by: 'day' } }
      })
      return data
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  // /prefill is split into /rates (FAST, local-config only) + /prefill (FULL,
  // Fastly stats + edge_ratio). Stat cards always need rates, so fire that
  // immediately. The Cost Estimator needs the full payload but lives below
  // the fold — defer the slow call until it scrolls into view.
  const { data: prefillRates } = useQuery({
    queryKey: ['usage', 'prefill-rates', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/prefill/rates", { signal })
      return data
    },
    enabled: isReady,
    // Rates only change on admin config edits; analyst can pay one round-trip
    // every 5 minutes for safety. (Frontend bound — backend has no cache here.)
    staleTime: 5 * 60_000,
  })

  const estimatorRef = useRef<HTMLDivElement>(null)
  const [estimatorVisible, setEstimatorVisible] = useState(false)
  useEffect(() => {
    if (estimatorVisible || !estimatorRef.current) return
    if (typeof IntersectionObserver === 'undefined') {
      setEstimatorVisible(true)
      return
    }
    const node = estimatorRef.current
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setEstimatorVisible(true)
          observer.disconnect()
        }
      },
      // 600px rootMargin matches the site-wide LazyMount default so the
      // fetch fires one screen ahead of the user reaching the section.
      { rootMargin: '600px' },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [estimatorVisible])

  const { data: prefillFull, isLoading: loadingPrefillFull } = useQuery({
    queryKey: ['usage', 'prefill-full', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/usage/prefill", { signal })
      return data
    },
    enabled: isReady && estimatorVisible,
    staleTime: 5 * 60_000,
  })

  // Stat cards always read from rates. Cost Estimator reads from the merged
  // shape (full overrides rates when present) so it shows real Fastly traffic
  // once /prefill resolves, but renders defaults from /rates while waiting.
  const prefill = prefillFull ?? prefillRates
  const loadingPrefill = !prefillRates && !prefillFull

  const isFetchingAny = fetchingStorage || fetchingOps || fetchingBw || fetchingActivity || fetchingRumBreakdown
  const isLoadingInitial = loadingStorage || loadingOps || loadingBw || loadingActivity

  // ── Chart colours ──────────────────────────────────────────────────────────
  const accent = isDark ? '#60a5fa' : '#3b82f6'
  const accentB = isDark ? '#34d399' : '#10b981'
  const gridColor = isDark ? '#27272a' : '#e4e4e7'

  const baseLayout = {
    legend: { orientation: 'h' as const, x: 0, y: -0.18, yanchor: 'top' as const, xanchor: 'left' as const },
    margin: { b: 55 },
  }

  // ── Ops chart ──────────────────────────────────────────────────────────────
  const opsDates = ops?.data.map((d: any) => d.date) ?? []
  const opsClassA = ops?.data.map((d: any) => d.class_a) ?? []
  const opsClassB = ops?.data.map((d: any) => d.class_b) ?? []

  const opsData = [
    { type: 'bar', name: 'Class A', x: opsDates, y: opsClassA, marker: { color: accent }, hovertemplate: 'Class A: %{y:,}<extra></extra>' },
    { type: 'bar', name: 'Class B', x: opsDates, y: opsClassB, marker: { color: accentB }, hovertemplate: 'Class B: %{y:,}<extra></extra>' },
  ]
  const opsLayout = { ...baseLayout, barmode: 'stack' as const }

  // ── Bandwidth chart ────────────────────────────────────────────────────────
  const bwTimes = bw?.data.map((p: any) => p.time) ?? []
  const bwBytes = bw?.data.map((p: any) => p.bandwidth_bytes ?? 0) ?? []
  const maxBw = bwBytes.length > 0 ? Math.max(...bwBytes) : 0

  let bwDiv = 1
  let bwUnit = 'B'
  if (maxBw >= 1e9) { bwDiv = 1e9; bwUnit = 'GB' }
  else if (maxBw >= 1e6) { bwDiv = 1e6; bwUnit = 'MB' }
  else if (maxBw >= 1e3) { bwDiv = 1e3; bwUnit = 'KB' }

  const bwY = bwBytes.map((b: number) => b / bwDiv)

  const bwData = [
    { type: 'bar', name: `Bandwidth (${bwUnit})`, x: bwTimes, y: bwY, marker: { color: '#8b5cf6', opacity: 0.8 }, hovertemplate: `CDN: %{y:.2f} ${bwUnit}<extra></extra>` },
  ]
  const bwLayout = { ...baseLayout, showlegend: true, yaxis: { title: bwUnit, gridcolor: gridColor, zerolinecolor: gridColor, showspikes: false } }

  // ── Log generation chart ───────────────────────────────────────────────────
  const genTimes = logActivity?.data.map(p => p.time) ?? []
  const ingestCounts = logActivity?.data.map(p => p.row_count) ?? []
  const apiCounts = logActivity?.data.map(p => p.api_requests) ?? []
  const hasApiCounts = apiCounts.some(v => v != null && v > 0)

  const logProcData: any[] = [
    { type: 'bar', name: 'Log Lines Ingested', x: genTimes, y: ingestCounts, marker: { color: accent }, hovertemplate: 'Ingested: %{y:,}<extra></extra>' }
  ]
  if (hasApiCounts) {
    logProcData.push({
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Fastly Requests',
      x: genTimes,
      y: apiCounts,
      line: { color: '#f59e0b', width: 2 },
      marker: { color: '#f59e0b', size: 6 },
      hovertemplate: 'Requests: %{y:,}<extra></extra>',
    })
  }

  // ── Totals for stat cards ──────────────────────────────────────────────────
  const totalClassA = ops?.total_class_a ?? 0
  const totalClassB = ops?.total_class_b ?? 0
  const totalBwGB = (bw?.total_bytes ?? 0) / 1e9

  // Effective rates from prefill API (falls back to global defaults)
  const rateA = prefill?.class_a_rate_per_1k ?? 0.005
  const rateB = (prefill?.class_b_rate_per_10k ?? 0.01) / 10 // Convert 10k rate to 1k for rough estimate
  const rateEgress = prefill?.cdn_egress_rate_per_gb ?? 0.12
  const rateStorage = prefill?.storage_rate_per_gb_month ?? 0.02

  // Rough cost estimate for the stat card
  const roughCostA = (totalClassA / 1000) * rateA
  const roughCostB = (totalClassB / 1000) * rateB
  const roughStorage = ((storage?.total_billed_gb_hours ?? 0) / 720) * rateStorage
  const roughBandwidth = totalBwGB * rateEgress
  const roughTotal = roughCostA + roughCostB + roughStorage + roughBandwidth
  const fmtUSD = (n: number) => n >= 1000 ? '$' + n.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',') : '$' + n.toFixed(2)

  // ── Cost chart ──────────────────────────────────────────────────────────────
  const costOpsY = opsClassA.map((a: number, i: number) => ((a ?? 0) / 1000) * rateA + ((opsClassB[i] ?? 0) / 1000) * rateB)
  const bwByTime: Record<string, number> = {}
  bwTimes.forEach((t: string, i: number) => { bwByTime[t] = (bwBytes[i] / 1e9) * rateEgress })
  const costEgressY = opsDates.map((t: string) => bwByTime[t] ?? 0)

  const costChartData = [
    { type: 'bar', name: 'Operations', x: opsDates, y: costOpsY, marker: { color: accent }, hovertemplate: '$%{y:.2f}<extra></extra>' },
    { type: 'bar', name: 'CDN Egress', x: opsDates, y: costEgressY, marker: { color: '#f59e0b' }, hovertemplate: '$%{y:.2f}<extra></extra>' },
  ]
  const costLayout = { ...baseLayout, barmode: 'stack' as const, yaxis: { tickprefix: '$', tickformat: '.2f', gridcolor: gridColor, zerolinecolor: gridColor, showspikes: false } }

  const prefillNote = prefill && !loadingPrefill
    ? 'Calculator pre-filled from your current service configuration.'
    : undefined



  return (
    <>
      {/* ── Stat cards ─────────────────────────────────────────────────────── */}
      <div className={cn("grid grid-cols-2 lg:grid-cols-4 gap-4 transition-opacity duration-100", isFetchingAny && !isLoadingInitial && "opacity-40 pointer-events-none")}>
        <StatCard
          title="Storage Impact (Period)"
          value={
            errorStorage
              ? <span className="text-muted-foreground">—</span>
              : <span className="text-green-600">{(storage?.total_billed_gb_hours ?? 0).toFixed(2)} GB-hrs</span>
          }
          sub={
            errorStorage ? (
              <span role="alert" className="text-[11px] text-destructive">Failed to load storage usage.</span>
            ) : loadingStorage ? (
              <div className="flex flex-col gap-1.5 mt-2">
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
              </div>
            ) : (
              <div className="flex flex-col gap-0.5 mt-1 text-[11px]">
                <div className="flex justify-between">
                  <span>Live Storage:</span>
                  <strong className="text-foreground">{formatBytes(storage?.live_bytes ?? 0)}</strong>
                </div>
                {(storage?.rum_bytes ?? 0) > 0 && (
                  <div className="flex justify-between text-blue-700 dark:text-blue-400">
                    <span>RUM Logs:</span>
                    <strong>{formatBytes(storage?.rum_bytes ?? 0)}</strong>
                  </div>
                )}
                {(storage?.regular_log_bytes ?? 0) > 0 && (
                  <div className="flex justify-between">
                    <span>Request Logs:</span>
                    <strong className="text-foreground">{formatBytes(storage?.regular_log_bytes ?? 0)}</strong>
                  </div>
                )}
                {(storage?.quarantine_bytes ?? 0) > 0 && (
                  <div className="flex justify-between">
                    <span>Quarantine:</span>
                    <strong className="text-foreground">{formatBytes(storage?.quarantine_bytes ?? 0)}</strong>
                  </div>
                )}
                <div className="flex justify-between text-amber-700 dark:text-amber-500">
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <span className=" underline underline-offset-2 decoration-dotted">Deleted (Still Billed):</span>
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-[200px] text-xs">
                        Objects created in this period but deleted early. Billed for minimum 30 days.
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                  <strong>{formatBytes(storage?.deleted_bytes ?? 0)}</strong>
                </div>
              </div>
            )
          }
          icon={HardDrive}
          loading={loadingStorage}
          tooltip="Estimated minimum billed storage hours for objects created in this period."
        />
        <StatCard
          title="Class A Ops (period)"
          value={fmtN(totalClassA)}
          sub={
            <div className="space-y-1">
              <div>Writes, lists, deletes</div>
              <div className="flex items-center justify-between">
                <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Rate: ${rateA}/1k</span>
                <Button variant="link" size="sm" className="h-auto p-0 text-[10px] font-bold text-primary" onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}>Edit</Button>
              </div>
            </div>
          }
          icon={Zap}
          loading={loadingOps}
          tooltip="Writes (log uploads) and lists (checking for new logs). Note: This is an account-wide total."
        />
        <StatCard
          title="Class B Ops (period)"
          value={fmtN(totalClassB)}
          sub={
            <div className="space-y-1">
              <div>Reads / downloads</div>
              <div className="flex items-center justify-between">
                <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Rate: ${prefill?.class_b_rate_per_10k ?? 0.01}/10k</span>
                <Button variant="link" size="sm" className="h-auto p-0 text-[10px] font-bold text-primary" onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}>Edit</Button>
              </div>
            </div>
          }
          icon={Database}
          loading={loadingOps}
          tooltip="Reads (downloading logs for processing). Note: This is an account-wide total."
        />
        <StatCard
          title="Estimated Incurred Cost"
          value={fmtUSD(roughTotal)}
          sub={
            (loadingOps || loadingStorage || loadingBw) ? (
              <div className="flex flex-col gap-1.5 mt-2">
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
                <Skeleton className="h-3 w-full" />
              </div>
            ) : (
              <div className="flex flex-col gap-0.5 mt-1 text-[11px] font-normal">
                <div className="flex justify-between">
                  <span>Storage:</span>
                  <div className="flex flex-col items-end">
                    {errorStorage
                      ? <strong className="text-destructive">n/a</strong>
                      : <strong className="text-foreground">{fmtUSD(roughStorage)}</strong>}
                    <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">Rate: ${rateStorage}/GB</span>
                  </div>
                </div>
                <div className="flex justify-between">
                  <span>Operations:</span>
                  <div className="flex flex-col items-end">
                    <strong className="text-foreground">{fmtUSD(roughCostA + roughCostB)}</strong>
                    <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">A: ${rateA}/1K · B: ${prefill?.class_b_rate_per_10k ?? 0.01}/10K</span>
                  </div>
                </div>
                <div className="flex justify-between">
                  <span>CDN Egress:</span>
                  <div className="flex flex-col items-end">
                    <strong className="text-foreground">{fmtUSD(roughBandwidth)}</strong>
                    <span className="text-[9px] uppercase tracking-tighter text-muted-foreground">Rate: ${rateEgress}/GB</span>
                  </div>
                </div>
                {errorStorage && (
                  <div role="alert" className="text-[10px] text-destructive mt-0.5">Partial estimate — storage cost excluded.</div>
                )}
              </div>
            )
          }
          icon={DollarSign}
          loading={loadingOps || loadingStorage || loadingBw}
          tooltip="Total estimated cost incurred for the selected period (Storage, Ops, Egress). Note: Operations are account-wide totals."
        />
      </div>

      {/* ── Charts ─────────────────────────────────────────────────────────── */}
      <div className={cn("grid grid-cols-1 lg:grid-cols-2 gap-6 transition-opacity duration-100", isFetchingAny && !isLoadingInitial && "opacity-40 pointer-events-none")}>
        <AnalyticsCard
          title="FOS Operations"
          description="Class A and Class B operations from the Historical Stats API"
          headerAction={fosOpsIntervalButtons}
          isLoading={loadingOps}
          isFetching={fetchingOps}
          error={errorOps as AnalyticsCardError | null}
          isEmpty={opsDates.length === 0}
          className="h-[360px]"
          contentClassName="p-2"
          helpContent={<FosOperationsHelp note={ops?.note} />}
        >
          <PlotlyChart data={opsData as any[]} layout={opsLayout} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="CDN Bandwidth"
          description="Egress bandwidth delivered by the CDN service fronting FOS"
          headerAction={restictedIntervalButtons}
          isLoading={loadingBw}
          isFetching={fetchingBw}
          error={errorBw as AnalyticsCardError | null}
          isEmpty={bwTimes.length === 0}
          className="h-[360px]"
          contentClassName="p-2"
        >
          <PlotlyChart data={bwData as any[]} layout={bwLayout} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="Estimated Cost"
          description="Operations and CDN egress cost per period"
          headerAction={fosOpsIntervalButtons}
          isLoading={loadingOps || loadingBw}
          isFetching={fetchingOps || fetchingBw}
          error={(errorOps ?? errorBw) as AnalyticsCardError | null}
          isEmpty={opsDates.length === 0}
          className="h-[360px]"
          contentClassName="p-2"
        >
          <PlotlyChart data={costChartData as any[]} layout={costLayout} height="100%" />
        </AnalyticsCard>

        <AnalyticsCard
          title="Log Activity (Processed)"
          description="Log rows ingested and processed (with Fastly emission overlay)"
          headerAction={restictedIntervalButtons}
          isLoading={loadingActivity}
          isFetching={fetchingActivity}
          error={errorActivity as AnalyticsCardError | null}
          isEmpty={genTimes.length === 0}
          className="h-[360px]"
          contentClassName="p-2"
        >
          <PlotlyChart data={logProcData as any[]} layout={baseLayout} height="100%" />
        </AnalyticsCard>

        {rumBreakdown && (
          <AnalyticsCard
            title="Real User Monitoring (RUM) Operations"
            description="RUM beacon volume and estimated FOS Class A operation cost"
            isLoading={loadingRumBreakdown}
            isFetching={fetchingRumBreakdown}
            error={errorRumBreakdown as AnalyticsCardError | null}
            isEmpty={!rumBreakdown?.data || rumBreakdown.data.length === 0}
          >
            <div className="space-y-4 p-4">
              {rumBreakdown.total_beacons === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <p className="text-sm mb-2">No RUM beacons collected yet</p>
                  <p className="text-xs">Once users visit your site with the RUM script installed, beacons will appear here</p>
                  <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-950 rounded text-xs text-blue-900 dark:text-blue-100">
                    Cost: ${rumBreakdown.class_a_rate_per_1k?.toFixed(3)} per 1,000 beacons (Class A operations)
                  </div>
                </div>
              ) : (
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <p className="text-xs text-muted-foreground">Total Beacons</p>
                    <p className="text-2xl font-bold">{fmtN(rumBreakdown.total_beacons)}</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {rumBreakdown.average_beacons_per_day?.toLocaleString()} / day avg
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Class A Operations</p>
                    <p className="text-2xl font-bold">{fmtN(rumBreakdown.total_estimated_class_a)}</p>
                    <p className="text-xs text-muted-foreground mt-1">@${rumBreakdown.class_a_rate_per_1k?.toFixed(3)}/1k</p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">Estimated Cost</p>
                    <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                      ${rumBreakdown.total_estimated_cost_usd?.toFixed(4)}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">This period</p>
                  </div>
                </div>
              )}
              {rumBreakdown.note && (
                <div className="p-3 bg-muted/50 border rounded text-xs text-muted-foreground">
                  {rumBreakdown.note}
                </div>
              )}
            </div>
          </AnalyticsCard>
        )}
        </div>

      {/* ── Cost Estimator ─────────────────────────────────────────────────── */}
      {!isAnalyst && (
        <div ref={estimatorRef}>
          <AnalyticsCard
            title="Cost Estimator"
            description="Estimate your monthly FOS cost based on your configuration and contract rates. Pre-filled from your active service and rate settings."
          >
            {loadingPrefill || (estimatorVisible && loadingPrefillFull) ? (
              <div className="space-y-3">
                <SkeletonGrid count={4} height="36px" className="rounded-md" />
              </div>
            ) : (
              <CostCalculator
                prefillData={prefill ?? undefined}
                prefillNote={prefillNote}
              />
            )}
          </AnalyticsCard>
        </div>
      )}
      </>
    )
  }}
  </ReportLayout>
)
}
