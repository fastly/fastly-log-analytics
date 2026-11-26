'use client'

import React, { useEffect, useMemo, useRef } from 'react'
import { Database, Zap } from 'lucide-react'
import { AnalyticsCard, type AnalyticsCardError, useInViewOnce } from '@/components/AnalyticsCard'
import { PlotlyChart } from '@/components/PlotlyChart'
import { Button } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import {
  CHART_COLORS,
  cn,
  formatAbbreviatedCount,
  formatCountMaybe,
  type ChartInterval,
} from '@/lib/utils'
import {
  rollupComputeByDay,
  type ComputeUsageResponse,
} from '@/lib/cloud-engine/usage/computeSeries'
import type { PlotlyTrace } from '@/types/api'
import type { operations } from '@/types/api.generated'

export type OperationsUsageData =
  operations['usage_operations_api_usage_operations_get']['responses'][200]['content']['application/json']
export type BandwidthUsageData =
  operations['usage_bandwidth_api_usage_bandwidth_get']['responses'][200]['content']['application/json']
export type LogActivityData =
  operations['usage_log_activity_api_usage_log_activity_get']['responses'][200]['content']['application/json']
export type RumBreakdownData =
  operations['usage_rum_breakdown_api_usage_rum_breakdown_get']['responses'][200]['content']['application/json']

export const FosOperationsHelp = ({ note }: { note?: string }) => (
  <div className="space-y-4">
    <p>Fastly Object Storage (FOS) bills operations in two classes:</p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Zap className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>Class A Operations:</strong> Includes state-changing operations like uploads
          (writes), list bucket requests, and deletes. Fastly&apos;s edge writes your raw logs
          here, and the backend lists the bucket to find them.
        </span>
      </li>
      <li className="flex gap-3">
        <Database className="h-5 w-5 shrink-0 text-green-500 mt-0.5" />
        <span>
          <strong>Class B Operations:</strong> Includes reads (downloads) and metadata checks. The
          backend downloads the raw log files to process them into Parquet format.
        </span>
      </li>
    </ul>
    {note && <div className="mt-4 p-3 bg-muted/50 border rounded-md text-xs">{note}</div>}
  </div>
)

/** Choose enough decimal places on the Estimated Cost y-axis so small ranges don't repeat `$0.00`. */
export function costAxisTickFormat(maxCost: number): string {
  if (!Number.isFinite(maxCost) || maxCost <= 0 || maxCost >= 0.05) return '.2f'
  if (maxCost >= 0.005) return '.3f'
  if (maxCost >= 0.0005) return '.4f'
  return '.5f'
}

export function UsageChartsGrid({
  ops,
  loadingOps,
  fetchingOps,
  errorOps,
  bw,
  loadingBw,
  fetchingBw,
  errorBw,
  logActivity,
  loadingActivity,
  fetchingActivity,
  errorActivity,
  rumBreakdown,
  loadingRumBreakdown,
  fetchingRumBreakdown,
  errorRumBreakdown,
  rumCardRef,
  rumVisible = true,
  onRumVisible,
  isRumEnabled,
  isFetchingAny,
  isLoadingInitial,
  isDark,
  rateA,
  rateB,
  rateEgress,
  effectiveInterval,
  setChartInterval,
  showCompute = false,
  compute,
  loadingCompute = false,
  fetchingCompute = false,
  errorCompute = null,
}: {
  ops: OperationsUsageData | undefined
  loadingOps: boolean
  fetchingOps: boolean
  errorOps: unknown
  bw: BandwidthUsageData | undefined
  loadingBw: boolean
  fetchingBw: boolean
  errorBw: unknown
  logActivity: LogActivityData | undefined
  loadingActivity: boolean
  fetchingActivity: boolean
  errorActivity: unknown
  rumBreakdown: RumBreakdownData | undefined
  loadingRumBreakdown: boolean
  fetchingRumBreakdown: boolean
  errorRumBreakdown: unknown
  rumCardRef?: React.RefObject<HTMLDivElement | null>
  rumVisible?: boolean
  onRumVisible?: () => void
  isRumEnabled: boolean
  isFetchingAny: boolean
  isLoadingInitial: boolean
  isDark: boolean
  rateA: number
  rateB: number
  rateEgress: number
  effectiveInterval: ChartInterval
  setChartInterval: (interval: ChartInterval) => void
  /** Cloud only: the Compute Requests and Compute vCPU charts. */
  showCompute?: boolean
  compute?: ComputeUsageResponse
  loadingCompute?: boolean
  fetchingCompute?: boolean
  errorCompute?: unknown
}) {
  const internalRumCardRef = useRef<HTMLDivElement>(null)
  const resolvedRumCardRef = rumCardRef ?? internalRumCardRef
  const observedRumVisible = useInViewOnce(resolvedRumCardRef, { rootMargin: '100px' })
  useEffect(() => {
    if (observedRumVisible) onRumVisible?.()
  }, [observedRumVisible, onRumVisible])
  const effectiveRumVisible = rumVisible || observedRumVisible

  // FOS Operations, Bandwidth, and Log Activity use historical Platform Metrics.
  // Render a custom button group
  // with just those two so the user sees no misleading 1s/1m buttons.
  const restictedIntervalButtons = (
    <ButtonGroup>
      {[
        { label: '1h', value: '1 hour' as const },
        { label: '1d', value: '1 day' as const },
      ].map((i) => {
        const isActive = effectiveInterval === i.value
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

  // ── Chart colours ──────────────────────────────────────────────────────────
  const accent = isDark ? '#60a5fa' : CHART_COLORS.info
  const accentB = isDark ? '#34d399' : CHART_COLORS.success
  const gridColor = isDark ? '#27272a' : '#e4e4e7'

  const baseLayout = useMemo(
    () => ({
      legend: {
        orientation: 'h' as const,
        x: 0,
        y: -0.18,
        yanchor: 'top' as const,
        xanchor: 'left' as const,
      },
      margin: { b: 55 },
    }),
    [],
  )

  // ── Ops chart ──────────────────────────────────────────────────────────────
  const opsDates = useMemo(() => ops?.data?.map((d) => d.date) ?? [], [ops?.data])
  const opsClassA = useMemo(() => ops?.data?.map((d) => d.class_a) ?? [], [ops?.data])
  const opsClassB = useMemo(() => ops?.data?.map((d) => d.class_b) ?? [], [ops?.data])
  const kvClassA = useMemo(
    () => ops?.data?.map((d) => ((d as Record<string, unknown>).kv_class_a as number | undefined) ?? 0) ?? [],
    [ops?.data],
  )
  const kvClassB = useMemo(
    () => ops?.data?.map((d) => ((d as Record<string, unknown>).kv_class_b as number | undefined) ?? 0) ?? [],
    [ops?.data],
  )
  const hasKvOps = useMemo(
    () => kvClassA.some((v) => v > 0) || kvClassB.some((v) => v > 0),
    [kvClassA, kvClassB],
  )

  const opsData = useMemo(
    () => [
      {
        type: 'bar',
        name: 'Class A',
        x: opsDates,
        y: opsClassA,
        marker: { color: accent },
        hovertemplate: 'Class A: %{y:,}<extra></extra>',
      },
      {
        type: 'bar',
        name: 'Class B',
        x: opsDates,
        y: opsClassB,
        marker: { color: accentB },
        hovertemplate: 'Class B: %{y:,}<extra></extra>',
      },
      ...(hasKvOps
        ? [
            {
              type: 'bar',
              name: 'KV Class A',
              x: opsDates,
              y: kvClassA,
              marker: { color: CHART_COLORS.warning },
              hovertemplate: 'KV Class A: %{y:,}<extra></extra>',
            },
            {
              type: 'bar',
              name: 'KV Class B',
              x: opsDates,
              y: kvClassB,
              marker: { color: '#a855f7' },
              hovertemplate: 'KV Class B: %{y:,}<extra></extra>',
            },
          ]
        : []),
    ],
    [opsDates, opsClassA, opsClassB, kvClassA, kvClassB, hasKvOps, accent, accentB],
  )
  const opsLayout = useMemo(() => ({ ...baseLayout, barmode: 'stack' as const }), [baseLayout])

  // ── Bandwidth chart ────────────────────────────────────────────────────────
  const bwTimes = useMemo(() => bw?.data?.map((p) => p.time) ?? [], [bw?.data])
  const bwBytes = useMemo(() => bw?.data?.map((p) => p.bandwidth_bytes ?? 0) ?? [], [bw?.data])
  const maxBw = bwBytes.length > 0 ? Math.max(...bwBytes) : 0

  let bwDiv = 1
  let bwUnit = 'B'
  if (maxBw >= 1e9) {
    bwDiv = 1e9
    bwUnit = 'GB'
  } else if (maxBw >= 1e6) {
    bwDiv = 1e6
    bwUnit = 'MB'
  } else if (maxBw >= 1e3) {
    bwDiv = 1e3
    bwUnit = 'KB'
  }

  const bwY = bwBytes.map((b: number) => b / bwDiv)

  const bwData = useMemo(
    () => [
      {
        type: 'bar',
        name: `Bandwidth (${bwUnit})`,
        x: bwTimes,
        y: bwY,
        marker: { color: '#8b5cf6', opacity: 0.8 },
        hovertemplate: `CDN: %{y:.2f} ${bwUnit}<extra></extra>`,
      },
    ],
    [bwUnit, bwTimes, bwY],
  )
  const bwLayout = useMemo(
    () => ({
      ...baseLayout,
      showlegend: true,
      yaxis: {
        title: bwUnit,
        gridcolor: gridColor,
        zerolinecolor: gridColor,
        showspikes: false,
      },
    }),
    [baseLayout, bwUnit, gridColor],
  )

  // ── Log generation chart ───────────────────────────────────────────────────
  const logProcData = useMemo<PlotlyTrace[]>(() => {
    const genTimes = logActivity?.data?.map((p) => p.time) ?? []
    const ingestCounts = logActivity?.data?.map((p) => p.row_count) ?? []
    const apiCounts = logActivity?.data?.map((p) => p.api_requests) ?? []
    const hasApiCounts = apiCounts.some((v) => v != null && v > 0)

    const traces: PlotlyTrace[] = [
      {
        type: 'bar',
        name: 'Log Lines Ingested',
        x: genTimes,
        y: ingestCounts,
        marker: { color: accent },
        hovertemplate: 'Ingested: %{y:,}<extra></extra>',
      },
    ]
    if (hasApiCounts) {
      traces.push({
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Fastly Requests',
        x: genTimes,
        y: apiCounts,
        line: { color: CHART_COLORS.warning, width: 2 },
        marker: { color: CHART_COLORS.warning, size: 6 },
        hovertemplate: 'Requests: %{y:,}<extra></extra>',
      })
    }
    return traces
  }, [logActivity?.data, accent])

  // ── Cost chart ──────────────────────────────────────────────────────────────
  const isDailyInterval = effectiveInterval === '1 day'
  const canonicalBucket = (raw: string): { key: string; sortMs: number } => {
    const trimmed = raw.trim()
    const iso = /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
      ? `${trimmed}T00:00:00Z`
      : /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?$/.test(trimmed)
        ? `${trimmed}Z`
        : trimmed
    const ms = Date.parse(iso)
    if (!Number.isFinite(ms)) return { key: trimmed, sortMs: Number.POSITIVE_INFINITY }
    const utc = new Date(ms).toISOString()
    const key =
      isDailyInterval && utc.endsWith('T00:00:00.000Z')
        ? utc.slice(0, 10)
        : `${utc.slice(0, 13)}:00`
    return { key, sortMs: ms }
  }

  const opsIncomplete = ops?.complete === false
  const costBucketMap = new Map<string, { sortMs: number; opsUsd: number; egressUsd: number }>()
  if (!opsIncomplete) {
    opsDates.forEach((raw: string, i: number) => {
      const { key, sortMs } = canonicalBucket(raw)
      const entry = costBucketMap.get(key) ?? { sortMs, opsUsd: 0, egressUsd: 0 }
      entry.opsUsd += ((opsClassA[i] ?? 0) / 1000) * rateA + ((opsClassB[i] ?? 0) / 1000) * rateB
      costBucketMap.set(key, entry)
    })
  }
  bwTimes.forEach((raw: string, i: number) => {
    const { key, sortMs } = canonicalBucket(raw)
    const entry = costBucketMap.get(key) ?? { sortMs, opsUsd: 0, egressUsd: 0 }
    entry.egressUsd += (bwBytes[i] / 1e9) * rateEgress
    costBucketMap.set(key, entry)
  })
  const costEntries = Array.from(costBucketMap.entries()).sort((a, b) =>
    a[1].sortMs !== b[1].sortMs ? a[1].sortMs - b[1].sortMs : a[0].localeCompare(b[0]),
  )
  const costDates = costEntries.map(([key]) => key)
  const costOpsY = costEntries.map(([, entry]) => (opsIncomplete ? 0 : entry.opsUsd))
  const costEgressY = costEntries.map(([, entry]) => entry.egressUsd)
  const maxStackedCost = costEntries.reduce(
    (max: number, [, entry], i: number) => Math.max(max, (costOpsY[i] ?? 0) + entry.egressUsd),
    0,
  )
  const costTickFormat = costAxisTickFormat(maxStackedCost)

  const costChartData = useMemo(
    () => [
      {
        type: 'bar',
        name: 'Operations',
        x: costDates,
        y: costOpsY,
        marker: { color: accent },
        hovertemplate: `$%{y:${costTickFormat}}<extra></extra>`,
      },
      {
        type: 'bar',
        name: 'CDN Egress',
        x: costDates,
        y: costEgressY,
        marker: { color: CHART_COLORS.warning },
        hovertemplate: `$%{y:${costTickFormat}}<extra></extra>`,
      },
    ],
    [costDates, costOpsY, costEgressY, accent, costTickFormat],
  )
  const costLayout = useMemo(
    () => ({
      ...baseLayout,
      barmode: 'stack' as const,
      yaxis: {
        tickprefix: '$',
        tickformat: costTickFormat,
        rangemode: 'tozero' as const,
        gridcolor: gridColor,
        zerolinecolor: gridColor,
        showspikes: false,
      },
    }),
    [baseLayout, gridColor, costTickFormat],
  )

  // ── Compute charts (Cloud: Platform Metrics, app + worker services) ──────
  const computePoints = useMemo(() => {
    if (compute?.source !== 'fastly') return []
    return effectiveInterval === '1 day' ? rollupComputeByDay(compute.data) : compute.data
  }, [compute, effectiveInterval])
  const computeDescription =
    compute?.source === 'unavailable' && compute.reason === 'range_over_31_days'
      ? 'Fastly Platform Metrics Compute figures cover ranges of up to 31 days'
      : 'Fastly Platform Metrics, app and worker services; the latest hour can still be filling in'
  const computeRequestsData = useMemo<PlotlyTrace[]>(() => {
    const x = computePoints.map((p) => p.time)
    return [
      {
        type: 'bar',
        name: 'App service',
        x,
        y: computePoints.map((p) => p.app_requests),
        marker: { color: accent },
        hovertemplate: 'App service: %{y:,}<extra></extra>',
      },
      {
        type: 'bar',
        name: 'Worker service',
        x,
        y: computePoints.map((p) => p.worker_requests),
        marker: { color: accentB },
        hovertemplate: 'Worker service: %{y:,}<extra></extra>',
      },
    ]
  }, [computePoints, accent, accentB])
  const computeVcpuData = useMemo<PlotlyTrace[]>(() => {
    const x = computePoints.map((p) => p.time)
    return [
      {
        type: 'bar',
        name: 'App service',
        x,
        y: computePoints.map((p) => p.app_vcpu_ms / 1000),
        marker: { color: accent },
        hovertemplate: 'App service: %{y:,.2f} s<extra></extra>',
      },
      {
        type: 'bar',
        name: 'Worker service',
        x,
        y: computePoints.map((p) => p.worker_vcpu_ms / 1000),
        marker: { color: accentB },
        hovertemplate: 'Worker service: %{y:,.2f} s<extra></extra>',
      },
    ]
  }, [computePoints, accent, accentB])
  const computeVcpuLayout = useMemo(
    () => ({
      ...baseLayout,
      barmode: 'stack' as const,
      yaxis: { title: 'vCPU s', gridcolor: gridColor, zerolinecolor: gridColor, showspikes: false },
    }),
    [baseLayout, gridColor],
  )

  const gridHeightClass =
    showCompute && isRumEnabled
      ? 'min-h-[1512px] [contain-intrinsic-size:1512px]'
      : showCompute || isRumEnabled
        ? 'min-h-[1128px] [contain-intrinsic-size:1128px]'
        : 'min-h-[744px] [contain-intrinsic-size:744px]'

  return (
    <div
      className={cn(
        'grid grid-cols-1 lg:grid-cols-2 gap-6 transition-opacity duration-100 [content-visibility:auto]',
        gridHeightClass,
        isFetchingAny && !isLoadingInitial && 'opacity-40 pointer-events-none',
      )}
    >
      <AnalyticsCard
        title="FOS Operations"
        description={
          ops?.complete === false
            ? 'Local Object Storage operation telemetry is unavailable for this period'
            : ops?.source === 'fastly'
              ? 'Fallback: account-wide Class A and Class B operations from Fastly Platform Metrics'
              : 'Service-local Class A and Class B Object Storage operations'
        }
        headerAction={fosOpsIntervalButtons}
        isLoading={loadingOps}
        isFetching={fetchingOps}
        error={errorOps as AnalyticsCardError | null}
        isEmpty={opsDates.length === 0}
        className="h-[360px]"
        contentClassName="p-2"
        helpContent={<FosOperationsHelp note={ops?.note} />}
      >
        <PlotlyChart data={opsData} layout={opsLayout} height="100%" />
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
        <PlotlyChart data={bwData} layout={bwLayout} height="100%" />
      </AnalyticsCard>

      <AnalyticsCard
        title="Estimated Cost"
        description="Operations and CDN egress cost per period"
        headerAction={fosOpsIntervalButtons}
        isLoading={loadingOps || loadingBw}
        isFetching={fetchingOps || fetchingBw}
        error={(errorOps ?? errorBw) as AnalyticsCardError | null}
        isEmpty={costDates.length === 0}
        className="h-[360px]"
        contentClassName="p-2"
      >
        <PlotlyChart data={costChartData} layout={costLayout} height="100%" />
      </AnalyticsCard>

      <AnalyticsCard
        title="Log Activity (Processed)"
        description="Log rows ingested and processed (with Fastly emission overlay)"
        headerAction={restictedIntervalButtons}
        isLoading={loadingActivity}
        isFetching={fetchingActivity}
        error={errorActivity as AnalyticsCardError | null}
        isEmpty={!logActivity?.data || logActivity.data.length === 0}
        className="h-[360px]"
        contentClassName="p-2"
      >
        <PlotlyChart data={logProcData} layout={baseLayout} height="100%" />
      </AnalyticsCard>

      {showCompute && (
        <>
          <AnalyticsCard
            title="Compute Requests"
            description={computeDescription}
            headerAction={restictedIntervalButtons}
            isLoading={loadingCompute}
            isFetching={fetchingCompute}
            error={errorCompute as AnalyticsCardError | null}
            isEmpty={computePoints.length === 0}
            className="h-[360px]"
            contentClassName="p-2"
          >
            <PlotlyChart data={computeRequestsData} layout={opsLayout} height="100%" />
          </AnalyticsCard>

          <AnalyticsCard
            title="Compute vCPU"
            description={computeDescription}
            headerAction={restictedIntervalButtons}
            isLoading={loadingCompute}
            isFetching={fetchingCompute}
            error={errorCompute as AnalyticsCardError | null}
            isEmpty={computePoints.length === 0}
            className="h-[360px]"
            contentClassName="p-2"
          >
            <PlotlyChart data={computeVcpuData} layout={computeVcpuLayout} height="100%" />
          </AnalyticsCard>
        </>
      )}
      {isRumEnabled && (
        <div ref={resolvedRumCardRef} className={!effectiveRumVisible ? 'min-h-[360px]' : undefined}>
          {Boolean(rumBreakdown || loadingRumBreakdown || errorRumBreakdown) && (
            <AnalyticsCard
              title="Real User Monitoring (RUM) Operations"
              description="RUM beacon volume and estimated FOS Class A operation cost"
              isLoading={loadingRumBreakdown}
              isFetching={fetchingRumBreakdown}
              error={errorRumBreakdown as AnalyticsCardError | null}
              isEmpty={!rumBreakdown?.data || rumBreakdown.data.length === 0}
              className="min-h-[360px]"
            >
          <div className="space-y-4 p-4">
            {!rumBreakdown ? null : rumBreakdown.total_beacons === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <p className="text-sm mb-2">No RUM beacons collected yet</p>
                <p className="text-xs">
                  Once users visit your site with the RUM script installed, beacons will appear here
                </p>
                <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-950 rounded text-xs text-blue-900 dark:text-blue-100">
                  Cost: ${rumBreakdown.class_a_rate_per_1k?.toFixed(3)} per 1,000 beacons (Class A
                  operations)
                </div>
              </div>
            ) : (
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <p className="text-xs text-muted-foreground">Total Beacons</p>
                  <p className="text-2xl font-bold">
                    {formatAbbreviatedCount(rumBreakdown.total_beacons)}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">
                    {formatCountMaybe(rumBreakdown.average_beacons_per_day, { maximumFractionDigits: 2 })} / day avg
                  </p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground">Class A Operations</p>
                  <p className="text-2xl font-bold">
                    {formatAbbreviatedCount(rumBreakdown.total_estimated_class_a)}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">
                    @${rumBreakdown.class_a_rate_per_1k?.toFixed(3)}/1k
                  </p>
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
            {rumBreakdown?.note && (
              <div className="p-3 bg-muted/50 border rounded text-xs text-muted-foreground">
                {rumBreakdown.note}
              </div>
            )}
          </div>
            </AnalyticsCard>
          )}
        </div>
      )}
    </div>
  )
}
