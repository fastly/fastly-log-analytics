'use client'

import React, { useRef } from 'react'
import { useRouter } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { Activity as ActivityIcon } from 'lucide-react'
import { useTheme } from 'next-themes'
import { AnalyticsCard, useInViewOnce } from '@/components/AnalyticsCard'
import { CostCalculator } from '@/components/CostCalculator/CostCalculator'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import type { ComputeUsageResponse } from '@/lib/cloud-engine/usage/computeSeries'
import type { MeasuredEstimate } from '@/lib/cloud-engine/usage/estimatePrefill'
import { ReportLayout } from '@/components/ReportLayout'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { useIsDataReady } from '@/hooks/useBootstrap'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'
import { adminFetch, client, unwrapClient } from '@/lib/api'
import { estimateLogLineBytes, type LogFieldsCatalogLike } from '@/lib/apiLogFields'
import type { LogFieldsConfig } from '@/types/api'
import { queryKeys } from '@/lib/apiQueryKeys'
import type { ChartInterval } from '@/lib/utils'
import { useFilterStore, useServiceStore } from '@/stores/filterStore'
import {
  FosOperationsHelp,
  UsageChartsGrid,
  type BandwidthUsageData,
  type LogActivityData,
  type OperationsUsageData,
  type RumBreakdownData,
} from './UsageChartsGrid'
import {
  BreakdownTable,
  UsageStatCards,
  type BreakdownRow,
  type StorageUsageData,
} from './UsageStatCards'

export {
  FosOperationsHelp,
  UsageChartsGrid,
  BreakdownTable,
  UsageStatCards,
  type BreakdownRow,
  type StorageUsageData,
  type OperationsUsageData,
  type BandwidthUsageData,
  type LogActivityData,
  type RumBreakdownData,
}

export function UsagePage() {
  return (
    <ReportLayout
      title="System Usage"
      description="Estimate Fastly Object Storage and CDN logging costs."
      icon={ActivityIcon}
      defaultInterval="1 day"
    >
      {(props) => <UsagePageContent {...props} />}
    </ReportLayout>
  )
}

function quantizeUsageTime(value: string | null, shouldQuantize: boolean): string | null {
  if (!value || !shouldQuantize) return value
  const ms = Date.parse(value)
  if (!Number.isFinite(ms) || ms % 60_000 === 0) return value
  return new Date(Math.floor(ms / 60_000) * 60_000).toISOString()
}

export function UsagePageContent({
  startTime,
  endTime,
  activeServiceId,
  config,
  setChartInterval,
}: {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  config: ReportConfiguration
  setChartInterval: (interval: ChartInterval) => void
}) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  const router = useRouter()
  const services = useServiceStore((s) => s.services)
  const shouldQuantizeWindow = useFilterStore((s) => s.relativeRange !== null || s.isAutoRange)
  const queryStartTime = quantizeUsageTime(startTime, shouldQuantizeWindow)
  const queryEndTime = quantizeUsageTime(endTime, shouldQuantizeWindow)
  const activeSvc = services.find((s) => s.id === activeServiceId)
  const isRumEnabled = activeSvc?.rum_enabled ?? false
  const isReady = useIsDataReady()
  const activityBy = config.effectiveInterval.split(' ')[1] || 'hour'
  const estimatorRef = useRef<HTMLDivElement>(null)
  const estimatorVisible = useInViewOnce(estimatorRef, { rootMargin: '100px' })
  const rumCardRef = useRef<HTMLDivElement>(null)
  const rumVisible = useInViewOnce(rumCardRef, { rootMargin: '100px' })
  const cloudMode = isCloudMode()

  // The Cost Estimator lives below the fold; defer /api/usage/prefill until it nears
  // the viewport so initial /usage loads avoid a second compute-billing call and a 7-day
  // cron_runs scan.
  const { data: prefillFull, isLoading: loadingPrefillFull, isError: prefillFullFailed } = useQuery({
    queryKey: ['usage', 'prefill-full', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/usage/prefill', { signal }))
    },
    enabled: isReady && estimatorVisible,
    staleTime: 5 * 60_000,
  })

  const {
    data: storage,
    isLoading: loadingStorage,
    isFetching: fetchingStorage,
    error: errorStorage,
  } = useQuery({
    queryKey: ['usage', 'storage', activeServiceId, queryStartTime, queryEndTime],
    queryFn: async ({ signal }) => {
      const query = {
        start: queryStartTime ?? undefined,
        end: queryEndTime ?? undefined,
        ...(cloudMode ? { include_intervals: '0' } : {}),
      } as { start?: string; end?: string }
      return unwrapClient(
        client.GET('/api/usage/current-storage', {
          signal,
          params: { query },
        }),
      )
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const {
    data: ops,
    isLoading: loadingOps,
    isFetching: fetchingOps,
    error: errorOps,
  } = useQuery({
    queryKey: ['usage', 'operations', activeServiceId, queryStartTime, queryEndTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/operations', {
          signal,
          params: {
            query: {
              start: queryStartTime ?? undefined,
              end: queryEndTime ?? undefined,
              by: activityBy,
            },
          },
        }),
      )
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const {
    data: bw,
    isLoading: loadingBw,
    isFetching: fetchingBw,
    error: errorBw,
  } = useQuery({
    queryKey: ['usage', 'bandwidth', activeServiceId, queryStartTime, queryEndTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/bandwidth', {
          signal,
          params: {
            query: {
              start: queryStartTime ?? undefined,
              end: queryEndTime ?? undefined,
              by: activityBy,
            },
          },
        }),
      )
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  const {
    data: logActivity,
    isLoading: loadingActivity,
    isFetching: fetchingActivity,
    error: errorActivity,
  } = useQuery({
    queryKey: ['usage', 'log-activity', activeServiceId, queryStartTime, queryEndTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/log-activity', {
          signal,
          params: {
            query: {
              start: queryStartTime ?? undefined,
              end: queryEndTime ?? undefined,
              by: activityBy,
            },
          },
        }),
      )
    },
    enabled: isReady,
    staleTime: 60_000,
  })

  // RUM Operations sits at the bottom of the charts grid (below the fold); defer
  // /api/usage/rum-breakdown until it nears the viewport so initial Cloud /usage loads
  // avoid booting DuckDB-WASM and fetching raw RUM Parquet files.
  const {
    data: rumBreakdown,
    isLoading: loadingRumBreakdown,
    isFetching: fetchingRumBreakdown,
    error: errorRumBreakdown,
  } = useQuery({
    queryKey: ['usage', 'rum-breakdown', activeServiceId, queryStartTime, queryEndTime],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/rum-breakdown', {
          signal,
          params: {
            query: { start: queryStartTime ?? undefined, end: queryEndTime ?? undefined, by: 'day' },
          },
        }),
      )
    },
    enabled: isReady && isRumEnabled && rumVisible,
    staleTime: 60_000,
  })

  // /prefill is split into /rates (FAST, local-config only) + /prefill (FULL,
  // Fastly stats + edge_ratio). Stat cards always need rates, so fire that
  // immediately. The Cost Estimator needs the full payload but lives below
  // the fold — defer the slow call until it scrolls into view.
  const { data: prefillRates } = useQuery({
    queryKey: ['usage', 'prefill-rates', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/usage/prefill/rates', { signal }))
    },
    enabled: isReady,
    staleTime: 5 * 60_000,
  })

  const { data: cloudLogFields } = useQuery({
    queryKey: ['usage', 'cloud-log-fields', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/services/{service_id}/log-fields', {
        params: { path: { service_id: activeServiceId! } },
        signal,
      }))
    },
    enabled: isReady && cloudMode && estimatorVisible && Boolean(activeServiceId),
    staleTime: 5 * 60_000,
  })

  const { data: cloudLogFieldsCatalog } = useQuery({
    queryKey: queryKeys.logFieldsCatalog(activeServiceId ?? undefined),
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/log-fields/catalog', { signal }))
    },
    enabled: isReady && cloudMode && estimatorVisible && Boolean(activeServiceId),
    staleTime: 5 * 60_000,
  })

  const cloudEstimatedBytesPerLine = cloudMode
    ? estimateLogLineBytes(
        cloudLogFields?.log_fields as LogFieldsConfig | undefined,
        cloudLogFieldsCatalog as LogFieldsCatalogLike,
      )
    : null

  const storageWithMeta = storage as StorageUsageData | undefined
  const opsWithMeta = ops as (typeof ops & {
    source?: string
    complete?: boolean
    provenance?: string[]
  }) | undefined
  const localStorageGbHours = storageWithMeta?.total_billed_gb_hours ?? null
  const localStoragePeriodHours = startTime && endTime
    ? (Date.parse(endTime) - Date.parse(startTime)) / 3_600_000
    : null

  // One Platform Metrics read per range, shared by the cost card and both Compute charts.
  const {
    data: compute,
    isLoading: loadingCompute,
    isFetching: fetchingCompute,
    error: errorCompute,
  } = useQuery({
    queryKey: ['usage', 'compute', activeServiceId, queryStartTime, queryEndTime],
    queryFn: async ({ signal }): Promise<ComputeUsageResponse> => {
      const params = new URLSearchParams()
      if (queryStartTime) params.set('start_time', queryStartTime)
      if (queryEndTime) params.set('end_time', queryEndTime)
      const response = await adminFetch(`/api/usage/compute?${params}`, { signal })
      if (!response.ok) throw new Error(`Compute usage failed (${response.status})`)
      return response.json()
    },
    enabled: isReady && cloudMode,
    staleTime: 60_000,
  })

  const prefill = prefillFull ?? prefillRates
  const cloudPrefill = cloudMode ? prefillFull as unknown as Partial<MeasuredEstimate> | undefined : undefined
  const rateSettings = cloudMode ? cloudPrefill?.rates ?? prefillRates : prefill
  const loadingPrefill = !prefillRates && !prefillFull

  const isFetchingAny =
    fetchingStorage || fetchingOps || fetchingBw || fetchingActivity || fetchingRumBreakdown
  const isLoadingInitial = loadingStorage || loadingOps || loadingBw || loadingActivity

  const totalClassA = ops?.total_class_a ?? 0
  const totalClassB = ops?.total_class_b ?? 0
  const totalBwGB = (bw?.total_bytes ?? 0) / 1e9

  const rateA = rateSettings?.class_a_rate_per_1k ?? 0.0025
  const rateBPer10k = rateSettings?.class_b_rate_per_10k ?? 0.004
  const rateB = rateBPer10k / 10
  const rateEgress = rateSettings?.cdn_egress_rate_per_gb ?? 0.12
  const rateStorage = rateSettings?.storage_rate_per_gb_month ?? 0.02

  const prefillNote =
    prefill && !loadingPrefill && (prefillFull || prefillFullFailed)
      ? prefillFull?.requests_per_day == null || (prefill.edge_only && prefillFull?.edge_requests_per_day == null)
        ? 'Traffic measurements are unavailable; reference traffic assumptions are shown. Service configuration and rates are pre-filled where available.'
        : 'Calculator pre-filled from your current service configuration.'
      : undefined

  return (
    <>
      <UsageStatCards
        storage={storageWithMeta}
        loadingStorage={loadingStorage}
        errorStorage={errorStorage}
        loadingOps={loadingOps}
        errorOps={errorOps}
        loadingBw={loadingBw}
        errorBw={errorBw}
        isFetchingAny={isFetchingAny}
        isLoadingInitial={isLoadingInitial}
        totalClassA={totalClassA}
        totalClassB={totalClassB}
        totalBwGB={totalBwGB}
        rateA={rateA}
        rateB={rateB}
        rateBPer10k={rateBPer10k}
        rateEgress={rateEgress}
        rateStorage={rateStorage}
        activeServiceId={activeServiceId}
        onNavigateToAdmin={(href) => router.push(href)}
        compute={compute}
        localStorageGbHours={localStorageGbHours}
        storageBillingComplete={storageWithMeta?.storage_billing_complete}
        storageBillingProvenance={storageWithMeta?.storage_billing_provenance}
        operationsSource={opsWithMeta?.source}
        operationsComplete={opsWithMeta?.complete}
        cloudMode={cloudMode}
      />

      <UsageChartsGrid
        ops={ops}
        loadingOps={loadingOps}
        fetchingOps={fetchingOps}
        errorOps={errorOps}
        bw={bw}
        loadingBw={loadingBw}
        fetchingBw={fetchingBw}
        errorBw={errorBw}
        logActivity={logActivity}
        loadingActivity={loadingActivity}
        fetchingActivity={fetchingActivity}
        errorActivity={errorActivity}
        rumBreakdown={rumBreakdown}
        loadingRumBreakdown={loadingRumBreakdown}
        fetchingRumBreakdown={fetchingRumBreakdown}
        errorRumBreakdown={errorRumBreakdown}
        rumCardRef={rumCardRef}
        rumVisible={rumVisible}
        isRumEnabled={isRumEnabled}
        isFetchingAny={isFetchingAny}
        isLoadingInitial={isLoadingInitial}
        isDark={isDark}
        rateA={rateA}
        rateB={rateB}
        rateEgress={rateEgress}
        effectiveInterval={config.effectiveInterval}
        setChartInterval={setChartInterval}
        showCompute={cloudMode}
        compute={compute}
        loadingCompute={loadingCompute}
        fetchingCompute={fetchingCompute}
        errorCompute={errorCompute}
      />

      <div
        ref={estimatorRef}
        className={
          cloudMode
            ? 'min-h-[920px] [contain-intrinsic-size:920px] [content-visibility:auto]'
            : 'min-h-[680px] [contain-intrinsic-size:680px] [content-visibility:auto]'
        }
      >
        <AnalyticsCard
          title="Cost Estimator"
          description={cloudMode
            ? 'Estimate monthly Cloud Compute, storage, CDN and store costs from your service and contract rates.'
            : 'Estimate your monthly FOS cost based on your configuration and contract rates. Pre-filled from your active service and rate settings.'}
        >
          {loadingPrefill || (estimatorVisible && loadingPrefillFull) ? (
            <div className={cloudMode ? 'space-y-3 min-h-[820px]' : 'space-y-3 min-h-[580px]'}>
              <SkeletonGrid count={4} height="36px" className="rounded-md" />
            </div>
          ) : cloudMode ? (
            <CloudCostCalculator
              key={activeServiceId}
              prefillData={cloudPrefill}
              rates={prefillRates}
              estimatedBytesPerLine={cloudEstimatedBytesPerLine}
              localStorageGbHours={localStorageGbHours}
              localStoragePeriodHours={localStoragePeriodHours}
            />
          ) : (
            <CostCalculator
              prefillData={prefill ?? undefined}
              prefillNote={prefillNote}
              localStorageGbHours={localStorageGbHours}
              localStoragePeriodHours={localStoragePeriodHours}
            />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}

export const UsageClient = UsagePage

export default UsagePage
