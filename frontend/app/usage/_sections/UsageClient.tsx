'use client'

import React, { useRef } from 'react'
import { useRouter } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { Activity as ActivityIcon } from 'lucide-react'
import { useTheme } from 'next-themes'
import { AnalyticsCard, useInViewOnce } from '@/components/AnalyticsCard'
import { CostCalculator } from '@/components/CostCalculator/CostCalculator'
import { ReportLayout } from '@/components/ReportLayout'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { useIsDataReady } from '@/hooks/useBootstrap'
import type { ReportConfiguration } from '@/hooks/useDashboardCards'
import { client, unwrapClient } from '@/lib/api'
import type { ChartInterval } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
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
  const activeSvc = services.find((s) => s.id === activeServiceId)
  const isRumEnabled = activeSvc?.rum_enabled ?? false
  const isReady = useIsDataReady()
  const activityBy = config.effectiveInterval.split(' ')[1] || 'hour'

  const {
    data: storage,
    isLoading: loadingStorage,
    isFetching: fetchingStorage,
    error: errorStorage,
  } = useQuery({
    queryKey: ['usage', 'storage', activeServiceId, startTime, endTime],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/current-storage', {
          signal,
          params: { query: { start: startTime ?? undefined, end: endTime ?? undefined } },
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
    queryKey: ['usage', 'operations', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/operations', {
          signal,
          params: {
            query: {
              start: startTime ?? undefined,
              end: endTime ?? undefined,
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
    queryKey: ['usage', 'bandwidth', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/bandwidth', {
          signal,
          params: {
            query: {
              start: startTime ?? undefined,
              end: endTime ?? undefined,
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
    queryKey: ['usage', 'log-activity', activeServiceId, startTime, endTime, activityBy],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/log-activity', {
          signal,
          params: {
            query: {
              start: startTime ?? undefined,
              end: endTime ?? undefined,
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
    data: rumBreakdown,
    isLoading: loadingRumBreakdown,
    isFetching: fetchingRumBreakdown,
    error: errorRumBreakdown,
  } = useQuery({
    queryKey: ['usage', 'rum-breakdown', activeServiceId, startTime, endTime],
    queryFn: async ({ signal }) => {
      return unwrapClient(
        client.GET('/api/usage/rum-breakdown', {
          signal,
          params: {
            query: { start: startTime ?? undefined, end: endTime ?? undefined, by: 'day' },
          },
        }),
      )
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
      return unwrapClient(client.GET('/api/usage/prefill/rates', { signal }))
    },
    enabled: isReady,
    staleTime: 5 * 60_000,
  })

  const estimatorRef = useRef<HTMLDivElement>(null)
  const estimatorVisible = useInViewOnce(estimatorRef, { rootMargin: '600px' })

  const { data: prefillFull, isLoading: loadingPrefillFull } = useQuery({
    queryKey: ['usage', 'prefill-full', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/usage/prefill', { signal }))
    },
    enabled: isReady && estimatorVisible,
    staleTime: 5 * 60_000,
  })

  const prefill = prefillFull ?? prefillRates
  const loadingPrefill = !prefillRates && !prefillFull

  const isFetchingAny =
    fetchingStorage || fetchingOps || fetchingBw || fetchingActivity || fetchingRumBreakdown
  const isLoadingInitial = loadingStorage || loadingOps || loadingBw || loadingActivity

  const totalClassA = ops?.total_class_a ?? 0
  const totalClassB = ops?.total_class_b ?? 0
  const totalBwGB = (bw?.total_bytes ?? 0) / 1e9

  const rateA = prefill?.class_a_rate_per_1k ?? 0.005
  const rateBPer10k = prefill?.class_b_rate_per_10k ?? 0.01
  const rateB = rateBPer10k / 10
  const rateEgress = prefill?.cdn_egress_rate_per_gb ?? 0.12
  const rateStorage = prefill?.storage_rate_per_gb_month ?? 0.02

  const prefillNote =
    prefill && !loadingPrefill
      ? 'Calculator pre-filled from your current service configuration.'
      : undefined

  return (
    <>
      <UsageStatCards
        storage={storage}
        loadingStorage={loadingStorage}
        errorStorage={errorStorage}
        loadingOps={loadingOps}
        loadingBw={loadingBw}
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
        isRumEnabled={isRumEnabled}
        isFetchingAny={isFetchingAny}
        isLoadingInitial={isLoadingInitial}
        isDark={isDark}
        rateA={rateA}
        rateB={rateB}
        rateEgress={rateEgress}
        effectiveInterval={config.effectiveInterval}
        setChartInterval={setChartInterval}
      />

      <div
        ref={estimatorRef}
        className="min-h-[420px] [contain-intrinsic-size:420px] [content-visibility:auto]"
      >
        <AnalyticsCard
          title="Cost Estimator"
          description="Estimate your monthly FOS cost based on your configuration and contract rates. Pre-filled from your active service and rate settings."
        >
          {loadingPrefill || (estimatorVisible && loadingPrefillFull) ? (
            <div className="space-y-3">
              <SkeletonGrid count={4} height="36px" className="rounded-md" />
            </div>
          ) : (
            <CostCalculator prefillData={prefill ?? undefined} prefillNote={prefillNote} />
          )}
        </AnalyticsCard>
      </div>
    </>
  )
}

export const UsageClient = UsagePage

export default UsagePage
