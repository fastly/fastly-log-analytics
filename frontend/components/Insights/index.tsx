'use client'

import React, { useMemo, useState } from 'react'
import { useQueryState, parseAsString } from 'nuqs'
import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query'
import Link from 'next/link'
import {
  AlertCircle,
  Info,
  Lightbulb,
  Loader2,
} from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import {
  useDebouncedFilterPayload,
  useDebounce,
  useInsightsDefaults,
} from '@/hooks/useDashboardCards'
import { useEffectiveServiceId } from '@/hooks/useBootstrap'
import { useDateFormat } from '@/hooks/useSSE'
import { client, queryKeys, unwrapClient } from '@/lib/api'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import {
  groupInsightsBySection,
  historyHoursFromExtents,
  normalizeTabKey,
  SECTION_LOADING_MESSAGES,
} from '@/lib/utils'
import { useSessionRoleStore } from '@/stores/filterStore'
import type { FiltersPayload, InsightCardData } from '@/types/api'
import { InsightCard } from './InsightCard'
import { InsightCardSkeleton } from './shared'
import { InsightTabs, InsightsHeaderControls } from './InsightTabsAndControls'

export * from './types'
export * from './shared'
export * from './InsightCard'
export * from './InsightDetailModal'
export { getSecurityContent } from './InsightHelpModal/sections/security'

// Lifted out of the ReportLayout render-prop so the hooks live at the
// top of a stable component instead of being recreated every time
// ReportLayout re-renders. Same shape as DashboardBody (item 30).
// Without this lift, React Query treats every ReportLayout re-render
// as a fresh mount and re-fires the /api/insights + /api/insight-
// availability requests — the local-dev duplicate-fetch pattern.
// High-Scale answers before a slow evaluation finishes: `pending` (first run, placeholder
// cards) or `refreshing` (previous result while a newer one runs). Cloud omits both.
const INSIGHTS_POLL_MS = 5000
type InsightsStatus = { pending?: boolean; refreshing?: boolean } | undefined
const isEvaluating = (data: InsightsStatus) => data?.pending === true || data?.refreshing === true
const isPendingPlaceholder = (insight: InsightCardData) =>
  'placeholder' in insight && insight.placeholder === true

interface InsightsBodyProps {
  activeServiceId: string | null | undefined
  windowHours: string
  baselineHours: string
  statusFilter: string
  filterPayload?: FiltersPayload
  initialTab?: string | null
  relative: (iso: string) => string
  full: (iso: string) => string
  abbr: () => string
  prefetchMonthBaseline?: boolean
  scanReady?: boolean
}

function InsightsBody({
  activeServiceId,
  windowHours,
  baselineHours,
  statusFilter,
  filterPayload,
  initialTab,
  relative,
  full,
  abbr,
  prefetchMonthBaseline = false,
  scanReady = true,
}: InsightsBodyProps) {
  // Which section tab is active. Controlled here (not inside InsightTabs) so the
  // selection survives the loading→loaded swap between the two InsightTabs
  // instances and the status-filter regrouping.
  const [activeTab, setActiveTab] = useQueryState('tab', parseAsString.withDefault(normalizeTabKey(initialTab)))
  const hasActiveFilters = Boolean(filterPayload && Object.keys(filterPayload).length > 0)

  const { data, isLoading: scanLoading, error, isSuccess, isFetching } = useQuery({
    queryKey: hasActiveFilters
      ? ['insights', activeServiceId, windowHours, baselineHours, filterPayload]
      : ['insights', activeServiceId, windowHours, baselineHours],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.POST('/api/insights', {
        signal,
        body: {
          window_size_hrs: parseFloat(windowHours),
          baseline_hours: parseFloat(baselineHours),
          filters: filterPayload ?? {},
          force_refresh: false,
        },
      }))
    },
    enabled: !!activeServiceId && scanReady,
    staleTime: (query) => (isEvaluating(query.state.data) ? 0 : 60000),
    refetchInterval: (query) => (isEvaluating(query.state.data) ? INSIGHTS_POLL_MS : false),
    placeholderData: keepPreviousData,
  })
  const isLoading = scanLoading || (!!activeServiceId && !scanReady && !data && !error)

  // The 24h/720h month comparison is a full-retention scan; start it only after the
  // selected scan has finished so the two never compete for the same backend.
  const queryClient = useQueryClient()
  const primarySettled = isSuccess && !isFetching && !isEvaluating(data)
  const monthPrefetchedFor = React.useRef<string | null>(null)
  React.useEffect(() => {
    if (!activeServiceId || !prefetchMonthBaseline || !primarySettled) return
    if (monthPrefetchedFor.current === activeServiceId) return
    const timer = setTimeout(() => {
      monthPrefetchedFor.current = activeServiceId
      void queryClient.prefetchQuery({
        queryKey: ['insights', activeServiceId, '24', '720'],
        queryFn: async ({ signal }) => {
          return unwrapClient(client.POST('/api/insights', {
            signal,
            // Cloud loads a speculative prefetch only within its own smaller budget.
            headers: { 'x-flp-cloud-prefetch': '1' },
            body: {
              window_size_hrs: 24,
              baseline_hours: 720,
              filters: {},
              force_refresh: false,
            },
          }))
        },
        staleTime: 60_000,
      })
    }, 3000)
    return () => clearTimeout(timer)
  }, [activeServiceId, prefetchMonthBaseline, primarySettled, queryClient])

  const { data: availability, error: availabilityError } = useQuery({
    queryKey: ['insights', 'availability', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/insight-availability', { signal }))
    },
    enabled: !!activeServiceId,
    // The active-insights list is derived from the service's column schema
    // and effectively never changes within a session. Long-cache it so a
    // warm navigation paints the per-insight skeleton cards instantly
    // instead of flashing an empty state for one round trip.
    staleTime: 5 * 60 * 1000,
  })

  const isPending = data?.pending === true
  const allInsights = data?.insights
  const pendingInsights = useMemo(
    () => (allInsights ?? []).filter((insight) => isPendingPlaceholder(insight)),
    [allInsights],
  )
  // Queued placeholders stay in the grid (as skeletons, in their section) so the
  // placeholder → result swap keeps every card's slot; they are never counted.
  const filteredInsights = useMemo(() => {
    if (!allInsights) return []
    const findings = allInsights.filter((insight) => !isPendingPlaceholder(insight))
    const shown = statusFilter === 'all' ? findings : findings.filter((insight) => insight.severity === statusFilter)
    return [...shown, ...pendingInsights]
  }, [allInsights, pendingInsights, statusFilter])
  // A queued evaluation with no cards yet keeps the availability skeleton grid.
  const showSkeletons = isLoading || (isPending && (allInsights?.length ?? 0) === 0)
  const refreshError = data?.refresh_error
  const reserveStatusSlot = !isCloudMode() || isEvaluating(data) || !!refreshError

  // Skeleton cards rendered while /api/insights is in flight come from the
  // /api/insight-availability response (titles + descriptions per available
  // insight). Single render path during loading — no SkeletonGrid → per-
  // insight swap; the only transition is content-fill when real data lands.
  const availableInsights = useMemo(() => {
    const list = availability?.insights
    if (!list) return []
    return list.filter((i) => i.available !== false)
  }, [availability])
  const unsupportedInsights = availability?.unavailable?.filter(
    (entry) => 'reason' in entry && entry.reason === 'cloud_detector_unsupported',
  ) ?? []
  const missingFieldCount = (availability?.unavailable?.length ?? 0) - unsupportedInsights.length

  // Group both real cards and skeleton entries by section, in the same fixed
  // order, so the loading tab bar mirrors the loaded tab bar (CLS). Skeleton
  // entries have no severity → sortInsights orders them by title.
  const groupedInsights = useMemo(
    () =>
      groupInsightsBySection(filteredInsights, {
        includeAll: true,
        includeEmptySections: true,
      }),
    [filteredInsights],
  )
  const groupedSkeletons = useMemo(
    () =>
      groupInsightsBySection(availableInsights, {
        includeAll: true,
        includeEmptySections: true,
      }),
    [availableInsights],
  )

  return (
    <>
      {availabilityError && !availability && (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Availability check failed</AlertTitle>
          <AlertDescription className="text-xs">
            Couldn&apos;t determine which insights apply to this service — results below may be incomplete.
          </AlertDescription>
        </Alert>
      )}

      {/* Fixed one-line slot, mounted before the first response, so background
          status and refresh errors never push the cards down (CLS). */}
      {reserveStatusSlot && (
        <div data-testid="insights-status-slot" className="flex h-5 min-w-0 items-center gap-4 text-sm">
          {data && isEvaluating(data) && (
            <span role="status" className="flex shrink-0 items-center gap-2 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin text-primary" aria-hidden="true" />
              {data.pending || !data.computed_at
                ? <span>Evaluating detectors in the background…</span>
                : <span>Refreshing (computed {relative(data.computed_at)})</span>}
            </span>
          )}
          {refreshError && (
            <span role="alert" title={refreshError} className="flex min-w-0 items-center gap-1.5 text-destructive">
              <AlertCircle className="h-4 w-4 shrink-0" aria-hidden="true" />
              <span className="truncate">Last refresh failed: {refreshError}</span>
            </span>
          )}
        </div>
      )}

      {showSkeletons ? (
        groupedSkeletons.length > 0 ? (
          <InsightTabs
            groups={groupedSkeletons}
            activeTab={activeTab}
            onTabChange={setActiveTab}
            isLoading={true}
            renderCards={({ cards, section }) => (
              <div
                className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
                data-testid="insights-grid-loading"
              >
                {cards.map((i) => (
                  <InsightCardSkeleton
                    key={i.id}
                    title={i.title ?? ''}
                    description={i.description}
                    loadingMessage={SECTION_LOADING_MESSAGES[section.key] ?? 'Analyzing log patterns…'}
                  />
                ))}
              </div>
            )}
          />
        ) : (
          <div className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]">
            <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
            <span>Evaluating anomaly detector baseline comparisons…</span>
          </div>
        )
      ) : error ? (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error loading insights</AlertTitle>
          <AlertDescription>
            {error instanceof Error ? error.message : 'An unknown error occurred'}
          </AlertDescription>
        </Alert>
      ) : (
        <InsightTabs
          groups={groupedInsights}
          activeTab={activeTab}
          onTabChange={setActiveTab}
          isLoading={isPending}
          isCounted={(insight) => !isPendingPlaceholder(insight)}
          emptyMessage={isCloudMode() ? ({ section }) => {
            const supported = availableInsights.some((entry) => section.key === 'all' || entry.category === section.key)
            return supported && statusFilter !== 'all'
              ? { title: 'No insights match this status', description: 'Change the status filter to see the evaluated detectors.' }
              : { title: 'No detectors available in this category', description: 'No analysis was performed here. Check the Cloud algorithm and log-field availability below.' }
          } : undefined}
          renderCards={({ cards }) => (
            <div
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
              data-testid="insights-grid"
            >
              {cards.map((insight) => isPendingPlaceholder(insight) ? (
                <InsightCardSkeleton
                  key={insight.id}
                  title={insight.title}
                  description={insight.description}
                  loadingMessage={insight.summary}
                />
              ) : (
                <InsightCard
                  key={insight.id}
                  insight={insight}
                  windowHours={windowHours}
                  baselineHours={baselineHours}
                />
              ))}
            </div>
          )}
        />
      )}

      <div className="min-h-[16px] text-[10px] text-muted-foreground text-right italic">
        {data && data.computed_at && (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className="" />}>
                Computed {relative(data.computed_at)}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {full(data.computed_at)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}
      </div>

      <div className="min-h-[68px]">
        {availability?.unavailable && availability.unavailable.length > 0 && (
          // Informational, not a failure: Cloud always lists its unimplemented detectors here.
          <Alert role="status" className="border-amber-500/30 bg-amber-500/5">
            <Info className="h-4 w-4 text-amber-500 shrink-0" />
            <AlertTitle className="text-amber-600 dark:text-amber-400 font-medium">
              {availability.unavailable.length} {availability.unavailable.length === 1 ? 'insight is' : 'insights are'} unavailable
            </AlertTitle>
            <AlertDescription className="text-xs text-muted-foreground mt-1 flex flex-wrap items-center justify-between gap-2">
              <span>
                {isCloudMode() ? (
                  <>
                    Cloud evaluates {availableInsights.length} detectors using local Parquet data.
                    {unsupportedInsights.length > 0 && ` ${unsupportedInsights.length} detector algorithms are not implemented in Cloud; enabling log fields will not enable them.`}
                    {missingFieldCount > 0 && ` ${missingFieldCount} additional detectors need log fields that are missing from the loaded data.`}
                    {' '}Detectors without enough baseline history are marked as not evaluated, not clean.
                  </>
                ) : (
                  <>{availability.unavailable.length} {availability.unavailable.length === 1 ? 'detector requires' : 'detectors require'} optional log fields to be enabled. Check service configuration.</>
                )}
              </span>
              {(!isCloudMode() || missingFieldCount > 0) && <Link
                href={activeServiceId ? `/admin?service=${activeServiceId}` : '/admin'}
                className="inline-flex items-center text-xs font-semibold text-primary underline underline-offset-4 hover:text-primary/80"
              >
                Configure Service Log Fields &rarr;
              </Link>}
              {unsupportedInsights.length > 0 && (
                <details className="w-full">
                  <summary className="cursor-pointer">Unavailable Cloud algorithms</summary>
                  <p className="mt-1">{unsupportedInsights.map((entry) => entry.title).join(', ')}</p>
                </details>
              )}
            </AlertDescription>
          </Alert>
        )}
      </div>
    </>
  )
}

export interface InsightsClientProps {
  initialTab?: string | null
  initialWindow?: string | null
  initialBaseline?: string | null
}

export function InsightsClient({
  initialTab,
  initialWindow,
  initialBaseline,
}: InsightsClientProps = {}) {
  // Window/baseline default adapt to how much history the active service has
  // (e.g. ~2h of data → "this hour vs the previous hour" instead of the 7-day
  // baseline that would just say "not enough data"). An explicit pick wins.
  const activeServiceId = useEffectiveServiceId()
  const isRemoteAnalyst = useSessionRoleStore((s) => s.isRemoteAnalyst)
  const defaults = useInsightsDefaults(activeServiceId)
  const filterPayload = useDebouncedFilterPayload(true)

  const [selectedWindow, setSelectedWindow] = useQueryState('window', initialWindow ? parseAsString.withDefault(initialWindow) : parseAsString)
  const [selectedBaseline, setSelectedBaseline] = useQueryState('baseline', initialBaseline ? parseAsString.withDefault(initialBaseline) : parseAsString)

  const windowHours = selectedWindow ?? defaults.windowHours
  const baselineHours = selectedBaseline ?? defaults.baselineHours

  const setWindowHours = (val: string) => {
    void setSelectedWindow(val)
    defaults.setWindowHours(val)
  }
  const setBaselineHours = (val: string) => {
    void setSelectedBaseline(val)
    defaults.setBaselineHours(val)
  }

  const [statusFilter, setStatusFilter] = useQueryState('status', parseAsString.withDefault('all'))
  const [isRescanning, setIsRescanning] = useState(false)
  const [rescanError, setRescanError] = useState<string | null>(null)
  const { relative, full, abbr } = useDateFormat()

  const queryClient = useQueryClient()
  const { data: extentsData, isPending: extentsPending } = useQuery({
    queryKey: queryKeys.logExtents(activeServiceId),
    queryFn: async () => {
      return unwrapClient(client.GET('/api/log-extents'))
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
  })

  const monthHistory = historyHoursFromExtents(extentsData?.earliest_log_at)
  const prefetchMonthBaseline = monthHistory !== null && monthHistory >= 720

  // Debounce window/baseline so chained dropdown changes (e.g. switching
  // both window AND baseline in succession) only fire one /api/insights
  // request instead of two. 400ms gives the user enough time to flip
  // both selects without firing the intermediate query.
  const appliedWindowHours = useDebounce(windowHours, 400)
  const appliedBaselineHours = useDebounce(baselineHours, 400)
  // Until the history-adapted defaults resolve (and the debounce catches up to
  // them), the applied pair is the static 24h/7d fallback; scanning it would
  // start a full-baseline scan that is cancelled moments later.
  const defaultsResolved = (selectedWindow != null && selectedBaseline != null) || !extentsPending
  const scanReady = defaultsResolved && appliedWindowHours === windowHours && appliedBaselineHours === baselineHours

  const handleRescan = async () => {
    if (!activeServiceId || isRescanning) return
    setIsRescanning(true)
    setRescanError(null)
    const hasActiveFilters = Boolean(filterPayload && Object.keys(filterPayload).length > 0)
    try {
      const data = await unwrapClient(client.POST('/api/insights', {
        body: {
          window_size_hrs: parseFloat(appliedWindowHours),
          baseline_hours: parseFloat(appliedBaselineHours),
          filters: filterPayload ?? {},
          force_refresh: true,
        },
      }))
      if (data) {
        queryClient.setQueryData(
          hasActiveFilters
            ? ['insights', activeServiceId, appliedWindowHours, appliedBaselineHours, filterPayload]
            : ['insights', activeServiceId, appliedWindowHours, appliedBaselineHours],
          data,
        )
      }
      await queryClient.invalidateQueries({
        queryKey: ['insights', activeServiceId],
      })
    } catch (err) {
      setRescanError(err instanceof Error ? err.message : 'An unknown error occurred')
    } finally {
      setIsRescanning(false)
    }
  }

  const headerControls = (
    <InsightsHeaderControls
      statusFilter={statusFilter}
      setStatusFilter={(val) => void setStatusFilter(val)}
      windowHours={windowHours}
      setWindowHours={setWindowHours}
      baselineHours={baselineHours}
      setBaselineHours={setBaselineHours}
      isRemoteAnalyst={isRemoteAnalyst}
      isRescanning={isRescanning}
      activeServiceId={activeServiceId}
      onRescan={handleRescan}
    />
  )

  return (
    <ReportLayout
      title="Anomaly Detection"
      description="Automated insights comparing recent traffic to historical baselines."
      icon={Lightbulb}
      headerActions={headerControls}
    >
      {(ctx) => (
        <>
          {rescanError && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>Rescan failed</AlertTitle>
              <AlertDescription>{rescanError}</AlertDescription>
            </Alert>
          )}
          <InsightsBody
            activeServiceId={ctx.activeServiceId}
            windowHours={appliedWindowHours}
            baselineHours={appliedBaselineHours}
            statusFilter={statusFilter}
            filterPayload={ctx.filterPayload}
            initialTab={initialTab}
            relative={relative}
            full={full}
            abbr={abbr}
            prefetchMonthBaseline={prefetchMonthBaseline}
            scanReady={scanReady}
          />
        </>
      )}
    </ReportLayout>
  )
}

export default InsightsClient
