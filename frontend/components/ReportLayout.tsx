'use client'

import React, { useEffect } from 'react'
import Link from 'next/link'
import { parseAsString, useQueryStates } from 'nuqs'
import { useQueryClient } from '@tanstack/react-query'
import { useEffectiveServiceId, useIsDataReady, useBootstrapResolved } from '@/hooks/useBootstrap'
import { useTimeRange } from '@/hooks/useDashboardCards'
import { useTimezone } from '@/hooks/useSSE'
import { useReportConfig, type ReportConfiguration } from '@/hooks/useDashboardCards'
import { useDebouncedFilterPayload } from '@/hooks/useDashboardCards'
import { applySavedViewToStore } from '@/components/FilterBar/FilterBar'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/filterStore'
import { client, unwrapClient, queryKeys } from "@/lib/api";
import { Button, buttonVariants } from '@/components/ui/button'
import { ButtonGroup } from '@/components/ui/button-group'
import { PageHeader } from '@/components/ui/confirm-dialog'
import {
  DashboardSkeleton,
  TableSkeleton,
  FormSkeleton,
  EditorSplitSkeleton,
  AuthCardSkeleton,
} from '@/components/ui/skeleton'
import { INTERVALS, INTERVAL_SECONDS, cn, type ChartInterval } from "@/lib/utils";
import { ArrowLeft, Loader2, type LucideIcon } from 'lucide-react'
import type { components } from '@/types/api.generated'
import type { FiltersPayload } from '@/types/api'

export {
  DashboardSkeleton,
  TableSkeleton,
  FormSkeleton,
  EditorSplitSkeleton,
  AuthCardSkeleton,
}

type ButtonVariant = 'default' | 'outline' | 'secondary' | 'ghost' | 'destructive' | 'link'

export function BackToAdminLink({
  variant = 'outline',
  prefetch = false,
}: {
  variant?: ButtonVariant
  prefetch?: boolean
}) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const href = activeServiceId ? `/admin?service=${activeServiceId}` : '/admin'
  return (
    <Link
      href={href}
      prefetch={prefetch}
      className={buttonVariants({ variant, size: 'sm' })}
      data-testid="back-to-admin-link"
    >
      <ArrowLeft className="h-4 w-4 mr-1" />
      Back to Admin
    </Link>
  )
}

interface NoServiceSelectedProps {
  icon: LucideIcon
  message: string
  title?: string
}

export function NoServiceSelected({
  icon: Icon,
  message,
  title = 'No Service Selected',
}: NoServiceSelectedProps) {
  return (
    <div className="flex flex-col items-center justify-center min-h-[40vh] max-h-[60vh] py-16 text-center">
      <Icon className="h-10 w-10 text-muted-foreground mb-4" />
      <h2 className="text-xl font-semibold">{title}</h2>
      <p className="text-muted-foreground">{message}</p>
    </div>
  )
}

interface ReportShellProps {
  title: string
  description?: string | React.ReactNode
  icon?: LucideIcon
  headerActions?: React.ReactNode
  children: React.ReactNode
  isReadyOverride?: boolean
  requireService?: boolean
  className?: string
  queryError?: { message: string; onRetry?: () => void } | null
}

export function ReportShell({
  title,
  description,
  icon: Icon,
  headerActions,
  children,
  isReadyOverride,
  requireService = true,
  className,
  queryError
}: ReportShellProps) {
  const activeServiceId = useEffectiveServiceId()
  const isDataReady = useIsDataReady()

  const isReady = isReadyOverride ?? (requireService ? isDataReady : true)
  const bootstrapResolved = useBootstrapResolved()

  if (requireService && !activeServiceId && bootstrapResolved) {
    const FallbackIcon = Icon || Loader2
    return (
      <NoServiceSelected
        icon={FallbackIcon}
        message={`Please select a service from the header to view ${title.toLowerCase()}.`}
      />
    )
  }

  return (
    <div className={cn("space-y-6", className)}>
      <PageHeader title={title} description={description}>
        {headerActions}
      </PageHeader>

      {queryError ? (
        <div
          role="alert"
          className="flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100"
        >
          <div className="font-semibold">Failed to load {title.toLowerCase()}.</div>
          <div className="font-mono text-xs opacity-80 break-all">{queryError.message}</div>
          {queryError.onRetry ? (
            <button
              type="button"
              onClick={queryError.onRetry}
              className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
            >
              Retry
            </button>
          ) : null}
        </div>
      ) : null}

      {!isReady ? (
        <DashboardSkeleton />
      ) : (
        children
      )}
    </div>
  )
}

// All sync URL → filterStore mapping (range, ?filters, ?filter_<col>=)
// now happens inside ``hydrateFilterStoreFromUrl`` BEFORE first render,
// so every consumer's first React Query key already reflects the URL.
// This hook only handles the two cases that can't run synchronously:
//
//   - ``?view=<id>`` — needs an async API call into /api/views/{sid}
//     (or the React Query cache the bootstrap seeds), so it has to wait
//     for ``activeServiceId`` to be known.
//   - ``?metric=<val>`` — feeds per-page useState in useReportConfig
//     (the report-level metric is a page-local choice, not on filterStore).
//
// Anything else in the URL has already been consumed and stripped by
// the time this effect runs.
const viewMetricParsers = { view: parseAsString, metric: parseAsString }
// Hoisted so nuqs hands back a stable setter and the effect below doesn't re-fire per render.
const viewMetricOptions = { history: 'replace', shallow: true } as const

export function useViewMetricUrlSync(options?: { setMetric?: (m: string) => void }) {
  // Consume-and-strip params: read once, apply, then clear from the URL.
  const [{ view: viewId, metric: urlMetric }, setUrlParams] = useQueryStates(viewMetricParsers, viewMetricOptions)
  const addFilter = useFilterStore(s => s.addFilter)
  const clearFilters = useFilterStore(s => s.clearFilters)
  const setRange = useFilterStore(s => s.setRange)
  const setRelativeRange = useFilterStore(s => s.setRelativeRange)
  const toggleEdgeOnly = useFilterStore(s => s.toggleEdgeOnly)
  const toggleCompareMode = useFilterStore(s => s.toggleCompareMode)
  const setCompareRange = useFilterStore(s => s.setCompareRange)
  // Subscribe to the toggle-state values so the restore can compare without
  // poking `useFilterStore.getState()` (which doesn't exist on the test mock).
  const currentEdgeOnly = useFilterStore(s => s.edgeOnly)
  const currentCompareMode = useFilterStore(s => s.compareMode)
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const setMetric = options?.setMetric

  useEffect(() => {
    if (!activeServiceId) return

    const loadView = async (id: string) => {
      // Prefer the views cache (seeded by /api/bootstrap or warmed by
      // ViewSelector's useQuery). Falls back to a direct GET only when
      // the cache is cold — keeps the legacy ?view=<id> deep-link path
      // working even before ViewSelector mounts.
      let views = queryClient.getQueryData<components['schemas']['SavedViewRecord'][]>(queryKeys.views(activeServiceId))
      if (!views) {
        views = await unwrapClient(
          client.GET("/api/views/{service_id}", {
            params: { path: { service_id: activeServiceId } }
          }),
        )
      }
      const view = views?.find((v) => v.id === id)
      if (view) {
        applySavedViewToStore(view, {
          edgeOnly: currentEdgeOnly,
          compareMode: currentCompareMode,
          addFilter,
          clearFilters,
          setRange,
          setRelativeRange,
          toggleEdgeOnly,
          toggleCompareMode,
          setCompareRange,
        })
        void setUrlParams({ view: null })
      }
    }

    if (viewId) {
      void loadView(viewId).catch(() => {})
      return
    }

    if (urlMetric) {
      setMetric?.(urlMetric)
      void setUrlParams({ metric: null })
    }
    // addFilter/setRange/setMetric/clearFilters are Zustand store methods —
    // their identities are stable across renders (Zustand returns the same
    // function refs), so listing them in deps documents the dependency
    // without re-firing the effect. The exhaustive-deps lint is happy and
    // a future reader sees the full data flow.
  }, [activeServiceId, addFilter, setRange, setRelativeRange, toggleEdgeOnly, toggleCompareMode, setCompareRange, setMetric, clearFilters, currentEdgeOnly, currentCompareMode, queryClient, viewId, urlMetric, setUrlParams])
}

interface ChartIntervalButtonsProps {
  effectiveInterval: string
  validIntervals: Set<string>
  onIntervalChange: (val: ChartInterval) => void
}

export function ChartIntervalButtons({
  effectiveInterval,
  validIntervals,
  onIntervalChange,
}: ChartIntervalButtonsProps) {
  return (
    <ButtonGroup>
      {INTERVALS.map(i => (
        <Button
          key={i.value}
          variant={effectiveInterval === i.value ? 'default' : 'ghost'}
          size="sm"
          onClick={() => React.startTransition(() => onIntervalChange(i.value))}
          disabled={!validIntervals.has(i.value)}
          aria-pressed={effectiveInterval === i.value}
          // Distinct accessible name: the visible label ("1h", "1d", …) is
          // identical to the main time-range buttons, so screen readers and
          // automation see duplicate "1h"/"1d" controls. Scope the name to
          // the chart-bucket role.
          aria-label={`Chart bucket size: ${i.label}`}
          className={cn(
            'h-9 text-xs px-2 shadow-none transition-colors disabled:opacity-30 sm:h-7 sm:text-[11px]',
            effectiveInterval === i.value
              ? 'bg-primary text-primary-foreground hover:bg-primary/90'
              : 'hover:text-primary hover:bg-muted'
          )}
        >
          {i.label}
        </Button>
      ))}
    </ButtonGroup>
  )
}

export interface ReportLayoutRenderProps {
  config: ReportConfiguration
  metric: string
  setMetric: (metric: string) => void
  setChartInterval: (interval: ChartInterval) => void
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  bucketSeconds: number
  startTime: string | null
  endTime: string | null
  timezone: string
  activeServiceId: string | null
  filterPayload: FiltersPayload
}

interface ReportLayoutProps {
  title: string
  description: string
  icon: LucideIcon
  queryKey?: string
  defaultInterval?: ChartInterval
  headerActions?: React.ReactNode
  serviceId?: string | null
  children: (props: ReportLayoutRenderProps) => React.ReactNode
}

export function ReportLayout({
  title,
  description,
  icon,
  queryKey,
  defaultInterval = '1 hour',
  headerActions,
  serviceId,
  children
}: ReportLayoutProps) {
  const { startTime, endTime } = useTimeRange()
  const effectiveServiceId = useEffectiveServiceId()
  const activeServiceId = (serviceId !== undefined ? serviceId : effectiveServiceId) ?? null
  const timezone = useTimezone()
  const { config, metric, setMetric, setChartInterval, trend, setTrend } = useReportConfig({ defaultInterval })
  // Pass `true` so the FilterBar's "Edge only" toggle injects
  // `edge=include[true]` into every report query. Without it the toggle
  // was visible in the UI but discarded before reaching the backend.
  const filterPayload = useDebouncedFilterPayload(true)

  const queryClient = useQueryClient()
  useEffect(() => {
    return () => {
      // Cancel report queries on unmount to free pool connections when navigating
      queryClient.cancelQueries({ queryKey: [queryKey || 'report'] })
    }
  }, [queryClient, queryKey])

  useViewMetricUrlSync({ setMetric })

  const bucketSeconds = INTERVAL_SECONDS[config.effectiveInterval as keyof typeof INTERVAL_SECONDS] ?? 3600

  const intervalButtons = (
    <ChartIntervalButtons
      effectiveInterval={config.effectiveInterval}
      validIntervals={config.validIntervals}
      onIntervalChange={setChartInterval}
    />
  )

  return (
    <ReportShell
      title={title}
      description={description}
      icon={icon}
      headerActions={headerActions}
    >
      {children({
        config,
        metric,
        setMetric,
        setChartInterval,
        trend,
        setTrend,
        intervalButtons,
        bucketSeconds,
        startTime,
        endTime,
        timezone,
        activeServiceId,
        filterPayload
      })}
    </ReportShell>
  )
}
