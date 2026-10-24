'use client'

import React, { useState, useMemo } from 'react'
import Link from 'next/link'
import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import type { FiltersPayload } from '@/types/filters'
import { cn } from '@/lib/utils'
import { useEffectiveServiceId } from '@/hooks/useIsDataReady'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'
import { InsightCard, SEVERITY_BADGE_CLASS } from '@/components/Insights/InsightCard'
import { InsightCardSkeleton } from '@/components/Insights/InsightCard'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from '@/components/ui/badge'
import { Info, AlertCircle, CheckCircle, Lightbulb, Filter, Loader2, RefreshCw } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { ReportLayout } from '@/components/ReportLayout'
import { WINDOW_OPTIONS, BASELINE_OPTIONS, historyHoursFromExtents } from '@/lib/insights-defaults'
import { useDebounce } from '@/hooks/useDebounce'
import { useInsightsDefaults } from '@/hooks/useInsightsDefaults'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import {
  normalizeTabKey,
  SECTION_LOADING_MESSAGES,
  groupInsightsBySection,
  summarizeSeverities,
  type InsightSectionGroup,
  type SectionableInsight,
  type SeveritySummary,
} from '@/lib/insight-sections'

const STATUS_OPTIONS = [
  { label: 'All Statuses', value: 'all' },
  { label: 'Critical', value: 'critical' },
  { label: 'Warning', value: 'warning' },
  { label: 'Info', value: 'info' },
  { label: 'Clean', value: 'clean' },
  // `error` severity insights (the insight computation itself failed) were
  // silently dropped from every non-"all" filter without this option.
  { label: 'Error', value: 'error' },
]

// Full per-severity rollup chips (e.g. "2 CRITICAL" "3 WARNING" "4 CLEAN"),
// reusing the exact card palette (SEVERITY_BADGE_CLASS) so the panel header and
// the cards read the same. Shown in the ACTIVE panel header — the whole
// breakdown, not the compact critical/warning-only badges on the tab triggers.
// Renders nothing when there are no known severities (skeleton entries have
// none), so the loading panel header shows only the count.
function SeverityChips({ severities }: { severities: SeveritySummary[] }) {
  if (severities.length === 0) return null
  return (
    <span className="ml-1 flex flex-wrap items-center gap-1">
      {severities.map(({ severity, count }) => (
        <Badge
          key={severity}
          variant="outline"
          className={cn(
            'border px-1.5 py-0 text-[10px] font-medium uppercase tracking-wide tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count} {severity}
        </Badge>
      ))}
    </span>
  )
}

// Compact per-tab severity badges: colored count-only pills for the high-signal
// severities (critical=red, warning=amber). clean/info/error are folded away
// here to keep the tab row scannable — the FULL breakdown lives in the active
// panel header (SeverityChips). Screen readers still get the severity name via
// the sr-only span so a bare "2" isn't ambiguous. Colors reuse the AA-cleared
// SEVERITY_BADGE_CLASS palette (axe color-contrast gate).
const TAB_BADGE_SEVERITIES = new Set(['critical', 'warning'])

function TabSeverityBadges({ severities }: { severities: SeveritySummary[] }) {
  const compact = severities.filter((s) => TAB_BADGE_SEVERITIES.has(s.severity))
  // Always reserve one pill's width: the skeleton tab bar (no severities) and the loaded one must
  // be the same width or every tab shifts when /api/insights lands (CLS).
  if (compact.length === 0) return <span aria-hidden="true" className="inline-block min-w-[1.125rem]" />
  return (
    <span className="flex min-w-[1.125rem] items-center gap-1">
      {compact.map(({ severity, count }) => (
        <span
          key={severity}
          className={cn(
            'inline-flex min-w-[1.125rem] items-center justify-center rounded-full border px-1 text-[10px] font-semibold leading-4 tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count}
          <span className="sr-only"> {severity}</span>
        </span>
      ))}
    </span>
  )
}

// The tabs UI. One tab per NON-EMPTY section in fixed INSIGHT_SECTIONS order
// (+ Other last), the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// The tabs UI. One tab per section in fixed INSIGHT_SECTIONS order
// (All Categories first, then security, origin, edge, network, traffic, + Other last),
// the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// panels by default (keepMounted defaults false) — so a section's cards only
// render when its tab is active (the "load when I click it" behaviour the user
// asked for, even though /api/insights returns everything in one call).
//
// The active tab is controlled by the parent so the selection survives the
// loading→loaded swap and status-filter changes; `activeKey` falls back to the
// first group whenever the current selection isn't among the rendered groups.
function InsightTabs<T extends SectionableInsight>({
  groups,
  activeTab,
  onTabChange,
  isLoading,
  renderCards,
}: {
  groups: InsightSectionGroup<T>[]
  activeTab: string | null
  onTabChange: (key: string) => void
  isLoading?: boolean
  renderCards: (group: InsightSectionGroup<T>) => React.ReactNode
}) {
  const normalizedActive = normalizeTabKey(activeTab)
  const activeKey =
    groups.some((g) => g.section.key === normalizedActive)
      ? normalizedActive
      : groups[0]?.section.key ?? 'all'

  return (
    <Tabs value={activeKey} onValueChange={(value) => onTabChange(normalizeTabKey(String(value)))}>
      {/* h-auto + flex-wrap so the richer triggers (icon + label + count +
          severity pills) aren't clipped by the primitive's default h-8 and
          wrap instead of overflowing on narrow screens. */}
      <TabsList className="group-data-horizontal/tabs:h-auto w-full flex-wrap justify-start gap-1 p-1">
        {groups.map(({ section, cards }) => {
          const Icon = section.icon
          return (
            <TabsTrigger
              key={section.key}
              value={section.key}
              data-tab={section.key}
              data-category={section.key}
              data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
              className="h-auto flex-none gap-2 px-3 py-2 data-active:border-border data-active:font-semibold"
            >
              <Icon className="size-4 shrink-0" aria-hidden="true" />
              <span>{section.label}</span>
              <span className="min-w-[2ch] text-right text-xs tabular-nums">{cards.length}</span>
              <TabSeverityBadges severities={summarizeSeverities(cards)} />
            </TabsTrigger>
          )
        })}
      </TabsList>

      {groups.map((group) => {
        const { section, cards } = group
        const Icon = section.icon
        return (
          <TabsContent
            key={section.key}
            value={section.key}
            data-tab-content={section.key}
            data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
            className="mt-4 space-y-4"
          >
            <div>
              <h2 className="flex flex-wrap items-center gap-2 text-lg font-semibold tracking-tight">
                <Icon className="h-5 w-5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span>{section.label}</span>
                <span className="text-xs font-normal text-muted-foreground tabular-nums">
                  ({cards.length})
                </span>
                <SeverityChips severities={summarizeSeverities(cards)} />
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">{section.description}</p>
            </div>
            {cards.length === 0 ? (
              isLoading ? (
                <div
                  className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-loading"
                >
                  <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
                  <span>{SECTION_LOADING_MESSAGES[section.key] ?? 'Loading insights…'}</span>
                </div>
              ) : (
                <div
                  className="py-16 text-center border rounded-xl border-dashed bg-muted/10 min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-empty-pass"
                >
                  <CheckCircle className="h-10 w-10 text-emerald-500 mb-3" />
                  <h3 className="text-base font-semibold">
                    {section.key === 'all'
                      ? 'No anomalies detected'
                      : `No anomalies detected in ${section.label}`}
                  </h3>
                  <p className="text-xs text-muted-foreground mt-1 max-w-sm">
                    {section.key === 'all'
                      ? 'All monitored metrics across all categories are operating within normal baseline parameters.'
                      : 'All monitored metrics in this category are operating within normal baseline parameters.'}
                  </p>
                </div>
              )
            ) : (
              renderCards(group)
            )}
          </TabsContent>
        )
      })}
    </Tabs>
  )
}

// Lifted out of the ReportLayout render-prop so the hooks live at the
// top of a stable component instead of being recreated every time
// ReportLayout re-renders. Same shape as DashboardBody (item 30).
// Without this lift, React Query treats every ReportLayout re-render
// as a fresh mount and re-fires the /api/insights + /api/insight-
// availability requests — the local-dev duplicate-fetch pattern.
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
}: InsightsBodyProps) {
  // Which section tab is active. Controlled here (not inside InsightTabs) so the
  // selection survives the loading→loaded swap between the two InsightTabs
  // instances and the status-filter regrouping.
  const [activeTab, setActiveTab] = useState<string | null>(() => normalizeTabKey(initialTab))
  const hasActiveFilters = Boolean(filterPayload && Object.keys(filterPayload).length > 0)

  const { data, isLoading, error } = useQuery({
    queryKey: hasActiveFilters
      ? ['insights', activeServiceId, windowHours, baselineHours, filterPayload]
      : ['insights', activeServiceId, windowHours, baselineHours],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.POST("/api/insights", { signal,
        body: {
          window_size_hrs: parseFloat(windowHours),
          baseline_hours: parseFloat(baselineHours),
          filters: filterPayload ?? {},
          force_refresh: false,
        }
      }))
    },
    enabled: !!activeServiceId,
    staleTime: 60000,
    placeholderData: keepPreviousData,
  })

  const { data: availability, error: availabilityError } = useQuery({
    queryKey: ['insights', 'availability', activeServiceId],
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET("/api/insight-availability", { signal }))
    },
    enabled: !!activeServiceId,
    // The active-insights list is derived from the service's column schema
    // and effectively never changes within a session. Long-cache it so a
    // warm navigation paints the per-insight skeleton cards instantly
    // instead of flashing an empty state for one round trip.
    staleTime: 5 * 60 * 1000,
  })

  const allInsights = data?.insights
  const filteredInsights = useMemo(() => {
    if (!allInsights) return []
    if (statusFilter === 'all') return allInsights
    return allInsights.filter((insight) => insight.severity === statusFilter)
  }, [allInsights, statusFilter])

  // Skeleton cards rendered while /api/insights is in flight come from the
  // /api/insight-availability response (titles + descriptions per available
  // insight). Single render path during loading — no SkeletonGrid → per-
  // insight swap; the only transition is content-fill when real data lands.
  const availableInsights = useMemo(() => {
    const list = availability?.insights
    if (!list) return []
    return list.filter((i) => i.available !== false)
  }, [availability])

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
      {availability?.unavailable && availability.unavailable.length > 0 && (
        <Alert className="border-amber-500/30 bg-amber-500/5">
          <Info className="h-4 w-4 text-amber-500 shrink-0" />
          <AlertTitle className="text-amber-600 dark:text-amber-400 font-medium">
            {availability.unavailable.length} {availability.unavailable.length === 1 ? 'insight is' : 'insights are'} unavailable
          </AlertTitle>
          <AlertDescription className="text-xs text-muted-foreground mt-1 flex flex-wrap items-center justify-between gap-2">
            <span>
              {availability.unavailable.length} {availability.unavailable.length === 1 ? 'detector requires' : 'detectors require'} optional log fields to be enabled. Check service configuration.
            </span>
            <Link
              href={activeServiceId ? `/service-config?service=${activeServiceId}` : '/service-config'}
              className="inline-flex items-center text-xs font-semibold text-primary underline underline-offset-4 hover:text-primary/80"
            >
              Configure Service Log Fields &rarr;
            </Link>
          </AlertDescription>
        </Alert>
      )}

      {availabilityError && !availability && (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Availability check failed</AlertTitle>
          <AlertDescription className="text-xs">
            Couldn&apos;t determine which insights apply to this service — results below may be incomplete.
          </AlertDescription>
        </Alert>
      )}

      {isLoading ? (
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
          isLoading={false}
          renderCards={({ cards }) => (
            <div
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]"
              data-testid="insights-grid"
            >
              {cards.map((insight) => (
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

      {data && data.computed_at && (
        <div className="text-[10px] text-muted-foreground text-right italic">
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
        </div>
      )}
    </>
  )
}

interface InsightsClientProps {
  initialTab?: string | null
  initialWindow?: string | null
  initialBaseline?: string | null
}

export default function InsightsClient({
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

  const [selectedWindow, setSelectedWindow] = useState<string | null>(initialWindow ?? null)
  const [selectedBaseline, setSelectedBaseline] = useState<string | null>(initialBaseline ?? null)

  const windowHours = selectedWindow ?? defaults.windowHours
  const baselineHours = selectedBaseline ?? defaults.baselineHours

  const setWindowHours = (val: string) => {
    setSelectedWindow(val)
    defaults.setWindowHours(val)
  }
  const setBaselineHours = (val: string) => {
    setSelectedBaseline(val)
    defaults.setBaselineHours(val)
  }

  const [statusFilter, setStatusFilter] = useState('all')
  const [isRescanning, setIsRescanning] = useState(false)
  const { relative, full, abbr } = useDateFormat()

  const queryClient = useQueryClient()
  const { data: extentsData } = useQuery({
    queryKey: queryKeys.logExtents(activeServiceId),
    queryFn: async () => {
      return unwrapClient(client.GET('/api/log-extents'))
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
  })

  // Pre-load / prefetch secondary month option (24h vs 720h) when enough data exists (>=30d history).
  React.useEffect(() => {
    if (!activeServiceId) return
    const history = historyHoursFromExtents(extentsData?.earliest_log_at)
    if (history !== null && history >= 720) {
      queryClient.prefetchQuery({
        queryKey: ['insights', activeServiceId, '24', '720'],
        queryFn: async ({ signal }) => {
          return unwrapClient(client.POST('/api/insights', {
            signal,
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
    }
  }, [activeServiceId, extentsData?.earliest_log_at, queryClient])

  // Debounce window/baseline so chained dropdown changes (e.g. switching
  // both window AND baseline in succession) only fire one /api/insights
  // request instead of two. 400ms gives the user enough time to flip
  // both selects without firing the intermediate query.
  const appliedWindowHours = useDebounce(windowHours, 400)
  const appliedBaselineHours = useDebounce(baselineHours, 400)

  const handleRescan = async () => {
    if (!activeServiceId || isRescanning) return
    setIsRescanning(true)
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
      console.error('Failed to re-scan insights:', err)
    } finally {
      setIsRescanning(false)
    }
  }

  // Compact header controls (Status / Window / vs / Baseline / Re-scan) — rendered
  // in the header row next to the title instead of a separate band below.
  const headerControls = (
    <div className="flex items-end gap-2">
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Status Filter</span>
          <span className="w-3 h-3 shrink-0" />
        </div>
        <Select value={statusFilter} onValueChange={(val) => val && setStatusFilter(val)}>
          <SelectTrigger className="w-[140px]">
            <Filter className="w-3 h-3 mr-2 opacity-50" />
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            {STATUS_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-1 ml-2">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Window</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The current time period you want to check for anomalies
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={windowHours} onValueChange={(val) => val && setWindowHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Window" />
          </SelectTrigger>
          <SelectContent>
            {WINDOW_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest pb-2">vs</span>

      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Baseline</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The historical period to compare against (acts as the &apos;normal&apos; baseline)
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={baselineHours} onValueChange={(val) => val && setBaselineHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Baseline" />
          </SelectTrigger>
          <SelectContent>
            {BASELINE_OPTIONS.map(o => (
              <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {!isRemoteAnalyst && (
        <div className="flex flex-col gap-1 ml-2 justify-end">
          <Button
            variant="outline"
            size="sm"
            onClick={handleRescan}
            disabled={isRescanning || !activeServiceId}
            className="h-9 gap-1.5 px-3 text-xs font-medium"
            title="Force re-evaluation of all anomaly detectors against recent logs"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", isRescanning && "animate-spin")} />
            <span>{isRescanning ? 'Scanning…' : 'Re-scan'}</span>
          </Button>
        </div>
      )}
    </div>
  )

  return (
    <ReportLayout
      title="Anomaly Detection"
      description="Automated insights comparing recent traffic to historical baselines."
      icon={Lightbulb}
      headerActions={headerControls}
    >
      {(ctx) => (
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
        />
      )}
    </ReportLayout>
  )
}
