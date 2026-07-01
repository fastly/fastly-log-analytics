'use client'

import React, { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { InsightCard } from '@/components/Insights/InsightCard'
import { InsightCardSkeleton } from '@/components/Insights/InsightCardSkeleton'
import { InsightCardData } from '@/types/api'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "@/components/ui/select"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Info, AlertCircle, CheckCircle, Lightbulb, Filter, Loader2 } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { ReportLayout } from '@/components/ReportLayout'
import { WINDOW_OPTIONS, BASELINE_OPTIONS } from '@/lib/insights-defaults'
import { useInsightsDefaults } from '@/hooks/useInsightsDefaults'

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
  relative: (iso: string) => string
  full: (iso: string) => string
  abbr: () => string
}

function InsightsBody({
  activeServiceId,
  windowHours,
  baselineHours,
  statusFilter,
  relative,
  full,
  abbr,
}: InsightsBodyProps) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['insights', activeServiceId, windowHours, baselineHours],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/insights", { signal,
        body: {
          window_size_hrs: parseFloat(windowHours),
          baseline_hours: parseFloat(baselineHours),
          filters: {},
        }
      })
      return data
    },
    enabled: !!activeServiceId,
    staleTime: 60000
  })

  const { data: availability, error: availabilityError } = useQuery({
    queryKey: ['insights', 'availability', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/insight-availability", { signal })
      return data
    },
    enabled: !!activeServiceId,
    // The active-insights list is derived from the service's column schema
    // and effectively never changes within a session. Long-cache it so a
    // warm navigation paints the per-insight skeleton cards instantly
    // instead of flashing an empty state for one round trip.
    staleTime: 5 * 60 * 1000,
  })

  const filteredInsights = useMemo(() => {
    if (!data?.insights) return []
    if (statusFilter === 'all') return data.insights
    return (data.insights as InsightCardData[]).filter((insight: InsightCardData) => insight.severity === statusFilter)
  }, [data?.insights, statusFilter])

  // Skeleton cards rendered while /api/insights is in flight come from the
  // /api/insight-availability response (titles + descriptions per available
  // insight). Single render path during loading — no SkeletonGrid → per-
  // insight swap; the only transition is content-fill when real data lands.
  const availableInsights = useMemo(() => {
    const list = (availability as any)?.insights as
      | Array<{ id: string; title: string; description?: string; available?: boolean }>
      | undefined
    if (!list) return []
    return list.filter((i) => i.available !== false)
  }, [availability])

  return (
    <>
      {(availability as any)?.unavailable && (availability as any).unavailable.length > 0 && (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Some insights are unavailable</AlertTitle>
          <AlertDescription className="text-xs">
            {(availability as any).unavailable.length} insights require additional log fields to be enabled.
            Check your service configuration.
          </AlertDescription>
        </Alert>
      )}

      {availabilityError && !availability && (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Availability check failed</AlertTitle>
          <AlertDescription className="text-xs">
            Couldn't determine which insights apply to this service — results below may be incomplete.
          </AlertDescription>
        </Alert>
      )}

      {isLoading ? (
        availableInsights.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {availableInsights.map((i) => (
              <InsightCardSkeleton
                key={i.id}
                title={i.title}
                description={i.description}
              />
            ))}
          </div>
        ) : (
          <div className="text-center py-20 text-sm text-muted-foreground">
            <Loader2 className="inline-block animate-spin h-4 w-4 mr-2" />
            Loading insights…
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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredInsights.map((insight: InsightCardData) => (
            <InsightCard
              key={insight.id}
              insight={insight}
              windowHours={windowHours}
              baselineHours={baselineHours}
            />
          ))}
          {filteredInsights.length === 0 && (
            <div className="col-span-full py-20 text-center border rounded-xl border-dashed">
              <CheckCircle className="h-10 w-10 text-green-500 mx-auto mb-4" />
              <h3 className="text-lg font-medium">
                {statusFilter === 'all' ? 'No anomalies detected' : `No insights matching '${statusFilter}'`}
              </h3>
              <p className="text-muted-foreground">
                {statusFilter === 'all' ? 'Traffic patterns are within normal baseline ranges.' : 'Try changing your filter criteria.'}
              </p>
            </div>
          )}
        </div>
      )}

      {data && (data as any).computed_at && (
        <div className="text-[10px] text-muted-foreground text-right italic">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className="" />}>
                Computed {relative((data as any).computed_at)}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {full((data as any).computed_at)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      )}
    </>
  )
}

export default function InsightsClient() {
  // Window/baseline default adapt to how much history the active service has
  // (e.g. ~2h of data → "this hour vs the previous hour" instead of the 7-day
  // baseline that would just say "not enough data"). An explicit pick wins.
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { windowHours, baselineHours, setWindowHours, setBaselineHours } =
    useInsightsDefaults(activeServiceId)
  const [statusFilter, setStatusFilter] = useState('all')
  const { relative, full, abbr } = useDateFormat()

  // Debounce window/baseline so chained dropdown changes (e.g. switching
  // both window AND baseline in succession) only fire one /api/insights
  // request instead of two. 400ms gives the user enough time to flip
  // both selects without firing the intermediate query.
  const [appliedWindowHours, setAppliedWindowHours] = useState(windowHours)
  const [appliedBaselineHours, setAppliedBaselineHours] = useState(baselineHours)
  React.useEffect(() => {
    const t = setTimeout(() => {
      setAppliedWindowHours(windowHours)
      setAppliedBaselineHours(baselineHours)
    }, 400)
    return () => clearTimeout(t)
  }, [windowHours, baselineHours])

  // Compact header controls (Status / Window / vs / Baseline) — rendered
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
                The historical period to compare against (acts as the 'normal' baseline)
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
          relative={relative}
          full={full}
          abbr={abbr}
        />
      )}
    </ReportLayout>
  )
}
