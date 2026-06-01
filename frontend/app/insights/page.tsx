'use client'

import React, { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { InsightCard } from '@/components/Insights/InsightCard'
import { InsightCardData } from '@/types/api'
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from "@/components/ui/select"
import { SkeletonGrid } from '@/components/ui/skeleton-grid'
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Info, AlertCircle, CheckCircle, Lightbulb, Filter } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { ReportLayout } from '@/components/ReportLayout'

const WINDOW_OPTIONS = [
  { label: 'Last 15 Minutes', value: '0.25' },
  { label: 'Last 1 Hour', value: '1' },
  { label: 'Last 4 Hours', value: '4' },
  { label: 'Last 24 Hours', value: '24' },
]

const BASELINE_OPTIONS = [
  { label: 'Previous 1 Hour', value: '1' },
  { label: 'Last 24 Hours', value: '24' },
  { label: 'Last 7 Days', value: '168' },
  { label: 'Last 30 Days', value: '720' },
]

const STATUS_OPTIONS = [
  { label: 'All Statuses', value: 'all' },
  { label: 'Critical', value: 'critical' },
  { label: 'Warning', value: 'warning' },
  { label: 'Info', value: 'info' },
  { label: 'Clean', value: 'clean' },
]

export default function InsightsPage() {
  const [windowHours, setWindowHours] = useState('1')
  const [baselineHours, setBaselineHours] = useState('168')
  const [statusFilter, setStatusFilter] = useState('all')
  const { relative, full, abbr } = useDateFormat()

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

      <span className="text-xs font-bold text-muted-foreground opacity-50 uppercase tracking-widest pb-2">vs</span>

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
      {({
        activeServiceId,
      }) => {
        const { data, isLoading, error } = useQuery({
    queryKey: ['insights', activeServiceId, windowHours, baselineHours],
    queryFn: async () => {
      const { data } = await client.POST("/api/insights", {
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

  const { data: availability } = useQuery({
    queryKey: ['insights', 'availability', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/insight-availability")
      return data
    },
    enabled: !!activeServiceId
  })

  const filteredInsights = useMemo(() => {
    if (!data?.insights) return []
    if (statusFilter === 'all') return data.insights
    return (data.insights as InsightCardData[]).filter((insight: InsightCardData) => insight.severity === statusFilter)
  }, [data?.insights, statusFilter])

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

      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <SkeletonGrid count={6} height="250px" />
        </div>
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
            <InsightCard key={insight.id} insight={insight} />
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
          ) }
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
    }}
  </ReportLayout>
  )
}
