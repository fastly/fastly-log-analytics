'use client'
import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { Label } from '@/components/ui/label'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useDateFormat } from '@/hooks/useDateFormat'
import { useNowMs } from '@/hooks/useNowSeconds'
import { formatCompactDuration, toUTCDate } from '@/lib/date'

export function SystemJobBox({ job }: { job: any }) {
  const { timeAgo, full, abbr } = useDateFormat()
  const nowMs = useNowMs()

  const lastRunText = job.last_run_at ? timeAgo(job.last_run_at) : 'Never'

  // Pre-fix this had a per-instance setInterval(compute, 1000) that
  // re-rendered every box every second. On a 10-cron page that's 10
  // independent timers firing on the same 1s boundary, each forcing a
  // setState — the main thread was constantly busy and clicks queued
  // behind the cascade ("admin page takes 2 seconds to respond").
  // Now we derive nextRunText on-render from useNowMs() (a single
  // shared global ticker). Same UX, ~10x fewer timers + state updates.
  const nextRunText = job.next_run_at
    ? formatCompactDuration(Math.floor((toUTCDate(job.next_run_at).getTime() - nowMs) / 1000))
    : 'Disabled'

  const isError = job.status === 'error'
  const borderColor = isError ? 'border-destructive/50' : 'border-muted'
  const bgColor = isError ? 'bg-destructive/10' : 'bg-muted/20'

  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 ${bgColor} ${borderColor} min-w-[250px] max-w-[320px] flex-1`}>
      <div className="flex items-center gap-2 w-full">
        <TooltipProvider delay={200}>
          <Tooltip>
            <TooltipTrigger render={<span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 truncate max-w-[120px] ${isError ? 'text-destructive' : 'text-muted-foreground'}`} />}>
              {job.name}
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-[250px] text-xs">
              {job.detail || job.name}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
        <div className="w-px h-4 bg-border shrink-0" />
        <div className={`flex-1 min-w-0 flex items-center justify-between text-[9px] whitespace-nowrap ${isError ? 'text-destructive/80' : 'text-muted-foreground'}`}>
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="truncate pr-2 " />}>
                Last: {lastRunText}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {job.last_run_at ? `${full(job.last_run_at)} ${abbr()}` : 'Never'}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="truncate " />}>
                Next: {nextRunText}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {job.next_run_at ? `${full(job.next_run_at)} ${abbr()}` : 'Disabled'}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </div>
    </div>
  )
}

export function SystemJobsStrip() {
  const { data: systemJobsData } = useQuery({
    queryKey: ['system-jobs'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/system-jobs", { signal })
      return data as any
    },
    staleTime: 30_000,
    refetchInterval: 30_000,
  })

  return (
    <div className="space-y-3 pt-2">
      <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Background Jobs</Label>
      <div className="flex flex-wrap gap-2">
        {(systemJobsData?.jobs ?? []).map((job: any) => (
          <SystemJobBox key={job.id} job={job} />
        ))}
        {!systemJobsData && (
          <div className="text-xs text-muted-foreground italic px-1 py-1">Loading background jobs...</div>
        )}
      </div>
    </div>
  )
}
