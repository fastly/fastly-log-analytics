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
import { useMounted } from '@/hooks/useMounted'
import { useNowMs } from '@/hooks/useNowSeconds'
import { formatCompactDuration, toUTCDate } from '@/lib/date'

function NextRunCountdown({ when }: { when: string | null | undefined }) {
  const nowMs = useNowMs()
  if (!when) return <>Disabled</>
  return <>{formatCompactDuration(Math.floor((toUTCDate(when).getTime() - nowMs) / 1000))}</>
}

function SystemJobBox({ job }: { job: any }) {
  const { timeAgo, full, abbr } = useDateFormat()

  const lastRunText = job.last_run_at ? timeAgo(job.last_run_at) : 'Never'

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
                Next: <NextRunCountdown when={job.next_run_at} />
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
  // SSR-safe: ['system-jobs'] is a live, client-only query never seeded into
  // the dehydrated SSR cache, so the server always renders the "Loading…"
  // placeholder. If the client paints the resolved job boxes on its first
  // (hydration) render the markup diverges from the server → React #418 on
  // /admin (the webkit hydration failure). The job boxes also render
  // now-relative times (NextRunCountdown / timeAgo) that would mismatch on
  // their own. Force the first client render to the server's loading state,
  // then swap to data after mount — same pattern as SystemHealthCard.
  const mounted = useMounted()

  // Freshness via useAdminEventStream. Safety-net poll only.
  const { data: systemJobsData } = useQuery({
    queryKey: ['system-jobs'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/system-jobs", { signal })
      return data as any
    },
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  const ready = mounted && systemJobsData

  return (
    <div className="space-y-3 pt-2">
      <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Background Jobs</Label>
      <div className="flex flex-wrap gap-2">
        {ready ? (
          (systemJobsData.jobs ?? []).map((job: any) => (
            <SystemJobBox key={job.id} job={job} />
          ))
        ) : (
          <div className="text-xs text-muted-foreground italic px-1 py-1">Loading background jobs...</div>
        )}
      </div>
    </div>
  )
}
