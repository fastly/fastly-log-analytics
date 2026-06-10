'use client'

import React, { useState, useEffect } from 'react'
import { Loader2 } from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useNowMs } from '@/hooks/useNowSeconds'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useElapsedTime } from '@/hooks/useElapsedTime'
import { formatCompactDuration, toUTCDate } from '@/lib/date'
import { CronLiveLog } from '@/components/CronLiveLog'
import { CRON_EXPLANATIONS } from './CronExplanations'

export function LiveTimer({ startedAt }: { startedAt: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className="font-mono text-blue-500 tabular-nums text-xs font-medium animate-pulse">{fmt}</span>
}

export function CronJobBox({ job, onRemove }: { job: any, onRemove: (id: number) => void }) {
  const [isDone, setIsDone] = useState(false)
  const [fading, setFading] = useState(false)

  useEffect(() => {
    if (!isDone) return
    const fadeTimer = setTimeout(() => setFading(true), 2000)
    const removeTimer = setTimeout(() => onRemove(job.id), 2600) // 2s delay + 600ms fade
    return () => { clearTimeout(fadeTimer); clearTimeout(removeTimer) }
  }, [isDone, job.id, onRemove])

  return (
    <div
      className={[
        'relative flex items-center gap-2 border rounded-md px-2.5 h-8 shrink-0 min-w-[220px] max-w-[280px]',
        fading
          ? 'opacity-0 transition-opacity duration-500 bg-muted/20 border-muted'
          : isDone
            ? 'bg-muted/20 border-muted'
            : 'bg-muted/30 border-blue-500/20',
      ].join(' ')}
    >
      {!isDone && !fading && (
        <div className="absolute inset-0 rounded-md border border-blue-500/60 animate-pulse pointer-events-none" />
      )}
      <TooltipProvider delay={200}>
        <Tooltip>
          <TooltipTrigger render={<span className="text-[9px] font-bold uppercase text-blue-500 tracking-wider shrink-0" />}>
            {job.task === 'metadata_sync' ? 'sync' : job.task}
          </TooltipTrigger>
          <TooltipContent side="top" className="max-w-[250px] text-xs">
            {CRON_EXPLANATIONS[job.task] || 'Background job.'}
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      <div className="w-px h-4 bg-border shrink-0" />
      <div className="flex-1 overflow-hidden min-w-0">
        <CronLiveLog runId={job.id} singleLine={true} onDone={() => setIsDone(true)} />
      </div>
    </div>
  )
}

export function CronScheduleBox({
  schedule,
  compact = false,
  activeJob = null,
  onOpenConsole
}: {
  schedule: any;
  compact?: boolean;
  activeJob?: any;
  onOpenConsole?: (jobId: number | string) => void
}) {
  const { relative, timeAgo, full, abbr } = useDateFormat()
  const nowMs = useNowMs()

  // Pre-fix this had a per-instance setInterval(compute, 1000) that
  // re-rendered every CronScheduleBox every second. On /logs that
  // typically meant 5+ independent 1s tickers firing on the same
  // boundary, each forcing a setState. Now we derive nextRunText
  // on-render from useNowMs() — a single shared global ticker —
  // same UX but one timer for the whole tree.
  const nextRunText = schedule.next_run_time
    ? formatCompactDuration(Math.floor((toUTCDate(schedule.next_run_time).getTime() - nowMs) / 1000))
    : 'Disabled'

  if (schedule.disabled_reason === 'no_alerts_configured') {
    return (
      <div className="relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 bg-muted/20 border-muted min-w-[130px] flex-1">
        <div className="flex items-center gap-2 w-full">
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="text-[9px] font-bold uppercase text-muted-foreground tracking-wider shrink-0" />}>
                alerts
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[250px] text-xs">
                {CRON_EXPLANATIONS.alerts}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <div className="w-px h-4 bg-border shrink-0" />
          <span className="flex-1 min-w-0 truncate text-[9px] text-muted-foreground italic">
            No alerts configured.
          </span>
        </div>
      </div>
    )
  }

  const lastRunText = schedule.last_run_time ? timeAgo(schedule.last_run_time) : 'Never'
  const isRunning = !!activeJob
  const borderColor = isRunning ? 'border-blue-500/60 shadow-[0_0_8px_rgba(59,130,246,0.15)] bg-blue-500/5' : 'border-muted bg-muted/20'

  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 transition-all ${borderColor} min-w-[130px] flex-1`}>
      {isRunning && (
        <div className="absolute inset-0 rounded-md border border-blue-500/50 animate-pulse pointer-events-none" />
      )}
      <div className="flex items-center gap-2 w-full">
        <TooltipProvider delay={200}>
          <Tooltip>
            <TooltipTrigger render={
              <span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 flex items-center gap-1 ${isRunning ? 'text-blue-500' : 'text-muted-foreground'}`} />
            }>
              {isRunning && <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-500" />}
              {schedule.task === 'metadata_sync' ? 'sync' : schedule.task}
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-[250px] text-xs">
              {CRON_EXPLANATIONS[schedule.task] || 'Background job.'}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
        <div className="w-px h-4 bg-border shrink-0" />

        {isRunning ? (
          <button
            onClick={() => onOpenConsole?.(activeJob.id)}
            className="flex-1 min-w-0 text-left text-[9px] text-blue-500 hover:text-blue-600 hover:underline font-medium flex items-center justify-between cursor-pointer truncate"
          >
            <span className="truncate">Running...</span>
            <span className="text-[8px] bg-blue-500/20 px-1 py-0.2 rounded border border-blue-500/20 shrink-0 ml-1">LOGS</span>
          </button>
        ) : (
          <div className="flex-1 min-w-0 flex items-center justify-between text-[9px] text-muted-foreground whitespace-nowrap overflow-hidden">
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate pr-2" />}>
                  Last: {lastRunText}
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate" />}>
                  Next: {nextRunText}
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.next_run_time ? `${full(schedule.next_run_time)} ${abbr()}` : 'Disabled'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        )}
      </div>
    </div>
  )
}
