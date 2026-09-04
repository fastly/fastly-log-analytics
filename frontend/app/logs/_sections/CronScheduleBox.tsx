'use client'

import React from 'react'
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
import { TimeAgo } from '@/components/TimeAgo'
import { formatCompactDuration, toUTCDate } from '@/lib/date'
import { CRON_EXPLANATIONS, CRON_DISPLAY_NAMES } from './CronExplanations'

export function LiveTimer({ startedAt }: { startedAt: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-xs font-medium animate-pulse">{fmt}</span>
}

// Tile-sized variant: inline-styled for the cron schedule pill so the
// 9px tile typography isn't blown out by LiveTimer's text-xs (12px).
// Same pulse + tabular-nums so widths stay stable across digit counts.
function TileLiveTimer({ startedAt }: { startedAt: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className="font-mono text-blue-700 dark:text-blue-300 tabular-nums text-[9px] font-medium animate-pulse">{fmt}</span>
}

function NextRunCountdown({ when }: { when: string | null | undefined }) {
  const nowMs = useNowMs()
  if (!when) return <>Disabled</>
  return <>{formatCompactDuration(Math.floor((toUTCDate(when).getTime() - nowMs) / 1000))}</>
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
  const { full, abbr } = useDateFormat()

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
              <span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 ${isRunning ? 'text-blue-700 dark:text-blue-300' : 'text-muted-foreground'}`} />
            }>
              {CRON_DISPLAY_NAMES[schedule.task] || schedule.task}
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
            className="flex-1 min-w-0 text-left text-[9px] text-blue-700 dark:text-blue-300 hover:text-blue-800 dark:hover:text-blue-200 hover:underline font-medium flex items-center justify-between cursor-pointer"
            aria-label={`Running for ${schedule.task} — click for logs`}
          >
            <span className="flex items-center gap-1 min-w-0">
              <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-700 dark:text-blue-300" aria-hidden="true" />
              {activeJob.started_at && <TileLiveTimer startedAt={activeJob.started_at} />}
            </span>
            <span className="text-[8px] bg-blue-500/20 px-1 py-0.2 rounded border border-blue-500/20 shrink-0 ml-1">LOGS</span>
          </button>
        ) : (
          <div className="flex-1 min-w-0 flex items-center justify-between text-[9px] text-muted-foreground whitespace-nowrap overflow-hidden">
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate pr-2" />}>
                  Last: {schedule.last_run_time ? <TimeAgo timestamp={schedule.last_run_time} /> : 'Never'}
                </TooltipTrigger>
                <TooltipContent className="text-xs">
                  {schedule.last_run_time ? `${full(schedule.last_run_time)} ${abbr()}` : 'Never'}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            <TooltipProvider delay={200}>
              <Tooltip>
                <TooltipTrigger render={<span className="truncate" />}>
                  Next: <NextRunCountdown when={schedule.next_run_time} />
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
