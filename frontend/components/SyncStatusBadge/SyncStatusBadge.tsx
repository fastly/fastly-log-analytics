'use client'

import React from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { useSyncStatus } from '@/hooks/useSyncStatus'
import { useDateFormat } from '@/hooks/useDateFormat'
import { formatTimeAgo } from '@/lib/date'
import { Badge } from '@/components/ui/badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export function SyncStatusBadge() {
  const { activeServiceId } = useServiceStore()
  const { full, abbr } = useDateFormat()

  const { data: status } = useSyncStatus()

  // Pre-fix this had a 1-second setState ticker so the "Xs ago" label
  // advanced between the 15s polls. That ticker re-rendered the entire
  // header chrome every second — observable as visual jumpiness even
  // when nothing was actively loading. The "Xs ago" granularity is
  // good enough at the 15s poll cadence: the label updates whenever
  // the query refetches OR when any other state in the badge changes,
  // and the formatTimeAgo call below uses the latest fileTs each
  // render. Cost: the "X seconds" portion isn't real-time but the
  // value is at most 15s stale — fine for an operator glance.

  if (!activeServiceId || !status) return null

  const fileTs = status.latest_log_at || status.latest_available_file_at || status.latest_ingested_file_at

  return (
    <div className="hidden lg:flex items-center gap-2 mr-2 animate-in fade-in zoom-in-95">
      {status.local_rows != null && (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10 hover:bg-muted transition-colors">
          <strong className="text-foreground mr-1">Total Logs:</strong>
          {status.local_rows.toLocaleString()}
        </Badge>
      )}

      {fileTs ? (
        <Tooltip>
          <TooltipTrigger render={
            <Badge
              variant="secondary"
              className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10 hover:bg-muted transition-colors "
            >
              <strong className="text-foreground mr-1">Latest Log:</strong>
              {formatTimeAgo(fileTs)}
            </Badge>
          } />
          <TooltipContent className="text-xs">
            {full(fileTs)} {abbr()}
          </TooltipContent>
        </Tooltip>
      ) : (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10">
          <strong className="text-foreground mr-1">Latest Log:</strong>
          Never
        </Badge>
      )}
    </div>
  )
}
