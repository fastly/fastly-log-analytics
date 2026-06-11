'use client'

import React from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { useSyncStatus } from '@/hooks/useSyncStatus'
import { useBootstrap } from '@/hooks/useBootstrap'
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
  // Bootstrap fallback for analyst sessions — /api/sync-status is
  // admin-only (RemoteAccessMiddleware blocks analysts → 403), so
  // useSyncStatus returns no data for them. Bootstrap exposes an
  // analyst-safe `header_badge` with the two fields this badge
  // renders so analysts see Latest Log / Total Logs the same way
  // admins do. Refreshes at bootstrap's 5-min staleTime — fine for an
  // at-a-glance header.
  const { data: bootstrap } = useBootstrap()
  const headerBadge = (bootstrap as any)?.header_badge as
    | { latest_log_at?: string | null; local_rows?: number | null }
    | null
    | undefined

  // Pre-fix this had a 1-second setState ticker so the "Xs ago" label
  // advanced between the 15s polls. That ticker re-rendered the entire
  // header chrome every second — observable as visual jumpiness even
  // when nothing was actively loading. The "Xs ago" granularity is
  // good enough at the 15s poll cadence: the label updates whenever
  // the query refetches OR when any other state in the badge changes,
  // and the formatTimeAgo call below uses the latest fileTs each
  // render. Cost: the "X seconds" portion isn't real-time but the
  // value is at most 15s stale — fine for an operator glance.

  if (!activeServiceId) return null
  // Prefer the admin sync-status data (richer, polled every 30s);
  // fall back to bootstrap's header_badge for analyst sessions or
  // before sync-status has resolved.
  const fileTs =
    status?.latest_log_at ||
    status?.latest_available_file_at ||
    status?.latest_ingested_file_at ||
    headerBadge?.latest_log_at ||
    null
  const localRows = status?.local_rows ?? headerBadge?.local_rows ?? null
  if (!status && !headerBadge) return null

  return (
    <div className="hidden lg:flex items-center gap-2 mr-2 animate-in fade-in zoom-in-95">
      {localRows != null && (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10 hover:bg-muted transition-colors">
          <strong className="text-foreground mr-1">Total Logs:</strong>
          {localRows.toLocaleString()}
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
