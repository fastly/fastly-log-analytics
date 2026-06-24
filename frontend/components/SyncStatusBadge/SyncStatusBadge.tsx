'use client'

import React, { useEffect, useRef, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { usePathname } from 'next/navigation'
import { useServiceStore } from '@/stores/serviceStore'
import { useSyncStatus, useIsAnalyst } from '@/hooks/useSyncStatus'
import { useSyncStatusStream } from '@/hooks/useSyncStatusStream'
import { useHeaderBadgeStream } from '@/hooks/useHeaderBadgeStream'
import { useCronRunsStream } from '@/hooks/useCronRunsStream'
import { useSystemMetricsStream } from '@/hooks/useSystemMetricsStream'
import { useLastSync } from '@/hooks/useLastSync'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useElapsedTime } from '@/hooks/useElapsedTime'
import { useNowMs } from '@/hooks/useNowSeconds'
import { useMounted } from '@/hooks/useMounted'
import { TimeAgo } from '@/components/TimeAgo'
import { Badge } from '@/components/ui/badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

// The live "X ago" text node now lives in the shared `@/components/TimeAgo`
// (text-only leaf so the surrounding header chrome stays stable between
// cron pushes — putting useNowMs() on SyncStatusBadge itself would
// re-render the whole badge every second). Imported above.

// Mirrors the LiveTimer pattern from CronScheduleBox but sized for
// the header badge (font-size inherited from the badge instead of
// hardcoded text-xs / text-[9px]). Used while a sync is mid-flight
// so the header surfaces the live elapsed time instead of the static
// "running" label that doesn't change between SSE pushes.
function HeaderLiveTimer({ startedAt }: { startedAt: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className="font-mono text-blue-500 tabular-nums font-medium animate-pulse">{fmt}</span>
}

// SRE-07: standing escalation for a pure ingest stall. The "Latest Log" badge
// is on every page's header, but it only showed a neutral "X ago" — a stall
// (orphan-row / OOM-restart / FOS-slow modes where data simply stops landing)
// escalated nothing here; the only "stalled" classifier lived on the
// must-navigate /admin/usage-log chart. This text-leaf owns the 1 Hz tick (so
// the surrounding badge chrome stays stable) and shows an amber dot once the
// newest log is >1h old (well past normal Fastly delivery lag) → red at >3h.
// Plain `title` attr, not a nested Tooltip — the badge is already a tooltip
// trigger; mirrors the existing last-sync-error dot.
function StalenessDot({ timestamp }: { timestamp: string }) {
  const now = useNowMs()
  const mounted = useMounted()
  if (!mounted) return null
  const ageMs = now - new Date(timestamp).getTime()
  if (!(ageMs > 3_600_000)) return null
  const ageH = ageMs / 3_600_000
  const crit = ageH >= 3
  const label = `Latest log is ${ageH.toFixed(1)}h old — ingestion may be stalled`
  return (
    <span
      role="img"
      aria-label={label}
      title={label}
      className={`ml-1.5 ${crit ? 'text-red-500' : 'text-amber-500'}`}
    >
      ●
    </span>
  )
}

export function SyncStatusBadge() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { full, abbr } = useDateFormat()
  const pathname = usePathname()

  const { data: status } = useSyncStatus()
  const { data: lastSync } = useLastSync()
  const isAnalyst = useIsAnalyst()

  // /share-login is the anonymous landing for unauthenticated remote
  // visitors. The header still mounts during the redirect window, but
  // every backend SSE endpoint here will 403 → reconnect-loop without
  // ever delivering an event. Gate the streams off so the activeServiceId
  // (which can linger in zustand across the redirect) doesn't open a
  // burst of failing connections.
  const streamsEnabled = !pathname.startsWith('/share-login')

  // Three SSE channels feed the header, each with its own gate:
  //  - useSyncStatusStream (admin): full snapshot → ['sync-status', svc]
  //  - useHeaderBadgeStream (analyst): projected snapshot →
  //      ['bootstrap'].header_badge (closes the gap that left analyst
  //      badges stale at the 5-min bootstrap staleTime)
  //  - useCronRunsStream (admin): cron-run state changes → invalidates
  //      ['admin','cron-logs'], ['admin','cron-logs-recent'], and
  //      ['last-sync', svc] when event.task === 'sync'. Mounted here so
  //      the channel stays open across navigation (header is mounted
  //      on every page); /logs reads from the warm cache for free.
  const adminStreamState = useSyncStatusStream(streamsEnabled && !isAnalyst)
  const analystStreamState = useHeaderBadgeStream(streamsEnabled && isAnalyst)
  useCronRunsStream(streamsEnabled && !isAnalyst)
  // The active stream's connection state — picks whichever side is gated on
  // for the current session (admin OR analyst), defaults to idle when both
  // are off (e.g. /share-login).
  const liveStreamState = isAnalyst ? analystStreamState.state : adminStreamState.state

  // System-metrics stream feeds the seven admin-overview cards
  // (SystemHealthCard, OperationsOverview's three teasers,
  // MetadataStorageCard, SystemStatus). Gate on the pages that
  // actually mount those components so we don't open a sampler
  // loop server-side just because the header is on screen.
  const adminPageMounted =
    pathname.startsWith('/admin') || pathname.startsWith('/logs')
  useSystemMetricsStream(streamsEnabled && !isAnalyst && adminPageMounted)

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

  // a11y (WCAG 4.1.2): announce sync state transitions to screen readers
  // via an sr-only role="status" live region. We only fire on actual
  // state changes (tracked via useRef) — not on every poll/render — so
  // assistive tech doesn't spam "running" each time the badge re-renders.
  const prevSyncStatusRef = useRef<string | null | undefined>(undefined)
  const [a11yAnnouncement, setA11yAnnouncement] = useState<string>('')
  useEffect(() => {
    const current = lastSync?.status ?? null
    const prev = prevSyncStatusRef.current
    if (prev !== undefined && prev !== current) {
      if (current === 'running') {
        setA11yAnnouncement('Sync started')
      } else if (current === 'error') {
        setA11yAnnouncement('Sync errored')
      } else if (prev === 'running' && current && current !== 'running') {
        setA11yAnnouncement('Sync finished')
      }
    }
    prevSyncStatusRef.current = current
  }, [lastSync?.status])

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

  // SSE live dot. Green = stream open, amber = (re)connecting, hidden = idle.
  // Sized to match the existing badges (px-2 py-0.5 / h-5-ish). aria-live
  // off because the surrounding badges already announce sync state; this
  // is a visual at-a-glance affordance and shouldn't spam SR users.
  const liveDotTitle =
    liveStreamState === 'open' ? 'Live updates connected'
    : liveStreamState === 'connecting' ? 'Connecting to live updates…'
    : liveStreamState === 'reconnecting' ? 'Reconnecting to live updates…'
    : null
  const showLiveDot = liveDotTitle !== null

  return (
    <div className="hidden md:flex items-center gap-2 mr-2 animate-in fade-in zoom-in-95">
      {showLiveDot && (
        <Tooltip>
          <TooltipTrigger render={
            <span
              tabIndex={0}
              role="status"
              aria-label={liveDotTitle ?? ''}
              className="inline-flex items-center justify-center h-5 w-5 rounded-full hover:bg-muted/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <span className="relative flex h-2 w-2">
                {liveStreamState === 'open' ? (
                  <>
                    <span className="absolute inline-flex h-full w-full rounded-full bg-emerald-500 opacity-60 animate-ping" />
                    <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500" />
                  </>
                ) : (
                  <>
                    <span className="absolute inline-flex h-full w-full rounded-full bg-amber-500 opacity-60 animate-ping" />
                    <span className="relative inline-flex h-2 w-2 rounded-full bg-amber-500" />
                  </>
                )}
              </span>
            </span>
          } />
          <TooltipContent className="text-xs">{liveDotTitle}</TooltipContent>
        </Tooltip>
      )}
      {localRows != null && (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/70 border-muted-foreground/10 hover:bg-muted transition-colors min-w-[172px] tabular-nums">
          <strong className="text-foreground mr-1">Total Logs:</strong>
          {localRows.toLocaleString()}
        </Badge>
      )}

      {lastSync?.started_at && (
        <Tooltip>
          {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so keyboard
              users can focus this badge and read the full timestamp tooltip. */}
          <TooltipTrigger render={
            <Badge
              variant="secondary"
              tabIndex={0}
              role="button"
              aria-label="Last sync details"
              aria-live="off"
              className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/70 border-muted-foreground/10 hover:bg-muted transition-colors min-w-[156px] tabular-nums focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <strong className="text-foreground mr-1">Last Sync:</strong>
              {lastSync.status === 'running' ? (
                <span className="inline-flex items-center gap-1" aria-label="Sync in progress">
                  <Loader2 className="h-3 w-3 animate-spin shrink-0 text-blue-500" aria-hidden="true" />
                  {lastSync.started_at && <HeaderLiveTimer startedAt={lastSync.started_at} />}
                </span>
              ) : (
                <TimeAgo timestamp={
                  lastSync.duration_s != null && lastSync.started_at
                    ? new Date(new Date(lastSync.started_at).getTime() + lastSync.duration_s * 1000).toISOString()
                    : lastSync.started_at
                } />
              )}
              {lastSync.status === 'error' && (
                <span
                  role="img"
                  aria-label="Last sync errored"
                  title="Last sync errored"
                  className="ml-1.5 text-red-500"
                >●</span>
              )}
            </Badge>
          } />
          <TooltipContent className="text-xs">
            {full(
              lastSync.duration_s != null && lastSync.started_at
                ? new Date(new Date(lastSync.started_at).getTime() + lastSync.duration_s * 1000).toISOString()
                : lastSync.started_at
            )} {abbr()} — status: {lastSync.status ?? 'unknown'}
            {lastSync.duration_s != null && ` — duration ${lastSync.duration_s.toFixed(1)}s`}
          </TooltipContent>
        </Tooltip>
      )}

      {fileTs ? (
        <Tooltip>
          {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so keyboard
              users can focus this badge and read the full timestamp tooltip. */}
          <TooltipTrigger render={
            <Badge
              variant="secondary"
              tabIndex={0}
              role="button"
              aria-label="Latest log details"
              aria-live="off"
              className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/70 border-muted-foreground/10 hover:bg-muted transition-colors min-w-[156px] tabular-nums focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <strong className="text-foreground mr-1">Latest Log:</strong>
              <TimeAgo timestamp={fileTs} />
              <StalenessDot timestamp={fileTs} />
            </Badge>
          } />
          <TooltipContent className="text-xs">
            {full(fileTs)} {abbr()}
          </TooltipContent>
        </Tooltip>
      ) : (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/70 border-muted-foreground/10">
          <strong className="text-foreground mr-1">Latest Log:</strong>
          Never
        </Badge>
      )}

      {/* a11y: sr-only live region for screen readers — announces sync
          state transitions (started / finished / errored) only when the
          status actually changes, not on every poll. */}
      <span role="status" aria-live="polite" className="sr-only">
        {a11yAnnouncement}
      </span>
    </div>
  )
}
