'use client'

import React, { useMemo, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Calendar, Check, ChevronsUpDown, CloudDownload, Loader2, Pin, PinOff } from 'lucide-react'
import { usePathname } from 'next/navigation'
import { Button, buttonVariants } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { useIsAnalyst, useBootstrap, useMounted } from '@/hooks/useBootstrap'
import {
  useAdminEventStream,
  useElapsedTime,
  useHeaderBadgeStream,
  useLastSync,
  useLocalStorageString,
  useNowMs,
  useOpenGeneration,
  useSeededReady,
  useSyncStatus,
  type AdminEventChannel,
} from '@/hooks/useSSE'
import { formatBytes, formatCompactDuration, formatCount, formatForInput, formatTimeAgo, parseFromInput, toUTCDate, cn, formatDateTime, panelDialogContent, panelDialogHeaderMuted } from "@/lib/utils";
import { adminJsonResult, queryKeys } from "@/lib/api";
import type { components } from '@/types/api'

const PREFS_KEY = 'fla-service-switcher-prefs'
const MAX_MRU = 5

interface ServicePrefs {
  pinned: string[]
  mru: string[]
}

function parsePrefs(raw: string | null): ServicePrefs {
  if (!raw) return { pinned: [], mru: [] }
  try {
    const parsed = JSON.parse(raw)
    return {
      pinned: Array.isArray(parsed.pinned) ? parsed.pinned : [],
      mru: Array.isArray(parsed.mru) ? parsed.mru : [],
    }
  } catch {
    return { pinned: [], mru: [] }
  }
}

export function ServiceSwitcher() {
  const [open, setOpen] = React.useState(false)
  const services = useServiceStore((s) => s.services)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const pathname = usePathname()

  const [prefsRaw, writePrefsRaw] = useLocalStorageString(PREFS_KEY)
  const prefs = React.useMemo(() => parsePrefs(prefsRaw), [prefsRaw])
  const savePrefs = React.useCallback((next: ServicePrefs) => writePrefsRaw(JSON.stringify(next)), [writePrefsRaw])

  React.useEffect(() => {
    if (!activeServiceId || prefs.mru[0] === activeServiceId) return
    savePrefs({
      ...prefs,
      mru: [activeServiceId, ...prefs.mru.filter((x) => x !== activeServiceId)].slice(0, MAX_MRU),
    })
  }, [activeServiceId, prefs, savePrefs])

  const activeService = services.find((s) => s.id === activeServiceId)
  const byId = React.useMemo(() => {
    const m = new Map<string, (typeof services)[number]>()
    for (const s of services) m.set(s.id, s)
    return m
  }, [services])

  const pinnedServices = React.useMemo(
    () => prefs.pinned.map((id) => byId.get(id)).filter(Boolean) as typeof services,
    [prefs.pinned, byId],
  )
  const pinnedSet = React.useMemo(() => new Set(prefs.pinned), [prefs.pinned])
  const recentServices = React.useMemo(
    () =>
      prefs.mru
        .filter((id) => !pinnedSet.has(id))
        .map((id) => byId.get(id))
        .filter(Boolean) as typeof services,
    [prefs.mru, pinnedSet, byId],
  )
  const recentSet = React.useMemo(() => new Set(recentServices.map((s) => s.id)), [recentServices])
  const otherServices = React.useMemo(
    () =>
      services
        .filter((s) => !pinnedSet.has(s.id) && !recentSet.has(s.id))
        .slice()
        .sort((a, b) => a.name.localeCompare(b.name)),
    [services, pinnedSet, recentSet],
  )

  const handleSelect = (id: string) => {
    setActiveServiceId(id)
    setOpen(false)
    window.location.assign(`${pathname}?service=${id}`)
  }

  const handleTogglePin = (id: string) => {
    savePrefs({
      ...prefs,
      pinned: prefs.pinned.includes(id)
        ? prefs.pinned.filter((x) => x !== id)
        : [...prefs.pinned, id],
    })
  }

  const renderRow = (
    service: (typeof services)[number],
    opts: { pinned: boolean },
  ) => (
    <CommandItem
      key={service.id}
      value={service.name}
      onSelect={() => handleSelect(service.id)}
      className="group/row flex items-center gap-1 pr-1"
    >
      <Check
        className={cn(
          'mr-2 h-4 w-4 shrink-0',
          activeServiceId === service.id ? 'opacity-100' : 'opacity-0',
        )}
      />
      <span className="truncate flex-1">{service.name}</span>
      <Button
        variant="ghost"
        size="icon"
        aria-label={opts.pinned ? `Unpin ${service.name}` : `Pin ${service.name} to top`}
        title={opts.pinned ? 'Unpin' : 'Pin to top'}
        onClick={(e) => {
          e.stopPropagation()
          handleTogglePin(service.id)
        }}
        className={cn(
          'h-6 w-6 shrink-0 text-muted-foreground hover:text-foreground',
          opts.pinned
            ? 'opacity-90'
            : 'opacity-0 group-hover/row:opacity-60 focus-visible:opacity-100',
        )}
      >
        {opts.pinned ? <Pin className="h-3 w-3" /> : <PinOff className="h-3 w-3" />}
      </Button>
    </CommandItem>
  )

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        role="combobox"
        aria-expanded={open}
        aria-label="Active service"
        className={cn(
          buttonVariants({ variant: 'outline' }),
          'w-auto max-w-[60vw] sm:w-[250px] sm:max-w-none justify-between',
        )}
      >
        <span className="flex min-w-0 flex-1 items-center justify-between">
          <span className="truncate">
            {activeService ? activeService.name : 'Select service...'}
          </span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </span>
      </PopoverTrigger>
      <PopoverContent className="w-[260px] p-0">
        <Command>
          <CommandInput placeholder="Search service..." />
          <CommandList>
            <CommandEmpty>No service found.</CommandEmpty>
            {pinnedServices.length > 0 && (
              <CommandGroup heading="Pinned">
                {pinnedServices.map((s) => renderRow(s, { pinned: true }))}
              </CommandGroup>
            )}
            {recentServices.length > 0 && (
              <>
                {pinnedServices.length > 0 && <CommandSeparator />}
                <CommandGroup heading="Recent">
                  {recentServices.map((s) => renderRow(s, { pinned: false }))}
                </CommandGroup>
              </>
            )}
            {otherServices.length > 0 && (
              <>
                {(pinnedServices.length > 0 || recentServices.length > 0) && (
                  <CommandSeparator />
                )}
                <CommandGroup
                  heading={
                    pinnedServices.length > 0 || recentServices.length > 0
                      ? 'All services'
                      : undefined
                  }
                >
                  {otherServices.map((s) => renderRow(s, { pinned: false }))}
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

interface SyncFromCloudModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onStartSync: (range?: { start: string; end: string }) => void
}

type LakeInfo = components['schemas']['ProvisionLakeInfoResponse']

export function SyncFromCloudModal(props: SyncFromCloudModalProps) {
  const generation = useOpenGeneration(props.open)
  return <SyncFromCloudModalBody key={generation} {...props} />
}

function SyncFromCloudModalBody({ open, onOpenChange, onStartSync }: SyncFromCloudModalProps) {
  const { timezone } = useTimezoneStore()
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  const lakeQuery = useQuery({
    queryKey: ['lake-info', activeServiceId],
    queryFn: async (): Promise<LakeInfo> => {
      const resp = await adminJsonResult(
        '/api/services/{service_id}/lake-info',
        `/api/services/${activeServiceId}/lake-info`,
      )
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}: ${resp.statusText}`)
      }
      const data: LakeInfo = resp.data
      if (!data.ok) {
        throw new Error(data.error || data.message || 'Failed to analyze data lake.')
      }
      return data
    },
    enabled: open && !!activeServiceId,
    retry: false,
    staleTime: 0,
    gcTime: 0,
    refetchOnWindowFocus: false,
  })
  const lakeInfo = lakeQuery.data ?? null
  const isAnalyzing = lakeQuery.isFetching
  const lakeError = lakeQuery.error
    ? lakeQuery.error.message || 'Connection error while analyzing data lake.'
    : null
  const handleAnalyzeLake = () => {
    void lakeQuery.refetch()
  }

  const [importMode, setImportMode] = useState<"all" | "range">("all")
  const [rangeDraft, setRangeDraft] = useState<{ start: string; end: string } | null>(null)
  const importRange = rangeDraft ?? (
    lakeInfo?.range?.start && lakeInfo.range.end
      ? { start: lakeInfo.range.start, end: lakeInfo.range.end }
      : { start: "", end: "" }
  )
  const setImportRange = (update: (prev: { start: string; end: string }) => { start: string; end: string }) =>
    setRangeDraft(update(importRange))

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0
    let total = 0
    const start = importRange.start
    const end = importRange.end

    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue

      if (importMode === "range") {
        if (start && dateStr < start.split('T')[0]) continue
        if (end && dateStr > end.split('T')[0]) continue
      }

      total += stats.size_bytes
    }
    return total
  }, [lakeInfo, importMode, importRange])

  const handleStartSync = () => {
    if (importMode === "range") {
      onStartSync({ start: importRange.start, end: importRange.end })
    } else {
      onStartSync()
    }
    onOpenChange(false)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2">
              <CloudDownload className="h-5 w-5 text-primary" />
              Sync from Cloud
            </DialogTitle>
          </div>
          <div className="text-sm text-muted-foreground mt-1">
            Import missing historical data from the shared Iceberg table.
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto p-6 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Available Cloud Data</h3>
              <p className="text-[10px] text-muted-foreground mt-1">Select the range of data you want to import into your local cache.</p>
            </div>
            {lakeInfo && !isAnalyzing && (
              <Badge variant="secondary" className="font-mono bg-muted/50 border shadow-sm">
                ~{formatBytes(estimatedImportSize)}
              </Badge>
            )}
          </div>

          {isAnalyzing ? (
            <div className="p-4 border border-dashed rounded-lg bg-muted/5 flex flex-col items-center justify-center space-y-3 py-12">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              <div className="text-center">
                <p className="text-sm font-medium">Scanning Cloud Catalog...</p>
                <p className="text-xs text-muted-foreground">Checking Iceberg metadata for available snapshots.</p>
              </div>
            </div>
          ) : lakeError ? (
            <div className="p-4 border border-destructive/20 rounded-lg bg-destructive/5 text-center space-y-2">
              <p className="text-xs text-destructive font-medium">Analysis Failed</p>
              <p className="text-[10px] text-muted-foreground">{lakeError}</p>
              <Button variant="outline" size="sm" onClick={handleAnalyzeLake} className="h-7 text-[10px] mt-2">
                Try Again
              </Button>
            </div>
          ) : lakeInfo?.table_exists ? (
            <div className="space-y-4">
              <div className="bg-background/50 border rounded-lg p-3 grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Available From</span>
                  <div className="text-xs font-mono font-semibold flex items-center gap-1.5">
                    <Calendar className="h-3 w-3 text-primary" />
                    {formatDateTime(lakeInfo.range?.start, timezone)}
                  </div>
                </div>
                <div className="space-y-1">
                  <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Available To</span>
                  <div className="text-xs font-mono font-semibold flex items-center gap-1.5">
                    <Calendar className="h-3 w-3 text-primary" />
                    {formatDateTime(lakeInfo.range?.end, timezone)}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <button
                  onClick={() => setImportMode("all")}
                  className={`flex flex-col items-center gap-2 p-3 border rounded-lg transition-all text-center ${importMode === "all" ? "border-primary bg-primary/5 ring-1 ring-primary/20" : "border-muted hover:bg-muted/50"}`}
                >
                  <div className="font-bold text-xs">Import All</div>
                  <p className="text-[9px] text-muted-foreground">Sync every available file</p>
                </button>
                <button
                  onClick={() => setImportMode("range")}
                  className={`flex flex-col items-center gap-2 p-3 border rounded-lg transition-all text-center ${importMode === "range" ? "border-primary bg-primary/5 ring-1 ring-primary/20" : "border-muted hover:bg-muted/50"}`}
                >
                  <div className="font-bold text-xs">Custom Range</div>
                  <p className="text-[9px] text-muted-foreground">Choose specific times</p>
                </button>
              </div>

              {importMode === "range" && (
                <div className="p-4 border rounded-lg bg-muted/5 grid grid-cols-2 gap-4">
                  <div className="space-y-1.5">
                    <Label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Start Time</Label>
                    <input
                      type="datetime-local"
                      step="1"
                      value={formatForInput(importRange.start, timezone)}
                      min={formatForInput(lakeInfo.range?.start ?? '', timezone)}
                      max={formatForInput(importRange.end || lakeInfo.range?.end || '', timezone)}
                      onChange={(e) => setImportRange(prev => ({ ...prev, start: parseFromInput(e.target.value, timezone) ?? '' }))}
                      className="flex h-8 w-full rounded-md border border-input bg-background px-3 py-1 text-xs shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">End Time</Label>
                    <input
                      type="datetime-local"
                      step="1"
                      value={formatForInput(importRange.end, timezone)}
                      min={formatForInput(importRange.start || lakeInfo.range?.start || '', timezone)}
                      max={formatForInput(lakeInfo.range?.end ?? '', timezone)}
                      onChange={(e) => setImportRange(prev => ({ ...prev, end: parseFromInput(e.target.value, timezone) ?? '' }))}
                      className="flex h-8 w-full rounded-md border border-input bg-background px-3 py-1 text-xs shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
                    />
                  </div>
                </div>
              )}

              <Button onClick={handleStartSync} className="w-full h-8 text-xs mt-2" variant="default">
                Start Sync
              </Button>
            </div>
          ) : (
            <div className="p-4 border border-dashed rounded-lg bg-muted/5 text-center space-y-2">
              <p className="text-xs text-muted-foreground">No data found in the lake.</p>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

export function TimeAgo({
  timestamp,
  fallback = null,
}: {
  timestamp: string | null | undefined
  fallback?: ReactNode
}) {
  useNowMs()
  const mounted = useMounted()
  if (!timestamp) return <>{fallback}</>
  if (!mounted) return <>{fallback}</>
  return <>{formatTimeAgo(timestamp)}</>
}

/** Ticking "Ns" / "Nm Ns" elapsed label for a running job. Callers supply the
 *  typography via ``className`` (the tick itself is the shared useNowMs ticker). */
export function LiveElapsed({ startedAt, className }: { startedAt: string; className?: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className={className}>{fmt}</span>
}

/** Ticking countdown until ``when`` (or "Disabled" when nullish). */
export function NextRunCountdown({ when }: { when: string | null | undefined }) {
  const nowMs = useNowMs()
  if (!when) return <>Disabled</>
  return <>{formatCompactDuration(Math.floor((toUTCDate(when).getTime() - nowMs) / 1000))}</>
}

const SEED_KEYS = [queryKeys.syncStatus, queryKeys.lastSync] as const

export function SyncStatusBadge() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // Sibling Warning and Cache Mount Race Fix: returning null on the first client
  // render lets us safely and synchronously seed the query cache with empty/placeholder
  // records inside a post-mount useEffect BEFORE we call useSyncStatus / useLastSync.
  // This guarantees that when the inner component mounts and calls those hooks, the cache
  // keys already exist and we never trigger a mid-render QueryCache.build -> add notify
  // update that causes React 19 setState-in-render violations.
  const ready = useSeededReady(activeServiceId, SEED_KEYS, true)

  if (!ready) return null

  return <SyncStatusBadgeInner />
}

function SyncStatusBadgeInner() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
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

  // ONE multiplexed admin SSE connection feeds the header (collapsed from
  // three separate streams that each held an HTTP/1.1 connection open and
  // starved the tunnel). Channels:
  //  - sync-status: full snapshot → ['sync-status', svc]
  //  - cron-runs: cron-run state changes → invalidates ['admin','cron-logs'],
  //      ['admin','cron-logs-recent'], ['last-sync', svc] (on task === 'sync'),
  //      and the iceberg/audit/ingested/schema keys.
  //  - system-metrics: bundled snapshot → the seven admin-overview slice keys.
  //  - share: lean tunnel-live snapshot → ['admin','share','live'] (only on
  //      /admin/share; replaces the page's separate useShareStream connection).
  // Mounted in the always-present header so the channels stay open across
  // navigation; /logs + /admin read from the warm cache for free.
  // The analyst path keeps its own single projected stream (log-extents).
  useHeaderBadgeStream(streamsEnabled && isAnalyst)

  // system-metrics only feeds the admin-overview cards (SystemHealthCard,
  // OperationsOverview teasers, MetadataStorageCard, SystemStatus) — add it
  // to the channel set only on the pages that mount those components so we
  // don't run a server-side sampler loop just because the header is on screen.
  // share likewise only feeds the /admin/share dashboard's live tile; gating
  // it here (instead of a second useShareStream connection on that page) keeps
  // /admin/share at ONE SSE connection over the H1 admin tunnel.
  const adminPageMounted =
    pathname.startsWith('/admin') || pathname.startsWith('/logs')
  const sharePageMounted = pathname.startsWith('/admin/share')
  const adminChannels = useMemo<AdminEventChannel[]>(() => {
    const ch: AdminEventChannel[] = ['sync-status', 'cron-runs']
    if (adminPageMounted) ch.push('system-metrics')
    if (sharePageMounted) ch.push('share')
    return ch
  }, [adminPageMounted, sharePageMounted])
  useAdminEventStream(streamsEnabled && !isAnalyst, adminChannels)

  // Bootstrap fallback for analyst sessions — /api/sync-status is
  // admin-only (RemoteAccessMiddleware blocks analysts → 403), so
  // useSyncStatus returns no data for them. Bootstrap exposes an
  // analyst-safe `header_badge` with the two fields this badge
  // renders so analysts see Latest Log / Total Logs the same way
  // admins do. Refreshes at bootstrap's 5-min staleTime — fine for an
  // at-a-glance header.
  const { data: bootstrap } = useBootstrap()
  const headerBadge = bootstrap?.header_badge

  // a11y (WCAG 4.1.2): announce sync state transitions to screen readers
  // via an sr-only role="status" live region. We only fire on actual
  // state changes (tracked against the previously rendered status) — not on every
  // poll/render — so assistive tech doesn't spam "running" each time the badge re-renders.
  const currentSyncStatus = lastSync?.status ?? null
  const [prevSyncStatus, setPrevSyncStatus] = useState<string | null>(currentSyncStatus)
  const [a11yAnnouncement, setA11yAnnouncement] = useState<string>('')
  if (prevSyncStatus !== currentSyncStatus) {
    setPrevSyncStatus(currentSyncStatus)
    if (currentSyncStatus === 'running') setA11yAnnouncement('Sync started')
    else if (currentSyncStatus === 'error') setA11yAnnouncement('Sync errored')
    else if (prevSyncStatus === 'running' && currentSyncStatus) setA11yAnnouncement('Sync finished')
  }

  if (!activeServiceId) return null
  if (!status && !headerBadge) return null

  const activeSvc = bootstrap?.services?.find(s => s.service_id === activeServiceId)
  const isRumEnabled = activeSvc?.rum_enabled ?? false

  // A current snapshot can explicitly clear metrics after a reset; do not
  // fill those clears with older bootstrap values.
  const snapshot = status ?? headerBadge
  const rumMetrics = snapshot?.rum
  const requestMetrics = snapshot?.request

  // Derive real-time values for REQUEST using the live-updated sync status query
  const rumTotal = rumMetrics?.total_rows ?? 0
  const liveLocalRows = snapshot?.local_rows ?? null
  const requestTotal =
    requestMetrics?.total_rows ??
    (liveLocalRows !== null
      ? (liveLocalRows > rumTotal && rumTotal > 0 ? liveLocalRows - rumTotal : liveLocalRows)
      : null)

  const requestLatestLogAt = snapshot?.request !== undefined
    ? requestMetrics?.latest_log_at ?? null
    : snapshot?.latest_log_at ?? null
  const requestLastSyncAt = lastSync?.started_at ?? requestMetrics?.last_sync_at ?? null

  const renderStreamRow = (
    label: string,
    latestTs: string | null | undefined,
    totalRows: number | null | undefined,
    lastSyncTs: string | null | undefined,
    isRunning: boolean = false,
    startedAt: string | null | undefined = null,
  ) => {
    return (
      <div key={label} className="flex items-center gap-1 min-w-0 text-[10px]">
        {/* Column 0: Label (fixed w-[58px]) */}
        <span className="w-[58px] flex-shrink-0 font-semibold text-muted-foreground whitespace-nowrap">{label}</span>

        {/* Column 1: Latest Log TimeAgo (fixed w-[124px] + tabular-nums) */}
        {latestTs ? (
          <span className="w-[124px] flex-shrink-0 text-muted-foreground whitespace-nowrap tabular-nums inline-flex items-center gap-1">
            <span className="text-muted-foreground/80 font-normal">latest:</span>
            <TimeAgo timestamp={latestTs} />
          </span>
        ) : (
          <span className="w-[124px] flex-shrink-0 text-muted-foreground whitespace-nowrap inline-flex items-center gap-1">
            <span className="text-muted-foreground/80 font-normal">latest:</span>
            <span>Never</span>
          </span>
        )}

        {/* Column 2: Row Count (fixed w-[110px] + tabular-nums + text-right + pr-2) */}
        {totalRows != null && totalRows > 0 ? (
          <span className="w-[110px] flex-shrink-0 text-muted-foreground whitespace-nowrap text-left pr-2 tabular-nums">total: {formatCount(totalRows)}</span>
        ) : (
          <span className="w-[110px] flex-shrink-0 text-muted-foreground whitespace-nowrap text-left pr-2">—</span>
        )}

        {/* Column 3: Last Sync (fixed w-[105px] + tabular-nums + text-right) */}
        {lastSyncTs ? (
          <span className="w-[105px] flex-shrink-0 text-muted-foreground whitespace-nowrap text-[9px] inline-flex items-center justify-start gap-1 tabular-nums">
            <span className="text-muted-foreground/80">sync:</span>
            {isRunning && startedAt ? (
              <span className="inline-flex items-center gap-1 font-semibold text-blue-500 animate-pulse" aria-label="Sync in progress">
                <Loader2 className="h-2.5 w-2.5 animate-spin shrink-0 text-blue-500" aria-hidden="true" />
                <LiveElapsed startedAt={startedAt} className="font-mono text-blue-500 tabular-nums font-medium animate-pulse" />
              </span>
            ) : (
              <TimeAgo timestamp={lastSyncTs} />
            )}
          </span>
        ) : (
          <span className="w-[105px] flex-shrink-0 text-muted-foreground whitespace-nowrap text-[9px] text-left">—</span>
        )}
      </div>
    )
  }

  return (
    <div className="hidden md:flex flex-col gap-0.5 mr-2 animate-in fade-in zoom-in-95">
      <div className="flex flex-col gap-0.5">
        {/* REQUEST logs row */}
        {renderStreamRow(
          'REQUEST',
          requestLatestLogAt,
          requestTotal,
          requestLastSyncAt,
          lastSync?.status === 'running',
          lastSync?.started_at,
        )}

        {/* RUM logs row */}
        {isRumEnabled && renderStreamRow(
          'RUM',
          rumMetrics?.latest_log_at,
          rumMetrics?.total_rows,
          rumMetrics?.last_sync_at,
        )}
      </div>

      {/* a11y: sr-only live region for screen readers */}
      <span role="status" aria-live="polite" className="sr-only">
        {a11yAnnouncement}
      </span>
    </div>
  )
}
