'use client'

import * as React from 'react'
import { X, Plus, Bot } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useFilterStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useServiceStore } from '@/stores/serviceStore'
import { formatForInput, parseFromInput } from '@/lib/date'
import { resolveSnappedWindow } from '@/lib/log-extents-snap'
import { useQuery, useIsFetching } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useBootstrapPending } from '@/hooks/useIsDataReady'
import { usePathname } from 'next/navigation'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { AddFilterDialog } from './AddFilterDialog'
import { SaveViewDialog } from './SaveViewDialog'
import { ViewSelector } from './ViewSelector'

import { useShallow } from 'zustand/react/shallow'

// First-element of every React Query key whose results depend on the
// global start/end time range. Used by the active-pill loading dot's
// predicate so background polls (admin metrics, log-extents, etc.)
// don't blink the pill while their refetches are in flight.
const TIME_BOUND_QUERY_KEYS = new Set([
  'dashboard',
  'sessions',
  'usage',
  'insights',
  'usage-log',
])

export const FilterBar = React.memo(function FilterBar() {
  const pathname = usePathname()
  const [mounted, setMounted] = React.useState(false)
  // Selector form so unrelated serviceStore mutations (setServices,
  // setInitialized) don't trigger FilterBar re-renders.
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const {
    startTime,
    endTime,
    filters,
    edgeOnly,
    hasSyncedExtents,
    isAutoRange,
    relativeRange,
    setRange,
    setRelativeRange,
    autoSetRange,
    setHasSyncedExtents,
    removeFilter,
    toggleFilterMode,
    toggleEdgeOnly,
    clearFilters,
    resetAll,
    resetRange,
    compareMode,
    compareStartTime,
    compareEndTime,
    toggleCompareMode,
    setCompareRange
  } = useFilterStore(useShallow(state => ({
    startTime: state.startTime,
    endTime: state.endTime,
    filters: state.filters,
    edgeOnly: state.edgeOnly,
    hasSyncedExtents: state.hasSyncedExtents,
    isAutoRange: state.isAutoRange,
    relativeRange: state.relativeRange,
    setRange: state.setRange,
    setRelativeRange: state.setRelativeRange,
    autoSetRange: state.autoSetRange,
    setHasSyncedExtents: state.setHasSyncedExtents,
    removeFilter: state.removeFilter,
    toggleFilterMode: state.toggleFilterMode,
    toggleEdgeOnly: state.toggleEdgeOnly,
    clearFilters: state.clearFilters,
    resetAll: state.resetAll,
    resetRange: state.resetRange,
    compareMode: state.compareMode,
    compareStartTime: state.compareStartTime,
    compareEndTime: state.compareEndTime,
    toggleCompareMode: state.toggleCompareMode,
    setCompareRange: state.setCompareRange
  })))

  // Local state for custom date picker
  const [localStart, setLocalStart] = React.useState('')
  const [localEnd, setLocalEnd] = React.useState('')
  const [localCompareStart, setLocalCompareStart] = React.useState('')
  const [localCompareEnd, setLocalCompareEnd] = React.useState('')

  // Local state for optimistic UI toggles
  const [localEdgeOnly, setLocalEdgeOnly] = React.useState(edgeOnly)
  const [localCompareMode, setLocalCompareMode] = React.useState(compareMode)

  React.useEffect(() => {
    setLocalEdgeOnly(edgeOnly)
  }, [edgeOnly])

  React.useEffect(() => {
    setLocalCompareMode(compareMode)
  }, [compareMode])

  const handleToggleEdgeOnly = React.useCallback((checked: boolean) => {
    setLocalEdgeOnly(checked)
    React.startTransition(() => {
      if (checked !== edgeOnly) toggleEdgeOnly()
    })
  }, [edgeOnly, toggleEdgeOnly])

  const handleToggleCompareMode = React.useCallback((checked: boolean) => {
    setLocalCompareMode(checked)
    React.startTransition(() => {
      if (checked !== compareMode) {
        toggleCompareMode()
      }
    })
  }, [compareMode, toggleCompareMode])

  React.useEffect(() => {
    setMounted(true)
  }, [])

  function fmtBotId(id: string): string {
    return id.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
  }

  // Auto-sync bounds from API when changing service.
  // Uses /api/log-extents — an analyst-safe sibling of /api/sync-status that
  // returns only {configured, earliest_log_at, latest_log_at} with none of
  // the admin-only fields (ngwaf_workspace_id, active_run, cron task state)
  // that get the admin endpoint 403'd for remote analysts. Swapping here
  // closes the analyst-403-every-3s polling loop.
  //
  // Perf audit Phase D: useBootstrap seeds ['log-extents', sid] in its
  // queryFn from bootstrap's log_extents field. Gate on bootstrap
  // pending so this query hits the seeded cache on cold load.
  const bootstrapPending = useBootstrapPending()
  const { data: status } = useQuery({
    queryKey: ['log-extents', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/log-extents")
      return data
    },
    enabled: !!activeServiceId && !bootstrapPending,
    refetchInterval: (query) => {
      // Keep polling if we haven't seen valid log extents yet
      const data = query.state.data;
      if (data && (!data.earliest_log_at || !data.latest_log_at)) {
        return 3000;
      }
      // Or if we haven't successfully synced extents locally
      if (!hasSyncedExtents) {
        return 3000;
      }
      return false; // Stop polling once we have data and have synced
    },
    refetchIntervalInBackground: false,
  })

  React.useEffect(() => {
    // If the app just loaded OR the user clicked Reset, snap to available extents.
    // Also re-snap if extents were initially empty but isAutoRange is still true.
    // The snap decision itself (spanDays/ageMinutes/15-min staleness threshold,
    // shared with the SSR seed so first paint doesn't disagree with this effect —
    // see lib/log-extents-snap.ts) lives in resolveSnappedWindow.
    if (status && (!hasSyncedExtents || isAutoRange)) {
      if (isAutoRange) {
        const snapped = resolveSnappedWindow(status, new Date())
        if (snapped) {
          autoSetRange(snapped.start, snapped.end)
        } else {
          // Missing extents, or data is fresh (last log <15min old) — the
          // default "last 24h from now" already captures it. Keep it, just
          // mark auto-range as done.
          autoSetRange(useFilterStore.getState().startTime, useFilterStore.getState().endTime)
        }
      }
      setHasSyncedExtents(true)
    }
  }, [status, hasSyncedExtents, isAutoRange, autoSetRange, setHasSyncedExtents])

  // Reset extents sync state ONLY when the user explicitly switches services,
  // not on initial page load or hydration.
  const prevServiceId = React.useRef(activeServiceId)
  React.useEffect(() => {
    if (activeServiceId && prevServiceId.current && activeServiceId !== prevServiceId.current) {
      resetRange()
    }
    prevServiceId.current = activeServiceId
  }, [activeServiceId, resetRange])

  // Sync local inputs with global store state
  const { timezone } = useTimezoneStore()
  React.useEffect(() => {
    if (startTime && endTime) {
      setLocalStart(formatForInput(startTime, timezone))
      setLocalEnd(formatForInput(endTime, timezone))
    }
    if (compareStartTime && compareEndTime) {
      setLocalCompareStart(formatForInput(compareStartTime, timezone))
      setLocalCompareEnd(formatForInput(compareEndTime, timezone))
    }
  }, [startTime, endTime, compareStartTime, compareEndTime, timezone])

  const handleApplyCustomDate = React.useCallback(() => {
    React.startTransition(() => {
      if (localStart && localEnd) {
        const parsedStart = parseFromInput(localStart, timezone)
        const parsedEnd = parseFromInput(localEnd, timezone)
        if (parsedStart && parsedEnd) {
          setRange(parsedStart, parsedEnd)
        }
      }
      if (compareMode && localCompareStart && localCompareEnd) {
        const parsedStart = parseFromInput(localCompareStart, timezone)
        const parsedEnd = parseFromInput(localCompareEnd, timezone)
        if (parsedStart && parsedEnd) {
          setCompareRange(parsedStart, parsedEnd)
        }
      }
    })
  }, [localStart, localEnd, compareMode, localCompareStart, localCompareEnd, timezone, setRange, setCompareRange])

  // Same-duration compare presets. Offsets the CURRENT primary range
  // backwards by N days and commits straight to the store — local inputs
  // re-sync from the effect at L243-246. Same-duration matches what
  // toggleCompareMode auto-seeds, so "1 week ago" means "the same window
  // a week earlier" regardless of how long the primary window is. A
  // "previous period" button is intentionally absent because the compare
  // toggle already seeds that on activation.
  const applyComparePreset = React.useCallback((offsetDays: number) => {
    // Prefer the in-flight datetime input values so users get a preset
    // relative to what they're typing, not the last-applied range. Fall
    // back to the committed store range when locals haven't been touched
    // yet (compare toggle just flipped on).
    const primaryStartIso = parseFromInput(localStart, timezone) ?? startTime
    const primaryEndIso = parseFromInput(localEnd, timezone) ?? endTime
    const offsetMs = offsetDays * 24 * 60 * 60 * 1000
    const start = new Date(new Date(primaryStartIso).getTime() - offsetMs).toISOString()
    const end = new Date(new Date(primaryEndIso).getTime() - offsetMs).toISOString()
    React.startTransition(() => {
      setCompareRange(start, end)
    })
  }, [localStart, localEnd, startTime, endTime, timezone, setCompareRange])

  const handleReset = React.useCallback(() => {
    React.startTransition(() => {
      resetAll()
    })
  }, [resetAll])

  const spanHours = React.useMemo(() => {
    if (!startTime || !endTime) return null
    return (new Date(endTime).getTime() - new Date(startTime).getTime()) / (1000 * 3600)
  }, [startTime, endTime])

  // Prefer the explicit `relativeRange` flag (set by pill click) over
  // duration-derivation. Derivation is the fallback for legacy bookmarks
  // and saved views whose absolute timestamps happen to match a pill.
  const activePreset = React.useMemo(() => {
    if (relativeRange) return relativeRange
    if (!spanHours || !endTime) return null
    if (Math.abs(new Date(endTime).getTime() - new Date().getTime()) > 60000) {
      return null
    }
    const h = Math.round(spanHours * 10) / 10
    if (h === 1) return '1h'
    if (h === 3) return '3h'
    if (h === 6) return '6h'
    if (h === 12) return '12h'
    if (h === 24) return '24h'
    if (h === 72) return '3d'
    if (h === 168) return '7d'
    if (h === 720) return '30d'
    return null
  }, [relativeRange, spanHours, endTime])

  // Pills call setRelativeRange so the URL persists as ?range=<label>
  // instead of ?start_time=&end_time=. Reload re-derives [now-duration, now]
  // from the label, so "last 24h" stays rolling.
  const pickRelative = React.useCallback((label: string, hours: number) => {
    const now = new Date()
    const start = new Date(now.getTime() - hours * 3600 * 1000).toISOString()
    React.startTransition(() => setRelativeRange(label, start, now.toISOString()))
  }, [setRelativeRange])

  // Pill flashes only for queries the date range actually affects.
  // Allowlist beats blocklist here: a new background poll added later
  // would silently re-introduce flicker if we just excluded known noisy
  // keys.
  const inFlightCount = useIsFetching({
    predicate: (q) => {
      const key = q.queryKey?.[0]
      return typeof key === 'string' && TIME_BOUND_QUERY_KEYS.has(key)
    }
  })

  const quickPresets = React.useMemo(() => [
    { label: '1h',  value: () => pickRelative('1h', 1) },
    { label: '3h',  value: () => pickRelative('3h', 3) },
    { label: '6h',  value: () => pickRelative('6h', 6) },
    { label: '12h', value: () => pickRelative('12h', 12) },
    { label: '24h', value: () => pickRelative('24h', 24) },
    { label: '3d',  value: () => pickRelative('3d', 72) },
    { label: '7d',  value: () => pickRelative('7d', 168) },
    { label: '30d', value: () => pickRelative('30d', 720) },
  ], [pickRelative])

  // Render the real bar structure from first paint instead of a fixed-height
  // placeholder. The old `if (!mounted) return <div h-20/>` placeholder was
  // 80px, but the real bar is 89px (one row) to 133px (wrapped, ≤1440px or
  // sidebar-collapsed) — so the placeholder→real swap pushed all page content
  // down ~53px on every filter-bar page (the dominant CLS source, ~0.20).
  // Rendering the real structure keeps the height correct (it wraps the same
  // way pre/post hydration); the persisted stores use skipHydration so they
  // return identical defaults on server + client-initial render. The only
  // hydration-sensitive bits — the `new Date()`-derived active preset and the
  // URL-hydrated filter pills / compare row — stay gated on `mounted` so they
  // appear only after hydration, within already-reserved fixed-height boxes.
  return (
    <div className="flex flex-col gap-1.5 px-4 py-2 border-b bg-background sticky top-0 z-10 shrink-0">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-0.5 border rounded-md p-0.5 bg-muted/20 h-10 sm:h-8">
          {quickPresets.map((preset) => {
            // `mounted` gate: activePreset derives from `new Date()` (non-
            // deterministic server vs client), so highlight only post-hydration.
            // Active state is className-only — no layout impact either way.
            const isActive = mounted && activePreset === preset.label
            const isActiveLoading = isActive && inFlightCount > 0
            return (
              <Button
                key={preset.label}
                variant={isActive ? 'default' : 'ghost'}
                size="sm"
                onClick={preset.value}
                aria-pressed={isActive}
                aria-busy={isActiveLoading || undefined}
                className={cn("relative h-9 sm:h-6.5 px-2.5 sm:px-2 text-xs sm:text-[11px]", isActive ? "shadow-sm" : "")}
              >
                {preset.label}
                {isActiveLoading && (
                  <span
                    aria-hidden="true"
                    className="ml-1 inline-flex h-1 w-1 rounded-full bg-primary-foreground animate-pulse"
                  />
                )}
              </Button>
            )
          })}
        </div>

        <div className="flex flex-col gap-0.5 w-full sm:w-auto">
          <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-muted/10 h-10 sm:h-8 w-full sm:w-auto">
            <Input
              type="datetime-local"
              aria-label="Range start"
              value={localStart}
              onChange={e => setLocalStart(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
            />
            <span className="text-muted-foreground text-[10px]">-</span>
            <Input
              type="datetime-local"
              aria-label="Range end"
              value={localEnd}
              onChange={e => setLocalEnd(e.target.value)}
              className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1"
            />
          </div>
          {pathname?.startsWith('/dashboard') && mounted && compareMode && (
            <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-orange-500/10 border-orange-500/20 h-10 sm:h-8 w-full sm:w-auto">
              <Input
                type="datetime-local"
                aria-label="Compare range start"
                value={localCompareStart}
                onChange={e => setLocalCompareStart(e.target.value)}
                className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
              />
              <span className="text-muted-foreground text-[10px]">-</span>
              <Input
                type="datetime-local"
                aria-label="Compare range end"
                value={localCompareEnd}
                onChange={e => setLocalCompareEnd(e.target.value)}
                className="h-9 sm:h-7 text-xs sm:text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-full sm:w-[160px] flex-1 sm:flex-none px-1 text-orange-600 dark:text-orange-400"
              />
              <div
                className="hidden sm:flex items-center gap-0.5 ml-1 pl-1 border-l border-orange-500/20"
                role="group"
                aria-label="Compare range presets"
              >
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => applyComparePreset(7)}
                  title="Same window, 7 days earlier"
                  className="h-7 sm:h-6 px-1.5 text-[10px] font-medium text-orange-700 dark:text-orange-300 hover:bg-orange-500/20"
                >
                  1w
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => applyComparePreset(28)}
                  title="Same window, 28 days earlier"
                  className="h-7 sm:h-6 px-1.5 text-[10px] font-medium text-orange-700 dark:text-orange-300 hover:bg-orange-500/20"
                >
                  4w
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => applyComparePreset(365)}
                  title="Same window, 1 year earlier"
                  className="h-7 sm:h-6 px-1.5 text-[10px] font-medium text-orange-700 dark:text-orange-300 hover:bg-orange-500/20"
                >
                  1y
                </Button>
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-col border-l pl-3 ml-1 gap-1 sm:gap-0 sm:-space-y-0.5">
          <Tooltip>
            <TooltipTrigger render={
              <div className="flex items-center space-x-2 h-7 sm:h-5">
                <Switch id="edge-only" size="sm" checked={localEdgeOnly} onCheckedChange={handleToggleEdgeOnly} />
                <Label htmlFor="edge-only" className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer">Edge only</Label>
              </div>
            } />
            <TooltipContent side="bottom" className="max-w-[260px] text-xs">
              Restrict results to edge (CDN) requests only — excludes origin and shield hops so each request counts once.
            </TooltipContent>
          </Tooltip>
          {pathname?.startsWith('/dashboard') && (
            <Tooltip>
              <TooltipTrigger render={
                <div className="flex items-center space-x-2 h-7 sm:h-5">
                  <Switch id="compare-mode" size="sm" checked={localCompareMode} onCheckedChange={handleToggleCompareMode} />
                  <Label htmlFor="compare-mode" className="text-xs sm:text-[11px] font-medium leading-none cursor-pointer">Compare</Label>
                </div>
              } />
              <TooltipContent side="bottom" className="max-w-[260px] text-xs">
                Overlay a second time range to compare deltas. The compare range appears below the primary range.
              </TooltipContent>
            </Tooltip>
          )}
        </div>

        <div className="flex items-center gap-1.5">
          <Button size="sm" onClick={handleApplyCustomDate} className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]">
            Apply
          </Button>

          <Button variant="secondary" size="sm" onClick={handleReset} className="h-9 sm:h-7 px-4 sm:px-3 text-xs sm:text-[11px]">
            Reset
          </Button>
        </div>

        <div className="flex items-center gap-1.5 border-l pl-3 ml-1">
          <ViewSelector />
          <SaveViewDialog />
        </div>

        <div className="ml-auto" />
      </div>

      {!pathname?.startsWith('/usage') && (
        <div className="flex items-center gap-2 flex-wrap min-h-8 sm:min-h-7">
          {/* Pills derive from the URL-hydrated filter store; render post-mount
              so server (no pills) and client-initial render match. The row
              reserves min-h above so pills land in already-allocated space. */}
          {mounted && filters.map((filter) => (
            <Badge
              key={filter.id}
              variant={filter.mode === 'include' ? 'default' : 'destructive'}
              className="gap-1 pr-1 pl-1 h-8 sm:h-7 text-xs sm:text-[11px]"
            >
              <Button
                variant="ghost"
                size="icon"
                aria-label={`${filter.mode === 'include' ? 'Including' : 'Excluding'} ${filter.column}=${filter.value}. Activate to toggle.`}
                aria-pressed={filter.mode === 'include'}
                className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent mr-1"
                onClick={() => React.startTransition(() => toggleFilterMode(filter.id))}
                title="Toggle Include/Exclude"
              >
                <span className="font-bold text-[10px]" aria-hidden="true">{filter.mode === 'include' ? '+' : '-'}</span>
              </Button>
              {filter.column === '_bot_name' ? (
                <>
                  <Bot className="h-3 w-3 opacity-70 mr-0.5" />
                  <span className="opacity-70">Bot:</span>
                  <span className="max-w-[180px] truncate">{fmtBotId(filter.value)}</span>
                </>
              ) : filter.column === '_ngwaf_bot_name' ? (
                <>
                  <Bot className="h-3 w-3 opacity-70 mr-0.5" />
                  <span className="opacity-70">Fastly Verified Bot:</span>
                  <span className="max-w-[180px] truncate">{filter.value}</span>
                </>
              ) : (
                <>
                  <span className="opacity-70">{filter.column}:</span>
                  <span className="max-w-[180px] truncate">{filter.value}</span>
                </>
              )}
              <Button
                variant="ghost"
                size="icon"
                aria-label={`Remove filter ${filter.column}: ${filter.value}`}
                title="Remove filter"
                className="h-6 w-6 sm:h-5 sm:w-5 p-0 hover:bg-transparent ml-1"
                onClick={() => React.startTransition(() => removeFilter(filter.id))}
              >
                <X className="h-3 w-3" aria-hidden="true" />
              </Button>
            </Badge>
          ))}

          <AddFilterDialog />
        </div>
      )}
    </div>


  )
})
