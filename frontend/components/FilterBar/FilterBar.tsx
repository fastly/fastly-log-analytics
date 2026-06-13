'use client'

import * as React from 'react'
import { X, Plus, Bot } from 'lucide-react'
import { subDays } from 'date-fns'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useFilterStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useServiceStore } from '@/stores/serviceStore'
import { formatForInput, parseFromInput, toUTCDate } from '@/lib/date'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useDateFormat } from '@/hooks/useDateFormat'
import { usePathname } from 'next/navigation'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { AddFilterDialog } from './AddFilterDialog'
import { SaveViewDialog } from './SaveViewDialog'
import { ViewSelector } from './ViewSelector'

import { useShallow } from 'zustand/react/shallow'

export const FilterBar = React.memo(function FilterBar() {
  const pathname = usePathname()
  const [mounted, setMounted] = React.useState(false)
  const { activeServiceId } = useServiceStore()
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
  // closes the analyst-403-every-3s polling loop documented in
  // pending-docs/session_2026-06-10_otel_dump_and_log_extents.md.
  //
  // Perf audit Phase D: useBootstrap seeds ['log-extents', sid] in its
  // queryFn from bootstrap's log_extents field. Gate on bootstrap
  // pending so this query hits the seeded cache on cold load.
  const queryClient = useQueryClient()
  const bootstrapState = queryClient.getQueryState(['bootstrap'])
  const bootstrapPending = bootstrapState !== undefined && bootstrapState.status === 'pending'
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
    }
  })

  React.useEffect(() => {
    // If the app just loaded OR the user clicked Reset, snap to available extents
    // Also re-snap if extents were initially empty but isAutoRange is still true
    if (status && (!hasSyncedExtents || isAutoRange)) {
      if (status.earliest_log_at && status.latest_log_at) {
        // Parse available log extents
        const earliestLog = toUTCDate(status.earliest_log_at.length === 10 ? status.earliest_log_at + "T00:00:00.000Z" : status.earliest_log_at)
        const latestLog = toUTCDate(status.latest_log_at.length === 10 ? status.latest_log_at + "T23:59:59.999Z" : status.latest_log_at)

        // Requirement:
        // 1. If we have 1 day of data or less, default to the full available range.
        // 2. If we have more than 1 day, default to the most recent 24 hours of data.
        // This ensures the dashboard is never empty on load if data exists, while prioritizing recent traffic.
        // To prevent double-fetching on every page load, only snap the range if
        // the available data is stale (>15 mins old). If data is actively flowing,
        // the default "last 24h from now" is correct and captures everything.

        const spanDays = (latestLog.getTime() - earliestLog.getTime()) / (1000 * 3600 * 24)
        const ageMinutes = (new Date().getTime() - latestLog.getTime()) / (1000 * 60)

        let finalStart: string
        let finalEnd: string

        if (spanDays <= 1 && spanDays >= 0) {
          // If we have 1 day of data or less, show the entire available range
          finalStart = earliestLog.toISOString()
          finalEnd = latestLog.toISOString()
        } else {
          // If we have more than 1 day, show the most recent 24 hours of data
          finalEnd = latestLog.toISOString()
          finalStart = subDays(latestLog, 1).toISOString()
        }

        if (isAutoRange) {
          // Only snap if the data is stale. If it's fresh, the default "last 24 hours"
          // from the store is perfectly fine and avoids an unnecessary duplicate query.
          if (ageMinutes > 15) {
            autoSetRange(finalStart, finalEnd)
          } else {
            // Data is fresh. Keep the default range but mark auto-range as done
            autoSetRange(useFilterStore.getState().startTime, useFilterStore.getState().endTime)
          }
        }
        setHasSyncedExtents(true)
      } else {
        // Status came back but no log extents yet (DB may still be empty or view
        // is stale). Unblock the dashboard so it can fire with the default range
        // rather than spinning indefinitely. Will re-snap if extents arrive later.
        setHasSyncedExtents(true)
      }
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

  // Prevent hydration mismatch on date rendering
  if (!mounted) {
    return <div className="h-20 border-b bg-background sticky top-0 z-10" />
  }

  return (
    <div className="flex flex-col gap-1.5 px-4 py-2 border-b bg-background sticky top-0 z-10 shrink-0">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-0.5 border rounded-md p-0.5 bg-muted/20 h-8">
          {quickPresets.map((preset) => (
            <Button
              key={preset.label}
              variant={activePreset === preset.label ? 'secondary' : 'ghost'}
              size="sm"
              onClick={preset.value}
              aria-pressed={activePreset === preset.label}
              className={cn("h-6.5 px-2 text-[11px]", activePreset === preset.label ? "bg-background shadow-sm text-foreground" : "")}
            >
              {preset.label}
            </Button>
          ))}
        </div>

        <div className="flex flex-col gap-0.5">
          <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-muted/10 h-8">
            <Input
              type="datetime-local"
              aria-label="Range start"
              value={localStart}
              onChange={e => setLocalStart(e.target.value)}
              className="h-7 text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-[160px] px-1"
            />
            <span className="text-muted-foreground text-[10px]">-</span>
            <Input
              type="datetime-local"
              aria-label="Range end"
              value={localEnd}
              onChange={e => setLocalEnd(e.target.value)}
              className="h-7 text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-[160px] px-1"
            />
          </div>
          {pathname?.startsWith('/dashboard') && compareMode && (
            <div className="flex items-center gap-1.5 border rounded-md p-0.5 bg-orange-500/10 border-orange-500/20 h-8">
              <Input
                type="datetime-local"
                aria-label="Compare range start"
                value={localCompareStart}
                onChange={e => setLocalCompareStart(e.target.value)}
                className="h-7 text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-[160px] px-1 text-orange-600 dark:text-orange-400"
              />
              <span className="text-muted-foreground text-[10px]">-</span>
              <Input
                type="datetime-local"
                aria-label="Compare range end"
                value={localCompareEnd}
                onChange={e => setLocalCompareEnd(e.target.value)}
                className="h-7 text-[11px] border-0 bg-transparent shadow-none focus-visible:ring-0 w-[160px] px-1 text-orange-600 dark:text-orange-400"
              />
            </div>
          )}
        </div>

        <div className="flex flex-col border-l pl-3 ml-1 -space-y-0.5">
          <div className="flex items-center space-x-2 h-5">
            <Switch id="edge-only" size="sm" checked={localEdgeOnly} onCheckedChange={handleToggleEdgeOnly} />
            <Label htmlFor="edge-only" className="text-[11px] font-medium leading-none cursor-pointer">Edge only</Label>
          </div>
          {pathname?.startsWith('/dashboard') && (
            <div className="flex items-center space-x-2 h-5">
              <Switch id="compare-mode" size="sm" checked={localCompareMode} onCheckedChange={handleToggleCompareMode} />
              <Label htmlFor="compare-mode" className="text-[11px] font-medium leading-none cursor-pointer">Compare</Label>
            </div>
          )}
        </div>

        <div className="flex items-center gap-1.5">
          <Button size="sm" onClick={handleApplyCustomDate} className="h-7 px-3 text-[11px]">
            Apply
          </Button>

          <Button variant="secondary" size="sm" onClick={handleReset} className="h-7 text-[11px]">
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
        <div className="flex items-center gap-2 flex-wrap min-h-7">
          {filters.map((filter) => (
            <Badge
              key={filter.id}
              variant={filter.mode === 'include' ? 'default' : 'destructive'}
              className="gap-1 pr-1 pl-1 h-7 text-[11px]"
            >
              <Button
                variant="ghost"
                size="icon"
                aria-label="Toggle Include/Exclude"
                className="h-5 w-5 p-0 hover:bg-transparent mr-1"
                onClick={() => React.startTransition(() => toggleFilterMode(filter.id))}
                title="Toggle Include/Exclude"
              >
                <span className="font-bold text-[10px]">{filter.mode === 'include' ? '+' : '-'}</span>
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
                className="h-5 w-5 p-0 hover:bg-transparent ml-1"
                onClick={() => React.startTransition(() => removeFilter(filter.id))}
              >
                <X className="h-3 w-3" />
              </Button>
            </Badge>
          ))}

          <AddFilterDialog />
        </div>
      )}
    </div>


  )
})
