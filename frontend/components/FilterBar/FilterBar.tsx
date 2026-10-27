'use client'

import * as React from 'react'
import { usePathname } from 'next/navigation'
import { useIsFetching } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import { useFilterStore, useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { formatForInput, parseFromInput } from '@/lib/utils'
import { useMounted, useServiceSwitchReset } from '@/hooks/useBootstrap'
import { AddFilterDialog, AddFilterDialogContent } from './AddFilterDialog'
import {
  ActiveFiltersBanner,
  FilterChip,
  OperatorSelector,
  fmtBotId,
} from './OperatorSelector'
import {
  SaveViewDialog,
  ViewSelector,
  applySavedViewToStore,
  type SavedViewStoreSnapshot,
} from './SavedViewControls'
import {
  PRESET_WINDOWS,
  TIME_BOUND_QUERY_KEYS,
  TimePickerPopover,
} from './TimePickerPopover'
import { COMMON_FIELD_IDS, TokenAutocomplete, useCommonFilterFields } from './TokenAutocomplete'

export {
  ActiveFiltersBanner,
  AddFilterDialog,
  AddFilterDialogContent,
  COMMON_FIELD_IDS,
  FilterChip,
  OperatorSelector,
  PRESET_WINDOWS,
  SaveViewDialog,
  TIME_BOUND_QUERY_KEYS,
  TimePickerPopover,
  TokenAutocomplete,
  ViewSelector,
  applySavedViewToStore,
  fmtBotId,
  useCommonFilterFields,
  type SavedViewStoreSnapshot,
}

export const FilterBar = React.memo(function FilterBar() {
  const pathname = usePathname()
  const mounted = useMounted()
  // Selector form so unrelated serviceStore mutations (setServices,
  // setInitialized) don't trigger FilterBar re-renders.
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const {
    startTime,
    endTime,
    filters,
    edgeOnly,
    relativeRange,
    setRange,
    setRelativeRange,
    removeFilter,
    toggleFilterMode,
    toggleEdgeOnly,
    resetAll,
    resetRange,
    compareMode,
    compareStartTime,
    compareEndTime,
    toggleCompareMode,
    setCompareRange,
  } = useFilterStore(
    useShallow((state) => ({
      startTime: state.startTime,
      endTime: state.endTime,
      filters: state.filters,
      edgeOnly: state.edgeOnly,
      relativeRange: state.relativeRange,
      setRange: state.setRange,
      setRelativeRange: state.setRelativeRange,
      removeFilter: state.removeFilter,
      toggleFilterMode: state.toggleFilterMode,
      toggleEdgeOnly: state.toggleEdgeOnly,
      resetAll: state.resetAll,
      resetRange: state.resetRange,
      compareMode: state.compareMode,
      compareStartTime: state.compareStartTime,
      compareEndTime: state.compareEndTime,
      toggleCompareMode: state.toggleCompareMode,
      setCompareRange: state.setCompareRange,
    }))
  )

  // Local state for custom date picker
  const [localStart, setLocalStart] = React.useState('')
  const [localEnd, setLocalEnd] = React.useState('')
  const [localCompareStart, setLocalCompareStart] = React.useState('')
  const [localCompareEnd, setLocalCompareEnd] = React.useState('')

  const handleToggleEdgeOnly = React.useCallback(
    (checked: boolean) => {
      React.startTransition(() => {
        if (checked !== edgeOnly) toggleEdgeOnly()
      })
    },
    [edgeOnly, toggleEdgeOnly]
  )

  const handleToggleCompareMode = React.useCallback(
    (checked: boolean) => {
      React.startTransition(() => {
        if (checked !== compareMode) toggleCompareMode()
      })
    },
    [compareMode, toggleCompareMode]
  )

  // Reset extents sync state ONLY when the user explicitly switches services,
  // not on initial page load or hydration.
  useServiceSwitchReset(activeServiceId, () => {
    resetRange()
  })

  const { timezone } = useTimezoneStore()
  const dateSource = JSON.stringify([
    startTime,
    endTime,
    compareStartTime,
    compareEndTime,
    timezone,
  ])
  const [syncedDateSource, setSyncedDateSource] = React.useState<string | null>(null)
  if (mounted && syncedDateSource !== dateSource) {
    setSyncedDateSource(dateSource)
    if (startTime && endTime) {
      setLocalStart(formatForInput(startTime, timezone))
      setLocalEnd(formatForInput(endTime, timezone))
    }
    if (compareStartTime && compareEndTime) {
      setLocalCompareStart(formatForInput(compareStartTime, timezone))
      setLocalCompareEnd(formatForInput(compareEndTime, timezone))
    }
  }

  const handleApplyCustomDate = React.useCallback(() => {
    React.startTransition(() => {
      if (localStart && localEnd) {
        const parsedStart = parseFromInput(localStart, timezone)
        const parsedEnd = parseFromInput(localEnd, timezone)
        if (parsedStart && parsedEnd) setRange(parsedStart, parsedEnd)
      }
      if (compareMode && localCompareStart && localCompareEnd) {
        const parsedStart = parseFromInput(localCompareStart, timezone)
        const parsedEnd = parseFromInput(localCompareEnd, timezone)
        if (parsedStart && parsedEnd) setCompareRange(parsedStart, parsedEnd)
      }
    })
  }, [
    localStart,
    localEnd,
    compareMode,
    localCompareStart,
    localCompareEnd,
    timezone,
    setRange,
    setCompareRange,
  ])

  const applyComparePreset = React.useCallback(
    (offsetDays: number) => {
      const primaryStartIso = parseFromInput(localStart, timezone) ?? startTime
      const primaryEndIso = parseFromInput(localEnd, timezone) ?? endTime
      const offsetMs = offsetDays * 24 * 60 * 60 * 1000
      const start = new Date(new Date(primaryStartIso).getTime() - offsetMs).toISOString()
      const end = new Date(new Date(primaryEndIso).getTime() - offsetMs).toISOString()
      React.startTransition(() => {
        setCompareRange(start, end)
      })
    },
    [localStart, localEnd, startTime, endTime, timezone, setCompareRange]
  )

  const handleReset = React.useCallback(() => {
    React.startTransition(() => {
      resetAll()
    })
  }, [resetAll])

  const spanHours = React.useMemo(() => {
    if (!startTime || !endTime) return null
    return (new Date(endTime).getTime() - new Date(startTime).getTime()) / (1000 * 3600)
  }, [startTime, endTime])

  const activePreset = React.useMemo(() => {
    if (relativeRange) return relativeRange
    if (!spanHours || !endTime) return null
    if (Math.abs(new Date(endTime).getTime() - new Date().getTime()) > 60000) return null
    const h = Math.round(spanHours * 10) / 10
    return PRESET_WINDOWS.find(([, hrs]) => hrs === h)?.[0] ?? null
  }, [relativeRange, spanHours, endTime])

  const pickRelative = React.useCallback(
    (label: string, hours: number) => {
      const now = new Date()
      const start = new Date(now.getTime() - hours * 3600 * 1000).toISOString()
      React.startTransition(() => setRelativeRange(label, start, now.toISOString()))
    },
    [setRelativeRange]
  )

  const inFlightCount = useIsFetching({
    predicate: (q) => {
      const key = q.queryKey?.[0]
      return typeof key === 'string' && TIME_BOUND_QUERY_KEYS.has(key)
    },
  })

  const quickPresets = React.useMemo(
    () => [
      { label: '1h', value: () => pickRelative('1h', 1) },
      { label: '3h', value: () => pickRelative('3h', 3) },
      { label: '6h', value: () => pickRelative('6h', 6) },
      { label: '12h', value: () => pickRelative('12h', 12) },
      { label: '24h', value: () => pickRelative('24h', 24) },
      { label: '3d', value: () => pickRelative('3d', 72) },
      { label: '7d', value: () => pickRelative('7d', 168) },
      { label: '30d', value: () => pickRelative('30d', 720) },
    ],
    [pickRelative]
  )

  return (
    <div
      data-testid="filter-bar"
      className="flex flex-col gap-1.5 px-4 py-2 border-b bg-background sticky top-0 z-10 shrink-0"
    >
      <div className="flex items-center gap-3 flex-wrap">
        <TimePickerPopover
          mounted={mounted}
          pathname={pathname}
          activePreset={activePreset}
          inFlightCount={inFlightCount}
          quickPresets={quickPresets}
          localStart={localStart}
          setLocalStart={setLocalStart}
          localEnd={localEnd}
          setLocalEnd={setLocalEnd}
          localCompareStart={localCompareStart}
          setLocalCompareStart={setLocalCompareStart}
          localCompareEnd={localCompareEnd}
          setLocalCompareEnd={setLocalCompareEnd}
          edgeOnly={edgeOnly}
          compareMode={compareMode}
          onToggleEdgeOnly={handleToggleEdgeOnly}
          onToggleCompareMode={handleToggleCompareMode}
          onApplyComparePreset={applyComparePreset}
          onApplyCustomDate={handleApplyCustomDate}
          onReset={handleReset}
        />

        <div className="flex items-center gap-1.5 border-l pl-3 ml-1">
          <ViewSelector />
          <SaveViewDialog />
        </div>

        <div className="ml-auto" />
      </div>

      {!pathname?.startsWith('/usage') && (
        <div className="flex items-center gap-2 flex-wrap min-h-8 sm:min-h-7">
          {mounted &&
            filters.map((filter) => (
              <FilterChip
                key={filter.id}
                filter={filter}
                onToggleMode={(id) => React.startTransition(() => toggleFilterMode(id))}
                onRemove={(id) => React.startTransition(() => removeFilter(id))}
              />
            ))}
          <AddFilterDialog />
        </div>
      )}
    </div>
  )
})
