import { create } from 'zustand'
import { FilterPill, FilterMode } from '@/types/filters'
import { subDays, formatISO } from 'date-fns'

interface FilterState {
  startTime: string
  endTime: string
  filters: FilterPill[]
  edgeOnly: boolean
  hasSyncedExtents: boolean
  isAutoRange: boolean
  compareMode: boolean
  compareStartTime: string | null
  compareEndTime: string | null
  setRange: (start: string, end: string) => void
  autoSetRange: (start: string, end: string) => void
  setHasSyncedExtents: (synced: boolean) => void
  addFilter: (column: string, value: string, mode: FilterMode) => void
  removeFilter: (id: string) => void
  toggleFilterMode: (id: string) => void
  toggleCompareMode: () => void
  setCompareRange: (start: string | null, end: string | null) => void
  toggleEdgeOnly: () => void
  clearFilters: () => void
  resetAll: () => void
  resetRange: () => void
}

export const useFilterStore = create<FilterState>((set) => ({
  // Default to last 24h. FilterBar.autoSetRange will snap this to the
  // real latest-log-extent once /api/sync-status returns; when data is
  // fresh (latest_log_at ~ now) the snapped range matches the default,
  // so the dashboard query doesn't refire — no flicker on the common
  // path. Was 7 days, which guaranteed a refire to 24h after
  // sync-status returned (the auto-snap target).
  startTime: formatISO(subDays(new Date(), 1)),
  endTime: formatISO(new Date()),
  filters: [],
  edgeOnly: false,
  hasSyncedExtents: false,
  isAutoRange: true, // Start with auto-range enabled for first data discovery
  compareMode: false,
  compareStartTime: null,
  compareEndTime: null,

  setHasSyncedExtents: (synced) => set({ hasSyncedExtents: synced }),

  setRange: (startTime, endTime) => set({ startTime, endTime, isAutoRange: false }),

  resetRange: () => set({ isAutoRange: true, hasSyncedExtents: false }),

  autoSetRange: (startTime, endTime) => set((state) => {
    if (!state.isAutoRange) return state
    return { startTime, endTime, isAutoRange: false }
  }),

  toggleCompareMode: () => set((state) => {
    const nextMode = !state.compareMode
    if (nextMode) {
      // Default to matching the duration of the current selection, placed immediately prior.
      // We snap to minutes to ensure consistency with the UI inputs and prevent precision mismatches
      // that can cause chart rendering delays until manual "Apply" is hit.
      const s = new Date(state.startTime)
      const e = new Date(state.endTime)
      s.setSeconds(0, 0)
      e.setSeconds(0, 0)

      const diff = e.getTime() - s.getTime()
      const compEnd = new Date(s.getTime())
      const compStart = new Date(compEnd.getTime() - diff)
      return { 
        startTime: s.toISOString(),
        endTime: e.toISOString(),
        compareMode: nextMode,
        compareStartTime: compStart.toISOString(),
        compareEndTime: compEnd.toISOString()
      }
    }
    return { compareMode: nextMode }
  }),

  setCompareRange: (startTime, endTime) => set({ compareStartTime: startTime, compareEndTime: endTime }),

  addFilter: (column, value, mode) => set((state) => {
    // If exact filter already exists, don't duplicate
    const exists = state.filters.find(f => f.column === column && f.value === value)
    if (exists) return state

    return {
      filters: [
        ...state.filters,
        {
          id: crypto.randomUUID(),
          column,
          value,
          mode
        }
      ]
    }
  }),

  removeFilter: (id) => set((state) => ({
    filters: state.filters.filter(f => f.id !== id)
  })),

  toggleFilterMode: (id) => set((state) => ({
    filters: state.filters.map(f => 
      f.id === id 
        ? { ...f, mode: f.mode === 'include' ? 'exclude' : 'include' }
        : f
    )
  })),

  toggleEdgeOnly: () => set((state) => ({ edgeOnly: !state.edgeOnly })),

  clearFilters: () => set({ filters: [] }),

  resetAll: () => {
    // Restore startTime/endTime to the store-init defaults (last 24h from
    // now) BEFORE re-flipping the auto-snap flags. Otherwise: on fresh
    // data the snap effect in FilterBar takes its "keep current range"
    // branch (because ageMinutes < 15), which means Reset would leave a
    // user-selected narrow window untouched. With this restore, fresh-
    // data Reset always returns to the same 24h window the page showed
    // on load, and stale-data Reset still snaps to extents via the same
    // effect (autoSetRange overwrites these defaults when it fires).
    const now = new Date()
    set({
      filters: [],
      isAutoRange: true,
      hasSyncedExtents: false,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
      startTime: formatISO(subDays(now, 1)),
      endTime: formatISO(now),
    })
  },
}))
