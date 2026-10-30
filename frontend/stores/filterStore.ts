import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { subDays, formatISO } from 'date-fns'
import type { FilterPill, FilterMode } from '@/types/api'
import type { PopGeo } from '@/components/DataTable'
import { setActiveServiceCookie } from '@/lib/utils'

interface FilterState {
  startTime: string
  endTime: string
  filters: FilterPill[]
  edgeOnly: boolean
  isAutoRange: boolean
  // When a quick-preset pill is active, holds its label ("24h", "3d", ...).
  // Null means custom range (datetime inputs, chart zoom, saved view) or
  // implicit default. The URL-sync hook persists this as ?range=<label> and
  // re-derives [now-duration, now] on hydrate so reloads track the rolling
  // window instead of pinning the absolute timestamps from the click moment.
  relativeRange: string | null
  compareMode: boolean
  compareStartTime: string | null
  compareEndTime: string | null
  setRange: (start: string, end: string) => void
  setRelativeRange: (range: string, start: string, end: string) => void
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
  // Default to last 24h.
  startTime: formatISO(subDays(new Date(), 1)),
  endTime: formatISO(new Date()),
  filters: [],
  edgeOnly: false,
  isAutoRange: true, // Start with auto-range enabled for first data discovery
  relativeRange: null,
  compareMode: false,
  compareStartTime: null,
  compareEndTime: null,

  // Explicit absolute-range selection (custom datetime, chart zoom, saved
  // view). Clears relativeRange — this range no longer corresponds to a
  // rolling preset. Early-bail when the range hasn't actually changed and
  // we're already in absolute mode — a re-emit forces subscribers to
  // re-render (and useQuery to re-fetch with a new key tuple) for no
  // observable change.
  setRange: (startTime, endTime) => set((state) => {
    if (state.startTime === startTime && state.endTime === endTime && !state.isAutoRange && state.relativeRange === null) {
      return state
    }
    return { startTime, endTime, isAutoRange: false, relativeRange: null }
  }),

  // Preset pill click. Records the label so the URL persists as
  // ?range=<label> and reload re-derives [now-duration, now].
  setRelativeRange: (relativeRange, startTime, endTime) =>
    set({ startTime, endTime, isAutoRange: false, relativeRange }),

  resetRange: () => set({ isAutoRange: true, relativeRange: null }),

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
    // Reject column names matching /_\d+$/. buildFiltersPayload uses
    // `_<n>` as a dedup suffix when the same column needs both include
    // and exclude buckets, and hydrateFilterStoreFromUrl (lib/urlFilterHydration.ts) strips that suffix on
    // URL hydration. A column literally ending in `_<digit>` (e.g.
    // `response_1`) would be silently corrupted on round-trip. The
    // field catalog (source schema) is the source of truth for column
    // names; any future field naming convention must avoid the collision.
    if (/_\d+$/.test(column)) {
      console.warn(
        `[filterStore] addFilter: dropping column "${column}" — column names ending in _<digit> ` +
        `collide with the buildFiltersPayload dedup suffix scheme.`
      )
      return state
    }

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
    const now = new Date()
    set({
      filters: [],
      edgeOnly: false,
      isAutoRange: true,
      relativeRange: null,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
      startTime: formatISO(subDays(now, 1)),
      endTime: formatISO(now),
    })
  },
}))

// ── PoP Geo Store ────────────────────────────────────────────────────────────
// Module-singleton store for the PoP -> {city,region,country} map. Populated
// once from the bootstrap response (see useBootstrap), read by <PopLabel> via
// usePopGeoMap. A store (not the React Query cache) so PopLabel never needs a
// QueryClientProvider in context — it renders the bare code anywhere the map
// hasn't loaded (tests, isolated renders) instead of crashing.
interface PopGeoState {
  map: Record<string, PopGeo>
  setMap: (map: Record<string, PopGeo>) => void
  locations: Record<string, [number, number]>
  setLocations: (locations: Record<string, [number, number]>) => void
}

export const usePopGeoStore = create<PopGeoState>((set) => ({
  map: {},
  setMap: (map) => set({ map }),
  locations: {},
  setLocations: (locations) => set({ locations }),
}))

// ── Debug Store ──────────────────────────────────────────────────────────────
interface DebugState {
  enabled: boolean
  setEnabled: (val: boolean) => void
  apiCallsEnabled: boolean
  setApiCallsEnabled: (val: boolean) => void
}

export const useDebugStore = create<DebugState>()(
  persist(
    (set) => ({
      enabled: false,
      setEnabled: (val) => set({ enabled: val }),
      apiCallsEnabled: false,
      setApiCallsEnabled: (val) => set({ apiCallsEnabled: val }),
    }),
    {
      name: 'fastly-debug-settings',
      // SSR safety — see serviceStore. Rehydrated post-mount by
      // <StoreHydrator> so the first client render matches the server's
      // `enabled: false` default instead of a persisted `true`.
      skipHydration: true,
    }
  )
)

// ── Session Role Store ───────────────────────────────────────────────────────
// Mirrors the caller's role as the backend reports it in /api/bootstrap's
// settings.is_remote_analyst. Set by useBootstrap on every bootstrap resolve
// (queryFn for the pure-CSR fast path AND the useEffect for the SSR-hydrated
// path).
interface SessionRoleState {
  isRemoteAnalyst: boolean
  setIsRemoteAnalyst: (value: boolean) => void
}

export const useSessionRoleStore = create<SessionRoleState>((set) => ({
  isRemoteAnalyst: false,
  setIsRemoteAnalyst: (isRemoteAnalyst) => set({ isRemoteAnalyst }),
}))

// ── Timezone Store ───────────────────────────────────────────────────────────
interface TimezoneState {
  mode: 'system' | 'manual'
  timezone: string
  setTimezone: (timezone: string) => void
  setSystemTimezone: (timezone: string) => void
}

export const useTimezoneStore = create<TimezoneState>()(
  persist(
    (set) => ({
      // Deterministic default so the server and the first client render
      // agree. Reading Intl.DateTimeFormat().resolvedOptions().timeZone
      // here returns the browser zone on the client but 'UTC' on the
      // server — any SSR-rendered absolute time would then mismatch on
      // hydration (React #418). <StoreHydrator> resolves the live system
      // zone post-mount whenever mode is 'system' (and rehydrates any
      // saved preference).
      mode: 'system',
      timezone: 'UTC',
      setTimezone: (timezone) => set({ mode: 'manual', timezone }),
      setSystemTimezone: (timezone) => set({ mode: 'system', timezone }),
    }),
    {
      name: 'timezone-storage',
      // SSR safety — see serviceStore. Rehydrated post-mount.
      skipHydration: true,
      version: 1,
      migrate: (persistedState) => {
        const state = persistedState as { timezone?: string; mode?: 'system' | 'manual' }
        // Pre-v1 blobs only ever had `timezone` — written either by the
        // old one-time browser-zone capture or an explicit user pick.
        // Treat both as a manual preference so upgrading never changes
        // what an existing user already sees.
        if (state.mode) return state
        return { ...state, mode: 'manual' as const }
      },
    }
  )
)

// ── Service Store ────────────────────────────────────────────────────────────
export interface Service {
  id: string
  name: string
  cmcdEnabled?: boolean
  rum_enabled?: boolean
  is_high_scale?: boolean
}

interface ServiceState {
  activeServiceId: string | null
  services: Service[]
  isInitialized: boolean
  setActiveServiceId: (id: string | null) => void
  setServices: (services: Service[]) => void
  setInitialized: (initialized: boolean) => void
}

export const useServiceStore = create<ServiceState>()(
  persist(
    (set) => ({
      activeServiceId: null,
      services: [],
      isInitialized: false,
      setActiveServiceId: (id) => {
        set({ activeServiceId: id })
        setActiveServiceCookie(id)
      },
      setServices: (services) => set({ services }),
      setInitialized: (initialized) => set({ isInitialized: initialized }),
    }),
    {
      name: 'service-storage',
      partialize: (state) => ({
        activeServiceId: state.activeServiceId,
        services: state.services,
      }),
      // SSR safety: do NOT read localStorage during module load. A
      // synchronous rehydrate would make the first client render use the
      // persisted activeServiceId while the server rendered with the null
      // default (no localStorage on the server) — the sidebar nav
      // `?service=` hrefs and the ServicesTable "Active" badge would then
      // diverge and throw React #418 across the whole tree. <StoreHydrator>
      // calls persist.rehydrate() in a post-mount effect instead.
      skipHydration: true,
    }
  )
)
