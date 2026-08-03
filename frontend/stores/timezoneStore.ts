import { create } from 'zustand'
import { persist } from 'zustand/middleware'

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
