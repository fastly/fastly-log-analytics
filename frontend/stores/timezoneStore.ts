import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface TimezoneState {
  timezone: string
  setTimezone: (timezone: string) => void
}

export const useTimezoneStore = create<TimezoneState>()(
  persist(
    (set) => ({
      // Deterministic default so the server and the first client render
      // agree. Reading Intl.DateTimeFormat().resolvedOptions().timeZone
      // here returns the browser zone on the client but 'UTC' on the
      // server — any SSR-rendered absolute time would then mismatch on
      // hydration (React #418). <StoreHydrator> adopts the browser zone
      // post-mount (and rehydrates any saved preference).
      timezone: 'UTC',
      setTimezone: (timezone) => set({ timezone }),
    }),
    {
      name: 'timezone-storage',
      // SSR safety — see serviceStore. Rehydrated post-mount.
      skipHydration: true,
    }
  )
)
