import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface TimezoneState {
  timezone: string
  setTimezone: (timezone: string) => void
}

const getDefaultTimezone = () => {
  if (typeof window !== 'undefined') {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone
    } catch {
      // fallback
    }
  }
  return 'UTC'
}

export const useTimezoneStore = create<TimezoneState>()(
  persist(
    (set) => ({
      timezone: getDefaultTimezone(),
      setTimezone: (timezone) => set({ timezone }),
    }),
    {
      name: 'timezone-storage',
    }
  )
)
