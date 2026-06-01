import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export type { DebugQuery, DebugCall } from '@/types/api'

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
    }
  )
)
