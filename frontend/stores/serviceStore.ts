import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface Service {
  id: string
  name: string
  accessLevel?: string
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
      setActiveServiceId: (id) => set({ activeServiceId: id }),
      setServices: (services) => set({ services }),
      setInitialized: (initialized) => set({ isInitialized: initialized }),
    }),
    {
      name: 'service-storage',
      partialize: (state) => ({
        activeServiceId: state.activeServiceId,
        services: state.services,
      }),
    }
  )
)
