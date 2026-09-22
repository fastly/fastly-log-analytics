import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { setActiveServiceCookie } from '@/lib/active-service-cookie'

export interface Service {
  id: string
  name: string
  accessLevel?: string
  cmcdEnabled?: boolean
  rum_enabled?: boolean
  analystPathASupported?: boolean
  analystPathAReason?: string | null
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
