import { create } from 'zustand'

import type { PopGeo } from '@/lib/pop'

// Module-singleton store for the PoP -> {city,region,country} map. Populated
// once from the bootstrap response (see useBootstrap), read by <PopLabel> via
// usePopGeoMap. A store (not the React Query cache) so PopLabel never needs a
// QueryClientProvider in context — it renders the bare code anywhere the map
// hasn't loaded (tests, isolated renders) instead of crashing.
interface PopGeoState {
  map: Record<string, PopGeo>
  setMap: (map: Record<string, PopGeo>) => void
}

export const usePopGeoStore = create<PopGeoState>((set) => ({
  map: {},
  setMap: (map) => set({ map }),
}))
