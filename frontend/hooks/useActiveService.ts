'use client'

import { useShallow } from 'zustand/react/shallow'
import { useServiceStore } from '@/stores/serviceStore'

/**
 * Active service selection. Subscribe here when a component cares about
 * which service the page is scoped to (or needs to render a service list).
 */
export function useActiveService() {
  return useServiceStore(
    useShallow(s => ({ activeServiceId: s.activeServiceId, services: s.services }))
  )
}
