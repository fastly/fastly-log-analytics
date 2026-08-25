'use client'

import { useShallow } from 'zustand/react/shallow'
import { useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { queryKeys } from '@/lib/query-keys'

/**
 * Active service selection. Subscribe here when a component cares about
 * which service the page is scoped to (or needs to render a service list).
 */
export function useActiveService() {
  return useServiceStore(
    useShallow(s => ({ activeServiceId: s.activeServiceId, services: s.services }))
  )
}

/**
 * Hook to determine if the active service has CMCD enabled.
 * Solves visual pop-in by looking at pre-seeded bootstrap data on first paint
 * before Zustand hydration completes.
 */
export function useActiveServiceCmcdEnabled(activeServiceId: string | null): boolean {
  const isInitialized = useServiceStore(s => s.isInitialized)
  const services = useServiceStore(s => s.services)
  const queryClient = useQueryClient()

  if (isInitialized) {
    const activeService = services.find(s => s.id === activeServiceId)
    return activeService?.cmcdEnabled ?? false
  }

  // Fallback to preloaded bootstrap cache on first paint
  const bootstrap = queryClient.getQueryData(queryKeys.bootstrap()) as any
  const bootstrapServices = bootstrap?.services
  if (Array.isArray(bootstrapServices)) {
    const activeService = bootstrapServices.find((s: any) => s.service_id === activeServiceId)
    return !!activeService?.cmcd_enabled
  }

  return false
}
