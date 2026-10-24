'use client'

import { useQuery } from '@tanstack/react-query'
import { adminJsonResult } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import type { Simplify, WithRequired, components } from '@/types/api'

type RumStatusWire = WithRequired<components['schemas']['RumStatusResponse'], 'enabled'>

type RumStatus = Simplify<RumStatusWire & { unauthorized?: boolean }>

interface UseRumStatusOptions {
  enabled?: boolean
  refetchInterval?: number
}

/**
 * Single owner of the `['rum-status', serviceId]` cache entry. Every observer
 * must share one queryFn, otherwise whichever fetches last decides the cached
 * shape (403 → `{ unauthorized: true }` vs `null`).
 */
export function useRumStatus(
  serviceId: string | null | undefined,
  { enabled = true, refetchInterval }: UseRumStatusOptions = {},
) {
  return useQuery<RumStatus | null>({
    queryKey: queryKeys.rumStatus(serviceId),
    queryFn: async () => {
      if (!serviceId) return null
      const res = await adminJsonResult<'/api/services/{service_id}/rum/status', RumStatusWire>(
        '/api/services/{service_id}/rum/status',
        `/api/services/${serviceId}/rum/status`,
      )
      if (res.status === 403) return { enabled: false, unauthorized: true }
      return res.ok ? res.data : null
    },
    enabled: !!serviceId && enabled,
    refetchInterval,
  })
}
