'use client'

import { useQuery } from '@tanstack/react-query'
import { adminFetch } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'

export interface RumStatus {
  enabled: boolean
  enabled_at?: string | null
  deployed_vcl_sha?: string | null
  current_vcl_sha?: string
  vcl_drift?: boolean
  unauthorized?: boolean
  capture_vitals?: boolean
  capture_performance?: boolean
  capture_errors?: boolean
  capture_events?: boolean
  custom_condition?: string | null
}

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
      const res = await adminFetch(`/api/services/${serviceId}/rum/status`)
      if (res.status === 403) return { enabled: false, unauthorized: true }
      return res.ok ? ((await res.json()) as RumStatus) : null
    },
    enabled: !!serviceId && enabled,
    refetchInterval,
  })
}
