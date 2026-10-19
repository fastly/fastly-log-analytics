'use client'

import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

type KeyFn = (serviceId: string) => readonly unknown[]

/**
 * Pre-seed ``null`` into each per-service cache entry that does not exist yet,
 * then report ``true`` once that has happened. Gate ``useQuery({ enabled })``
 * on the result so the query is never built mid-render: building a cache entry
 * during render notifies the QueryCache and trips React 19's
 * setState-in-render warning on sibling subscribers.
 *
 * The seed must stay in an effect (not render) and ``ready`` flips through a
 * microtask so the seeded entry is visible before the first observer mounts.
 * ``keyFns`` must be a module-level constant (it is an effect dependency).
 *
 * ``readyWithoutService`` keeps ``ready`` true when no service is active
 * (SyncStatusBadge); the default drops it back to false (the data hooks).
 */
export function useSeededReady(
  serviceId: string | null | undefined,
  keyFns: readonly KeyFn[],
  readyWithoutService = false,
): boolean {
  const queryClient = useQueryClient()
  const [ready, setReady] = useState(false)
  useEffect(() => {
    if (serviceId) {
      for (const keyFn of keyFns) {
        const key = keyFn(serviceId)
        if (queryClient.getQueryData(key) === undefined) {
          queryClient.setQueryData(key, null)
        }
      }
    }
    const next = serviceId ? true : readyWithoutService
    queueMicrotask(() => {
      setReady(next)
    })
  }, [serviceId, queryClient, keyFns, readyWithoutService])
  return ready
}
