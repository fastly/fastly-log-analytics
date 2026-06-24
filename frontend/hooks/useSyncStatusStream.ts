'use client'

import { useQueryClient } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { useServiceStream } from '@/hooks/useServiceStream'
import type { SyncStatus } from './useSyncStatus'

/**
 * Subscribe to the sync-status SSE channel and push every event into the
 * React Query cache under the same key ``useSyncStatus`` uses
 * (``['sync-status', activeServiceId]``). Components that read via
 * ``useSyncStatus()`` re-render automatically — this hook has no return
 * value of its own.
 *
 * Pair with ``useSyncStatus`` (which keeps a slow ~5-min fallback poll for
 * resilience against silently-dropped streams): the stream is the
 * primary update path, the poll is the safety net.
 *
 * Caller is responsible for gating ``enabled`` to admin sessions only —
 * analysts are middleware-blocked from ``/api/sync-status/stream`` and
 * the stream would just 403 in a loop.
 */
export function useSyncStatusStream(enabled: boolean) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()

  return useServiceStream(
    enabled,
    '/api/sync-status/stream',
    (raw) => {
      try {
        const payload = JSON.parse(raw) as SyncStatus
        queryClient.setQueryData(['sync-status', activeServiceId], payload)
      } catch {
        // Malformed event; skip. Could be an sse_starlette keepalive
        // comment-line that arrived in an unexpected frame layout.
      }
    },
    { cache: 'no-store' },
  )
}
