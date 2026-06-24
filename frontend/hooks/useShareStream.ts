'use client'

import { useQueryClient } from '@tanstack/react-query'
import { useServiceStream } from '@/hooks/useServiceStream'

/**
 * Subscribe to ``/api/admin/share/stream`` and write each pushed payload
 * into the ``['admin', 'share', 'live']`` React Query cache. Replaces the
 * /admin/share page's 10s ``/api/admin/share/live`` poll.
 *
 * The share endpoint is global-admin (no x-service-id required), so this
 * delegates to ``useServiceStream`` with ``requireService: false`` — that
 * shares the fetch/reader lifecycle, the spec-compliant SSE-frame parser,
 * the exponential reconnect (1s → 30s), and the string-concat URL shape
 * (per sse-hook-url-pitfall) instead of re-implementing them here.
 *
 * Caller is responsible for gating ``enabled`` — analysts 403-loop on
 * /api/admin/share/* and an unmounted /admin/share page doesn't need the
 * stream open.
 */

const SHARE_LIVE_QUERY_KEY = ['admin', 'share', 'live'] as const

export function useShareStream(enabled: boolean): void {
  const queryClient = useQueryClient()

  useServiceStream(
    enabled,
    '/api/admin/share/stream',
    (raw) => {
      try {
        queryClient.setQueryData(SHARE_LIVE_QUERY_KEY, JSON.parse(raw))
      } catch {
        // Malformed event / sse_starlette keepalive in an unexpected frame
        // layout — skip; the next push will resync.
      }
    },
    { cache: 'no-store', requireService: false },
  )
}
