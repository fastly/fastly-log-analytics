'use client'

import { useQueryClient } from '@tanstack/react-query'
import { useServiceStream } from '@/hooks/useServiceStream'
import { queryKeys } from '@/lib/query-keys'
import type { Schema } from '@/types/api'

/**
 * Analyst-safe sibling of ``useAdminEventStream``.
 *
 * Subscribes to ``/api/log-extents/stream`` — the projected, analyst-
 * safe header-badge channel (request/RUM metrics and log extents,
 * no ``ngwaf_workspace_id`` / ``active_run`` / etc). Writes incoming
 * payloads into the SAME React Query slot the analyst bootstrap
 * already seeds (``['bootstrap']`` → ``header_badge``). Omitted fields
 * preserve cached values; explicit nulls clear them after a reset.
 *
 * Closes Gap 3 from the badge SSE work: admins already get real-time
 * "Latest Log: Xs ago" / "Total Logs" updates via
 * ``useAdminEventStream``; this hook brings analysts to parity.
 */
type BootstrapHeaderBadge = Schema<'ShareHeaderBadgeResponse'>

interface BootstrapShape {
  header_badge?: BootstrapHeaderBadge | null
  [key: string]: unknown
}

type BadgeStreamEvent = Schema<'ShareHeaderBadgeResponse'>

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()

  return useServiceStream(
    enabled,
    '/api/log-extents/stream',
    (raw) => {
      try {
        const payload = JSON.parse(raw) as BadgeStreamEvent
        queryClient.setQueryData<BootstrapShape | undefined>(queryKeys.bootstrap(), (prev) => {
          // If bootstrap hasn't loaded yet (cold load before
          // hydration), skip — the bootstrap fetch will land
          // shortly and supply the initial header_badge slot.
          if (!prev) return prev
          return {
            ...prev,
            header_badge: {
              ...(prev.header_badge ?? {}),
              rum: payload.rum === undefined ? prev.header_badge?.rum
                : payload.rum === null ? null : { ...prev.header_badge?.rum, ...payload.rum },
              request: payload.request === undefined ? prev.header_badge?.request
                : payload.request === null ? null : { ...prev.header_badge?.request, ...payload.request },
              latest_log_at: payload.latest_log_at !== undefined ? payload.latest_log_at : prev.header_badge?.latest_log_at ?? null,
              local_rows: payload.local_rows !== undefined ? payload.local_rows : prev.header_badge?.local_rows ?? null,
            },
          }
        })
      } catch {
        // Malformed payload; skip.
      }
    },
  )
}
