'use client'

import { useQueryClient } from '@tanstack/react-query'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useServiceStream } from '@/hooks/useServiceStream'
import { queryKeys } from '@/lib/query-keys'

/**
 * Analyst-safe sibling of ``useAdminEventStream``.
 *
 * Subscribes to ``/api/log-extents/stream`` — the projected, analyst-
 * safe header-badge channel (only ``latest_log_at`` + ``local_rows``,
 * no ``ngwaf_workspace_id`` / ``active_run`` / etc). Writes incoming
 * payloads into the SAME React Query slot the analyst bootstrap
 * already seeds (``['bootstrap']`` → ``settings.header_badge``), so
 * the existing ``SyncStatusBadge`` fallback chain
 * (``status?.latest_log_at || ... || headerBadge?.latest_log_at``)
 * picks up pushed values with zero render-side changes.
 *
 * Closes Gap 3 from the badge SSE work: admins already get real-time
 * "Latest Log: Xs ago" / "Total Logs" updates via
 * ``useAdminEventStream``; this hook brings analysts to parity.
 */
interface StreamMetrics {
  latest_log_at?: string | null
  total_rows?: number | null
  last_sync_at?: string | null
}

interface BootstrapHeaderBadge {
  rum?: StreamMetrics
  request?: StreamMetrics
  latest_log_at?: string | null
  local_rows?: number | null
}

interface BootstrapShape {
  header_badge?: BootstrapHeaderBadge
  [key: string]: unknown
}

interface BadgeStreamEvent {
  rum?: StreamMetrics
  request?: StreamMetrics
  latest_log_at?: string | null
  local_rows?: number | null
}

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()
  // Reactive subscription to bootstrap. Required because the caller's
  // ``enabled`` value typically derives from ``useIsAnalyst()``, which
  // is a non-reactive synchronous read of ``queryClient.getQueryData(['bootstrap'])``.
  // Without this hook subscribing to bootstrap directly, the badge
  // component's first render sees ``isAnalyst === false`` (bootstrap
  // hasn't resolved yet), this hook gets ``enabled === false``, the
  // useEffect bails, and when bootstrap eventually arrives the dep
  // never flips because nothing in the chain forced a re-render with
  // the now-correct value. Observed 2026-06-16 on the Fastly URL:
  // backend SSE worked end-to-end via direct fetch, hook never fired
  // (zero setQueryData writes in 75s of waiting). Subscribing here
  // forces a re-render the moment bootstrap data lands.
  const { data: bootstrap } = useBootstrap()
  const isRemoteAnalyst = (bootstrap as { settings?: { is_remote_analyst?: boolean } } | undefined)?.settings?.is_remote_analyst === true
  const effectiveEnabled = enabled && isRemoteAnalyst

  return useServiceStream(
    effectiveEnabled,
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
              rum: payload.rum ? { ...prev.header_badge?.rum, ...payload.rum } : prev.header_badge?.rum,
              request: payload.request ? { ...prev.header_badge?.request, ...payload.request } : prev.header_badge?.request,
              latest_log_at: payload.latest_log_at ?? prev.header_badge?.latest_log_at ?? null,
              local_rows: payload.local_rows ?? prev.header_badge?.local_rows ?? null,
            },
          }
        })
      } catch {
        // Malformed payload; skip.
      }
    },
  )
}
