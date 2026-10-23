'use client'

import { useBootstrapCacheSelector } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/serviceStore'

/**
 * Whether the current session is an "analyst" (read-only role).
 *
 * Mirrors the analyst-detection used in app/alerts/page.tsx and gates
 * any caller that would otherwise 403 against an admin-only endpoint
 * (sync-status, sync-status SSE stream, etc.). Extracted so multiple
 * hooks share the exact same predicate — divergence here causes
 * silent "analyst opened a stream that 403s in a loop" bugs.
 */
export function useIsAnalyst(): boolean {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const services = useServiceStore(s => s.services)
  const isRemoteAnalyst = useBootstrapCacheSelector(
    (bootstrapData) =>
      (bootstrapData?.settings as Record<string, unknown> | undefined)?.is_remote_analyst === true,
    false,
  )
  const activeService = services?.find(s => s.id === activeServiceId)
  return (
    activeService?.accessLevel === 'read_only' ||
    isRemoteAnalyst
  )
}
