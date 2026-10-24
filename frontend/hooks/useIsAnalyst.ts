'use client'

import * as React from 'react'
import { useBootstrapCacheSelector } from '@/hooks/useBootstrap'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { isIpFamilyField } from '@/lib/utils'

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

/**
 * Whether the current session has client-IP PII masking enabled — i.e. an
 * analyst whose invite carries `pii_policy.mask_ips=true`. False for admins
 * and for non-masking analysts.
 *
 * Used to hide IP drill-down affordances (the `ip` Top-N card, IP-family
 * `FilterValueCell`s, the `ip` entry in the Add-Filter dialog). The actual
 * enforcement is server-side (the RemoteAccessMiddleware filter lock 403s any
 * IP-family filter from a masking analyst); this hook only drives the UX so
 * the user isn't offered a control that would 403.
 *
 * Subscribes reactively to the seeded bootstrap query cache, mirroring
 * {@link useIsAnalyst}.
 */
export function useMaskIps(): boolean {
  return useBootstrapCacheSelector(
    (bootstrapData) =>
      (bootstrapData?.settings as { mask_ips?: boolean } | undefined)?.mask_ips === true,
    false,
  )
}

/**
 * Defense-in-depth UX guard for PII masking: strip any IP-family filter from
 * the filter store when the current session masks client IPs.
 *
 * The synchronous `hydrateFilterStoreFromUrl` runs before bootstrap resolves,
 * so a bookmarked `/dashboard?filters={"ip":...}` URL can seed an `ip` filter
 * the analyst isn't allowed to use. Left in place it would POST to the backend
 * and 403 (the RemoteAccessMiddleware IP-filter lock). This effect removes such
 * filters once `mask_ips` is known, degrading the bookmark to "no IP filter"
 * instead of a dead-end error. Mounted once in the app shell (AppLayout).
 */
export function useEnforceMaskedFilters(): void {
  const maskIps = useMaskIps()
  const filters = useFilterStore(s => s.filters)
  const removeFilter = useFilterStore(s => s.removeFilter)

  React.useEffect(() => {
    if (!maskIps) return
    // Strip the `_<n>` dedup suffix before comparing, mirroring the backend
    // normalize_filter_key so `ip_2` / `filter_ip` variants are caught too.
    for (const f of filters) {
      if (isIpFamilyField(f.column.replace(/_\d+$/, ''))) removeFilter(f.id)
    }
  }, [maskIps, filters, removeFilter])
}
