'use client'

import * as React from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useMaskIps } from '@/hooks/useMaskIps'
import { isIpFamilyField } from '@/lib/pii'

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
