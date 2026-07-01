'use client'

import { useQueryClient } from '@tanstack/react-query'
import { queryKeys } from '@/lib/query-keys'

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
 * Reads the seeded bootstrap query cache, mirroring {@link useIsAnalyst}.
 */
export function useMaskIps(): boolean {
  const queryClient = useQueryClient()
  const bootstrapData = queryClient.getQueryData<{ settings?: { mask_ips?: boolean } }>(
    queryKeys.bootstrap(),
  )
  return bootstrapData?.settings?.mask_ips === true
}
