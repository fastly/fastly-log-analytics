// Server-safe SSR seeding helpers shared by the per-request RSC page
// shells (alerts/logs/usage-log/trends). These touch only
// @tanstack/react-query — no next/headers — so they're importable from
// any server component.

import { QueryClient, dehydrate } from '@tanstack/react-query'
import type { DehydratedState, QueryKey } from '@tanstack/react-query'

/**
 * Build the DehydratedState for a single pre-fetched query entry, or null
 * when there's nothing to seed. Equivalent to the
 * `new QueryClient() + setQueryData + dehydrate` idiom each page inlined;
 * dehydrate of a one-entry client is deterministic, so the produced state
 * is identical. Keys stay declared at the call site (load-bearing for
 * SSR-seed-key ↔ useQuery-key alignment), so pass the key explicitly.
 */
export function seedDehydratedState(key: QueryKey, data: unknown): DehydratedState | null {
  if (data == null) return null
  const qc = new QueryClient()
  qc.setQueryData(key, data)
  return dehydrate(qc)
}

/**
 * Collapse an optional `string | string[]` search param to the first
 * string (or undefined). Reproduces `Array.isArray(v) ? v[0] : v`,
 * including the undefined passthrough.
 */
export const firstParam = (v: string | string[] | undefined): string | undefined =>
  Array.isArray(v) ? v[0] : v
