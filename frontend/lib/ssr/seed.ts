import { QueryClient, dehydrate } from '@tanstack/react-query'
import type { DehydratedState, QueryKey } from '@tanstack/react-query'
import { fetchBootstrapServerSide } from './bootstrap'

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

/**
 * Resolve the active service ID for an RSC page render:
 * 1. Explicit `?service=` URL searchParam
 * 2. `bootstrap.active_service_id`
 * 3. Optionally `bootstrap.services[0].id` when `fallbackFirstService` is set
 */
export function resolveServerServiceId(
  params: { service?: string | string[] },
  bootstrap:
    | {
        active_service_id?: string | null
        services?: ReadonlyArray<{ id?: unknown; service_id?: unknown }> | null
      }
    | null
    | undefined,
  options?: { fallbackFirstService?: boolean },
): string | undefined {
  const urlServiceId = firstParam(params.service)
  if (urlServiceId) return urlServiceId
  if (bootstrap?.active_service_id) return bootstrap.active_service_id
  if (options?.fallbackFirstService) {
    const first = bootstrap?.services?.[0]
    const firstId = first?.id ?? first?.service_id
    if (typeof firstId === 'string') return firstId
  }
  return undefined
}

/**
 * Resolve the active service ID for an RSC page render, short-circuiting
 * `fetchBootstrapServerSide()` when `?service=` is already present in the URL.
 */
export async function resolvePageServiceId(
  params: { service?: string | string[] },
  options?: { fallbackFirstService?: boolean },
): Promise<string | undefined> {
  const urlServiceId = firstParam(params.service)
  if (urlServiceId) return urlServiceId
  const bootstrap = await fetchBootstrapServerSide()
  return resolveServerServiceId(params, bootstrap, options)
}
