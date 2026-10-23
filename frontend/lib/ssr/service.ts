import { firstParam } from './seed'
import { fetchBootstrapServerSide } from './bootstrap'

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
