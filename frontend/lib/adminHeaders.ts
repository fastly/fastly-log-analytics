import { useAdminTokenStore } from '@/stores/adminTokenStore'

/**
 * Headers for admin-backend requests built outside the openapi-fetch client
 * and `adminFetch` (raw-fetch SSE streams): `x-service-id` when a service is
 * set and `X-Admin-Token` when one is available. `adminToken: undefined` reads
 * the same Zustand store `adminFetch` reads; `null`/'' means "no token". Only
 * for calls to this app's own backend; never attach to third-party URLs.
 *
 * Lives in its own module (re-exported from `lib/api`) so stream helpers can
 * use it without importing `lib/api`'s client side effects.
 */
export function buildAdminHeaders({
  serviceId,
  adminToken,
}: {
  serviceId?: string | null
  adminToken?: string | null
} = {}): Record<string, string> {
  let token = adminToken
  if (token === undefined) {
    try {
      token = useAdminTokenStore.getState().token
    } catch { /* SSR pre-hydration: no token */ }
  }
  return {
    ...(serviceId ? { 'x-service-id': serviceId } : {}),
    ...(token ? { 'X-Admin-Token': token } : {}),
  }
}
