/**
 * Headers for admin-backend requests built outside the openapi-fetch client
 * and `adminFetch` (raw-fetch SSE streams): `x-service-id` when a service is
 * set. Only for calls to this app's own backend; never attach to third-party URLs.
 *
 * Lives in its own module (re-exported from `lib/api`) so stream helpers can
 * use it without importing `lib/api`'s client side effects.
 */
export function buildAdminHeaders({
  serviceId,
}: {
  serviceId?: string | null
} = {}): Record<string, string> {
  return {
    ...(serviceId ? { 'x-service-id': serviceId } : {}),
  }
}
