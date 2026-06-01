/**
 * Builds a dashboard URL pre-filtered to a specific session time window.
 * Centralised so the param names can't drift between link generation and useUrlFilterSync.
 */
export function buildSessionDashboardUrl(
  serviceId: string | null | undefined,
  filterKey: string,
  filterValue: string | null | undefined,
  sessionStart: string | null | undefined,
  sessionEnd: string | null | undefined,
): string {
  const params = new URLSearchParams()
  if (serviceId != null) params.set('service', serviceId)
  if (filterValue != null) params.set(`filter_${filterKey}`, filterValue)
  if (sessionStart != null) params.set('start_time', sessionStart)
  if (sessionEnd != null) params.set('end_time', sessionEnd)
  return `/dashboard?${params.toString()}`
}
