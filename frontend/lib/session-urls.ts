export function buildStreamDetailUrl(token: string, serviceId?: string | null): string {
  const base = `/sessions/stream?token=${encodeURIComponent(token)}`
  return serviceId ? `${base}&service=${encodeURIComponent(serviceId)}` : base
}

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
