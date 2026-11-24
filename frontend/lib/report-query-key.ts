export function analyticsReportQueryKey({
  namespace,
  serviceId,
  rangeToken,
  anchor,
  filterPayload,
  keyParts = [],
}: {
  namespace: readonly unknown[]
  serviceId: string | null | undefined
  rangeToken: string
  anchor: string
  filterPayload: unknown
  keyParts?: readonly unknown[]
}): unknown[] {
  return [
    ...namespace,
    serviceId,
    rangeToken,
    anchor,
    filterPayload,
    ...keyParts,
  ]
}

export function insightsReportQueryKey({
  serviceId,
  windowHours,
  baselineHours,
  filterPayload,
}: {
  serviceId: string | null | undefined
  windowHours: string
  baselineHours: string
  filterPayload?: Record<string, unknown>
}): unknown[] {
  const key: unknown[] = ['insights', serviceId, windowHours, baselineHours]
  if (filterPayload && Object.keys(filterPayload).length > 0) {
    key.push(filterPayload)
  }
  return key
}
