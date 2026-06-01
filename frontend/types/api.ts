import type { components } from '@/types/api.generated'
import type { Service } from '@/stores/serviceStore'

/** Map a raw BootstrapService (snake_case from backend) to the internal Service shape. */
export function toService(s: components['schemas']['BootstrapService']): Service {
  return { id: s.service_id, name: s.name || s.service_id, accessLevel: s.access_level ?? undefined }
}

/**
 * Valid chart metric keys. Mirrors the ChartMetric Literal in backend/models/dashboard.py.
 */
export type ChartMetric = components['schemas']['AggregatesRequest']['chart_metric']

// ── Shared request/response primitives ───────────────────────────────────────

/** Sourced from the generated OpenAPI schema — includes the caller field. */
export type DebugQuery = components['schemas']['DebugQuery']

/** Sourced from the generated OpenAPI schema — includes the caller field. */
export type DebugCall = components['schemas']['DebugCall']

// ─────────────────────────────────────────────────────────────────────────────

export type { Service } from '@/stores/serviceStore'

export type { components } from '@/types/api.generated'

export type BootstrapService = components['schemas']['BootstrapService']

export type BootstrapResponse = components['schemas']['BootstrapResponse']

export type DashboardTableData = components['schemas']['FieldAggregate']

export type DashboardMapData = components['schemas']['MapPoint']

export type InsightItem = Omit<components['schemas']['InsightItem'], 'meta'> & {
  meta?: Record<string, any>
}

export type InsightCardData = Omit<components['schemas']['InsightCard'], 'items'> & {
  items: InsightItem[]
}

export type InsightsResponse = components['schemas']['InsightsResponse']

export type InsightAvailabilityResponse = components['schemas']['InsightsAvailabilityResponse']

export type Session = components['schemas']['Session']

export type SessionsResponse = components['schemas']['SessionsResponse']

export type SessionDetailResponse = components['schemas']['SessionDetailResponse']
