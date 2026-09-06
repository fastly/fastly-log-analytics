import type { components } from '@/types/api.generated'
import type { Service } from '@/stores/serviceStore'

/** Map a raw BootstrapService (snake_case from backend) to the internal Service shape. */
export function toService(s: components['schemas']['BootstrapService']): Service {
  return {
    id: s.service_id,
    name: s.name || s.service_id,
    accessLevel: s.access_level ?? undefined,
    cmcdEnabled: s.cmcd_enabled ?? undefined,
  }
}

/**
 * Valid chart metric keys. Mirrors the ChartMetric Literal in backend/models/dashboard.py.
 */
export type ChartMetric = components['schemas']['AggregatesRequest']['chart_metric']

// ── Wire-parity guards ────────────────────────────────────────────────────────
//
// The wire-safe backend response models (extra=allow + exclude_unset) generate
// every field optional AND nullable, so several components keep deliberate
// local row narrowings that encode what the producer actually always emits
// (required fields, literal unions). These helpers pin such a narrowing to its
// generated schema at compile time: a backend field rename/removal, or a value
// type change, fails `tsc` on the `Expect<WireParity<...>>` line instead of
// silently breaking at runtime. The local narrowing stays the ergonomic type
// components consume; the generated schema stays the source of truth for keys.

export type Expect<T extends true> = T

/** Drop the `[key: string]: unknown` index signature the extra=allow models
 *  generate, keeping only the declared keys. */
type StripIndex<T> = { [K in keyof T as string extends K ? never : number extends K ? never : K]: T[K] }

/** StripIndex applied through arrays and nested objects, so interface-typed
 *  local fields stay assignable to their (index-signature-carrying) wire
 *  counterparts in the value-compatibility check. Record<string, X> fields
 *  collapse to {} on both sides — give X its own WireParity guard. */
type DeepStripIndex<T> = T extends (infer E)[]
  ? DeepStripIndex<E>[]
  : T extends object
    ? { [K in keyof T as string extends K ? never : number extends K ? never : K]: DeepStripIndex<T[K]> }
    : T

export type WireParity<Local, Wire, W = StripIndex<Wire>> =
  Exclude<keyof W, keyof Local> extends never
    ? Exclude<keyof Local, keyof W> extends never
      ? DeepStripIndex<{ [K in keyof Local]-?: NonNullable<Local[K]> }> extends DeepStripIndex<{
          [K in keyof Local]-?: NonNullable<W[K & keyof W]>
        }>
        ? true
        : 'local field type is not assignable to its wire type'
      : { keys_not_on_wire: Exclude<keyof Local, keyof W> }
    : { wire_keys_missing_from_local: Exclude<keyof W, keyof Local> }

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
