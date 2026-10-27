import type { components } from '@/types/api.generated'
import type { Service } from '@/stores/filterStore'

export type Schema<K extends keyof components['schemas']> = components['schemas'][K]

/** Map a raw BootstrapService (snake_case from backend) to the internal Service shape. */
export function toService(s: components['schemas']['BootstrapService']): Service {
  return {
    id: s.service_id,
    name: s.name || s.service_id,
    cmcdEnabled: s.cmcd_enabled ?? undefined,
    rum_enabled: s.rum_enabled ?? undefined,
    is_high_scale: s.is_high_scale ?? undefined,
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
export type StripIndex<T> = { [K in keyof T as string extends K ? never : number extends K ? never : K]: T[K] }

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

/** Flatten an intersection into a single object type for readable tooltips. */
export type Simplify<T> = { [K in keyof T]: T[K] } & {}

/** Wire models generated from `extra=allow` Pydantic classes are all-optional
 *  and nullable. Mark `K` required and non-null where the producer always
 *  emits a value, so consumers get the narrowed type while the key set and
 *  value types stay derived from the generated schema. */
export type WithRequired<T, K extends keyof StripIndex<T>> = Simplify<
  Omit<StripIndex<T>, K> & { [P in K]-?: NonNullable<StripIndex<T>[P]> }
>

/** Like WithRequired but keeps `null` in the value (key always present, value may be null). */
export type WithRequiredNullable<T, K extends keyof StripIndex<T>> = Simplify<
  Omit<StripIndex<T>, K> & { [P in K]-?: Exclude<StripIndex<T>[P], undefined> }
>

// ── Shared request/response primitives ───────────────────────────────────────

/** Sourced from the generated OpenAPI schema — includes the caller field. */
export type DebugQuery = components['schemas']['DebugQuery']

/** Sourced from the generated OpenAPI schema — includes the caller field. */
export type DebugCall = components['schemas']['DebugCall']

// ─────────────────────────────────────────────────────────────────────────────

export type { components } from '@/types/api.generated'

export type ServiceConfig = components['schemas']['ServiceConfig']

export type LogFieldsConfig = components['schemas']['LogFieldsConfig']

export type BootstrapResponse = components['schemas']['BootstrapResponse']

export type DashboardTableData = components['schemas']['FieldAggregate']

export type DashboardMapData = components['schemas']['MapPoint']

/** One card, discriminated on `id`; `items[].meta` is the per-detection model for that id. */
export type InsightCardData = components['schemas']['InsightsResponse']['insights'][number]

export type Session = components['schemas']['Session']

export type ShareBanner = WithRequiredNullable<
  WithRequired<components['schemas']['ShareBannerResponse'], 'sharing_active'>,
  'public_url'
>

export type SessionsResponse = components['schemas']['SessionsResponse']

export type SessionDetailResponse = components['schemas']['SessionDetailResponse']

// ── Filter Types & Payload Builder (folded from filters.ts) ──────────────────

type FilterSpec = components["schemas"]["FilterSpec"]
export type FilterMode = FilterSpec["mode"]
export type FiltersPayload = Record<string, FilterSpec>

export interface FilterPill {
  id: string
  column: string
  value: string
  mode: FilterMode
}

/**
 * Build a FiltersPayload from an array of FilterPills.
 *
 * Dedup scheme: when the same column needs both an include AND an exclude
 * bucket, the second bucket gets a `_<n>` suffix (`country`, `country_1`).
 * hydrateFilterStoreFromUrl (lib/urlFilterHydration.ts) strips this suffix
 * on URL hydration, and the backend
 * (backend/repositories/utils/filters.py) strips it when building WHERE
 * clauses. As a consequence, column names literally ending in `_<digit>`
 * would be corrupted on round-trip. filterStore.addFilter guards entry —
 * any future field naming convention must avoid the collision.
 */
export function buildFiltersPayload(filters: FilterPill[]): FiltersPayload {
  const payload: FiltersPayload = {}

  filters.forEach(f => {
    let index = 0;
    let targetKey: string | null = null;

    while (true) {
      const currentKey = index === 0 ? f.column : `${f.column}_${index}`;
      if (!payload[currentKey]) {
        targetKey = currentKey;
        break;
      } else if (payload[currentKey].mode === f.mode) {
        targetKey = currentKey;
        break;
      }
      index++;
    }

    if (!payload[targetKey]) {
      payload[targetKey] = { mode: f.mode, values: [] };
    }
    payload[targetKey].values.push(f.value);
  })

  return payload
}

// ── Minimal Local Plotly Shapes (folded from plotly.ts) ──────────────────────
//
// react-plotly.js types `data` / `layout` / `config` as `unknown` and ships no plotly.js
// typings; `plotly.js-cartesian-dist-min` has none either. These interfaces name the keys
// this app actually reads or sets and leave every other Plotly attribute open through the
// `unknown` index signature, so callers keep full attribute freedom without `any`.

export type PlotlyDatum = string | number | boolean | Date | null | undefined

export interface PlotlyFont {
  color?: string
  family?: string
  size?: number
  [key: string]: unknown
}

export interface PlotlyAxis {
  title?: string | { text?: string; [key: string]: unknown }
  type?: string
  range?: readonly PlotlyDatum[]
  nticks?: number
  tickangle?: number
  automargin?: boolean
  fixedrange?: boolean
  showgrid?: boolean
  showspikes?: boolean
  gridcolor?: string
  zerolinecolor?: string
  separatethousands?: boolean
  exponentformat?: string
  ticksuffix?: string
  tickformat?: string
  autorange?: boolean | string
  [key: string]: unknown
}

export interface PlotlyLegend {
  orientation?: 'h' | 'v'
  x?: number
  y?: number
  xanchor?: 'auto' | 'left' | 'center' | 'right'
  yanchor?: 'auto' | 'top' | 'middle' | 'bottom'
  font?: PlotlyFont
  bgcolor?: string
  [key: string]: unknown
}

export interface PlotlyMargin {
  l?: number
  r?: number
  t?: number
  b?: number
  pad?: number
}

export interface PlotlyLayout {
  autosize?: boolean
  height?: number
  width?: number
  title?: string | { text?: string; [key: string]: unknown }
  barmode?: 'stack' | 'group' | 'overlay' | 'relative'
  showlegend?: boolean
  hovermode?: string | false
  paper_bgcolor?: string
  plot_bgcolor?: string
  font?: PlotlyFont
  margin?: PlotlyMargin
  legend?: PlotlyLegend
  xaxis?: PlotlyAxis
  yaxis?: PlotlyAxis
  yaxis2?: PlotlyAxis
  shapes?: readonly Record<string, unknown>[]
  annotations?: readonly Record<string, unknown>[]
  [key: string]: unknown
}

export interface PlotlyConfig {
  responsive?: boolean
  displayModeBar?: boolean
  [key: string]: unknown
}

export interface PlotlyTrace {
  type?: string
  mode?: string
  name?: string
  x?: readonly PlotlyDatum[]
  y?: readonly PlotlyDatum[]
  z?: readonly (readonly PlotlyDatum[])[]
  labels?: readonly PlotlyDatum[]
  values?: readonly PlotlyDatum[]
  text?: readonly string[] | string
  fill?: string
  stackgroup?: string
  hovertemplate?: string
  width?: number | readonly number[]
  marker?: Record<string, unknown>
  line?: Record<string, unknown>
  [key: string]: unknown
}

/** `plotly_relayout` payload: flat dotted keys (`xaxis.range[0]`) or a nested `xaxis.range`. */
export type PlotlyRelayoutEvent = Record<string, unknown>

export interface PlotlyFigure {
  data: readonly unknown[]
  layout: unknown
  frames: readonly unknown[] | null
}

/** The `graphDiv` handed to `onInitialized`: an element carrying Plotly's `on` emitter. */
export interface PlotlyGraphDiv extends HTMLElement {
  on?: (event: string, handler: (payload: PlotlyRelayoutEvent) => void) => void
}
