import type { ReportConfiguration } from '@/hooks/useReportConfig'
import type { DashboardCard } from '@/hooks/useDashboardCards'
import type { FiltersPayload } from '@/types/filters'
import type { components } from '@/types/api.generated'

type CatalogEntry = components['schemas']['LogFieldCatalogEntry']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']
type Aggregates = components['schemas']['AggregatesResponse']

/** The slice of the log-fields catalog the dashboard cards read. */
export type DashboardCatalog = {
  fields?: Pick<CatalogEntry, 'id' | 'label' | 'group' | 'unit' | 'precision'>[] | null
  groups?: Pick<CatalogGroup, 'id' | 'label'>[] | null
}

/** The slice of the aggregates payload each dashboard section reads. */
export type DashboardAggregates = Partial<Pick<Aggregates, 'data' | 'map_data' | 'time_series' | 'metric' | 'interval'>>

export type CardTint = {
  bg: string
  border: string
  dot: string
}

export type CardCategory = {
  id: string
  label: string
  cardIds: string[]
  tint: CardTint
}

export interface DashboardBodyProps {
  startTime: string | null
  endTime: string | null
  timezone: string
  activeServiceId: string | null
  filterPayload: FiltersPayload
  config: ReportConfiguration
  metric?: string
  setMetric?: (metric: string) => void
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  allCards: DashboardCard[]
  visibleCards: Set<string>
  // Time-range wire inputs (lib/range-wire.ts; resolved in useDashboardBundle).
  // relativeRange + isAutoRange decide token-vs-absolute: a preset / the
  // cold-load default → a server-reproducible token (lib/range-wire.ts);
  // a custom absolute range → the explicit start/end
  // bounds. anchor is the quantized mount instant (token mode only).
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

export type { ReportConfiguration }
