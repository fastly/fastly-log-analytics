import type { ReportConfiguration } from '@/hooks/useReportConfig'

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
  filterPayload: any
  config: ReportConfiguration
  trend: string
  setTrend: (trend: string) => void
  intervalButtons: React.ReactNode
  allCards: any[]
  visibleCards: Set<string>
  // Time-range wire inputs (lib/range-wire.ts; resolved in useDashboardBundle).
  // relativeRange + isAutoRange decide token-vs-absolute: a preset / the
  // cold-load default → a server-reproducible token (SSR-seed contract, see
  // lib/ssr/dashboard.ts); a custom absolute range → the explicit start/end
  // bounds. anchor is the quantized mount instant (token mode only).
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
}

export type { ReportConfiguration }
