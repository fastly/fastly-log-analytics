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
}

export type { ReportConfiguration }
