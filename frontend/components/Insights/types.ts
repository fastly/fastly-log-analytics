import type React from 'react'
import type { InsightCardData } from '@/types/api'

type CardOf<Id extends InsightCardData['id']> = Extract<InsightCardData, { id: Id }>

export type ImpossibleDistanceItem = CardOf<'impossible_distance'>['items'][number]
export type RepeatedPatternsItem = CardOf<'repeated_patterns'>['items'][number]
export type RepeatedPatternsFpItem = CardOf<'repeated_patterns_fp'>['items'][number]

export type BotFindingModalData =
  | {
      insightId: 'crawler_disparity'
      item: CardOf<'crawler_disparity'>['items'][number]
    }
  | {
      insightId: 'stale_browser_version'
      item: CardOf<'stale_browser_version'>['items'][number]
    }
  | {
      insightId: 'orphaned_deep_crawl'
      item: CardOf<'orphaned_deep_crawl'>['items'][number]
    }
  | {
      insightId: 'residential_fingerprint_dispersion'
      item: CardOf<'residential_fingerprint_dispersion'>['items'][number]
    }

export interface ImpossibleDistanceData {
  label: string
  client_lat: number
  client_lon: number
  pop_lat: number
  pop_lon: number
  pop: string
  tcp_rtt: number
  distance_km: number
  max_km: number
  country?: string
  city?: string
}

export interface ScriptedTrafficData {
  label: string
  score: number
  cv: number
  modal_frac: number
  mean_interval_s: number
  stddev_s: number
  mode_gap_s: number | null
  n_gaps: number
  n_events: number
  span_s: number
  rps: number
  distinct_ua: number
  distinct_ip?: number
}

export interface InsightHelpModalProps {
  insightId: string
  isOpen: boolean
  onOpenChange: (open: boolean) => void
}

export interface InsightContent {
  title: string
  icon: React.ReactNode
  description: React.ReactNode
  diagram?: React.ReactNode
  fields: string[]
}
