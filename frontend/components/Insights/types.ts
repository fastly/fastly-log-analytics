import type { InsightCardData } from '@/types/api'

type CardOf<Id extends InsightCardData['id']> = Extract<InsightCardData, { id: Id }>

export type ImpossibleDistanceItem = CardOf<'impossible_distance'>['items'][number]
export type RepeatedPatternsItem = CardOf<'repeated_patterns'>['items'][number]
export type RepeatedPatternsFpItem = CardOf<'repeated_patterns_fp'>['items'][number]
export type CrawlerDisparityItem = CardOf<'crawler_disparity'>['items'][number]
export type BotBandwidthDisparityItem = CardOf<'bot_bandwidth_disparity'>['items'][number]
export type StaleBrowserVersionItem = CardOf<'stale_browser_version'>['items'][number]
export type OrphanedDeepCrawlItem = CardOf<'orphaned_deep_crawl'>['items'][number]
export type ResidentialFingerprintDispersionItem = CardOf<'residential_fingerprint_dispersion'>['items'][number]

export type BotFindingModalData =
  | {
      insightId: 'crawler_disparity'
      item: CrawlerDisparityItem
    }
  | {
      insightId: 'bot_bandwidth_disparity'
      item: BotBandwidthDisparityItem
    }
  | {
      insightId: 'stale_browser_version'
      item: StaleBrowserVersionItem
    }
  | {
      insightId: 'orphaned_deep_crawl'
      item: OrphanedDeepCrawlItem
    }
  | {
      insightId: 'residential_fingerprint_dispersion'
      item: ResidentialFingerprintDispersionItem
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

// Evidence behind a "Scripted Traffic Patterns" (repeated_patterns) flag.
// Every field is already serialised into InsightItem.meta by the backend
// repeated_patterns_processor — the modal is pure presentational (no fetch).
export interface ScriptedTrafficData {
  label: string
  score: number          // 0-100 regularity score (higher = more machine-like)
  cv: number             // Sheppard-corrected coefficient of variation
  modal_frac: number     // fraction of inter-arrival gaps equal to the modal gap
  mean_interval_s: number
  stddev_s: number       // jitter (σ)
  mode_gap_s: number | null
  n_gaps: number
  n_events: number
  span_s: number
  rps: number
  distinct_ua: number
  distinct_ip?: number
}
