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
}
