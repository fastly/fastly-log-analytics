// Plain (non-'use client') module: loading.tsx is a server component and must read these arrays
// directly. Importing them from a 'use client' file hands it a client reference proxy instead.

export type TrendWindow = '1h' | '6h' | '24h' | '7d'

export const WINDOWS: { label: string; value: TrendWindow }[] = [
  { label: 'Last hour', value: '1h' },
  { label: 'Last 6h', value: '6h' },
  { label: 'Last 24h', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
]

// Series displayed on the page, top-down. Each entry binds a metric key
// (global → bare metric name, per-service → metric + '|svc'; we resolve
// per-service inline below) to a display label, unit, and chart domain.
export const SERIES_LAYOUT: {
  title: string
  description: string
  metric: string
  unit: string
  yDomain?: [number | 'auto', number | 'auto']
  aggregateBy?: 'max-per-ts'
}[] = [
  { title: 'CPU load (1m avg)', description: 'Linux load average over the trailing minute.', metric: 'cpu_load_1m', unit: '' },
  { title: 'Memory used', description: 'Physical memory consumption.', metric: 'mem_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Data disk used', description: 'Disk consumption on the data mount.', metric: 'disk_used_pct', unit: '%', yDomain: [0, 100] },
  { title: 'Boot disk used', description: 'Disk consumption on the root mount.', metric: 'disk_used_pct_root', unit: '%', yDomain: [0, 100] },
  { title: 'Active DuckDB queries', description: 'Concurrent in-flight query count from the registry.', metric: 'active_query_count', unit: '' },
  { title: 'Pool wait p95', description: 'Worst-case DuckDB pool checkout wait (ms) — max across services per timestamp.', metric: 'pool_wait_p95_ms', unit: 'ms', aggregateBy: 'max-per-ts' },
  { title: 'Ingest lag', description: 'Seconds since the most recent ingest per service — max across services per timestamp.', metric: 'ingest_lag_s', unit: 's', aggregateBy: 'max-per-ts' },
]
