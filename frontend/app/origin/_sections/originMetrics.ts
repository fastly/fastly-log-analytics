export const ORIGIN_METRICS = ['ttfb', 'ttlb'] as const
export const ORIGIN_PERCENTILES = ['p50', 'p95', 'p99'] as const
export type OriginMetric = (typeof ORIGIN_METRICS)[number]
export type OriginPercentile = (typeof ORIGIN_PERCENTILES)[number]
