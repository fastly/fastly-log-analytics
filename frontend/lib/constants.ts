/**
 * Shared constants for report pages and components.
 */

export const INTERVALS = [
  { label: '1s', value: '1 second' },
  { label: '1m', value: '1 minute' },
  { label: '1h', value: '1 hour' },
  { label: '1d', value: '1 day' },
] as const

export type ChartInterval = typeof INTERVALS[number]['value']

export const INTERVAL_SECONDS: Record<ChartInterval, number> = {
  '1 second': 1,
  '1 minute': 60,
  '1 hour': 3600,
  '1 day': 86400,
}

export const TRENDS = [
  { label: 'Off', value: 'off' },
  { label: 'Auto', value: 'auto' },
  { label: '1m', value: '1m' },
  { label: '5m', value: '5m' },
  { label: '1h', value: '1h' },
  { label: '1d', value: '1d' },
]

export const CHART_LAYOUT_DEFAULTS = {
  tickformatstops: [
    { dtickrange: [0, 1000], value: "%H:%M:%S.%f" },
    { dtickrange: [1000, 60000], value: "%H:%M:%S" },
    { dtickrange: [60000, 3600000], value: "%H:%M" },
    { dtickrange: [3600000, 86400000], value: "%H:%M<br>%b %d" },
    { dtickrange: [86400000, 604800000], value: "%b %d" },
    { dtickrange: [604800000, "M1"], value: "%b %d" },
    { dtickrange: ["M1", "M12"], value: "%b %Y" },
    { dtickrange: ["M12", null], value: "%Y" }
  ]
}
