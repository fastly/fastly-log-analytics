export type UsageLogEntry = {
  id: number
  timestamp: string
  service_id: string | null
  operation_class: string | null
  operation_type: string | null
  url: string | null
  bytes: number | null
  duration_ms: number | null
  function_name: string | null
  process_context: string | null
  status: string | null
  estimated_cost: number | null
}

export type UsageLogAggregate = {
  total_class_a: number
  total_class_b: number
  total_cdn_downloads: number
  total_cdn_bytes: number
  total_fos_bytes: number
  estimated_cost_class_a: number
  estimated_cost_class_b: number
  estimated_cost_cdn: number
  estimated_cost_total: number
  class_a_breakdown: Record<string, number>
  class_b_breakdown: Record<string, number>
}

export const DATE_PRESETS = [
  { label: 'Last 1h', hours: 1 },
  { label: 'Last 24h', hours: 24 },
  { label: 'Last 7d', hours: 168 },
  { label: 'Last 30d', hours: 720 },
]

export function toQueryDate(d: Date): string {
  return d.toISOString().slice(0, 19) + 'Z'
}

export function fmtCost(n: number): string {
  if (n === 0) return '$0.000000'
  if (n < 0.000001) return `$${n.toExponential(2)}`
  return `$${n.toFixed(6)}`
}

export function fmtOps(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString()
}
