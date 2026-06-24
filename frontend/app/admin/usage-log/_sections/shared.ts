import type { components } from '@/types/api.generated'

/** The backend UsageLogEntry omits service_id on the wire (it's hoisted
 *  to the parent UsageLogResponse to save bytes), but the table renderer
 *  expects each row to carry service_id — the page mapper re-injects it
 *  before passing rows to the table. */
export type UsageLogEntry = components['schemas']['UsageLogEntry'] & {
  service_id: string | null
}

export type UsageLogAggregate = components['schemas']['UsageLogAggregate']

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
