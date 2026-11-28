import type { components } from '@/types/api'

/** A route as the map and table consume it: derived flags may be absent until adjusted. */
export type ShieldingRowInput = Partial<components['schemas']['ShieldingRow']>

export function adjustShieldingRows(
  rows: readonly ShieldingRowInput[] | null | undefined,
  minRequests: number,
): ShieldingRowInput[] {
  if (!Array.isArray(rows)) return []
  return rows.map((row) => {
    const lowSample = minRequests > 0 && (row.requests ?? 0) < minRequests
    return { ...row, low_sample: lowSample, anomaly_static: Boolean(row.anomaly_eligible) && !lowSample }
  })
}

export const SHIELDING_MIN_REQUESTS_OPTIONS: ReadonlyArray<{ value: string; label: string }> = [
  { value: '0', label: 'No minimum' },
  { value: '10', label: '10' },
  { value: '30', label: '30' },
  { value: '50', label: '50' },
  { value: '100', label: '100' },
]

export const SHIELDING_MIN_REQUESTS_DEFAULT = 10
