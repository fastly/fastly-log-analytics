// Re-derive a shielding route's low-sample / anomaly treatment for a
// user-chosen minimum-requests floor, entirely client-side.
//
// The backend already ships ``anomaly_eligible`` — the latency verdict
// (efficiency > 3× the light-speed floor AND ≥20ms absolute overhead) that is
// *independent* of how many requests the route saw. That keeps the actual
// "is this peering bad?" rule in one place (the repository); here we only own
// the sample-count comparison the user is adjusting, so the (3×/20ms) rule is
// never duplicated on the wire side.
//
// ``low_sample`` greys the route and suppresses its flag; ``anomaly_static`` is
// just ``anomaly_eligible && !low_sample``. At the server default (30) this
// reproduces exactly what the backend already computed. ``minRequests <= 0``
// means "no floor": nothing is low-sample and every eligible route is flagged.

export interface ShieldingRowLike {
  requests?: number | null
  anomaly_eligible?: boolean | null
  low_sample?: boolean
  anomaly_static?: boolean
  // Other fields (pops, percentiles, coords, efficiency_ratio…) pass through.
  [key: string]: unknown
}

export function adjustShieldingRows(
  rows: ShieldingRowLike[] | null | undefined,
  minRequests: number,
): ShieldingRowLike[] {
  if (!Array.isArray(rows)) return []
  return rows.map((row) => {
    const lowSample = minRequests > 0 && (row.requests ?? 0) < minRequests
    return {
      ...row,
      low_sample: lowSample,
      anomaly_static: Boolean(row.anomaly_eligible) && !lowSample,
    }
  })
}

// Dropdown choices for the "Min requests" control. "No minimum" (0) disables
// the floor entirely. The default (10) sits deliberately below the backend's
// trustworthy floor (SHIELDING_ANOMALY_MIN_REQUESTS = 30) so low-volume
// services still surface their eligible anomaly arcs on first paint; raise it
// to 30+ on busy services to suppress small-sample noise.
export const SHIELDING_MIN_REQUESTS_OPTIONS: ReadonlyArray<{ value: string; label: string }> = [
  { value: '0', label: 'No minimum' },
  { value: '10', label: '10' },
  { value: '30', label: '30' },
  { value: '50', label: '50' },
  { value: '100', label: '100' },
]

export const SHIELDING_MIN_REQUESTS_DEFAULT = 10
