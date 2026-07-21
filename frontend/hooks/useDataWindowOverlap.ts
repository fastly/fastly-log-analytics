'use client'

import { useQuery } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { client } from '@/lib/api'

export type WindowOverlapStatus =
  | 'ok'              // selected range overlaps retained data
  | 'before-earliest' // entire selected range is older than earliest retained
  | 'after-latest'    // entire selected range is newer than latest retained
  | 'no-data'         // extents query succeeded but service has no logs at all
  | 'unknown'         // extents query has not succeeded yet

export interface WindowOverlap {
  status: WindowOverlapStatus
  earliestLogAt: string | null
  latestLogAt: string | null
  pickedStart: string
  pickedEnd: string
}

// Snapshot lag: /api/log-extents is computed from a cached DuckDB stats
// snapshot refreshed every ~60s. A user picking "now" or right at the
// boundary would otherwise see the banner flicker as fresh ingest moves
// `latest_log_at` forward. Grace window suppresses the boundary noise
// without hiding genuinely-out-of-range picks (anything that would render
// an empty chart is well outside ±60s).
const GRACE_MS = 60_000

// Determine whether the user's selected time range overlaps the retained
// log extents. Read-only: never mutates filterStore. The banner consumer
// must NOT auto-snap on a non-ok status — that conflicts with the
// explicit-user-action norm (filter changes affect every queryKey
// downstream and shareable links would silently swap to a different
// window than the URL specifies).
//
// Reuses the `['log-extents', activeServiceId]` query that FilterBar
// already owns, so this hook is free when called alongside FilterBar
// (same key, same staleTime — TanStack just returns the cached entry).
export function useDataWindowOverlap(): WindowOverlap {
  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const startTime = useFilterStore(state => state.startTime)
  const endTime = useFilterStore(state => state.endTime)

  const { data, isSuccess } = useQuery({
    queryKey: ['log-extents', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET('/api/log-extents')
      return data
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
  })

  const earliestLogAt = data?.earliest_log_at ?? null
  const latestLogAt = data?.latest_log_at ?? null

  let status: WindowOverlapStatus = 'unknown'
  if (isSuccess) {
    if (!earliestLogAt || !latestLogAt) {
      // Extents query came back empty — service has no logs yet (just-
      // provisioned, between syncs). Different from a partial overlap;
      // banner copy will say "no logs yet" instead of "outside range."
      status = 'no-data'
    } else {
      const startMs = Date.parse(startTime)
      const endMs = Date.parse(endTime)
      const earliestMs = Date.parse(toIsoFromLooseExtent(earliestLogAt, false))
      const latestMs = Date.parse(toIsoFromLooseExtent(latestLogAt, true))

      if (Number.isFinite(startMs) && Number.isFinite(endMs)
        && Number.isFinite(earliestMs) && Number.isFinite(latestMs)) {
        if (endMs < earliestMs - GRACE_MS) {
          status = 'before-earliest'
        } else if (startMs > latestMs + GRACE_MS) {
          status = 'after-latest'
        } else {
          status = 'ok'
        }
      }
    }
  }

  return {
    status,
    earliestLogAt,
    latestLogAt,
    pickedStart: startTime,
    pickedEnd: endTime,
  }
}

// /api/log-extents returns either a full ISO timestamp ("2026-06-15T03:14:00Z")
// or a date-only string ("2026-06-15") depending on the storage tier the
// underlying view materialised from. FilterBar's snap-to-extents code does
// the same widening (see FilterBar.tsx:185-186). Earliest gets the day's
// start, latest gets the day's end, so a date-only extent doesn't falsely
// fail the overlap check for a sub-day window.
function toIsoFromLooseExtent(value: string, isEnd: boolean): string {
  if (value.length === 10) {
    return value + (isEnd ? 'T23:59:59.999Z' : 'T00:00:00.000Z')
  }
  return value
}
