'use client'

import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { historyHoursFromExtents, pickInsightsDefault } from '@/lib/insights-defaults'

interface InsightsDefaults {
  windowHours: string
  baselineHours: string
  setWindowHours: (value: string) => void
  setBaselineHours: (value: string) => void
}

// An explicit user pick, scoped to the service it was made on. Keying the
// override by service id lets a manual selection stick for the current
// service while being dropped automatically when the active service changes —
// computed during render, so no effect/reset is needed (and no setState-in-
// effect, which this repo's eslint forbids).
interface Override {
  sid: string | null | undefined
  window?: string
  baseline?: string
}

const NO_OVERRIDE: Override = { sid: undefined }

// Picks the Insights window/baseline default from how much history the active
// service has (via the shared ['log-extents', sid] query that useBootstrap
// seeds), while letting an explicit user selection win. Returns the same
// { windowHours, baselineHours, setWindowHours, setBaselineHours } shape the
// page already consumed from useState, so wiring is a drop-in.
export function useInsightsDefaults(
  activeServiceId: string | null | undefined,
): InsightsDefaults {
  const { data } = useQuery({
    queryKey: ['log-extents', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET('/api/log-extents')
      return data
    },
    enabled: !!activeServiceId,
    staleTime: 30_000,
  })

  const adaptive = useMemo(
    () => pickInsightsDefault(historyHoursFromExtents(data?.earliest_log_at)),
    [data?.earliest_log_at],
  )

  const [override, setOverride] = useState<Override>({ sid: activeServiceId })
  // Drop a stale override the moment the service changes (derived, not reset).
  const eff = override.sid === activeServiceId ? override : NO_OVERRIDE

  const windowHours = eff.window ?? adaptive.window
  const baselineHours = eff.baseline ?? adaptive.baseline

  const setWindowHours = useCallback(
    (value: string) =>
      setOverride((prev) => ({
        sid: activeServiceId,
        baseline: prev.sid === activeServiceId ? prev.baseline : undefined,
        window: value,
      })),
    [activeServiceId],
  )
  const setBaselineHours = useCallback(
    (value: string) =>
      setOverride((prev) => ({
        sid: activeServiceId,
        window: prev.sid === activeServiceId ? prev.window : undefined,
        baseline: value,
      })),
    [activeServiceId],
  )

  return { windowHours, baselineHours, setWindowHours, setBaselineHours }
}
