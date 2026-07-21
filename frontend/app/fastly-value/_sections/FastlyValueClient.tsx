'use client'

import React from 'react'
import { TrendingUp } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { useQueryClient } from '@tanstack/react-query'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveSnappedWindow, type LogExtents } from '@/lib/log-extents-snap'
import { ReportLayout } from '@/components/ReportLayout'
import FastlyValueBody from './FastlyValueBody'

export default function FastlyValueClient() {
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const hasSyncedExtents = useFilterStore((s) => s.hasSyncedExtents)
  const storeEndTime = useFilterStore((s) => s.endTime)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()

  const anchor = React.useMemo(() => {
    if (isAutoRange && !hasSyncedExtents && activeServiceId) {
      const logExtents = queryClient.getQueryData(['log-extents', activeServiceId]) as LogExtents | undefined
      const snapped = resolveSnappedWindow(logExtents, new Date())
      if (snapped) return quantizeAnchor(snapped.end)
    }
    return quantizeAnchor(storeEndTime)
  }, [isAutoRange, hasSyncedExtents, activeServiceId, storeEndTime, queryClient])

  return (
    <ReportLayout
      title="Service Summary"
      description="Executive summary of how Fastly delivers value across all products."
      icon={TrendingUp}
      defaultInterval="1 day"
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <FastlyValueBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
        />
      )}
    </ReportLayout>
  )
}
