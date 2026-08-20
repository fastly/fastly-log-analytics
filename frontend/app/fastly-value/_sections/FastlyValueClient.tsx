'use client'

import React from 'react'
import { TrendingUp } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { quantizeAnchor } from '@/lib/time-window'
import { ReportLayout } from '@/components/ReportLayout'
import FastlyValueBody from './FastlyValueBody'

export default function FastlyValueClient() {
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = React.useMemo(() => {
    return quantizeAnchor(storeEndTime)
  }, [storeEndTime])

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
