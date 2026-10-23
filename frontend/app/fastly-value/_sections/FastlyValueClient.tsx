'use client'

import React from 'react'
import { TrendingUp } from 'lucide-react'
import { useReportRangeAnchor } from '@/hooks/useReportRangeAnchor'
import { ReportLayout } from '@/components/ReportLayout'
import FastlyValueBody from './FastlyValueBody'

export default function FastlyValueClient() {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor()

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
