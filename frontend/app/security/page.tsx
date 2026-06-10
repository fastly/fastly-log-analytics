'use client'

import React from 'react'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useTimezone } from '@/hooks/useTimezone'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { useTimeLayout } from '@/lib/chart-helpers'
import { ReportLayout } from '@/components/ReportLayout'
import { client } from '@/lib/api'
import { Shield } from 'lucide-react'
import { BotsSection } from './_sections/BotsSection'
import { HeaderAnomaliesSection } from './_sections/HeaderAnomaliesSection'
import { NetworkSection } from './_sections/NetworkSection'

export default function SecurityPage() {
  const getFieldLabel = useFieldLabel()
  const { startTime, endTime } = useTimeRange()
  const timezone = useTimezone()

  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange] = useColumnVisibility()

  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)

  return (
    <ReportLayout
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
      icon={Shield}
      queryKey="security"
      apiCall={async ({ startTime, endTime, filters, bucketSeconds }) => {
        const { data } = await client.POST("/api/security/aggregates", {
          body: {
            start_time: startTime,
            end_time: endTime,
            filters,
            bucket_seconds: bucketSeconds,
          }
        })
        return data
      }}
    >
      {({ data, isLoading, isFetching, intervalButtons, bucketSeconds }) => (
        <>
          <BotsSection
            data={data}
            isLoading={isLoading}
            isFetching={isFetching}
            intervalButtons={intervalButtons}
            bucketSeconds={bucketSeconds}
            timezone={timezone}
            commonTimeLayout={commonTimeLayout}
            getFieldLabel={getFieldLabel}
            ngwafBotVisibility={ngwafBotVisibility}
            setNgwafBotVisibility={setNgwafBotVisibility}
            onNgwafBotVisChange={onNgwafBotVisChange}
            botVisibility={botVisibility}
            setBotVisibility={setBotVisibility}
            onBotVisChange={onBotVisChange}
            fingerprintVisibility={fingerprintVisibility}
            setFingerprintVisibility={setFingerprintVisibility}
            onFingerprintVisChange={onFingerprintVisChange}
          />
          <HeaderAnomaliesSection
            data={data}
            isLoading={isLoading}
            isFetching={isFetching}
            getFieldLabel={getFieldLabel}
            topIpVisibility={topIpVisibility}
            setTopIpVisibility={setTopIpVisibility}
            onTopIpVisChange={onTopIpVisChange}
          />
          <NetworkSection
            data={data}
            isLoading={isLoading}
            isFetching={isFetching}
            timezone={timezone}
            commonTimeLayout={commonTimeLayout}
          />
        </>
      )}
    </ReportLayout>
  )
}
