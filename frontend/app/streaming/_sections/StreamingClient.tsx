'use client'

import React from 'react'
import { parseAsStringLiteral, useQueryState } from 'nuqs'
import { useFilterStore } from '@/stores/filterStore'
import { quantizeAnchor } from '@/lib/time-window'
import { Gauge, Play, ShieldCheck } from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import PoliciesView from './tabs/PoliciesView'
import QualityTab from './tabs/QualityTab'
import SecurityTab from './tabs/SecurityTab'

const TABS = [
  {
    id: 'quality',
    label: 'Quality of Experience',
    icon: Gauge,
    description: 'CMCD-powered video streaming quality analytics — buffer health, bitrate, throughput, and rebuffering.',
  },
  {
    id: 'security',
    label: 'Content Security',
    icon: ShieldCheck,
    description: 'Player-reported content security and anti-piracy metrics.',
  },
] as const

type TabId = (typeof TABS)[number]['id']

// ?tab=security deep-links to a tab; the default (quality) is omitted from the URL.
const tabParser = parseAsStringLiteral(TABS.map((t) => t.id)).withDefault('quality').withOptions({
  history: 'replace',
  shallow: true,
})

const SECURITY_VIEWS = ['monitor', 'policies'] as const

// ?csview=policies selects Content Security's Policies view; Monitor (the default) is omitted from the URL.
const securityViewParser = parseAsStringLiteral(SECURITY_VIEWS).withDefault('monitor').withOptions({
  history: 'replace',
  shallow: true,
})

export default function StreamingClient() {
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)
  const [activeTab, setActiveTab] = useQueryState('tab', tabParser)
  const [securityView, setSecurityView] = useQueryState('csview', securityViewParser)
  // Policies are admin-only (the endpoints 403 analysts), so analysts only get Monitor.
  const isAdmin = !useIsAnalyst()

  const anchor = React.useMemo(() => quantizeAnchor(storeEndTime), [storeEndTime])

  return (
    <ReportLayout
      title="Streaming"
      description="Video streaming analytics — playback quality and content security for your streaming services."
      icon={Play}
    >
      {({ startTime, endTime, activeServiceId, filterPayload, timezone, bucketSeconds }) => {
        const tabProps = {
          activeServiceId,
          filterPayload,
          startTime,
          endTime,
          relativeRange,
          isAutoRange,
          anchor,
          timezone,
        }
        return (
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
            <TabsList>
              {TABS.map((tab) => (
                <TabsTrigger key={tab.id} value={tab.id}>
                  <tab.icon className="h-4 w-4" />
                  {tab.label}
                </TabsTrigger>
              ))}
            </TabsList>
            <p className="text-sm text-muted-foreground max-w-2xl mb-4">
              {TABS.find((t) => t.id === activeTab)?.description}
            </p>
            <TabsContent value="quality">
              <QualityTab {...tabProps} />
            </TabsContent>
            <TabsContent value="security">
              {isAdmin ? (
                <Tabs value={securityView} onValueChange={(v) => setSecurityView(v as (typeof SECURITY_VIEWS)[number])}>
                  <TabsList variant="line" aria-label="Content Security views">
                    <TabsTrigger value="monitor">Monitor</TabsTrigger>
                    <TabsTrigger value="policies">Policies</TabsTrigger>
                  </TabsList>
                  <TabsContent value="monitor" className="pt-4">
                    <SecurityTab {...tabProps} bucketSeconds={bucketSeconds} />
                  </TabsContent>
                  <TabsContent value="policies" className="pt-4">
                    <PoliciesView {...tabProps} bucketSeconds={bucketSeconds} />
                  </TabsContent>
                </Tabs>
              ) : (
                <SecurityTab {...tabProps} bucketSeconds={bucketSeconds} />
              )}
            </TabsContent>
          </Tabs>
        )
      }}
    </ReportLayout>
  )
}
