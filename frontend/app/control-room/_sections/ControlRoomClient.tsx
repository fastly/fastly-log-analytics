'use client'

import { useState, useEffect, useMemo } from 'react'
import { Radio, ArrowRight } from 'lucide-react'
import Link from 'next/link'
import { ReportShell } from '@/components/ReportShell'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/serviceStore'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { useTimezone } from '@/hooks/useTimezone'
import { formatDate } from '@/lib/date'

// ---------------------------------------------------------------------------
// Tab definitions
// ---------------------------------------------------------------------------

const TABS = [
  { id: 'overview', label: 'Overview', historicalHref: '/dashboard' },
  { id: 'performance', label: 'Performance', historicalHref: '/performance' },
  { id: 'origin', label: 'Origin', historicalHref: '/origin' },
  { id: 'security', label: 'Security', historicalHref: '/security' },
  { id: 'network', label: 'Network', historicalHref: '/network' },
  { id: 'sessions', label: 'Sessions', historicalHref: '/sessions' },
  { id: 'cost', label: 'Cost', historicalHref: '/usage' },
  { id: 'insights', label: 'Insights', historicalHref: '/insights' },
  { id: 'admin', label: 'Admin', historicalHref: null, adminOnly: true },
] as const

type TabId = (typeof TABS)[number]['id']

// ---------------------------------------------------------------------------
// useRealtimeStream - wraps useSSE for the realtime endpoint
// ---------------------------------------------------------------------------

interface RealtimeStreamState {
  connected: boolean
  lastTickTime: Date | null
  data: SSELine[]
}

function useRealtimeStream(): RealtimeStreamState {
  const { lines, status, start, stop } = useSSE()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)

  useEffect(() => {
    if (!activeServiceId) return
    start(`/api/services/${activeServiceId}/realtime-stream`)
    return () => {
      stop()
    }
  }, [activeServiceId, start, stop])

  const lastTickTime = useMemo(() => {
    if (lines.length === 0) return null
    const latest = lines[lines.length - 1]
    return latest?.event === 'metrics_tick' ? new Date() : null
  }, [lines])

  return {
    connected: status === 'streaming',
    lastTickTime,
    data: lines,
  }
}

// ---------------------------------------------------------------------------
// Freshness helpers
// ---------------------------------------------------------------------------

type FreshnessLevel = 'fresh' | 'stale' | 'expired'

function getFreshnessLevel(lastTick: Date | null): FreshnessLevel {
  if (!lastTick) return 'expired'
  const ageMs = Date.now() - lastTick.getTime()
  if (ageMs < 10_000) return 'fresh'
  if (ageMs < 30_000) return 'stale'
  return 'expired'
}

function freshnessVariant(level: FreshnessLevel) {
  switch (level) {
    case 'fresh':
      return 'success' as const
    case 'stale':
      return 'warning' as const
    case 'expired':
      return 'destructive' as const
  }
}

// ---------------------------------------------------------------------------
// Status indicators
// ---------------------------------------------------------------------------

function ConnectionBadge({ connected }: { connected: boolean }) {
  return (
    <Badge variant={connected ? 'success' : 'destructive'} className="gap-1.5">
      <span
        className={`inline-block h-2 w-2 rounded-full ${
          connected ? 'bg-emerald-500 dark:bg-emerald-400' : 'bg-red-500 dark:bg-red-400'
        }`}
      />
      {connected ? 'Connected' : 'Disconnected'}
    </Badge>
  )
}

function FreshnessBadge({ lastTickTime, timezone }: { lastTickTime: Date | null; timezone: string }) {
  const [, setTick] = useState(0)

  // Force re-render every second to keep the age display current
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1_000)
    return () => clearInterval(id)
  }, [])

  const level = getFreshnessLevel(lastTickTime)
  const label = lastTickTime
    ? `Fresh as of ${formatDate(lastTickTime, timezone, 'h:mm:ss a')}`
    : 'No data yet'

  return (
    <Badge variant={freshnessVariant(level)} className="gap-1.5">
      {label}
    </Badge>
  )
}

// ---------------------------------------------------------------------------
// Placeholder tab content
// ---------------------------------------------------------------------------

function TabPlaceholder({
  label,
  historicalHref,
}: {
  label: string
  historicalHref: string | null
}) {
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
      <Card>
        <CardHeader>
          <CardTitle>{label} - Requests/s</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-sm">
            Real-time request rate will appear here once the backend stream is connected.
          </p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{label} - Error Rate</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-sm">
            Live error rate metrics will render here in Phase 1.
          </p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{label} - Latency</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-sm">
            Latency percentiles (p50/p95/p99) will populate in Phase 1.
          </p>
        </CardContent>
      </Card>
      {historicalHref && (
        <div className="md:col-span-2 lg:col-span-3 flex justify-end">
          <Link href={historicalHref}>
            <Button variant="outline" size="sm">
              View historical {label.toLowerCase()} data
              <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
            </Button>
          </Link>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Mobile overview (stripped-down for < 768px)
// ---------------------------------------------------------------------------

function MobileOverview({ connected, lastTickTime, timezone }: {
  connected: boolean
  lastTickTime: Date | null
  timezone: string
}) {
  return (
    <div className="space-y-4 md:hidden">
      <div className="flex items-center gap-2 flex-wrap">
        <ConnectionBadge connected={connected} />
        <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Live Overview</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-sm">
            Real-time service overview will appear here once the backend stream is connected.
          </p>
        </CardContent>
      </Card>
      <Link href="/dashboard">
        <Button variant="outline" size="sm" className="w-full">
          View full dashboard
          <ArrowRight className="ml-1.5 h-3.5 w-3.5" />
        </Button>
      </Link>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function ControlRoomClient() {
  const [activeTab, setActiveTab] = useState<TabId>('overview')
  const isAnalyst = useIsAnalyst()
  const timezone = useTimezone()
  const { connected, lastTickTime } = useRealtimeStream()

  const visibleTabs = useMemo(
    () => TABS.filter((tab) => !('adminOnly' in tab && tab.adminOnly) || !isAnalyst),
    [isAnalyst],
  )

  const headerActions = (
    <div className="flex items-center gap-2 flex-wrap">
      <ConnectionBadge connected={connected} />
      <FreshnessBadge lastTickTime={lastTickTime} timezone={timezone} />
    </div>
  )

  return (
    <ReportShell
      title="Control Room"
      icon={Radio}
      description="Real-time SRE & FinOps monitoring"
      headerActions={headerActions}
    >
      {/* Mobile: show stripped-down overview only */}
      <MobileOverview
        connected={connected}
        lastTickTime={lastTickTime}
        timezone={timezone}
      />

      {/* Desktop: full tabbed interface */}
      <div className="hidden md:block">
        <Tabs
          defaultValue="overview"
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as TabId)}
        >
          <TabsList className="mb-4 flex-wrap">
            {visibleTabs.map((tab) => (
              <TabsTrigger key={tab.id} value={tab.id}>
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>

          {visibleTabs.map((tab) => (
            <TabsContent key={tab.id} value={tab.id}>
              <TabPlaceholder
                label={tab.label}
                historicalHref={tab.historicalHref}
              />
            </TabsContent>
          ))}
        </Tabs>
      </div>
    </ReportShell>
  )
}
