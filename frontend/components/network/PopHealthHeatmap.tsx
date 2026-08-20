'use client'

import React from 'react'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { client, extractApiError } from '@/lib/api'
import { formatBytes, formatCompactCount } from '@/lib/format'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Activity, ShieldAlert, Globe, Wifi, Zap, Percent, HardDrive, AlertTriangle } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'

export interface PopHealthItem {
  pop: string
  requests: number
  errors: number
  error_rate: number
  p50_rtt_us: number | null
  p95_ttfb_ms: number | null
  cache_hit_rate: number
  bandwidth_bytes: number
}

interface PopHealthHeatmapProps {
  serviceId: string
  startTime: string | null
  endTime: string | null
}

export function PopHealthHeatmap({ serviceId, startTime, endTime }: PopHealthHeatmapProps) {
  const [selectedPop, setSelectedPop] = React.useState<PopHealthItem | null>(null)

  // Query PoP Health data using the service-query hook
  const popHealthQuery = useServiceQuery<PopHealthItem[]>(
    ['network', 'pop-health', serviceId, startTime, endTime],
    async ({ signal }) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { data } = await client.GET('/api/network/pop-health' as any, {
        signal,
        params: {
          query: {
            start_time: startTime ?? undefined,
            end_time: endTime ?? undefined,
          },
        },
      })
      return data?.data ?? []
    }
  )

  const items = React.useMemo(() => popHealthQuery.data ?? [], [popHealthQuery.data])
  const isLoading = popHealthQuery.isLoading || (popHealthQuery.isFetching && !popHealthQuery.data)
  const error = popHealthQuery.error

  // Metrics aggregates
  const totalRequests = React.useMemo(() => items.reduce((acc, curr) => acc + curr.requests, 0), [items])
  const totalBandwidth = React.useMemo(() => items.reduce((acc, curr) => acc + curr.bandwidth_bytes, 0), [items])
  const activePopsCount = items.length

  if (isLoading) {
    return (
      <Card className="shadow-md">
        <CardHeader>
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="space-y-1">
              <CardTitle className="text-lg font-bold flex items-center gap-2">
                <Globe className="h-5 w-5 text-muted-foreground animate-spin" />
                Fastly Edge PoP Health Heatmap
              </CardTitle>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-3">
            {Array.from({ length: 16 }).map((_, i) => (
              // eslint-disable-next-line react/no-array-index-key
              <div key={i} className="h-20 rounded-lg bg-muted/40 animate-pulse border border-muted/50" />
            ))}
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card className="shadow-md border-destructive/20 bg-destructive/5">
        <CardHeader>
          <CardTitle className="text-lg font-bold text-destructive flex items-center gap-2">
            <AlertTriangle className="h-5 w-5" />
            Edge PoP Health Error
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm font-medium text-destructive/90">
          {extractApiError(error)}
        </CardContent>
      </Card>
    )
  }

  if (items.length === 0) {
    return (
      <Card className="shadow-md">
        <CardHeader>
          <CardTitle className="text-lg font-bold flex items-center gap-2">
            <Globe className="h-5 w-5 text-muted-foreground" />
            Fastly Edge PoP Health Heatmap
          </CardTitle>
        </CardHeader>
        <CardContent className="h-40 flex flex-col items-center justify-center gap-2 border border-dashed rounded-lg m-6 bg-muted/10">
          <p className="text-muted-foreground font-semibold text-sm">No Edge PoP data recorded in this timeframe.</p>
          <p className="text-[11px] text-muted-foreground/80">Edge PoP analysis depends on core Geolocation Basic metrics.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className="shadow-md transition-all duration-300">
      <CardHeader className="border-b bg-muted/15">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="space-y-1">
            <CardTitle className="text-lg font-bold flex items-center gap-2">
              <Globe className="h-5 w-5 text-primary" />
              Fastly Edge PoP Health Heatmap
            </CardTitle>
            <p className="text-xs text-muted-foreground font-medium">
              Real-time monitoring of traffic distribution, cache efficiency, and edge metrics per Fastly PoP.
            </p>
          </div>
          <div className="flex items-center gap-2 font-mono text-[11px] font-semibold text-muted-foreground bg-muted/50 px-2.5 py-1 rounded-md">
            <span>{activePopsCount} Active PoPs</span>
            <span className="text-muted-foreground/30">•</span>
            <span>{formatCompactCount(totalRequests)} Total Reqs</span>
            <span className="text-muted-foreground/30">•</span>
            <span>{formatBytes(totalBandwidth)} Bandwidth</span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="pt-6">
        <TooltipProvider delay={100}>
          <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-3">
            {items.map((pop) => {
              const hasHighErrors = pop.error_rate > 5.0
              const hasHighLatency = !!(pop.p95_ttfb_ms && pop.p95_ttfb_ms > 400)
              const isCritical = pop.error_rate > 15.0

              let borderClass = 'border-green-500/20 dark:border-green-400/10'
              let bgClass = 'bg-green-500/5 hover:bg-green-500/10 text-green-700 dark:text-green-400'
              let dotClass = 'bg-green-500'

              if (hasHighErrors || hasHighLatency) {
                borderClass = 'border-amber-500/30 dark:border-amber-400/20'
                bgClass = 'bg-amber-500/5 hover:bg-amber-500/10 text-amber-700 dark:text-amber-400'
                dotClass = 'bg-amber-500'
              }

              if (isCritical) {
                borderClass = 'border-red-500/30 dark:border-red-400/20'
                bgClass = 'bg-red-500/5 hover:bg-red-500/10 text-red-700 dark:text-red-400'
                dotClass = 'bg-red-500'
              }

              return (
                <Tooltip key={pop.pop}>
                  <TooltipTrigger asChild>
                    <button
                      type="button"
                      onClick={() => setSelectedPop(pop)}
                      className={cn(
                        'p-3 rounded-lg border flex flex-col items-center justify-between gap-1 text-center w-full bg-transparent font-normal',
                        'hover:scale-[1.03] active:scale-[0.98] transition-all duration-200 cursor-pointer select-none shadow-sm',
                        borderClass,
                        bgClass
                      )}
                    >
                      <div className="flex items-center gap-1.5">
                        <span className={cn('h-2 w-2 rounded-full animate-pulse', dotClass)} />
                        <span className="font-extrabold text-sm tracking-widest text-foreground font-mono">
                          {pop.pop.toUpperCase()}
                        </span>
                      </div>
                      <div className="w-full flex flex-col items-center mt-1">
                        <span className="text-[10px] text-muted-foreground/90 font-medium font-mono tabular-nums">
                          {formatCompactCount(pop.requests)} reqs
                        </span>
                        <span className="text-[10px] font-semibold text-foreground/80 font-mono mt-0.5">
                          {pop.cache_hit_rate.toFixed(1)}% Hit
                        </span>
                      </div>
                    </button>
                  </TooltipTrigger>
                  <TooltipContent className="p-0 border border-muted bg-popover text-popover-foreground shadow-xl rounded-lg w-64 overflow-hidden">
                    <div className="bg-muted/50 border-b border-muted px-3 py-2 flex items-center justify-between">
                      <div className="flex items-center gap-1.5">
                        <span className={cn('h-2 w-2 rounded-full', dotClass)} />
                        <span className="font-bold text-sm tracking-wider uppercase font-mono">{pop.pop}</span>
                      </div>
                      <Badge variant="outline" className="text-[10px] py-0 px-1.5 uppercase bg-background font-semibold">
                        {isCritical ? 'Critical' : (hasHighErrors || hasHighLatency) ? 'Degraded' : 'Healthy'}
                      </Badge>
                    </div>
                    <div className="p-3 text-xs space-y-2">
                      <div className="grid grid-cols-2 gap-y-1.5 gap-x-3">
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <Activity className="h-3 w-3" /> Requests
                          </span>
                          <span className="font-bold text-foreground font-mono mt-0.5">
                            {pop.requests.toLocaleString()}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <Percent className="h-3 w-3" /> Cache Hit Rate
                          </span>
                          <span className="font-bold text-foreground font-mono mt-0.5">
                            {pop.cache_hit_rate.toFixed(2)}%
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <ShieldAlert className="h-3 w-3" /> Error Rate
                          </span>
                          <span className={cn('font-bold font-mono mt-0.5', pop.error_rate > 0 ? 'text-destructive' : 'text-foreground')}>
                            {pop.error_rate.toFixed(2)}%
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <HardDrive className="h-3 w-3" /> Bandwidth
                          </span>
                          <span className="font-bold text-foreground font-mono mt-0.5">
                            {formatBytes(pop.bandwidth_bytes)}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <Wifi className="h-3 w-3" /> P50 TCP RTT
                          </span>
                          <span className="font-bold text-foreground font-mono mt-0.5">
                            {pop.p50_rtt_us ? `${(pop.p50_rtt_us / 1000).toFixed(1)} ms` : '—'}
                          </span>
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <Zap className="h-3 w-3" /> P95 TTFB
                          </span>
                          <span className="font-bold text-foreground font-mono mt-0.5">
                            {pop.p95_ttfb_ms ? `${pop.p95_ttfb_ms.toFixed(1)} ms` : '—'}
                          </span>
                        </div>
                      </div>
                    </div>
                  </TooltipContent>
                </Tooltip>
              )
            })}
          </div>
        </TooltipProvider>

        <Dialog open={!!selectedPop} onOpenChange={(open) => !open && setSelectedPop(null)}>
          <DialogContent className="sm:max-w-xl max-h-[90vh] overflow-y-auto">
            {selectedPop && (
              <>
                <DialogHeader className="border-b pb-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span
                        className={cn(
                          'h-2.5 w-2.5 rounded-full animate-pulse',
                          selectedPop.error_rate > 5 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 800)
                            ? 'bg-red-500'
                            : selectedPop.error_rate > 2 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 400)
                            ? 'bg-amber-500'
                            : 'bg-green-500'
                        )}
                      />
                      <DialogTitle className="text-xl font-mono font-black tracking-widest uppercase">
                        {selectedPop.pop.toUpperCase()} Metrics
                      </DialogTitle>
                    </div>
                    <Badge
                      variant="outline"
                      className={cn(
                        'font-bold py-0.5 px-2.5 uppercase font-sans text-xs',
                        selectedPop.error_rate > 5 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 800)
                          ? 'border-red-500/30 bg-red-500/5 text-red-500'
                          : selectedPop.error_rate > 2 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 400)
                          ? 'border-amber-500/30 bg-amber-500/5 text-amber-500'
                          : 'border-green-500/30 bg-green-500/5 text-green-500'
                      )}
                    >
                      {selectedPop.error_rate > 5 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 800)
                        ? 'Critical'
                        : selectedPop.error_rate > 2 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 400)
                        ? 'Degraded'
                        : 'Healthy'}
                    </Badge>
                  </div>
                  <DialogDescription className="text-xs text-muted-foreground font-sans mt-1">
                    Granular edge telemetry and diagnostic health assessments compiled for Point-of-Presence <strong className="font-mono">{selectedPop.pop}</strong>.
                  </DialogDescription>
                </DialogHeader>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 py-5 font-sans">
                  {/* Total Requests Panel */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <Activity className="h-3.5 w-3.5 text-primary" /> Requests & Volume
                    </span>
                    <div className="mt-2.5">
                      <div className="text-xl font-mono font-black tracking-tight text-foreground">
                        {selectedPop.requests.toLocaleString()}
                      </div>
                      <span className="text-[10px] text-muted-foreground/90 font-medium block mt-1">
                        Routes <strong className="font-mono text-foreground">{((selectedPop.requests / (totalRequests || 1)) * 100).toFixed(2)}%</strong> of total edge traffic.
                      </span>
                    </div>
                  </div>

                  {/* Cache Efficiency Panel */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <Percent className="h-3.5 w-3.5 text-primary" /> Cache Efficiency
                    </span>
                    <div className="mt-2.5">
                      <div className="text-xl font-mono font-black tracking-tight text-foreground">
                        {selectedPop.cache_hit_rate.toFixed(2)}%
                      </div>
                      <div className="text-[10px] text-muted-foreground/90 font-medium mt-1 space-y-0.5 font-mono">
                        <div>Hits: {Math.round(selectedPop.requests * selectedPop.cache_hit_rate / 100).toLocaleString()}</div>
                        <div>Misses: {Math.round(selectedPop.requests * (100 - selectedPop.cache_hit_rate) / 100).toLocaleString()}</div>
                      </div>
                    </div>
                  </div>

                  {/* Bandwidth Egress Panel */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <HardDrive className="h-3.5 w-3.5 text-primary" /> Bandwidth Egress
                    </span>
                    <div className="mt-2.5">
                      <div className="text-xl font-mono font-black tracking-tight text-foreground">
                        {formatBytes(selectedPop.bandwidth_bytes)}
                      </div>
                      <span className="text-[10px] text-muted-foreground/90 font-medium block mt-1">
                        Accounts for <strong className="font-mono text-foreground">{((selectedPop.bandwidth_bytes / (totalBandwidth || 1)) * 100).toFixed(2)}%</strong> of payload delivery.
                      </span>
                    </div>
                  </div>

                  {/* Error Rate Panel */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <ShieldAlert className="h-3.5 w-3.5 text-primary" /> Edge Error Rate
                    </span>
                    <div className="mt-2.5">
                      <div
                        className={cn(
                          'text-xl font-mono font-black tracking-tight',
                          selectedPop.error_rate > 2 ? 'text-destructive' : 'text-foreground'
                        )}
                      >
                        {selectedPop.error_rate.toFixed(2)}%
                      </div>
                      <span className="text-[10px] text-muted-foreground/90 font-medium block mt-1">
                        Total Failed Reqs: <strong className="font-mono text-foreground">{Math.round(selectedPop.requests * selectedPop.error_rate / 100).toLocaleString()}</strong>
                      </span>
                    </div>
                  </div>

                  {/* TCP RTT Peer Quality */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <Wifi className="h-3.5 w-3.5 text-primary" /> p50 TCP Round-Trip (RTT)
                    </span>
                    <div className="mt-2.5">
                      <div className="text-xl font-mono font-black tracking-tight text-foreground">
                        {selectedPop.p50_rtt_us ? `${(selectedPop.p50_rtt_us / 1000).toFixed(1)} ms` : '—'}
                      </div>
                      <span className="text-[10px] text-muted-foreground/90 font-medium block mt-1">
                        {selectedPop.p50_rtt_us && selectedPop.p50_rtt_us < 50000
                          ? '⚡ Excellent, optimized regional peering'
                          : selectedPop.p50_rtt_us && selectedPop.p50_rtt_us < 150000
                          ? '👍 Satisfactory edge-to-client transit'
                          : selectedPop.p50_rtt_us
                          ? '⚠️ High latency packet transit'
                          : '— No RTT telemetry recorded'}
                      </span>
                    </div>
                  </div>

                  {/* p95 TTFB Edge Processing */}
                  <div className="p-3.5 rounded-lg border bg-muted/20 flex flex-col justify-between">
                    <span className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
                      <Zap className="h-3.5 w-3.5 text-primary" /> p95 Time to First Byte (TTFB)
                    </span>
                    <div className="mt-2.5">
                      <div className="text-xl font-mono font-black tracking-tight text-foreground">
                        {selectedPop.p95_ttfb_ms ? `${selectedPop.p95_ttfb_ms.toFixed(1)} ms` : '—'}
                      </div>
                      <span className="text-[10px] text-muted-foreground/90 font-medium block mt-1">
                        {selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms < 200
                          ? '⚡ Fast edge cache resolution'
                          : selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms < 400
                          ? '👍 Standard multi-tier routing response'
                          : selectedPop.p95_ttfb_ms
                          ? '⚠️ Slow origin/shield routing lag'
                          : '— No TTFB telemetry recorded'}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Regional Edge Diagnosis */}
                <div className="mt-2 p-3.5 rounded-lg border bg-muted/10">
                  <span className="text-xs font-bold text-foreground block">
                    Regional Edge Diagnostic Health
                  </span>
                  <p className="text-xs text-muted-foreground mt-1.5 leading-relaxed">
                    {selectedPop.error_rate > 5 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 800) ? (
                      <span className="text-destructive font-semibold">
                        CRITICAL: High error rates or excessive latency detected. This POP is experiencing degraded local connectivity or origin shield failures. Prompt diagnostic investigation is recommended.
                      </span>
                    ) : selectedPop.error_rate > 2 || (selectedPop.p95_ttfb_ms && selectedPop.p95_ttfb_ms > 400) ? (
                      <span className="text-amber-500 font-semibold">
                        DEGRADED: Regional metrics indicate slow-downs. Handshake times or backend origin latency is higher than typical baseline averages. Continue to monitor alert channels.
                      </span>
                    ) : (
                      <span>
                        HEALTHY: This Point-of-Presence is operating fully within normal edge SLA thresholds. Regional cache hit ratio and peer transit latency are highly optimized.
                      </span>
                    )}
                  </p>
                </div>
              </>
            )}
          </DialogContent>
        </Dialog>
      </CardContent>
    </Card>
  )
}
