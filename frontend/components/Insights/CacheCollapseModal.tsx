'use client'

import React, { useMemo } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter
} from '@/components/ui/dialog'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { useDateFormat } from '@/hooks/useDateFormat'
import { AlertCircle, Loader2, Clock, Activity, ShieldAlert, FileText, CheckCircle2 } from 'lucide-react'
import { cn } from '@/lib/utils'

interface CacheCollapseModalProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  url: string | null
  windowHours: string
  baselineHours: string
}

export function CacheCollapseModal({
  isOpen,
  onOpenChange,
  url,
  windowHours,
  baselineHours,
}: CacheCollapseModalProps) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()

  const { data, isLoading, error } = useQuery({
    queryKey: ['insights', 'cache-collapse-detail', activeServiceId, url, windowHours, baselineHours],
    queryFn: async ({ signal }) => {
      const res = await client.POST('/api/insights/cache-collapse-detail', {
        signal,
        body: {
          url: url!,
          window_size_hrs: parseFloat(windowHours),
          baseline_hours: parseFloat(baselineHours),
        },
      })
      if (res.error) {
        throw res.error
      }
      return res.data
    },
    enabled: isOpen && !!url && !!activeServiceId,
    staleTime: 30000,
  })

  const errorMsg = error ? extractApiError(error) : null

  // 1. Calculate high-fidelity stats aggregates
  const stats = useMemo(() => {
    if (!data) return null

    const totalRequests = data.timeline.reduce((acc, curr) => acc + (curr.total_requests || 0), 0)
    const realHits = data.timeline.reduce((acc, curr) => acc + (curr.real_hits || 0), 0)

    return {
      totalRequests,
      realHits,
      baselineHitRate: data.baseline_hit_rate,
      windowHitRate: data.window_hit_rate,
    }
  }, [data])

  // 2. Prepare plot traces
  const chartData = useMemo(() => {
    if (!data?.timeline) return []
    return [
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.expected_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Expected HITs',
        line: { color: '#3b82f6', width: 2, dash: 'dash' },
      },
      {
        x: data.timeline.map((p) => p.bucket),
        y: data.timeline.map((p) => p.real_hits),
        type: 'scatter',
        mode: 'lines',
        name: 'Real HITs',
        line: { color: '#ef4444', width: 2 },
      },
    ]
  }, [data])

  const chartLayout = useMemo(() => ({
    yaxis: { title: { text: 'Hits' } },
  }), [])

  // Helper for HTTP status color badge
  const getStatusBadgeClass = (status: number | null | undefined) => {
    if (!status) return 'bg-muted text-muted-foreground'
    if (status >= 200 && status < 300) return 'bg-green-500/10 text-green-500 border-green-500/20'
    if (status >= 300 && status < 400) return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'
    return 'bg-red-500/10 text-red-500 border-red-500/20'
  }

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] flex flex-col p-6">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            <ShieldAlert className="h-5 w-5 text-red-500 shrink-0" />
            <span className="truncate max-w-[90%]">Cache Analysis: {url}</span>
          </DialogTitle>
          <DialogDescription>
            Cacheable hit ratio (HIT/(HIT+MISS), PASS excluded) and cacheability (PASS share) vs. baseline.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">
          {isLoading && (
            <div className="flex-1 flex flex-col items-center justify-center py-20 text-muted-foreground text-sm gap-2">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <span>Analyzing historical logs and calculating cache timelines…</span>
            </div>
          )}

          {errorMsg && (
            <div className="flex-1 flex flex-col items-center justify-center py-20 text-center px-4">
              <AlertCircle className="h-10 w-10 text-destructive mb-3" />
              <h3 className="text-sm font-medium text-destructive">Failed to load collapse details</h3>
              <p className="text-xs text-muted-foreground mt-1 max-w-md">{errorMsg}</p>
            </div>
          )}

          {!isLoading && !errorMsg && data && stats && (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
                    <Clock className="h-3 w-3" /> Baseline Hit Ratio
                  </div>
                  <div className="mt-1.5 font-mono text-xl font-bold text-blue-500">
                    {stats.baselineHitRate.toFixed(2)}%
                  </div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">cacheable (HIT/(HIT+MISS))</div>
                </div>

                <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
                    <ShieldAlert className="h-3 w-3 text-red-400" /> Recent Hit Ratio
                  </div>
                  <div className="mt-1.5 font-mono text-xl font-bold text-red-500">
                    {stats.windowHitRate.toFixed(2)}%
                  </div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">cacheable (HIT/(HIT+MISS))</div>
                </div>

                <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
                    <Activity className="h-3 w-3" /> Total Requests
                  </div>
                  <div className="mt-1.5 font-mono text-xl font-bold text-foreground">
                    {stats.totalRequests.toLocaleString()}
                  </div>
                </div>

                <div className="p-3 rounded-lg bg-muted/40 border border-border flex flex-col justify-between">
                  <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex items-center gap-1">
                    <CheckCircle2 className="h-3 w-3 text-emerald-500" /> Total Cache Hits
                  </div>
                  <div className="mt-1.5 font-mono text-xl font-bold text-foreground">
                    {stats.realHits.toLocaleString()}
                  </div>
                </div>
              </div>

              {/* Recent-window cache disposition breakdown — tells apart a true
                  hit-ratio collapse (misses) from a cacheability regression (PASS). */}
              <div className="rounded-lg bg-muted/10 border border-border p-4">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                    <Activity className="h-3.5 w-3.5" /> Recent Window Breakdown
                  </h4>
                  <span className="text-[11px] text-muted-foreground">
                    PASS (uncacheable): {data.baseline_pass_rate.toFixed(1)}% → <span className="font-semibold text-amber-500">{data.window_pass_rate.toFixed(1)}%</span>
                  </span>
                </div>
                <div className="grid grid-cols-3 gap-3 text-center">
                  <div className="p-2 rounded-md bg-emerald-500/10 border border-emerald-500/20">
                    <div className="font-mono text-lg font-bold text-emerald-500">{data.breakdown.hits.toLocaleString()}</div>
                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Cacheable Hits</div>
                  </div>
                  <div className="p-2 rounded-md bg-red-500/10 border border-red-500/20">
                    <div className="font-mono text-lg font-bold text-red-500">{data.breakdown.misses.toLocaleString()}</div>
                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Misses</div>
                  </div>
                  <div className="p-2 rounded-md bg-amber-500/10 border border-amber-500/20">
                    <div className="font-mono text-lg font-bold text-amber-500">{data.breakdown.passes.toLocaleString()}</div>
                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Uncacheable (PASS)</div>
                  </div>
                </div>
              </div>

              <div className="border border-border rounded-lg bg-muted/10 p-4 flex flex-col min-h-[300px]">
                <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                  <Activity className="h-3.5 w-3.5" /> Hits Timeline (Expected vs. Real Hits)
                </h4>
                <div className="flex-1 w-full min-h-[220px]">
                  <TimeSeriesChart
                    data={chartData}
                    layout={chartLayout}
                    timezone="UTC"
                    height={220}
                  />
                </div>
              </div>

              {/* Recent cache misses (MISS only — cacheable requests that
                  weren't in cache; PASS is summarised in the breakdown above). */}
              <div className="flex flex-col min-h-[250px] border border-border rounded-lg bg-muted/10 p-4">
                <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
                  <FileText className="h-3.5 w-3.5" /> Recent Cache Misses (Max 50)
                </h4>
                {data.recent_misses.length === 0 ? (
                  <div className="flex-1 flex items-center justify-center text-xs text-muted-foreground italic">
                    No cache misses captured for this lookback.
                  </div>
                ) : (
                  <div className="flex-1 overflow-x-auto">
                    <table className="w-full text-xs text-left">
                      <thead>
                        <tr className="border-b border-border text-muted-foreground uppercase text-[10px] tracking-wider font-semibold">
                          <th className="py-2 pr-4">Timestamp ({abbr()})</th>
                          <th className="py-2 px-4">Cache Status</th>
                          <th className="py-2 px-4">POP</th>
                          <th className="py-2 px-4">Client IP</th>
                          <th className="py-2 pl-4 text-right">HTTP Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border/60">
                        {data.recent_misses.map((e) => (
                          <tr key={`${e.timestamp}-${e.ip || ''}-${e.cache}-${e.pop || ''}`} className="hover:bg-muted/30">
                            <td className="py-2 pr-4 font-mono font-medium whitespace-nowrap">
                              {full(e.timestamp)}
                            </td>
                            <td className="py-2 px-4 whitespace-nowrap">
                              <span className={cn(
                                "px-1.5 py-0.5 rounded text-[10px] font-bold border",
                                e.cache.includes("PASS") ? "bg-amber-500/10 text-amber-500 border-amber-500/20" :
                                e.cache.includes("MISS") ? "bg-red-500/10 text-red-500 border-red-500/20" :
                                "bg-muted text-muted-foreground border-border"
                              )}>
                                {e.cache}
                              </span>
                            </td>
                            <td className="py-2 px-4 font-mono whitespace-nowrap">
                              {e.pop || '—'}
                            </td>
                            <td className="py-2 px-4 font-mono text-muted-foreground whitespace-nowrap">
                              {e.ip || '—'}
                            </td>
                            <td className="py-2 pl-4 text-right whitespace-nowrap">
                              <span className={cn(
                                "px-1.5 py-0.5 rounded font-mono font-semibold border text-[10px]",
                                getStatusBadgeClass(e.status)
                              )}>
                                {e.status || '—'}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </>
          )}
        </div>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}
