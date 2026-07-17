'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle
} from "@/components/ui/card"
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import {
  Database,
  Layers,
  FileCode,
  Clock,
  Info,
  Archive
} from 'lucide-react'
import { formatBytes } from '@/lib/format'
import { formatRelative } from '@/lib/date'
import { useDateFormat } from '@/hooks/useDateFormat'
import { Button } from '@/components/ui/button'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export function IcebergStatus({ accessLevel }: { accessLevel?: string }) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { full, abbr } = useDateFormat()

  // Freshness is driven by useAdminEventStream invalidating
  // ['admin', 'iceberg'] when an iceberg-mutating cron task finishes.
  // The 5-min interval is a pure safety net (silently-dropped stream,
  // missed wakeup) — same shape as useSyncStatus/useLastSync.
  const { data: info, isLoading, error } = useQuery({
    queryKey: ['admin', 'iceberg', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/admin/iceberg-info")
      return data as any
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  if (!activeServiceId) return null

  if (error) {
    return (
      <Card className="border-destructive/20 bg-destructive/5">
        <CardContent className="pt-6">
          <div className="flex items-center gap-2 text-destructive">
            <Info className="h-4 w-4" />
            <span className="text-sm font-medium">Failed to load Iceberg table info</span>
          </div>
          <p className="text-xs text-muted-foreground mt-1">{(error as any)?.message || 'Unknown error'}</p>
        </CardContent>
      </Card>
    )
  }

  const stats = [
    {
      label: 'Snapshots',
      value: info?.snapshots ?? 0,
      icon: Layers,
      tooltip: 'Total number of atomic commits in the Iceberg table history.'
    },
    {
      label: 'Data Files',
      value: info?.data_files ?? 0,
      icon: FileCode,
      tooltip: 'Number of Parquet data files currently in the Iceberg table.'
    },
    {
      label: 'Total Size',
      value: formatBytes(info?.size_bytes ?? 0),
      icon: Database,
      tooltip: 'Cumulative size of all data files in the Iceberg table.'
    },
    {
      label: 'Latest Commit',
      value: info?.latest_snapshot_at ? formatRelative(info.latest_snapshot_at) : 'Never',
      icon: Clock,
      tooltip: info?.latest_snapshot_at ? `${full(info.latest_snapshot_at)} ${abbr()}` : 'No snapshots yet.'
    }
  ]

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <Card key={stat.label} className="shadow-none border-muted/60">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                {stat.label}
                <TooltipProvider>
                  <Tooltip>
                    {/* A-8 (a11y, WCAG 2.1.1): Button (not span) so keyboard
                        users can tab to the info icon. */}
                    <TooltipTrigger
                      render={
                        <Button
                          variant="ghost"
                          size="icon-xs"
                          aria-label={`More info: ${stat.label}`}
                          className="flex items-center opacity-40 hover:opacity-100"
                        />
                      }
                    >
                      <Info className="h-3 w-3 " />
                    </TooltipTrigger>
                    <TooltipContent className="text-[10px] max-w-[150px]">
                      {stat.tooltip}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </CardTitle>
              <stat.icon className="h-3.5 w-3.5 text-muted-foreground/60" />
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-7 w-20" />
              ) : (
                <TooltipProvider>
                  <Tooltip>
                    {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so
                        keyboard users can focus the value and reveal the
                        explanatory tooltip. */}
                    <TooltipTrigger render={
                      <div
                        className="text-xl font-mono font-bold tracking-tight rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        tabIndex={0}
                        role="button"
                        aria-label={`${stat.label}: ${stat.value}`}
                      >
                        {stat.value}
                      </div>
                    } />
                    <TooltipContent className="text-xs">
                      {stat.tooltip}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      {accessLevel !== 'read_only' && (
        <Card className="shadow-none border-muted/60 bg-muted/5">
          <CardContent className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/10 rounded-full">
                <Archive className="h-4 w-4 text-blue-500" />
              </div>
              <div>
                <h4 className="text-sm font-semibold">Local Buffer Status</h4>
                <p className="text-xs text-muted-foreground">
                  Data ingested but not yet committed to the Iceberg table.
                </p>
              </div>
            </div>

            <div className="flex items-center gap-6">
              <div className="flex flex-col">
                <span className="text-[10px] font-bold uppercase text-muted-foreground">Pending Files</span>
                {isLoading ? (
                  <Skeleton className="h-5 w-12 mt-1" />
                ) : (
                  <span className="text-sm font-mono font-bold">
                    {info?.buffer_files ?? 0}
                  </span>
                )}
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] font-bold uppercase text-muted-foreground">Buffer Size</span>
                {isLoading ? (
                  <Skeleton className="h-5 w-16 mt-1" />
                ) : (
                  <span className="text-sm font-mono font-bold">
                    {formatBytes(info?.buffer_size_bytes ?? 0)}
                  </span>
                )}
              </div>
              <div className="flex items-center gap-2 ml-4">
                 {info?.buffer_files ? info.buffer_files > 0 && (
                    <Badge variant="outline" className="h-6 px-2 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 animate-pulse shadow-none uppercase font-bold">
                      Pending Commit
                    </Badge>
                 ) : null}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {info?.table_location && (        <div className="px-4 py-2 bg-muted/30 rounded-md border border-dashed flex items-center justify-between">
            <div className="flex items-center gap-2 overflow-hidden">
                <Database className="h-3 w-3 text-muted-foreground shrink-0" />
                <span className="text-[10px] text-muted-foreground font-mono truncate">
                    {info.table_location}
                </span>
            </div>
            <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest ml-4 shrink-0">
                Hadoop Catalog (Fastly Object Storage {info.region ? `- ${info.region}` : ''})
            </span>
        </div>
      )}
    </div>
  )
}
