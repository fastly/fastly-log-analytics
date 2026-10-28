'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Archive,
  Clock,
  Database,
  FileCode,
  Info,
  Layers,
  Loader2,
} from 'lucide-react'
import { CardErrorState } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useSSE'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { cn, formatBytes, formatRelative } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import { MetadataStorageCard } from './MetadataStorageCard'

export { MetadataStorageCard }

export function StorageStatus() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()
  const { data: info, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.storage(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/storage-info')),
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  if (!activeServiceId) return null
  if (error) {
    return (
      <CardErrorState
        icon={<Info className="h-4 w-4" />}
        title="Failed to load DuckLake table info"
        message={error?.message || 'Unknown error'}
      />
    )
  }

  const stats = [
    {
      label: 'Snapshots',
      value: info?.snapshots ?? 0,
      icon: Layers,
      tooltip: 'Total number of atomic commits in the DuckLake table history.',
    },
    {
      label: 'Data Files',
      value: info?.data_files ?? 0,
      icon: FileCode,
      tooltip: 'Number of Parquet data files currently in the DuckLake table.',
    },
    {
      label: 'Total Size',
      value: formatBytes(info?.size_bytes ?? 0),
      icon: Database,
      tooltip: 'Cumulative size of all data files in the DuckLake table.',
    },
    {
      label: 'Latest Commit',
      value: info?.latest_snapshot_at ? formatRelative(info.latest_snapshot_at) : 'Never',
      icon: Clock,
      tooltip: info?.latest_snapshot_at
        ? `${full(info.latest_snapshot_at)} ${abbr()}`
        : 'No snapshots yet.',
    },
  ]

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <Card key={stat.label} className="shadow-none border-muted/60">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                <LabelWithInfo
                  label={stat.label}
                  info={stat.tooltip}
                  className="gap-1.5"
                  labelClassName="text-[10px] font-bold uppercase tracking-wider text-muted-foreground"
                />
              </CardTitle>
              <stat.icon className="h-3.5 w-3.5 text-muted-foreground/60" />
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-7 w-20" />
              ) : (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <div
                          className="text-xl font-mono font-bold tracking-tight rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label={`${stat.label}: ${stat.value}`}
                        >
                          {stat.value}
                        </div>
                      }
                    />
                    <TooltipContent className="text-xs">{stat.tooltip}</TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
      <Card className="shadow-none border-muted/60 bg-muted/5">
        <CardContent className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-500/10 rounded-full">
              <Archive className="h-4 w-4 text-blue-500" />
            </div>
            <div>
              <h4 className="text-sm font-semibold">Local Buffer Status</h4>
              <p className="text-xs text-muted-foreground">
                Data ingested but not yet committed to the DuckLake table.
              </p>
            </div>
          </div>
          <div className="flex items-center gap-6">
            {(
              [
                ['Pending Files', info?.buffer_files ?? 0, 'w-12'],
                ['Buffer Size', formatBytes(info?.buffer_size_bytes ?? 0), 'w-16'],
              ] as const
            ).map(([lbl, val, wCls]) => (
              <div key={lbl} className="flex flex-col">
                <span className="text-[10px] font-bold uppercase text-muted-foreground">
                  {lbl}
                </span>
                {isLoading ? (
                  <Skeleton className={`h-5 ${wCls} mt-1`} />
                ) : (
                  <span className="text-sm font-mono font-bold">{val}</span>
                )}
              </div>
            ))}
            <div className="flex items-center gap-2 ml-4">
              {info?.buffer_files && info.buffer_files > 0 ? (
                <Badge
                  variant="outline"
                  className="h-6 px-2 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 animate-pulse shadow-none uppercase font-bold"
                >
                  Pending Commit
                </Badge>
              ) : null}
            </div>
          </div>
        </CardContent>
      </Card>
      {info?.table_location && (
        <div className="px-4 py-2 bg-muted/30 rounded-md border border-dashed flex items-center justify-between">
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

export function StorageCalendar() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { data: calendar, isLoading } = useQuery({
    queryKey: queryKeys.admin.storageCalendar(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/storage-calendar')),
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  const days = React.useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    const pad = (n: number) => n.toString().padStart(2, '0')
    return Array.from({ length: 90 }, (_, i) => {
      const d = new Date(today)
      d.setDate(d.getDate() - i)
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
    }).reverse()
  }, [])

  if (!activeServiceId) return null

  return (
    <Card className="shadow-none border-muted/60">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">DuckLake Storage Distribution</CardTitle>
        <CardDescription className="text-xs">
          Physical data file counts and sizes partitioned by day.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground animate-pulse">
            <Loader2 className="h-3 w-3 animate-spin" /> Loading distribution data...
          </div>
        ) : (
          <div className="flex flex-wrap gap-1">
            {days.map((date) => {
              const entry = calendar?.[date]
              const dayData = Array.isArray(entry) ? undefined : entry
              const hasData = !!dayData
              return (
                <TooltipProvider key={date}>
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <span
                          className="inline-block rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label={
                            hasData
                              ? `${date}: ${dayData.data_files} files, ${formatBytes(dayData.size_bytes)}`
                              : `${date}: no data`
                          }
                        />
                      }
                    >
                      <div
                        className={cn(
                          'h-4 w-4 rounded-sm transition-colors border',
                          hasData
                            ? 'bg-blue-500 border-blue-600/20 hover:bg-blue-400'
                            : 'bg-muted/30 border-border/50 hover:bg-muted/50',
                        )}
                      />
                    </TooltipTrigger>
                    <TooltipContent className="text-xs max-w-[200px] p-2 space-y-1">
                      <div className="font-bold border-b border-muted/20 pb-1 mb-1">{date}</div>
                      {hasData ? (
                        <>
                          <div className="flex justify-between gap-4">
                            <span className="text-muted-foreground">Files:</span>
                            <span className="font-mono">{dayData.data_files}</span>
                          </div>
                          <div className="flex justify-between gap-4">
                            <span className="text-muted-foreground">Size:</span>
                            <span className="font-mono">{formatBytes(dayData.size_bytes)}</span>
                          </div>
                        </>
                      ) : (
                        <div className="text-muted-foreground italic">No data files</div>
                      )}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )
            })}
          </div>
        )}
        <div className="mt-4 flex items-center gap-4 text-[10px] text-muted-foreground uppercase font-bold tracking-wider">
          <div className="flex items-center gap-1.5">
            <div className="h-2 w-2 rounded-sm bg-blue-500" />
            <span>Committed Data</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="h-2 w-2 rounded-sm bg-muted/30 border" />
            <span>No Data</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
