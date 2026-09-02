'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription
} from "@/components/ui/card"
import { Loader2 } from 'lucide-react'
import { formatBytes } from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export function IcebergCalendar() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  // Freshness via useAdminEventStream invalidation (prefix-matches
  // ['admin', 'iceberg']). 5-min interval is a pure safety net.
  const { data: calendar, isLoading } = useQuery({
    queryKey: ['admin', 'iceberg', 'calendar', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/admin/iceberg-calendar")
      return data as any
    },
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  // Generate last 90 days
  const days = React.useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    return Array.from({ length: 90 }, (_, i) => {
      const d = new Date(today)
      d.setDate(d.getDate() - i)
      const pad = (n: number) => n.toString().padStart(2, '0')
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
          <>
            <div className="flex flex-wrap gap-1">
              {days.map((date) => {
                const dayData = (calendar as any)?.[date]
                const hasData = !!dayData

                return (
                  <TooltipProvider key={date}>
                    <Tooltip>
                      {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so
                          keyboard users can step through each day cell and
                          read its file/size tooltip. */}
                      <TooltipTrigger
                        render={
                          <span
                            className="inline-block rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                            tabIndex={0}
                            role="button"
                            aria-label={hasData
                              ? `${date}: ${dayData.data_files} files, ${formatBytes(dayData.size_bytes)}`
                              : `${date}: no data`}
                          />
                        }
                      >
                        <div
                          className={cn(
                            "h-4 w-4 rounded-sm transition-colors border ",
                            hasData
                              ? "bg-blue-500 border-blue-600/20 hover:bg-blue-400"
                              : "bg-muted/30 border-border/50 hover:bg-muted/50"
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
          </>
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
