'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { useDateFormat } from '@/hooks/useDateFormat'
import { formatTimeAgo } from '@/lib/date'
import { Badge } from '@/components/ui/badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export function SyncStatusBadge() {
  const { activeServiceId } = useServiceStore()
  const { full, abbr } = useDateFormat()

  const { data: status } = useQuery({
    queryKey: ['admin', 'status', activeServiceId],
    queryFn: async () => {
      const { data } = await client.GET("/api/sync-status", {
        params: { query: { skip_fos: true } },
      })
      return data
    },
    enabled: !!activeServiceId,
    refetchInterval: 15000 // Poll every 15s to keep status fresh
  })

  // 1s ticker so the "Xs ago" label advances between the 15s polls.
  const [, setTick] = React.useState(0)
  React.useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1000)
    return () => clearInterval(id)
  }, [])

  if (!activeServiceId || !status) return null

  const fileTs = status.latest_log_at || status.latest_available_file_at || status.latest_ingested_file_at

  return (
    <div className="hidden lg:flex items-center gap-2 mr-2 animate-in fade-in zoom-in-95">
      {status.local_rows != null && (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10 hover:bg-muted transition-colors">
          <strong className="text-foreground mr-1">Total Logs:</strong>
          {status.local_rows.toLocaleString()}
        </Badge>
      )}

      {fileTs ? (
        <Tooltip>
          <TooltipTrigger render={
            <Badge
              variant="secondary"
              className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10 hover:bg-muted transition-colors "
            >
              <strong className="text-foreground mr-1">Latest Log:</strong>
              {formatTimeAgo(fileTs)}
            </Badge>
          } />
          <TooltipContent className="text-xs">
            {full(fileTs)} {abbr()}
          </TooltipContent>
        </Tooltip>
      ) : (
        <Badge variant="secondary" className="px-2 py-0.5 shadow-none font-normal text-muted-foreground bg-muted/50 border-muted-foreground/10">
          <strong className="text-foreground mr-1">Latest Log:</strong>
          Never
        </Badge>
      )}
    </div>
  )
}
