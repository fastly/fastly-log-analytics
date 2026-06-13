'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  RefreshCw,
  Archive,
  Download,
  Bot,
  Terminal,
} from 'lucide-react'
import { Button } from "@/components/ui/button"
import { client } from '@/lib/api'

export function QuickActionsBar({
  isAnalyst,
  status,
  activeServiceId,
  recentCrons,
  cronLogs,
  setSseTitle,
  setSseDescription,
  setIsSSEModalOpen,
  setIsSyncModalOpen,
  setHasSyncedExtents,
  reset,
  start,
  setDisplayedJobs,
  setSelectedConsoleJobId,
  setConsoleOpen,
}: {
  isAnalyst: boolean
  status: any
  activeServiceId: string | null | undefined
  recentCrons: any
  cronLogs: any
  setSseTitle: (s: string) => void
  setSseDescription: (s: string) => void
  setIsSSEModalOpen: (open: boolean) => void
  setIsSyncModalOpen: (open: boolean) => void
  setHasSyncedExtents: (v: boolean) => void
  reset: () => void
  start: (url: string, opts?: any) => void
  setDisplayedJobs: React.Dispatch<React.SetStateAction<any[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
}) {
  const queryClient = useQueryClient()

  return (
    <div className="flex flex-wrap items-center gap-2 bg-muted/30 p-2 rounded-lg border">
      <div className="text-xs font-bold text-muted-foreground uppercase tracking-wider mx-2">Quick Actions</div>
      {!isAnalyst ? (
        <>
          <Button
            size="sm"
            variant="default"
            className="h-8 text-xs bg-primary/90 hover:bg-primary"
            disabled={status?.access_level === 'read_only'}
            onClick={async () => {
              try {
                const { data } = await client.POST("/api/admin/ingest-logs", {})
                setSseTitle('Importing Logs')
                setSseDescription('Downloading new raw logs from Fastly Object Storage and processing them...')
                setIsSSEModalOpen(true)
                setHasSyncedExtents(false)
                reset()
                start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
                queryClient.invalidateQueries({ queryKey: ['admin'] })
                queryClient.invalidateQueries({ queryKey: ['dashboard'] })
              } catch (e) {
                console.error(e)
              }
            }}
          >
            <RefreshCw className="h-3 w-3 mr-1.5" /> Import Logs
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-xs bg-background"
            disabled={status?.access_level === 'read_only'}
            onClick={async () => {
              try {
                const { data } = await client.POST("/api/admin/commit-iceberg", {})
                setSseTitle('Committing Buffer')
                setSseDescription('Flushing local Parquet buffer to the shared Iceberg table in Object Storage...')
                setIsSSEModalOpen(true)
                reset()
                start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
                queryClient.invalidateQueries({ queryKey: ['admin'] })
              } catch (e) {
                console.error(e)
              }
            }}
          >
            <Archive className="h-3 w-3 mr-1.5" /> Commit Buffer
          </Button>
        </>
      ) : (
        <Button
          size="sm"
          variant="default"
          className="h-8 text-xs bg-primary/90 hover:bg-primary"
          onClick={() => setIsSyncModalOpen(true)}
        >
          <Download className="h-3 w-3 mr-1.5" /> Sync from Cloud
        </Button>
      )}
      {!isAnalyst && status?.ngwaf_workspace_id && (
        <Button
          size="sm"
          variant="outline"
          className="h-8 text-xs bg-background"
          onClick={() => {
            setSseTitle('NGWAF Bot Sync')
            setSseDescription('Fetching verified bot records from Fastly NGWAF and caching them locally. Progress is saved after each page — run again if the time budget is reached.')
            setIsSSEModalOpen(true)
            reset()
            start(`/api/services/${activeServiceId}/ngwaf-sync`, {})
            queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })
          }}
        >
          <Bot className="h-3 w-3 mr-1.5" /> NGWAF Bot Sync
        </Button>
      )}
      <Button
        size="sm"
        variant="outline"
        className="h-8 text-xs bg-background"
        onClick={() => {
          const latestSync = recentCrons?.entries?.find((e: any) => e.task === 'sync') ||
                             cronLogs?.entries?.find((e: any) => e.task === 'sync')
          if (latestSync) {
            setDisplayedJobs(prev => {
              if (prev.some((j: any) => j.id === latestSync.id)) return prev
              return [...prev, { ...latestSync, status: latestSync.status }]
            })
            setSelectedConsoleJobId(latestSync.id)
            setConsoleOpen(true)
          } else {
            window.alert("No recent sync run was found for this service.")
          }
        }}
      >
        <Terminal className="h-3 w-3 mr-1.5" /> View Recent Logs
      </Button>
    </div>
  )
}
