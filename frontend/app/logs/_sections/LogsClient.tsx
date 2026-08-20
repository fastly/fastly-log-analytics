'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import dynamic from 'next/dynamic'
import { client } from '@/lib/api'
import { cronCacheBust } from '@/lib/cron-cache-bust'
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Database,
  HardDrive,
  History,
  FileCode,
  Archive,
  ClipboardList,
} from 'lucide-react'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { useBootstrapResolved } from '@/hooks/useIsDataReady'
import { ScrollArea } from '@/components/ui/scroll-area'
import { SyncFromCloudModal } from '@/components/SyncFromCloudModal/SyncFromCloudModal'
import { ingestedFilesColumns } from '@/lib/table-columns'
import { PageHeader } from '@/components/ui/page-header'

import { useLogsPageState } from '../_state'
import { useCronColumns } from './CronColumns'
import { useAuditColumns } from './AuditColumns'
import { FloatingOperationsDock } from './FloatingOperationsDock'
import { QuickActionsBar } from './QuickActionsBar'
import { CronTab } from './CronTab'
import { ServiceHistoryTab } from './ServiceHistoryTab'
import { IngestionTab } from './IngestionTab'
import { SSEModal } from './SSEModal'

// Lazy-mount the heavy tab content. Cron + ServiceHistory + Ingestion
// are the default landing tabs; the four below only matter when the
// operator clicks into them. ``ssr: false`` skips the SSR pass — none
// of them render server-side anyway (they hit DuckDB and S3 client-
// side via React Query hooks).
const FileBrowser = dynamic(
  () => import('@/components/FileBrowser/FileBrowser').then(m => m.FileBrowser),
  { ssr: false },
)
const IcebergStatus = dynamic(
  () => import('@/components/IcebergStatus/IcebergStatus').then(m => m.IcebergStatus),
  { ssr: false },
)
const IcebergCalendar = dynamic(
  () => import('@/components/IcebergStatus/IcebergCalendar').then(m => m.IcebergCalendar),
  { ssr: false },
)
const MetadataStorageCard = dynamic(
  () => import('@/components/MetadataStorageCard').then(m => m.MetadataStorageCard),
  { ssr: false },
)
const SchemaTab = dynamic(
  () => import('./SchemaTab').then(m => m.SchemaTab),
  { ssr: false },
)

export default function LogsClient() {
  const s = useLogsPageState()
  const queryClient = useQueryClient()
  const bootstrapResolved = useBootstrapResolved()

  const auditColumns = useAuditColumns(s.catalogMaps)
  const cronColumns = useCronColumns(s.isAnalyst)

  // Suppress the NoServiceSelected fallback until bootstrap has
  // resolved — otherwise a cold load with empty localStorage flashes
  // the fallback for one render tick before HydrationBoundary or the
  // client-side useBootstrap fetch lands the active_service_id into
  // the React Query cache that useEffectiveServiceId (now used inside
  // useLogsPageState) reads from.
  if (!s.activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Database} message="Please select a service from the header to access admin controls." />
  }
  if (!s.activeServiceId) {
    // Bootstrap in flight — render nothing rather than flash the
    // fallback. The page will re-render with real content as soon as
    // bootstrap commits.
    return null
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Management"
        description="Monitor and manage log ingestion history and active data syncs."
      />

      <QuickActionsBar
        isAnalyst={s.isAnalyst}
        status={s.status}
        activeServiceId={s.activeServiceId}
        recentCrons={s.recentCrons}
        cronLogs={s.cronLogs}
        setSseTitle={s.setSseTitle}
        setSseDescription={s.setSseDescription}
        setIsSSEModalOpen={s.setIsSSEModalOpen}
        setIsSyncModalOpen={s.setIsSyncModalOpen}
        reset={s.reset}
        start={s.start}
        setDisplayedJobs={s.setDisplayedJobs}
        setSelectedConsoleJobId={s.setSelectedConsoleJobId}
        setConsoleOpen={s.setConsoleOpen}
      />

      <Tabs value={s.activeTab} onValueChange={s.handleTabChange} className="w-full">
        <ScrollArea className="w-full max-w-full overflow-hidden">
          <TabsList className="w-full flex">
            <TabsTrigger value="cron" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <History className="h-4 w-4" /> Cron Runs
            </TabsTrigger>
            <TabsTrigger value="service_history" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <ClipboardList className="h-4 w-4" /> Service History
            </TabsTrigger>
            {!s.isAnalyst && (
              <TabsTrigger value="ingestion" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <Database className="h-4 w-4" /> Ingestion History
              </TabsTrigger>
            )}
            <TabsTrigger value="iceberg" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <Archive className="h-4 w-4" /> Iceberg Storage
            </TabsTrigger>
            {!s.isAnalyst && (
              <TabsTrigger value="metadata_storage" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <HardDrive className="h-4 w-4" /> Metadata Storage
              </TabsTrigger>
            )}
            {!s.isAnalyst && (
              <TabsTrigger value="raw" className="flex-1 flex items-center justify-center gap-2 text-xs">
                <FileCode className="h-4 w-4" /> Available Logs
              </TabsTrigger>
            )}
            <TabsTrigger value="schema" className="flex-1 flex items-center justify-center gap-2 text-xs">
              <FileCode className="h-4 w-4" /> DuckDB Schema
            </TabsTrigger>
          </TabsList>
        </ScrollArea>

        <TabsContent value="cron" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <CronTab
            cronColumns={cronColumns}
            cronLogs={s.cronLogs}
            isLoadingCron={s.isLoadingCron}
            isFetchingCron={s.isFetchingCron}
            orderedSchedules={s.orderedSchedules}
            taskFilter={s.taskFilter}
            setTaskFilter={s.setTaskFilter}
            statusFilter={s.statusFilter}
            setStatusFilter={s.setStatusFilter}
            isAnalyst={s.isAnalyst}
            activeServiceId={s.activeServiceId}
            setDisplayedJobs={s.setDisplayedJobs}
            setSelectedConsoleJobId={s.setSelectedConsoleJobId}
            setConsoleOpen={s.setConsoleOpen}
            isPurgeOpen={s.isPurgeOpen}
            setIsPurgeOpen={s.setIsPurgeOpen}
            purgeMutation={s.purgeMutation}
          />
        </TabsContent>

        <TabsContent value="service_history" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <ServiceHistoryTab
            auditColumns={auditColumns}
            auditLogs={s.auditLogs}
            isLoadingAudit={s.isLoadingAudit}
            isFetchingAudit={s.isFetchingAudit}
            eventFilter={s.eventFilter}
            setEventFilter={s.setEventFilter}
            activeServiceId={s.activeServiceId}
          />
        </TabsContent>

        <TabsContent value="ingestion" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <IngestionTab
            ingestedColumns={ingestedFilesColumns}
            ingestedFiles={s.ingestedFiles}
            isLoadingIngested={s.isLoadingIngested}
          />
        </TabsContent>

        {/* Gate the dynamic-imported tabs on activeTab so the bundled
         * chunks defer until the user actually clicks in. Radix' default
         * is render-all (CSS-hide non-active), which would mount every
         * panel on the cron landing — defeating the dynamic() split. */}
        <TabsContent value="iceberg" className="mt-4 space-y-4">
          {s.activeTab === 'iceberg' && (
            <>
              <IcebergStatus accessLevel={s.status?.access_level ?? undefined} />
              <IcebergCalendar />

              <div className="border rounded-lg overflow-hidden bg-card">
                <div className="p-4 border-b">
                  <h3 className="text-sm font-medium">Iceberg Data Lake Layout</h3>
                  <p className="text-xs text-muted-foreground mt-1">Direct view into the Iceberg metadata and data files in Object Storage.</p>
                </div>
                <div className="p-0">
                  <FileBrowser type="iceberg" />
                </div>
              </div>
            </>
          )}
        </TabsContent>

        <TabsContent value="metadata_storage" className="mt-4 space-y-4">
          {s.activeTab === 'metadata_storage' && <MetadataStorageCard />}
        </TabsContent>

        <TabsContent value="raw" className="mt-4 border rounded-lg overflow-hidden bg-card">
          {s.activeTab === 'raw' && (
            <>
              <div className="p-4 border-b">
                <h3 className="text-sm font-medium">Available Logs</h3>
                <p className="text-xs text-muted-foreground mt-1">Raw .gz files delivered by Fastly waiting to be processed.</p>
              </div>
              <div className="p-0">
                <FileBrowser type="raw" />
              </div>
            </>
          )}
        </TabsContent>

        <TabsContent value="schema" className="mt-4 border rounded-lg overflow-hidden bg-card">
          {s.activeTab === 'schema' && <SchemaTab schemaData={s.schemaData} isLoadingSchema={s.isLoadingSchema} />}
        </TabsContent>
      </Tabs>

      <SyncFromCloudModal
        open={s.isSyncModalOpen}
        onOpenChange={s.setIsSyncModalOpen}
        onStartSync={async (range) => {
          const apiRange = range ? { start_time: range.start, end_time: range.end } : {}
          try {
            const { data } = await client.POST("/api/admin/ingest-logs", {
              params: { query: apiRange }
            })
            s.setSseTitle('Syncing from Cloud')
            s.setSseDescription('Fetching latest snapshots and downloading new data files from the cloud...')
            s.setIsSSEModalOpen(true)
            s.reset()
            s.start(`/api/cron-runs/${(data as any)?.run_id}/stream`)
            cronCacheBust(queryClient, s.activeServiceId)
          } catch (e) {
            console.error(e)
          }
        }}
      />

      <SSEModal
        isSSEModalOpen={s.isSSEModalOpen}
        setIsSSEModalOpen={s.setIsSSEModalOpen}
        sseStatus={s.sseStatus}
        sseTitle={s.sseTitle}
        sseError={s.sseError}
        sseDescription={s.sseDescription}
        lines={s.lines}
        stop={s.stop}
      />

      <FloatingOperationsDock
        displayedJobs={s.displayedJobs}
        setDisplayedJobs={s.setDisplayedJobs}
        isOpen={s.consoleOpen}
        setIsOpen={s.setConsoleOpen}
        selectedJobId={s.selectedConsoleJobId}
        setSelectedJobId={s.setSelectedConsoleJobId}
        onDismiss={s.removeDisplayedJob}
        backgroundCronToast={s.backgroundCronToast}
        setBackgroundCronToast={s.setBackgroundCronToast}
      />
    </div>
  )
}
