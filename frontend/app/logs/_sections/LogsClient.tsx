'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { client, unwrapClient } from '@/lib/api'
import { cronCacheBust } from '@/hooks/useSSE'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Database, HardDrive, History, FileCode, Archive, ClipboardList } from 'lucide-react'
import { NoServiceSelected } from '@/components/ReportLayout'
import { useBootstrapResolved } from '@/hooks/useBootstrap'
import { ScrollArea } from '@/components/ui/scroll-area'
import { BackfillRangeModal } from '@/components/SyncStatusBadge/SyncStatusBadge'
import { ingestedFilesColumns } from '@/components/DataTable'
import Loading from '../loading'
import { ControlledSSEModal as SSEModal } from '@/components/SSEModal'
import { StorageStatus, StorageCalendar, MetadataStorageCard } from '@/components/SessionScoring/StoragePanels'
import { useLogsPageState, type CronRun, type CatalogMaps } from './useLogsPageState'
import {
  CRON_EXPLANATIONS,
  CRON_DISPLAY_NAMES,
  CRON_GROUPS,
  CronScheduleBox,
  CronTab,
  useCronColumns,
  CronLiveLog,
  FloatingOperationsDock,
} from './LogsCronTab'
import { useAuditColumns, ServiceHistoryTab } from './LogsAuditTab'
import { SchemaTab, IngestionTab, QuickActionsBar, FileBrowser } from './LogsFileBrowser'

export {
  useLogsPageState,
  CRON_EXPLANATIONS,
  CRON_DISPLAY_NAMES,
  CRON_GROUPS,
  CronScheduleBox,
  CronTab,
  useCronColumns,
  CronLiveLog,
  FloatingOperationsDock,
  useAuditColumns,
  ServiceHistoryTab,
  SchemaTab,
  IngestionTab,
  QuickActionsBar,
  FileBrowser,
}
export type { CronRun, CatalogMaps }

const LOG_TABS = [
  { value: 'cron', label: 'Cron Runs', icon: History },
  { value: 'service_history', label: 'Service History', icon: ClipboardList },
  { value: 'ingestion', label: 'Ingestion History', icon: Database },
  { value: 'storage', label: 'DuckLake Storage', icon: Archive },
  { value: 'metadata_storage', label: 'Metadata Storage', icon: HardDrive },
  { value: 'raw', label: 'Available Logs', icon: FileCode },
  { value: 'schema', label: 'DuckDB Schema', icon: FileCode },
] as const

export default function LogsClient({ initialTab }: { initialTab?: string } = {}) {
  const s = useLogsPageState(initialTab)
  const queryClient = useQueryClient()
  const bootstrapResolved = useBootstrapResolved()
  const auditColumns = useAuditColumns(s.catalogMaps)
  const cronColumns = useCronColumns()

  if (!s.activeServiceId && bootstrapResolved) {
    return (
      <NoServiceSelected
        icon={Database}
        message="Please select a service from the header to access admin controls."
      />
    )
  }
  if (!s.activeServiceId) return <Loading />

  return (
    <div className="space-y-6 min-h-[820px]">
      <PageHeader
        title="Data Management"
        description="Monitor and manage log ingestion history and active data syncs."
      />
      <QuickActionsBar
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
            {LOG_TABS.map(({ value, label, icon: Icon }) => (
              <TabsTrigger key={value} value={value} className="flex-1 flex items-center justify-center gap-2 text-xs">
                <Icon className="h-4 w-4" /> {label}
              </TabsTrigger>
            ))}
          </TabsList>
        </ScrollArea>
        <TabsContent value="cron" className="mt-4 border rounded-lg overflow-hidden bg-card">
          <CronTab
            cronColumns={cronColumns}
            cronLogs={s.cronLogs}
            isLoadingCron={s.isLoadingCron}
            isFetchingCron={s.isFetchingCron}
            orderedSchedules={s.orderedSchedules}
            isLoadingSchedule={s.isLoadingSchedule}
            taskFilter={s.taskFilter}
            setTaskFilter={s.setTaskFilter}
            statusFilter={s.statusFilter}
            setStatusFilter={s.setStatusFilter}
            status={s.status}
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
        <TabsContent value="storage" className="mt-4 space-y-4">
          {s.activeTab === 'storage' && (
            <>
              <StorageStatus />
              <StorageCalendar />
              <div className="border rounded-lg overflow-hidden bg-card">
                <div className="p-4 border-b">
                  <h3 className="text-sm font-medium">DuckLake Data Layout</h3>
                  <p className="text-xs text-muted-foreground mt-1">
                    Direct view of the DuckLake Parquet data files (the local data directory in Standard, Object Storage
                    in High-Scale).
                  </p>
                </div>
                <FileBrowser type="storage" />
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
                <p className="text-xs text-muted-foreground mt-1">
                  Raw .gz files delivered by Fastly waiting to be processed.
                </p>
              </div>
              <FileBrowser type="raw" />
            </>
          )}
        </TabsContent>
        <TabsContent value="schema" className="mt-4 border rounded-lg overflow-hidden bg-card">
          {s.activeTab === 'schema' && <SchemaTab schemaData={s.schemaData} isLoadingSchema={s.isLoadingSchema} />}
        </TabsContent>
      </Tabs>
      <BackfillRangeModal
        open={s.isSyncModalOpen}
        onOpenChange={s.setIsSyncModalOpen}
        onStartSync={async (range) => {
          try {
            const data = await unwrapClient(
              client.POST('/api/admin/ingest-logs', {
                params: { query: { start_time: range.start, end_time: range.end } },
              }),
            )
            s.setSseTitle('Backfilling Logs')
            s.setSseDescription('Re-importing raw logs for the selected window...')
            s.setIsSSEModalOpen(true)
            s.reset()
            s.start(`/api/cron-runs/${data?.run_id}/stream`)
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
      />
    </div>
  )
}
